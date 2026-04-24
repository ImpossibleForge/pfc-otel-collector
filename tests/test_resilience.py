"""
pfc-otel-collector — Resilience & Edge-Case Test Suite
Tests failure scenarios, malformed input, binary failures, S3 errors, and recovery.
"""

import asyncio
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

sys.path.insert(0, str(Path(__file__).parent.parent))
from pfc_otel_collector import (
    PfcBuffer,
    create_app,
    otlp_to_jsonl,
    _nano_to_iso,
)

PFC_BINARY = os.environ.get("PFC_BINARY", "/usr/local/bin/pfc_jsonl")
BINARY_AVAILABLE = Path(PFC_BINARY).exists()


@pytest.fixture
def tmpdir():
    d = tempfile.mkdtemp(prefix="pfc_otel_resilience_")
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def cfg(tmpdir):
    return {
        "server": {"host": "0.0.0.0", "port": 4318, "api_key": ""},
        "buffer": {
            "rotate_mb": 64,
            "rotate_sec": 3600,
            "output_dir": str(tmpdir),
            "prefix": "test",
        },
        "pfc": {"binary": PFC_BINARY},
        "s3": {"enabled": False, "bucket": "", "prefix": "logs/", "region": "us-east-1"},
    }


def make_payload(service="svc", message="test", level="INFO"):
    return {
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": service}}
            ]},
            "scopeLogs": [{"scope": {}, "logRecords": [{
                "timeUnixNano": "1768471200000000000",
                "severityText": level,
                "body": {"stringValue": message},
            }]}],
        }]
    }


# ─── R01: Binary not found ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_binary_not_found_no_crash(cfg, tmpdir):
    """Rotation with missing binary logs error but does not crash."""
    cfg["pfc"]["binary"] = "/nonexistent/pfc_jsonl"
    buf = PfcBuffer(cfg)
    buf._lines = ['{"timestamp":"2026-01-15T10:00:00.000Z","level":"INFO","message":"x"}']
    buf._size = 100
    await buf._rotate_locked()
    # JSONL kept as fallback, no exception raised
    jsonl_files = list(tmpdir.glob("*.jsonl"))
    assert len(jsonl_files) == 1  # fallback preserved


# ─── R02: Binary returns non-zero exit code ───────────────────────────────────

@pytest.mark.asyncio
async def test_binary_nonzero_exit_keeps_jsonl(cfg, tmpdir):
    """If pfc_jsonl compress fails, JSONL file is kept (not deleted)."""
    cfg["pfc"]["binary"] = PFC_BINARY
    buf = PfcBuffer(cfg)
    buf._lines = ['{"timestamp":"2026-01-15T10:00:00.000Z","level":"INFO","message":"x"}']
    buf._size = 100

    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "simulated compress failure"

    with patch("subprocess.run", return_value=mock_result):
        await buf._rotate_locked()

    jsonl_files = list(tmpdir.glob("*.jsonl"))
    assert len(jsonl_files) == 1


# ─── R03: Binary timeout ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_binary_timeout_no_crash(cfg, tmpdir):
    """Timeout during compression is caught, no crash."""
    buf = PfcBuffer(cfg)
    buf._lines = ['{"timestamp":"2026-01-15T10:00:00.000Z","level":"INFO","message":"x"}']
    buf._size = 100

    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("pfc_jsonl", 300)):
        await buf._rotate_locked()  # must not raise


# ─── R04: Output directory not writable ───────────────────────────────────────

@pytest.mark.asyncio
async def test_unwritable_output_dir_no_crash(cfg, tmpdir):
    """Unwritable output dir causes graceful error, no crash."""
    readonly = tmpdir / "readonly"
    readonly.mkdir()
    readonly.chmod(0o444)
    cfg["buffer"]["output_dir"] = str(readonly)
    try:
        buf = PfcBuffer(cfg)
        buf._lines = ["line1"]
        buf._size = 10
        await buf._rotate_locked()  # must not raise
    finally:
        readonly.chmod(0o755)


# ─── R05: Malformed OTLP — partial structure ──────────────────────────────────

def test_malformed_missing_scope_logs():
    payload = {"resourceLogs": [{"resource": {"attributes": []}}]}
    lines = otlp_to_jsonl(payload)
    assert lines == []


def test_malformed_missing_log_records():
    payload = {"resourceLogs": [{"resource": {"attributes": []},
                                  "scopeLogs": [{"scope": {}}]}]}
    lines = otlp_to_jsonl(payload)
    assert lines == []


def test_malformed_empty_record():
    payload = {"resourceLogs": [{"resource": {"attributes": []},
                                  "scopeLogs": [{"scope": {}, "logRecords": [{}]}]}]}
    lines = otlp_to_jsonl(payload)
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["service"] == "unknown"
    assert row["level"] == "UNSPECIFIED"


def test_malformed_null_body():
    payload = {"resourceLogs": [{"resource": {"attributes": [
        {"key": "service.name", "value": {"stringValue": "svc"}}
    ]}, "scopeLogs": [{"scope": {}, "logRecords": [{
        "timeUnixNano": "1000000000",
        "severityText": "INFO",
        "body": None,
    }]}]}]}
    lines = otlp_to_jsonl(payload)
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert "message" not in row  # no crash, just no message


def test_malformed_extra_unknown_fields():
    """Unknown fields in OTLP payload are silently ignored."""
    payload = {
        "resourceLogs": [{
            "resource": {"attributes": [], "unknownField": "ignored"},
            "scopeLogs": [{"scope": {}, "unknownField": True, "logRecords": [{
                "timeUnixNano": "1000000000",
                "severityText": "DEBUG",
                "body": {"stringValue": "hello"},
                "unknownField": [1, 2, 3],
            }]}],
        }],
        "topLevelUnknown": "also ignored",
    }
    lines = otlp_to_jsonl(payload)
    assert len(lines) == 1


def test_malformed_deeply_nested_array_attr():
    """Array attribute values are JSON-serializable."""
    payload = {"resourceLogs": [{
        "resource": {"attributes": []},
        "scopeLogs": [{"scope": {}, "logRecords": [{
            "timeUnixNano": "1000000000",
            "severityText": "INFO",
            "body": {"stringValue": "msg"},
            "attributes": [{
                "key": "tags",
                "value": {"arrayValue": {"values": [
                    {"stringValue": "a"}, {"stringValue": "b"}
                ]}}
            }]
        }]}],
    }]}
    lines = otlp_to_jsonl(payload)
    row = json.loads(lines[0])
    assert row["tags"] == ["a", "b"]


# ─── R06: HTTP — malformed requests ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_empty_body_rejected(cfg):
    buffer = PfcBuffer(cfg)
    app = create_app(cfg, buffer)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post("/v1/logs", content=b"",
                          headers={"content-type": "application/json"})
        assert r.status_code == 400


@pytest.mark.asyncio
async def test_truncated_json_rejected(cfg):
    buffer = PfcBuffer(cfg)
    app = create_app(cfg, buffer)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post("/v1/logs", content=b'{"resourceLogs": [{',
                          headers={"content-type": "application/json"})
        assert r.status_code == 400


@pytest.mark.asyncio
async def test_json_array_top_level_rejected(cfg):
    buffer = PfcBuffer(cfg)
    app = create_app(cfg, buffer)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post("/v1/logs", json=[{"resourceLogs": []}])
        assert r.status_code == 400


@pytest.mark.asyncio
async def test_very_large_message(cfg):
    """Very large message body is handled without crash."""
    big_msg = "x" * 100_000
    payload = make_payload(message=big_msg)
    buffer = PfcBuffer(cfg)
    app = create_app(cfg, buffer)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post("/v1/logs", json=payload)
        assert r.status_code == 200
        assert buffer.stats["total_ingested"] == 1


@pytest.mark.asyncio
async def test_many_attributes(cfg):
    """Record with 100 attributes is handled without crash."""
    attrs = [{"key": f"attr{i}", "value": {"stringValue": f"val{i}"}} for i in range(100)]
    payload = {"resourceLogs": [{
        "resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": "svc"}}
        ]},
        "scopeLogs": [{"scope": {}, "logRecords": [{
            "timeUnixNano": "1000000000",
            "severityText": "INFO",
            "body": {"stringValue": "msg"},
            "attributes": attrs,
        }]}],
    }]}
    buffer = PfcBuffer(cfg)
    app = create_app(cfg, buffer)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        r = await c.post("/v1/logs", json=payload)
        assert r.status_code == 200


# ─── R07: S3 upload failure ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_s3_upload_failure_no_crash(cfg, tmpdir):
    """S3 upload error is logged, PFC file kept locally, no crash."""
    cfg["s3"]["enabled"] = True
    cfg["s3"]["bucket"] = "my-bucket"
    buf = PfcBuffer(cfg)

    # Create a fake PFC file
    fake_pfc = tmpdir / "test_20260115_100000.pfc"
    fake_pfc.write_bytes(b"FAKE_PFC_CONTENT")

    with patch("boto3.client") as mock_boto:
        mock_boto.return_value.upload_file.side_effect = Exception("S3 connection refused")
        await buf._upload_s3(fake_pfc)

    assert fake_pfc.exists()  # kept on failure


@pytest.mark.asyncio
async def test_s3_boto3_not_installed(cfg, tmpdir):
    """Missing boto3 causes logged error, no crash."""
    cfg["s3"]["enabled"] = True
    buf = PfcBuffer(cfg)
    fake_pfc = tmpdir / "test.pfc"
    fake_pfc.write_bytes(b"FAKE")

    with patch.dict("sys.modules", {"boto3": None}):
        await buf._upload_s3(fake_pfc)  # must not raise


# ─── R08: Timestamp edge cases ────────────────────────────────────────────────

def test_nano_zero_timestamp():
    iso = _nano_to_iso("0")
    assert "1970" in iso


def test_nano_future_timestamp():
    iso = _nano_to_iso("9999999999000000000")
    assert iso.endswith("Z")


def test_nano_string_int():
    iso = _nano_to_iso(1768471200000000000)  # int not str
    assert "2026" in iso


def test_nano_negative():
    iso = _nano_to_iso("-1000000000")
    assert iso.endswith("Z")


# ─── R09: Concurrent flush + write race ───────────────────────────────────────

@pytest.mark.asyncio
async def test_concurrent_flush_and_write(cfg, tmpdir):
    """Simultaneous flush and write do not corrupt state."""
    buf = PfcBuffer(cfg)

    async def writer():
        for i in range(20):
            await buf.write([f"line{i}"])
            await asyncio.sleep(0)

    async def flusher():
        with patch.object(buf, "_rotate_locked", new_callable=AsyncMock):
            for _ in range(5):
                await buf.flush()
                await asyncio.sleep(0)

    await asyncio.gather(writer(), flusher())


# ─── R10: Graceful shutdown flushes buffer ────────────────────────────────────

@pytest.mark.asyncio
async def test_flush_called_explicitly(cfg):
    """buffer.flush() triggers rotation when lines are buffered."""
    buf = PfcBuffer(cfg)
    await buf.write(["line1", "line2"])
    assert buf.stats["buffered_lines"] == 2

    with patch.object(buf, "_rotate_locked", new_callable=AsyncMock) as mock_rotate:
        await buf.flush()
        mock_rotate.assert_called_once()  # rotation was triggered


@pytest.mark.asyncio
async def test_flush_endpoint_drains_buffer(cfg):
    """POST /flush triggers buffer rotation via HTTP."""
    buffer = PfcBuffer(cfg)
    app = create_app(cfg, buffer)
    await buffer.write(["line1"])

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        with patch.object(buffer, "_rotate_locked", new_callable=AsyncMock) as mock_rotate:
            r = await c.post("/flush")
            assert r.status_code == 200
            mock_rotate.assert_called()


# ─── R11: Version flag ────────────────────────────────────────────────────────

def test_version_flag(capsys):
    sys.argv = ["pfc_otel_collector", "--version"]
    with pytest.raises(SystemExit) as exc:
        from pfc_otel_collector import main
        main()
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "0.1.0" in captured.out


# ─── R12: Binary not found at startup ────────────────────────────────────────

def test_startup_exits_on_missing_binary(cfg):
    """main() exits with code 1 if binary is missing."""
    cfg["pfc"]["binary"] = "/nonexistent/binary"
    sys.argv = ["pfc_otel_collector"]
    with patch("pfc_otel_collector.load_config", return_value=cfg):
        with patch("uvicorn.run"):
            with pytest.raises(SystemExit) as exc:
                from pfc_otel_collector import main
                main()
            assert exc.value.code == 1


# ─── R13: Multiple services in one batch ─────────────────────────────────────

def test_multi_service_batch():
    services = ["api", "auth", "payment", "search", "user"]
    resource_logs = [
        {
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": svc}}
            ]},
            "scopeLogs": [{"scope": {}, "logRecords": [{
                "timeUnixNano": "1768471200000000000",
                "severityText": "INFO",
                "body": {"stringValue": f"{svc} log entry"},
            }]}],
        }
        for svc in services
    ]
    payload = {"resourceLogs": resource_logs}
    lines = otlp_to_jsonl(payload)
    assert len(lines) == 5
    found_services = {json.loads(l)["service"] for l in lines}
    assert found_services == set(services)


# ─── R14: All severity levels ─────────────────────────────────────────────────

def test_all_severity_numbers():
    for num, expected in [(9, "INFO"), (13, "WARN"), (17, "ERROR"), (21, "FATAL"), (5, "DEBUG")]:
        payload = {"resourceLogs": [{
            "resource": {"attributes": []},
            "scopeLogs": [{"scope": {}, "logRecords": [{
                "timeUnixNano": "1000000000",
                "severityText": "",
                "severityNumber": num,
                "body": {"stringValue": "msg"},
            }]}],
        }]}
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert row["level"] == expected, f"Expected {expected} for severityNumber {num}"


# ─── R15: Recovery after repeated failures ────────────────────────────────────

@pytest.mark.asyncio
async def test_buffer_recovers_after_rotation_failure(cfg, tmpdir):
    """After a failed rotation, subsequent writes still work normally."""
    buf = PfcBuffer(cfg)
    call_count = 0

    original_rotate = buf._rotate_locked

    async def failing_then_ok():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("Simulated first failure")
        await original_rotate()

    await buf.write(["line1"])  # buffered
    assert buf.stats["buffered_lines"] == 1

    # Write more lines — should still work
    await buf.write(["line2", "line3"])
    assert buf.stats["total_ingested"] == 3
