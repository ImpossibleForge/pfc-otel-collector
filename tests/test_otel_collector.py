"""
pfc-otel-collector — Happy-Path Test Suite
Tests all core functionality: OTLP parsing, ingestion, rotation, health, auth, stats, flush.
"""

import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

sys.path.insert(0, str(Path(__file__).parent.parent))
from pfc_otel_collector import (
    PfcBuffer,
    create_app,
    load_config,
    otlp_to_jsonl,
    _attr_value,
    _attrs_to_dict,
    _nano_to_iso,
    deep_merge,
)

# ─── Fixtures ─────────────────────────────────────────────────────────────────

PFC_BINARY = os.environ.get("PFC_BINARY", "/usr/local/bin/pfc_jsonl")
BINARY_AVAILABLE = Path(PFC_BINARY).exists()


@pytest.fixture
def tmpdir():
    d = tempfile.mkdtemp(prefix="pfc_otel_test_")
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


@pytest.fixture
def cfg_with_auth(cfg):
    cfg["server"]["api_key"] = "secret-token"
    return cfg


def make_otlp_payload(
    service="my-service",
    level="INFO",
    message="Hello World",
    ts_nano="1768471200000000000",
    extra_attrs=None,
    resource_attrs=None,
):
    attrs = [{"key": "http.method", "value": {"stringValue": "GET"}}]
    if extra_attrs:
        attrs.extend(extra_attrs)
    res_attrs = [{"key": "service.name", "value": {"stringValue": service}}]
    if resource_attrs:
        res_attrs.extend(resource_attrs)
    return {
        "resourceLogs": [
            {
                "resource": {"attributes": res_attrs},
                "scopeLogs": [
                    {
                        "scope": {"name": "my-instrumentation"},
                        "logRecords": [
                            {
                                "timeUnixNano": ts_nano,
                                "severityText": level,
                                "severityNumber": 9,
                                "body": {"stringValue": message},
                                "attributes": attrs,
                            }
                        ],
                    }
                ],
            }
        ]
    }


# ─── Unit: OTLP Parsing ───────────────────────────────────────────────────────

class TestAttrValue:
    def test_string(self):
        assert _attr_value({"stringValue": "hello"}) == "hello"

    def test_int(self):
        assert _attr_value({"intValue": "42"}) == 42

    def test_double(self):
        assert _attr_value({"doubleValue": 3.14}) == pytest.approx(3.14)

    def test_bool(self):
        assert _attr_value({"boolValue": True}) is True

    def test_array(self):
        v = {"arrayValue": {"values": [{"stringValue": "a"}, {"stringValue": "b"}]}}
        assert _attr_value(v) == ["a", "b"]

    def test_kvlist(self):
        v = {"kvlistValue": {"values": [{"key": "x", "value": {"intValue": "1"}}]}}
        assert _attr_value(v) == {"x": 1}

    def test_empty(self):
        assert _attr_value({}) is None


class TestAttrsToDict:
    def test_basic(self):
        attrs = [
            {"key": "http.method", "value": {"stringValue": "GET"}},
            {"key": "http.status_code", "value": {"intValue": "200"}},
        ]
        result = _attrs_to_dict(attrs)
        assert result == {"http.method": "GET", "http.status_code": 200}

    def test_missing_key(self):
        attrs = [{"value": {"stringValue": "no-key"}}]
        assert _attrs_to_dict(attrs) == {}

    def test_empty(self):
        assert _attrs_to_dict([]) == {}


class TestNanoToIso:
    def test_valid(self):
        iso = _nano_to_iso("1768471200000000000")
        assert iso.startswith("2026-01-15T10:00:00.")
        assert iso.endswith("Z")

    def test_with_ms(self):
        iso = _nano_to_iso("1768471200123000000")
        assert "123" in iso

    def test_zero(self):
        iso = _nano_to_iso("0")
        assert "1970" in iso

    def test_invalid(self):
        iso = _nano_to_iso("not-a-number")
        assert iso.endswith("Z")

    def test_none(self):
        iso = _nano_to_iso(None)
        assert iso.endswith("Z")


class TestOtlpToJsonl:
    def test_single_record(self):
        payload = make_otlp_payload()
        lines = otlp_to_jsonl(payload)
        assert len(lines) == 1
        row = json.loads(lines[0])
        assert row["service"] == "my-service"
        assert row["level"] == "INFO"
        assert row["message"] == "Hello World"
        assert "timestamp" in row

    def test_timestamp_format(self):
        payload = make_otlp_payload(ts_nano="1768471200000000000")
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert row["timestamp"].startswith("2026-01-15T10:00:00.")

    def test_multiple_records(self):
        payload = {
            "resourceLogs": [
                {
                    "resource": {"attributes": [
                        {"key": "service.name", "value": {"stringValue": "svc"}}
                    ]},
                    "scopeLogs": [
                        {
                            "scope": {},
                            "logRecords": [
                                {"timeUnixNano": "1000000000", "severityText": "INFO",
                                 "body": {"stringValue": f"msg{i}"}}
                                for i in range(5)
                            ],
                        }
                    ],
                }
            ]
        }
        lines = otlp_to_jsonl(payload)
        assert len(lines) == 5

    def test_multiple_resource_logs(self):
        payload = {
            "resourceLogs": [
                {
                    "resource": {"attributes": [
                        {"key": "service.name", "value": {"stringValue": f"svc{i}"}}
                    ]},
                    "scopeLogs": [{"scope": {}, "logRecords": [
                        {"timeUnixNano": "1000000000", "severityText": "WARN",
                         "body": {"stringValue": "test"}}
                    ]}],
                }
                for i in range(3)
            ]
        }
        lines = otlp_to_jsonl(payload)
        assert len(lines) == 3
        services = {json.loads(l)["service"] for l in lines}
        assert services == {"svc0", "svc1", "svc2"}

    def test_severity_number_fallback(self):
        payload = make_otlp_payload(level="")
        payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["severityNumber"] = 17
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert row["level"] == "ERROR"

    def test_resource_attributes_flattened(self):
        payload = make_otlp_payload(
            resource_attrs=[{"key": "host.name", "value": {"stringValue": "server-01"}}]
        )
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert row["host_name"] == "server-01"

    def test_record_attributes_flattened(self):
        payload = make_otlp_payload(extra_attrs=[
            {"key": "db.statement", "value": {"stringValue": "SELECT 1"}}
        ])
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert row["db_statement"] == "SELECT 1"

    def test_scope_name_included(self):
        payload = make_otlp_payload()
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert row["scope"] == "my-instrumentation"

    def test_trace_context(self):
        payload = make_otlp_payload()
        payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["traceId"] = "abc123"
        payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["spanId"] = "def456"
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert row["trace_id"] == "abc123"
        assert row["span_id"] == "def456"

    def test_empty_payload(self):
        assert otlp_to_jsonl({}) == []
        assert otlp_to_jsonl({"resourceLogs": []}) == []

    def test_body_json_fallback(self):
        payload = make_otlp_payload()
        payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["body"] = {
            "kvlistValue": {"values": [{"key": "event", "value": {"stringValue": "click"}}]}
        }
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert "message" in row

    def test_observed_time_fallback(self):
        payload = make_otlp_payload()
        rec = payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
        del rec["timeUnixNano"]
        rec["observedTimeUnixNano"] = "1768471200000000000"
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert "2026" in row["timestamp"]

    def test_missing_service_name(self):
        payload = make_otlp_payload()
        payload["resourceLogs"][0]["resource"]["attributes"] = []
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert row["service"] == "unknown"

    def test_unicode_message(self):
        payload = make_otlp_payload(message="日本語ログ 🔥")
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert row["message"] == "日本語ログ 🔥"

    def test_empty_scope(self):
        payload = make_otlp_payload()
        payload["resourceLogs"][0]["scopeLogs"][0]["scope"] = {}
        lines = otlp_to_jsonl(payload)
        row = json.loads(lines[0])
        assert "scope" not in row


# ─── Unit: Config ─────────────────────────────────────────────────────────────

class TestConfig:
    def test_defaults(self):
        cfg = load_config(None)
        assert cfg["server"]["port"] == 4318
        assert cfg["buffer"]["rotate_mb"] == 64
        assert cfg["s3"]["enabled"] is False

    def test_from_file(self, tmpdir):
        cfg_path = tmpdir / "test.toml"
        cfg_path.write_text('[server]\nport = 9999\n')
        cfg = load_config(str(cfg_path))
        assert cfg["server"]["port"] == 9999
        assert cfg["buffer"]["rotate_mb"] == 64  # default preserved

    def test_missing_file(self):
        cfg = load_config("/nonexistent/config.toml")
        assert cfg["server"]["port"] == 4318

    def test_deep_merge(self):
        base = {"a": {"x": 1, "y": 2}, "b": 3}
        override = {"a": {"y": 99, "z": 4}}
        result = deep_merge(base, override)
        assert result == {"a": {"x": 1, "y": 99, "z": 4}, "b": 3}


# ─── Integration: HTTP API ────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestHttpApi:
    @pytest_asyncio.fixture
    async def client(self, cfg):
        buffer = PfcBuffer(cfg)
        app = create_app(cfg, buffer)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c, buffer

    async def test_health_ok(self, client):
        c, _ = client
        r = await c.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["version"] == "0.1.0"
        assert "buffered_lines" in data

    async def test_health_binary_missing(self, cfg, tmpdir):
        cfg["pfc"]["binary"] = "/nonexistent/pfc_jsonl"
        buffer = PfcBuffer(cfg)
        app = create_app(cfg, buffer)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get("/health")
            assert r.status_code == 200
            assert r.json()["status"] == "degraded"

    async def test_ingest_valid(self, client):
        c, buf = client
        payload = make_otlp_payload()
        r = await c.post("/v1/logs", json=payload)
        assert r.status_code == 200
        assert buf.stats["buffered_lines"] == 1
        assert buf.stats["total_ingested"] == 1

    async def test_ingest_empty_payload(self, client):
        c, buf = client
        r = await c.post("/v1/logs", json={"resourceLogs": []})
        assert r.status_code == 200
        assert buf.stats["total_ingested"] == 0

    async def test_ingest_invalid_json(self, client):
        c, _ = client
        r = await c.post("/v1/logs", content=b"not json",
                          headers={"content-type": "application/json"})
        assert r.status_code == 400

    async def test_ingest_protobuf_rejected(self, client):
        c, _ = client
        r = await c.post("/v1/logs", content=b"\x00\x01",
                          headers={"content-type": "application/x-protobuf"})
        assert r.status_code == 415

    async def test_ingest_not_object(self, client):
        c, _ = client
        r = await c.post("/v1/logs", json=[1, 2, 3])
        assert r.status_code == 400

    async def test_stats(self, client):
        c, buf = client
        await c.post("/v1/logs", json=make_otlp_payload())
        r = await c.get("/stats")
        assert r.status_code == 200
        assert r.json()["total_ingested"] == 1

    async def test_flush(self, client):
        c, buf = client
        await c.post("/v1/logs", json=make_otlp_payload())
        assert buf.stats["buffered_lines"] == 1
        with patch.object(buf, "_rotate_locked", new_callable=AsyncMock) as mock_rotate:
            r = await c.post("/flush")
            assert r.status_code == 200

    async def test_multiple_records_counted(self, client):
        c, buf = client
        payload = {
            "resourceLogs": [{
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": "svc"}}
                ]},
                "scopeLogs": [{"scope": {}, "logRecords": [
                    {"timeUnixNano": "1000000000", "severityText": "INFO",
                     "body": {"stringValue": f"msg{i}"}}
                    for i in range(10)
                ]}],
            }]
        }
        r = await c.post("/v1/logs", json=payload)
        assert r.status_code == 200
        assert buf.stats["total_ingested"] == 10


# ─── Integration: Auth ────────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestAuth:
    @pytest_asyncio.fixture
    async def auth_client(self, cfg_with_auth):
        buffer = PfcBuffer(cfg_with_auth)
        app = create_app(cfg_with_auth, buffer)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c, buffer

    async def test_no_auth_rejected(self, auth_client):
        c, _ = auth_client
        r = await c.post("/v1/logs", json=make_otlp_payload())
        assert r.status_code == 401

    async def test_wrong_token_rejected(self, auth_client):
        c, _ = auth_client
        r = await c.post("/v1/logs", json=make_otlp_payload(),
                          headers={"Authorization": "Bearer wrong-token"})
        assert r.status_code == 401

    async def test_correct_token_accepted(self, auth_client):
        c, _ = auth_client
        r = await c.post("/v1/logs", json=make_otlp_payload(),
                          headers={"Authorization": "Bearer secret-token"})
        assert r.status_code == 200

    async def test_health_no_auth_required(self, auth_client):
        c, _ = auth_client
        r = await c.get("/health")
        assert r.status_code == 200

    async def test_stats_requires_auth(self, auth_client):
        c, _ = auth_client
        r = await c.get("/stats")
        assert r.status_code == 401

    async def test_flush_requires_auth(self, auth_client):
        c, _ = auth_client
        r = await c.post("/flush")
        assert r.status_code == 401


# ─── Integration: Buffer ──────────────────────────────────────────────────────

@pytest.mark.asyncio
class TestBuffer:
    async def test_write_accumulates(self, cfg, tmpdir):
        buf = PfcBuffer(cfg)
        await buf.write(["line1", "line2", "line3"])
        assert buf.stats["buffered_lines"] == 3
        assert buf.stats["total_ingested"] == 3

    async def test_size_threshold_triggers_rotation(self, cfg, tmpdir):
        cfg["buffer"]["rotate_mb"] = 0  # trigger immediately
        buf = PfcBuffer(cfg)
        with patch.object(buf, "_rotate_locked", new_callable=AsyncMock) as mock:
            await buf.write(["line1"])
            mock.assert_called_once()

    async def test_time_threshold_triggers_rotation(self, cfg, tmpdir):
        cfg["buffer"]["rotate_sec"] = 0
        buf = PfcBuffer(cfg)
        buf._last_rotate = time.monotonic() - 10  # simulate time passed
        with patch.object(buf, "_rotate_locked", new_callable=AsyncMock) as mock:
            await buf.write(["line1"])
            mock.assert_called_once()

    async def test_flush_empty_buffer_safe(self, cfg, tmpdir):
        buf = PfcBuffer(cfg)
        await buf.flush()  # should not raise
        assert buf.stats["buffered_lines"] == 0

    async def test_stats_zero_initial(self, cfg, tmpdir):
        buf = PfcBuffer(cfg)
        s = buf.stats
        assert s["buffered_lines"] == 0
        assert s["total_ingested"] == 0
        assert s["total_files_compressed"] == 0

    @pytest.mark.skipif(not BINARY_AVAILABLE, reason="pfc_jsonl binary not available")
    async def test_real_compression(self, cfg, tmpdir):
        cfg["buffer"]["rotate_mb"] = 0
        buf = PfcBuffer(cfg)
        lines = [json.dumps({"timestamp": "2026-01-15T10:00:00.000Z",
                              "level": "INFO", "service": "svc",
                              "message": f"msg {i}"}) for i in range(100)]
        await buf.write(lines)
        # Give compression a moment
        await asyncio.sleep(2)
        pfc_files = list(tmpdir.glob("*.pfc"))
        assert len(pfc_files) >= 1
        assert buf.stats["total_files_compressed"] >= 1


# ─── Integration: Concurrent Ingestion ───────────────────────────────────────

@pytest.mark.asyncio
class TestConcurrent:
    async def test_concurrent_writes_no_data_loss(self, cfg, tmpdir):
        buf = PfcBuffer(cfg)
        tasks = [
            buf.write([f"line from task {i}"])
            for i in range(50)
        ]
        await asyncio.gather(*tasks)
        assert buf.stats["total_ingested"] == 50

    async def test_concurrent_http_requests(self, cfg):
        buffer = PfcBuffer(cfg)
        app = create_app(cfg, buffer)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            tasks = [
                c.post("/v1/logs", json=make_otlp_payload(service=f"svc{i}"))
                for i in range(20)
            ]
            responses = await asyncio.gather(*tasks)
            assert all(r.status_code == 200 for r in responses)
            assert buffer.stats["total_ingested"] == 20
