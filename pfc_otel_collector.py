#!/usr/bin/env python3
"""
pfc-otel-collector v0.1.0
OpenTelemetry Collector exporter — receives OTLP/HTTP log data and compresses to PFC format.

Accepts: POST /v1/logs  (OTLP JSON or Protobuf)
Outputs: .pfc files (local or S3)
"""

import asyncio
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import toml
import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pfc-otel] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("pfc-otel")

VERSION = "0.1.0"

# ─── Config ───────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "server": {
        "host": "0.0.0.0",
        "port": 4318,
        "api_key": "",
    },
    "buffer": {
        "rotate_mb": 64,
        "rotate_sec": 3600,
        "output_dir": "/tmp/pfc-otel",
        "prefix": "otel",
    },
    "pfc": {
        "binary": "/usr/local/bin/pfc_jsonl",
    },
    "s3": {
        "enabled": False,
        "bucket": "",
        "prefix": "otel-logs/",
        "region": "us-east-1",
    },
}


def deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_config(path: Optional[str] = None) -> dict:
    cfg = DEFAULT_CONFIG.copy()
    if path and Path(path).exists():
        with open(path) as f:
            user_cfg = toml.load(f)
        cfg = deep_merge(cfg, user_cfg)
    return cfg


# ─── OTLP Parsing ─────────────────────────────────────────────────────────────

def _attr_value(av: dict) -> object:
    """Extract typed value from OTLP AnyValue."""
    if "stringValue" in av:
        return av["stringValue"]
    if "intValue" in av:
        return int(av["intValue"])
    if "doubleValue" in av:
        return float(av["doubleValue"])
    if "boolValue" in av:
        return av["boolValue"]
    if "arrayValue" in av:
        vals = av["arrayValue"].get("values", [])
        return [_attr_value(v) for v in vals]
    if "kvlistValue" in av:
        pairs = av["kvlistValue"].get("values", [])
        return {p["key"]: _attr_value(p["value"]) for p in pairs}
    return None


def _attrs_to_dict(attrs: list) -> dict:
    return {a["key"]: _attr_value(a.get("value", {})) for a in attrs if "key" in a}


SEVERITY_MAP = {
    0: "UNSPECIFIED", 1: "TRACE", 2: "TRACE2", 3: "TRACE3", 4: "TRACE4",
    5: "DEBUG", 6: "DEBUG2", 7: "DEBUG3", 8: "DEBUG4",
    9: "INFO", 10: "INFO2", 11: "INFO3", 12: "INFO4",
    13: "WARN", 14: "WARN2", 15: "WARN3", 16: "WARN4",
    17: "ERROR", 18: "ERROR2", 19: "ERROR3", 20: "ERROR4",
    21: "FATAL", 22: "FATAL2", 23: "FATAL3", 24: "FATAL4",
}


def _nano_to_iso(time_unix_nano: str) -> str:
    """Convert nanosecond Unix timestamp string to ISO 8601."""
    try:
        ns = int(time_unix_nano)
        sec = ns // 1_000_000_000
        ms = (ns % 1_000_000_000) // 1_000_000
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        return dt.strftime(f"%Y-%m-%dT%H:%M:%S.{ms:03d}Z")
    except (ValueError, TypeError, OSError):
        return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def otlp_to_jsonl(payload: dict) -> list[str]:
    """
    Convert an OTLP ExportLogsServiceRequest (JSON) to a list of JSONL lines.
    Each line is a flat JSON object with: timestamp, level, service + all attributes.
    """
    lines = []

    for resource_log in payload.get("resourceLogs", []):
        # Extract resource attributes (service.name, host.name, etc.)
        resource_attrs = _attrs_to_dict(
            resource_log.get("resource", {}).get("attributes", [])
        )
        service = resource_attrs.pop("service.name", resource_attrs.pop("service", "unknown"))

        for scope_log in resource_log.get("scopeLogs", []):
            scope_name = scope_log.get("scope", {}).get("name", "")

            for record in scope_log.get("logRecords", []):
                row: dict = {}

                # Timestamp — prefer timeUnixNano, fall back to observedTimeUnixNano
                ts_nano = record.get("timeUnixNano") or record.get("observedTimeUnixNano", "0")
                row["timestamp"] = _nano_to_iso(ts_nano)

                # Severity / level
                sev_text = record.get("severityText", "")
                sev_num = int(record.get("severityNumber", 0))
                row["level"] = sev_text if sev_text else SEVERITY_MAP.get(sev_num, "INFO")

                # Service
                row["service"] = service

                # Message body
                body = record.get("body") or {}
                if body and "stringValue" in body:
                    row["message"] = body["stringValue"]
                elif body:
                    row["message"] = json.dumps(body)

                # Scope (instrumentation library)
                if scope_name:
                    row["scope"] = scope_name

                # Resource attributes (minus service.name already extracted)
                for k, v in resource_attrs.items():
                    safe_k = k.replace(".", "_")
                    row[safe_k] = v

                # Log record attributes
                rec_attrs = _attrs_to_dict(record.get("attributes", []))
                for k, v in rec_attrs.items():
                    safe_k = k.replace(".", "_")
                    row[safe_k] = v

                # Trace context
                if record.get("traceId"):
                    row["trace_id"] = record["traceId"]
                if record.get("spanId"):
                    row["span_id"] = record["spanId"]

                lines.append(json.dumps(row, ensure_ascii=False))

    return lines


# ─── Buffer & Compression ─────────────────────────────────────────────────────

class PfcBuffer:
    """
    Thread-safe write buffer. Rotates when size or time threshold is reached.
    On rotation: flush to temp JSONL → pfc_jsonl compress → optional S3 upload.
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.buf_cfg = cfg["buffer"]
        self.pfc_cfg = cfg["pfc"]
        self.s3_cfg = cfg["s3"]

        self.output_dir = Path(self.buf_cfg["output_dir"])
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.rotate_bytes = int(self.buf_cfg["rotate_mb"]) * 1024 * 1024
        self.rotate_sec = int(self.buf_cfg["rotate_sec"])
        self.prefix = self.buf_cfg["prefix"]
        self.binary = self.pfc_cfg["binary"]

        self._lock = asyncio.Lock()
        self._lines: list[str] = []
        self._size: int = 0
        self._last_rotate = time.monotonic()
        self._total_ingested = 0
        self._total_compressed = 0

    def _timestamp_slug(self) -> str:
        return datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

    async def write(self, lines: list[str]) -> None:
        if not lines:
            return
        async with self._lock:
            for line in lines:
                encoded = line + "\n"
                self._lines.append(encoded)
                self._size += len(encoded.encode())
            self._total_ingested += len(lines)

            if (
                self._size >= self.rotate_bytes
                or (time.monotonic() - self._last_rotate) >= self.rotate_sec
            ):
                await self._rotate_locked()

    async def flush(self) -> None:
        async with self._lock:
            if self._lines:
                await self._rotate_locked()

    async def _rotate_locked(self) -> None:
        """Must be called with self._lock held."""
        if not self._lines:
            return

        lines_snapshot = self._lines[:]
        self._lines = []
        self._size = 0
        self._last_rotate = time.monotonic()

        slug = self._timestamp_slug()
        jsonl_path = self.output_dir / f"{self.prefix}_{slug}.jsonl"
        pfc_path = self.output_dir / f"{self.prefix}_{slug}.pfc"

        try:
            # Write JSONL
            with open(jsonl_path, "w", encoding="utf-8") as f:
                f.writelines(lines_snapshot)

            jsonl_size = jsonl_path.stat().st_size

            # Compress
            result = subprocess.run(
                [self.binary, "compress", str(jsonl_path), str(pfc_path)],
                capture_output=True,
                text=True,
                timeout=300,
            )

            if result.returncode != 0:
                log.error("pfc_jsonl compress failed: %s", result.stderr)
                # Keep JSONL as fallback
                return

            pfc_size = pfc_path.stat().st_size
            ratio = (pfc_size / jsonl_size * 100) if jsonl_size > 0 else 0
            log.info(
                "Compressed %d lines → %s (%.1f%% ratio)",
                len(lines_snapshot),
                pfc_path.name,
                ratio,
            )
            self._total_compressed += 1

            # Remove JSONL after successful compression
            jsonl_path.unlink(missing_ok=True)

            # Optional S3 upload
            if self.s3_cfg.get("enabled"):
                await self._upload_s3(pfc_path)

        except subprocess.TimeoutExpired:
            log.error("pfc_jsonl compress timed out for %s", jsonl_path)
        except FileNotFoundError:
            log.error("pfc_jsonl binary not found: %s", self.binary)
        except Exception as exc:
            log.error("Rotation error: %s", exc)
            jsonl_path.unlink(missing_ok=True)

    async def _upload_s3(self, pfc_path: Path) -> None:
        try:
            import boto3
            s3 = boto3.client("s3", region_name=self.s3_cfg["region"])
            key = self.s3_cfg["prefix"].rstrip("/") + "/" + pfc_path.name
            s3.upload_file(str(pfc_path), self.s3_cfg["bucket"], key)
            log.info("Uploaded %s → s3://%s/%s", pfc_path.name, self.s3_cfg["bucket"], key)
            pfc_path.unlink(missing_ok=True)
        except Exception as exc:
            log.error("S3 upload failed: %s", exc)

    @property
    def stats(self) -> dict:
        return {
            "buffered_lines": len(self._lines),
            "buffered_bytes": self._size,
            "total_ingested": self._total_ingested,
            "total_files_compressed": self._total_compressed,
        }


# ─── Watchdog ─────────────────────────────────────────────────────────────────

async def watchdog(buffer: PfcBuffer, interval: int = 30) -> None:
    """Periodically rotate if time threshold exceeded."""
    while True:
        await asyncio.sleep(interval)
        elapsed = time.monotonic() - buffer._last_rotate
        if elapsed >= buffer.rotate_sec and buffer._lines:
            log.info("Watchdog: time-based rotation triggered (%.0fs elapsed)", elapsed)
            await buffer.flush()


# ─── FastAPI App ──────────────────────────────────────────────────────────────

def create_app(cfg: dict, buffer: PfcBuffer) -> FastAPI:
    api_key = cfg["server"].get("api_key", "")

    def check_auth(request: Request) -> None:
        if not api_key:
            return
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {api_key}":
            raise HTTPException(status_code=401, detail="Unauthorized")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(watchdog(buffer))
        log.info("pfc-otel-collector v%s started on port %d", VERSION, cfg["server"]["port"])
        log.info("Buffer: rotate_mb=%d rotate_sec=%d output=%s",
                 cfg["buffer"]["rotate_mb"], cfg["buffer"]["rotate_sec"],
                 cfg["buffer"]["output_dir"])
        log.info("PFC binary: %s", cfg["pfc"]["binary"])
        if cfg["s3"]["enabled"]:
            log.info("S3 upload: s3://%s/%s", cfg["s3"]["bucket"], cfg["s3"]["prefix"])
        yield
        task.cancel()
        log.info("Flushing buffer on shutdown…")
        await buffer.flush()
        log.info("pfc-otel-collector stopped")

    app = FastAPI(title="pfc-otel-collector", version=VERSION, lifespan=lifespan)

    @app.get("/health")
    async def health():
        binary_ok = Path(cfg["pfc"]["binary"]).exists()
        return {
            "status": "ok" if binary_ok else "degraded",
            "version": VERSION,
            "binary_found": binary_ok,
            **buffer.stats,
        }

    @app.post("/v1/logs")
    async def ingest_logs(request: Request):
        check_auth(request)

        content_type = request.headers.get("content-type", "")

        # Protobuf: not supported in this version, return clear error
        if "application/x-protobuf" in content_type:
            raise HTTPException(
                status_code=415,
                detail="Protobuf encoding not supported. Use Content-Type: application/json",
            )

        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body")

        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="Expected JSON object")

        try:
            lines = otlp_to_jsonl(body)
        except Exception as exc:
            log.warning("OTLP parse error: %s", exc)
            raise HTTPException(status_code=400, detail=f"OTLP parse error: {exc}")

        if lines:
            await buffer.write(lines)

        return Response(
            content=json.dumps({"partialSuccess": {}}),
            media_type="application/json",
            status_code=200,
        )

    @app.get("/stats")
    async def stats(request: Request):
        check_auth(request)
        return buffer.stats

    @app.post("/flush")
    async def flush(request: Request):
        check_auth(request)
        await buffer.flush()
        return {"flushed": True}

    return app


# ─── Entry Point ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description=f"pfc-otel-collector v{VERSION}")
    parser.add_argument("--config", "-c", default="", help="Path to TOML config file")
    parser.add_argument("--port", type=int, default=0, help="Override server port")
    parser.add_argument("--host", default="", help="Override server host")
    parser.add_argument("--version", action="store_true", help="Print version and exit")
    args = parser.parse_args()

    if args.version:
        print(VERSION)
        sys.exit(0)

    cfg = load_config(args.config or None)
    if args.port:
        cfg["server"]["port"] = args.port
    if args.host:
        cfg["server"]["host"] = args.host

    # Validate binary
    binary = cfg["pfc"]["binary"]
    if not Path(binary).exists():
        log.error("pfc_jsonl binary not found: %s", binary)
        sys.exit(1)

    buffer = PfcBuffer(cfg)
    app = create_app(cfg, buffer)

    # Graceful shutdown on SIGTERM
    def _shutdown(*_):
        log.info("SIGTERM received — shutting down")
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)

    uvicorn.run(
        app,
        host=cfg["server"]["host"],
        port=cfg["server"]["port"],
        log_level="warning",
    )


if __name__ == "__main__":
    main()
