# Changelog

## [0.1.0] — 2026-04-24

### Added
- OTLP/HTTP log ingestion (`POST /v1/logs`) on port 4318 (standard OTLP port)
- Full OTLP JSON payload parsing: resource attributes, scope, log records, trace context
- Nanosecond timestamp conversion to ISO 8601
- Severity number → text mapping for all 24 OTLP severity levels
- Flat JSONL output: timestamp, level, service, message + all attributes
- Buffer management: size-based and time-based rotation
- PFC compression via `pfc_jsonl` binary after each rotation
- Optional S3 upload after compression
- TOML configuration file support
- `GET /health` endpoint (no auth required)
- `GET /stats` endpoint
- `POST /flush` endpoint (force immediate rotation)
- Optional Bearer token authentication
- Graceful shutdown with buffer flush
- Background watchdog for time-based rotation
- `--version`, `--config`, `--port`, `--host` CLI flags
- 87 tests (Happy-Path + Resilience): all passing
