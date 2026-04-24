# pfc-otel-collector

**OpenTelemetry log exporter for PFC-JSONL** — receive OTLP/HTTP log data and compress it directly to `.pfc` format.

Drop `pfc-otel-collector` in front of any OpenTelemetry Collector pipeline to compress your log exports on arrival — no extra storage step, no conversion script.

[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Part of PFC-JSONL Ecosystem](https://img.shields.io/badge/ecosystem-PFC--JSONL-brightgreen)](https://github.com/ImpossibleForge/pfc-jsonl)

---

## How it fits in your pipeline

```
Your App / OTel SDK
        │  OTLP/HTTP
        ▼
OpenTelemetry Collector
        │  otlphttp exporter  →  http://pfc-otel-collector:4318
        ▼
pfc-otel-collector          ← this service
        │  pfc_jsonl compress
        ▼
logs_20260115_100000.pfc    →  local disk or S3
        │
        ▼
Query with DuckDB / pfc-gateway
```

---

## Quickstart

### 1. Install

```bash
pip install fastapi uvicorn toml
# Optional S3 upload:
pip install boto3
```

### 2. Download pfc_jsonl binary

```bash
# Linux x86_64
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x86_64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# macOS ARM64
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-macos-arm64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl
```

### 3. Create config

```bash
cp config/config.toml ./config.toml
# Edit as needed
```

### 4. Start

```bash
python pfc_otel_collector.py --config config.toml
# 2026-01-15T10:00:00 [pfc-otel] INFO pfc-otel-collector v0.1.0 started on port 4318
```

### 5. Point your OTel Collector at it

```yaml
# otel-collector-config.yaml
exporters:
  otlphttp/pfc:
    endpoint: http://localhost:4318
    logs_endpoint: http://localhost:4318/v1/logs

service:
  pipelines:
    logs:
      exporters: [otlphttp/pfc]
```

---

## Configuration

```toml
[server]
host    = "0.0.0.0"
port    = 4318          # standard OTLP/HTTP port
api_key = ""            # optional Bearer token auth

[buffer]
rotate_mb  = 64         # rotate when buffer reaches this size (MB)
rotate_sec = 3600       # rotate after this many seconds even if not full
output_dir = "/tmp/pfc-otel"
prefix     = "otel"     # output filename prefix

[pfc]
binary = "/usr/local/bin/pfc_jsonl"

[s3]
enabled = false
bucket  = "my-log-archive"
prefix  = "otel-logs/"
region  = "us-east-1"
```

---

## Output format

Each OTLP log record becomes one flat JSONL line:

```json
{
  "timestamp": "2026-01-15T10:00:00.123Z",
  "level": "ERROR",
  "service": "payment-service",
  "message": "charge failed: timeout",
  "scope": "com.example.payments",
  "http_method": "POST",
  "http_status_code": 500,
  "host_name": "prod-node-07",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7"
}
```

Resource attributes (dots → underscores), log record attributes, scope name, and trace context are all included.

---

## Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/v1/logs` | optional | Ingest OTLP/HTTP log data |
| `GET`  | `/health`  | none     | Health check + binary status |
| `GET`  | `/stats`   | optional | Ingestion statistics |
| `POST` | `/flush`   | optional | Force immediate rotation |

### Health response

```json
{
  "status": "ok",
  "version": "0.1.0",
  "binary_found": true,
  "buffered_lines": 4271,
  "buffered_bytes": 892440,
  "total_ingested": 128500,
  "total_files_compressed": 3
}
```

---

## Authentication

Set `api_key` in config. All endpoints except `/health` will require:

```
Authorization: Bearer <your-api-key>
```

---

## S3 upload

Set `[s3] enabled = true` and provide your bucket. Credentials via environment:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
# or use an IAM instance role (recommended)
```

After successful upload the local `.pfc` file is removed.

---

## CLI flags

```
pfc-otel-collector --config config.toml   # use config file
pfc-otel-collector --port 4318            # override port
pfc-otel-collector --host 0.0.0.0         # override host
pfc-otel-collector --version              # print version
```

---

## Querying compressed logs

```sql
-- DuckDB
INSTALL pfc FROM community;
LOAD pfc;

SELECT level, count(*) 
FROM read_pfc_jsonl('otel_20260115_100000.pfc',
                    ts_from=1768471200::BIGINT,
                    ts_to=1768471500::BIGINT)
WHERE line LIKE '%payment-service%'
GROUP BY level;
```

Or decompress and query directly:

```bash
pfc_jsonl query otel_20260115_100000.pfc \
  --from '2026-01-15T10:00' --to '2026-01-15T10:05'
```

---

## Running tests

```bash
pip install pytest pytest-asyncio httpx
pytest tests/ -v
# 87 passed
```

---

## Part of the PFC-JSONL Ecosystem

| Repo | What it does |
|------|-------------|
| [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) | Core compressor (BWT + rANS) |
| [pfc-duckdb](https://github.com/ImpossibleForge/pfc-duckdb) | DuckDB community extension |
| [pfc-fluentbit](https://github.com/ImpossibleForge/pfc-fluentbit) | Native Fluent Bit output plugin |
| [pfc-vector](https://github.com/ImpossibleForge/pfc-vector) | High-performance HTTP ingest daemon |
| [pfc-gateway](https://github.com/ImpossibleForge/pfc-gateway) | HTTP query gateway |
| [pfc-migrate](https://github.com/ImpossibleForge/pfc-migrate) | Migrate from gzip/zstd/S3/Azure/GCS |
| [pfc-kafka-consumer](https://github.com/ImpossibleForge/pfc-kafka-consumer) | Kafka / Redpanda consumer |
| **pfc-otel-collector** | **OpenTelemetry OTLP/HTTP exporter** |

---


---

## Disclaimer

PFC-OTel-Collector is an independent open-source project and is not affiliated with, endorsed by, or associated with the Cloud Native Computing Foundation (CNCF) or the OpenTelemetry project.
## License

Free for personal and open-source use.
Commercial use requires a written license — [info@impossibleforge.com](mailto:info@impossibleforge.com)