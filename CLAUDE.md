# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Elastic Serverless Forwarder (ESF) is an AWS Lambda function that ships logs from AWS services (S3 via SQS, Kinesis Data Streams, CloudWatch Logs, SQS) to Elastic Stack (Elasticsearch or Logstash).

## Bootstrap

```bash
python -m venv venv
. venv/bin/activate
pip install -r requirements.txt -r requirements-lint.txt -r requirements-tests.txt
```

Or use the Makefile: `make all-requirements` (skips venv creation).

## Common Commands

### Testing
- `make test` — run all tests except benchmarks
- `make unit-test` — run unit tests only
- `make integration-test` — run integration tests only
- `make benchmark` — run benchmarks
- `make coverage` — run tests with coverage
- Run a single test: `python -m pytest tests/path/to/test_file.py::TestClass::test_method -m unit`

### Linting & Formatting
- `make lint` — run all linters (black, flake8, isort, mypy)
- `make black` / `make flake8` / `make isort` / `make mypy` — run individually
- Auto-fix formatting: `./tests/scripts/black.sh fix` and `./tests/scripts/isort.sh fix`
- Fix license headers: `./tests/scripts/license_headers_check.sh fix`

### Packaging
- `make package` — create Lambda deployment ZIP (`local_esf.zip`)

All test/lint commands have `docker-` prefixed equivalents (e.g., `make docker-test`) matching CI.

## Architecture

**Entry point:** `main_aws.py` → `handlers/aws/handler.py:lambda_handler()`

The handler detects the trigger type and routes to the appropriate trigger module. Each trigger reads events from a **storage** (input) and sends them through a **shipper** (output).

Four domain packages:

- **`handlers/`** — Cloud-specific Lambda logic. Trigger modules: `cloudwatch_logs_trigger.py`, `s3_sqs_trigger.py`, `sqs_trigger.py`, `kinesis_trigger.py`, `replay_trigger.py`. Config loading and trigger detection live in `utils.py`.
- **`storage/`** — Input sources (S3, direct payload). Protocol-based with `StorageFactory`.
- **`shippers/`** — Output destinations (Elasticsearch bulk API, Logstash TCP). Protocol-based with `ShipperFactory`. `CompositeShipper` handles batching and multi-output.
- **`share/`** — Shared utilities: config parsing/validation (`config.py`), JSON multi-backend support (`json.py` — orjson, ujson, simplejson, rapidjson, cysimdjson), logging (`logger.py`), multiline processing, include/exclude filtering, Secrets Manager integration.

## Code Style

- **Black** with line-length=120, target py39+
- **isort** for import ordering
- **flake8** with max-line-length=120
- **mypy** strict mode, Python 3.12
- All source files require Elastic License 2.0 headers
- Test coverage target: 100% for code outside `handlers/`

## Test Structure

Tests mirror the source tree under `tests/`. Markers: `unit`, `integration`, `benchmark`. Test containers for Elasticsearch and Logstash are in `tests/testcontainers/`.
