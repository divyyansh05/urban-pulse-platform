# ADR-002: Batch Ingestion Pattern

## Status
Accepted

## Context
Phase 1 requires ingesting 4 batch sources: NYC 311, NYC Crime, Open-Meteo
Weather, and AirNow EPA. Each has different schemas, update frequencies,
volumes, and pagination patterns.

## Decision

### Pattern: Extract → Validate → Stage (S3) → Load (BigQuery)

Every batch ingester follows this exact sequence:
1. **Extract** — pull from API with pagination, rate limiting, retry logic
2. **Validate** — run each record through Pydantic model, reject invalids to dead letter
3. **Stage** — write valid records as NDJSON to S3 raw zone (durable checkpoint)
4. **Load** — BigQuery streaming insert from validated rows
5. **Log** — write run metadata to `raw._ingestion_log`

### Why S3 as staging layer?
- Durable checkpoint — BQ load failure doesn't lose data
- Enables replay without re-hitting the API
- Raw files are the true audit trail

### Idempotency
Raw tables are append-only. Each run writes a new file with unique run_id.
Deduplication happens in the staging (dbt) layer on natural keys.

## Consequences
- Slightly more complex than direct-to-BQ but dramatically more robust
- Adding a new source = extend BaseIngester + define Pydantic schema
- S3 cost negligible at this data volume
