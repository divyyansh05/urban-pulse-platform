# ADR-001: Cloud Architecture Decision

## Status
Accepted

## Context
Urban Pulse Platform requires both batch and streaming processing across
multiple public data domains. We have access to GCP, AWS, and Databricks
free tiers with limited credits.

## Decision
- **Raw storage**: AWS S3 — cost-effective, vendor-neutral, portable
- **Streaming**: GCP Pub/Sub + Dataflow — managed, scales to zero
- **Warehouse**: GCP BigQuery — serverless, best for analytical workloads,
  1TB free query/month sufficient for development
- **Batch processing**: Databricks — free tier sufficient, Spark familiarity
- **Transformation**: dbt Core — open source, version controlled
- **Orchestration**: Apache Airflow (self-hosted Docker) → Cloud Composer
  for production

## Consequences
- Cross-cloud egress costs are minimal (raw data is small, processed data
  stays within GCP)
- Team must manage two cloud provider credentials
- Architecture demonstrates multi-cloud design pattern — positive for
  portfolio and interviews
