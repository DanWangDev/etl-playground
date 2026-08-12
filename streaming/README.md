# Phase 2: Streaming Extension (Redpanda ≈ Kinesis)

## Overview

This is an optional extension to the batch ETL pipeline. It adds real-time streaming
ingestion while retaining the S3-based data lake as the durable analytical source.

**Status:** Planned for Phase 2. Not yet implemented. The batch MVP (Phases 1–5) is complete.

## Architecture

```
Synthetic Event Producer
        |
        v
   Redpanda (Kafka-compatible)
        |
   +----+--------------------+
   |                         |
   v                         v
Rule Engine              Consumer → S3 Raw
(Lambda equiv)           (Firehose equiv)
   |
   v
Risk / Anomaly Flag
   |
   v
Policy / Action Simulator
   |
   v
Audit + Evaluation Metrics
```

## Local ↔ AWS Mapping

| Concept | Local | AWS |
|---------|-------|-----|
| Streaming ingestion | Redpanda (single binary, Kafka API) | Amazon Kinesis |
| Lightweight processing | Python consumer with rules | AWS Lambda |
| Durable storage | Write to `data/raw/` (same as batch) | Kinesis Firehose → S3 |
| Anomaly detection | Rule-based + optional LLM enrichment | SageMaker / custom models |

## Key Concepts to Demonstrate

- Streaming vs batch ingestion trade-offs
- At-least-once vs exactly-once delivery semantics
- Partition key design for ordered processing
- Consumer group offset management
- Late data handling in streaming context
- Lambda architecture: combining stream + batch results
- Precision/recall measurement for anomaly detection

## Prerequisites

- Docker for Redpanda (single `docker run` command)
- `kafka-python` package (`uv sync --extra streaming`)

## Quick Start (Future)

```bash
# Start Redpanda
docker run -d --name redpanda -p 9092:9092 \
  docker.redpanda.com/redpandadata/redpanda start \
  --smp 1 --overprovisioned

# Run streaming producer
uv run python -m etl_playground.m10_streaming.producer

# Run batch ETL (same as before — reads from data/raw/)
make run-pipeline
```

## When Streaming Is Justified

Not every pipeline needs streaming. Use streaming when:
- Latency SLA is sub-minute (batch is fine for hourly/daily)
- Real-time alerting/detection is required
- Event volume is continuous (not batched uploads)

Otherwise, a simpler batch pipeline is cheaper, more reliable, and easier to debug.
