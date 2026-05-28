# ADR-001: Architecture overview

**Status:** Accepted · **Date:** 2026-05-27

## Context

We need to reconcile multi-channel patient consent state across three independent retailer systems with the following real-world properties:

- **Multi-channel.** A patient can opt in/out per channel (SMS, mailed letter, in-home visit) independently at each retailer.
- **Multi-source.** Events arrive from three independent retailers, each with its own `patient_id` space. The same physical patient may exist under different IDs at different retailers.
- **Late arrival.** A fraction of events arrive with `event_ts` significantly in the past (paper forms, sync lag from third-party systems).
- **Read-heavy serving.** Downstream consumers (call center, campaign tools) check current consent state far more often than it changes.
- **Auditability.** Compliance requires a queryable history of every state change with valid-time windows.

Non-functional constraints from project scope:

- Zero monthly cost (no GCP billing — see [ADR-002](002-path-b-rationale.md)).
- End-to-end latency target: 1–5 minutes (event published → reflected in mart).
- Maintainable solo by one engineer at ~10 hrs/week for 8–10 weeks.

## Decision

A **micro-batch architecture** with these components:

| Layer | Choice | Why |
|---|---|---|
| Bus | Redpanda (Kafka API), local Docker | Kafka idioms are industry standard; Redpanda is a single binary, no ZooKeeper, no cloud bill. |
| Ingestion | Long-running Python consumer; flushes every 30s or 1000 events to BQ load jobs | BQ Sandbox forbids streaming inserts; load jobs are free and unlimited. 30s window fits the 1–5 min latency budget. |
| Warehouse | BigQuery Sandbox | Real BQ semantics with no billing. Partition + clustering preserved. 60-day table TTL is acceptable for portfolio scope. |
| Transformation | dbt Core, incremental + SCD2 | Layered staging → intermediate → marts. MERGE-based incrementals on `message_id` give idempotency. |
| Orchestration | Airflow (Docker), 5-min schedule | Standard for this cadence; user's existing comfort. Backfill DAG separate. |
| Data quality | Great Expectations Core 1.x | Suites at raw boundary and mart boundary; checkpoints as blocking Airflow tasks. Results trended in `dq.expectations_results`. |
| Serving | FastAPI + `X-API-Key` auth | Reads `mart_current_preferences`; writes publish back to Redpanda. |
| Cache | Redis (local), Upstash REST (prod) | Reads dominated by per-patient lookups; cache invalidated on every write. |
| Dashboard | Plotly Dash, local | Trends, freshness, recent-events view for the demo. |
| Cloud surface | Fly.io for FastAPI only | Free tier, no card mandate, Docker-native deploys. |

Key sub-decisions:

- **Dedupe at the dbt staging layer**, not at the consumer, using `row_number() over (partition by message_id order by ingested_at)`. Consumer stays at-least-once and simple.
- **Identity resolution is deterministic** on `coalesce(lower(trim(email)), phone_e164)`, falling back to `retailer_id || patient_id`. Probabilistic matching is documented as future work.
- **SCD2 history (`fct_consent_history`) is the audit source of truth**; `mart_current_preferences` is a derived read model.
- **Event schema is JSON, enforced by pydantic** at producer and consumer. No Schema Registry — adds infra without proportional resume value at this scope.

## Consequences

**Positive**

- Zero recurring cost; no credit-card mandates anywhere in the stack.
- Every tool in the stack is industry-standard and portable.
- The latency claim ("1–5 min near-real-time") matches the implementation — no marketing-vs-reality gap to defend in interviews.
- dbt + Airflow + GE is a recognizable, hire-able pattern.

**Negative**

- Local Redpanda means the deployed FastAPI's `POST /events` cannot reach the local bus without a tunnel. Documented limitation; mitigation deferred to Phase 9 (cloudflared tunnel, or disable the write endpoint in cloud demo).
- BQ Sandbox's 60-day table TTL means demo data needs periodic refresh — a `make seed-bq` target handles it.
- No streaming inserts to BQ means we can never honestly claim sub-minute latency without re-architecting.

## Alternatives considered

- **True streaming (Dataflow + materialized views, or Kafka → Redis hot path).** Rejected. Adds ~2 weeks for a marginal latency win the use case doesn't require. Listed as future work.
- **Pub/Sub native ingestion → BQ subscription.** Rejected by [ADR-002](002-path-b-rationale.md) — GCP billing was the blocker.
- **DuckDB or Postgres as the warehouse.** Rejected. Loses the BigQuery resume bullet without meaningful complexity savings.
- **Looker Studio instead of Dash.** Considered, viable. Kept Dash to retain Python charting work as a code artifact.
