# ADR-002: Local-first stack, no GCP billing

**Status:** Accepted · **Date:** 2026-05-27

## Context

The original plan was a GCP-native build: Pub/Sub → BigQuery → Cloud Run for FastAPI, deployed via Terraform.

When the author (based in India) created a Google Cloud account, GCP required:

1. A **₹15,000 (~$180) RBI e-mandate** — a pre-authorization for autopay. Not an actual charge, but a binding standing instruction on the bank account.
2. A **₹1,000 (~$12) refundable prepayment** to verify the payment method.

Both are standard for Indian GCP signups under RBI's 2021 recurring-payments rules. The mandate does not move money on its own; budget alerts and hard caps can keep actual spend at ₹0. The author cancelled the mandate on principle (no standing bank instruction).

This left the GCP account unusable for any service that requires active billing (Pub/Sub, Cloud Run, Memorystore, Composer). **BigQuery Sandbox still works without billing.**

## Decision

Pivot to a **fully local-first stack** that uses no GCP service requiring billing:

| Original (GCP-native) | Replacement (Path B) |
|---|---|
| Pub/Sub | Redpanda (Kafka API) in local Docker |
| BigQuery (full) | BigQuery Sandbox (load jobs only) |
| Cloud Run (FastAPI) | Fly.io free tier |
| Memorystore (Redis) | Local Redis (dev) + Upstash REST free tier (prod) |
| Cloud Composer | Airflow stays local in Docker (this was always the plan) |

GCP is retained only as the host for BigQuery Sandbox.

## Consequences

**Positive**

- Zero recurring cost, zero card mandates, no standing instruction on the user's bank.
- Kafka (via Redpanda) is arguably a stronger resume signal than Pub/Sub — broader recognition, more transferable across employers.
- Removes a recurring stressor from an 8–10 week project.
- Fly.io and Upstash both offer free tiers without card-on-file in India.

**Negative**

- Lose the "deployed on GCP" framing. Replaced with "deployed on Fly.io" — less marquee, but real and clickable.
- Cannot use streaming inserts to BQ; everything goes via load jobs. Acceptable given the micro-batch architecture (see [ADR-001](001-architecture.md)).
- Deployed FastAPI on Fly.io cannot reach local Redpanda for the `POST /events` write path. Mitigation deferred to Phase 9.

## Alternatives considered

- **Path A — re-enable the e-mandate with a ₹50 budget cap and hard auto-shutoff.** Technically safe; the mandate is a pre-auth not a charge, and the cap would prevent real spend. Rejected because (a) the author preferred not to have a standing instruction on the bank, (b) the GCP-native architecture had only marginal advantages over Path B for a portfolio project.
- **Path C — hybrid (BQ Sandbox via the existing GCP account, but Pub/Sub swapped for Kafka and FastAPI on Fly.io).** Essentially what Path B already does. Added no incremental simplification.
- **Switch to AWS or Azure free tier.** Both also require a card and have similar RBI-related verification flows in India. No advantage over Path B.

## Future work

If the project grows beyond portfolio scope or the author later opts into GCP billing, a clean v2 milestone is sketched here: swap the Python consumer for a Pub/Sub → BQ subscription, redeploy FastAPI on Cloud Run, and add a Terraform module for the cloud-native variant. The dbt, GE, and dashboard layers would be unchanged.
