# streaming-lab

![correctness](https://img.shields.io/badge/correctness--suite-passing-brightgreen)
![phase](https://img.shields.io/badge/phase-1%20walking%20skeleton-blue)

> **Exactly-once is not a switch — it is "progress and data in one transaction."**
> This repo builds a Kafka → ClickHouse + Iceberg dual-path pipeline, commits Kafka
> offsets *inside the Iceberg snapshot* alongside the data, and then proves it: an
> independent ground truth, a real `kill -9` recovery, and a safe-watermark seam
> that counts every event exactly once.

<!-- HERO ARTIFACT (Phase 3): split-screen GIF — naive dual-write GMV drifting from
     ground truth after chaos vs. correct-mode reconciliation staying flat.
     Placeholder until Phase 3. -->

## What Phase 1 proves (measured, not claimed)

100,000 order events (`order_created` / `order_cancelled`, append-only; GMV = the
algebraic sum) flow through both paths. The seam query reads one safe watermark T
and splits: Iceberg authoritative for `event_time < T`, ClickHouse for `>= T`.

| result | number |
|---|---|
| seam-combined count == independent ground truth | 100,000 == 100,000 |
| seam-combined GMV == ground truth (exact Decimal) | 10,037,799.94 == 10,037,799.94 |
| seam split at T | lake 99,341 (<T) + ClickHouse 659 (>=T) |
| survives `kill -9` of the landing job (4 hard kills) | no loss, no duplicate rows |
| correctness suite | **7 passed in ~94s** |

The offsets live in the snapshot summary, so recovery resumes from the last
committed offset. A crash between the Parquet write and the commit leaves an
invisible orphan file, never a double count — that is the whole point.

Numbers and interpretation: [`docs/FINDINGS.md`](docs/FINDINGS.md).

## Quickstart

```bash
make up      # start Redpanda + ClickHouse + MinIO + app (single node, all local)
make init    # create the orders topic (4 partitions) + Iceberg table
make demo    # run the correctness suite -> 7 passed
```

`make demo-pipeline` runs the canonical pipeline live (generate → land → seam query).

## How it fits together

```
event-gen --> Redpanda(orders) --+--> ClickHouse (Kafka engine + MV)      [seconds fresh, >= T]
                                 |
                                 +--> Python landing job --> Parquet + Iceberg commit  [minutes, < T]
                                        offsets + watermark T stamped INTO the snapshot summary (atomic)

seam query: read latest snapshot -> T -> Iceberg(event_time < T) UNION ClickHouse(event_time >= T)
```

- `src/streaming_lab/lake/landing_job.py` — the heart: manual offsets, atomic offset-in-snapshot commit.
- `src/streaming_lab/lake/watermark.py` — per-partition-min safe watermark.
- `src/streaming_lab/query/seam.py` — single-T union.
- `src/streaming_lab/naive_mode/` — the 2023 dual-write path, kept unmodified as the Phase 2 antagonist.

## Non-goals (Phase 1)

No CDC upsert / mutable rows (append-only only — see [ADR-003](docs/decisions/003-why-append-only.md));
no chaos injection, naive-mode comparison, reconciliation, or load test yet (Phase 2/3,
[`docs/roadmap.md`](docs/roadmap.md)); no autoscaling, multi-region, schema registry, or
HTTP serving endpoint. One landing writer, one ClickHouse node, one MinIO node.

## Decisions

Spec-first: [`SPEC.md`](SPEC.md) → [`docs/phases/phase-1-plan.md`](docs/phases/phase-1-plan.md)
(interface contracts) → tests. ADRs (options → choice → rejected-why → reversal trigger)
in [`docs/decisions/`](docs/decisions/).

## Provenance

This lab was split out of [`system-design/data-intensive-app`](https://github.com/a8356555/system-design)
(2023). That repo is intentionally kept public and un-archived: its 2023 commit history is the
evidence for the narrative — "I wrote this system in 2023 believing dual-write was a reasonable
architecture; in 2026 I understood why it is wrong, wrote tests proving where it produces wrong
numbers, and rebuilt the correct version." The self-review arc is the point.
