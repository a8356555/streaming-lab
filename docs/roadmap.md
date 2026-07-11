# Roadmap (not yet built — kept out of the README per "no aspirational README")

## Phase 2 — Attack it (the repo's value core)

Chaos suite, one pytest per scenario, reusable injectors (also the future
data-agent eval ground truth):
- Late events (old event_time, arrives late) → exercise T rollback / seam loss.
- Duplicate events (producer retry) → dedup behaviour.
- Out-of-order (cross-partition skew).
- Commit-time kill, multi-point injection (extends Phase 1's crash test).
- Drift injection: change only the CH-side MV logic (bugfix-one-side simulation).

Naive-mode comparison (one flag): dual-write via `naive_mode/hybrid_source_of_truth`
+ consumer-group offsets, run the same chaos suite → show it break. FINDINGS killer
table: scenario × naive/correct × error rate.

Reconciliation job: daily D-2 window, compare CH vs Iceberg core metrics, alert at
>0.1%; pair with drift injection to show the curve jump on the injection day.

## Phase 3 — Load + FINDINGS + de-marketized README

- k6 load test of the ingest gateway and the seam query; concurrency vs p50/p99;
  find a bottleneck, fix one round, before/after chart.
- Hero artifact (README first screen): split-screen GIF — same events in, naive
  GMV drifts from ground truth after chaos while correct-mode reconciliation stays
  flat. asciinema→gif or matplotlib animation.
- Writeup: "I proved my own 2023 architecture was wrong" → HN / r/dataengineering.

## Backlog (explicitly not this round)

CDC upsert / mutable seam (Paimon, equality-deletes, temporal join); schema
registry; autoscaling; multi-region; REST/JDBC Iceberg catalog; HTTP /gmv endpoint
with caching for high-QPS serving.
