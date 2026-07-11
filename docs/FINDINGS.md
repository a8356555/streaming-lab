# FINDINGS

> Numbers and raw data are filled from real test runs.
> **Every interpretation paragraph (marked TODO(Alan)) is hand-written by Alan** —
> the non-outsourceable part (portfolio overview, "人類不可外包的部分").
> A finding without a named production system it explains/challenges is not a
> finding (overview FINDINGS surprise rule).

## Phase 1 — Walking skeleton: does the mechanism hold?

Phase 1 is not where the surprising numbers live (that is Phase 2 chaos + Phase 3
load). Phase 1 establishes the baseline correctness the later phases attack.

### Raw data (from the correctness suite)

> Filled from `make demo`. See `docs/phases/phase-1-plan.md` §7 for the asserts.

| test | events | result | seam T (ms) | lake_count (<T) | ch_count (>=T) | committed batches | wall time |
|---|---|---|---|---|---|---|---|
| no_dup_no_loss | 100,000 | PASS | 1700000099828 | 99,341 | 659 | 21 | ~32s |
| crash_restart  | 100,000 | PASS | 1700000099893 | 100,000 total in lake | (seam) | 36 (across 4 SIGKILLs + final) | ~43s |
| watermark_safety | 50,000 | PASS | >0 | below-T set > 1,000 (asserted) | — | ~19s |

Full suite (`make demo`, 7 tests incl. 4 watermark unit tests): **7 passed in ~94s**.

> Note: the per-test rows are a **single-run snapshot**. Committed-batch count
> and wall time vary run-to-run with scheduling and micro-batch timing (e.g. an
> independent verifier observed 36 batches / 116s where an earlier run saw 21 / 94s);
> the *correctness* assertions (exact count/GMV, no-dup, completeness) are invariant.

Concrete seam split (canonical `make demo-pipeline`, 100k events, seed 42):
- total = 100,000; GMV = **10,037,799.94** (exact Decimal, == ground truth)
- seam at T=1700000099828: lake (<T) = 99,341 rows / 9,983,884.69; CH (>=T) = 659 rows / 53,915.25
- 99,341 + 659 = 100,000; 9,983,884.69 + 53,915.25 = 10,037,799.94 — every event counted exactly once.

GMV equality (Decimal, exact): `10037799.94` == ground truth `10037799.94`.
Lake dup check (crash_restart): total rows == distinct event_ids (asserted equal after 4 hard kills).

### What Phase 1 demonstrated (facts, no spin)

- Offsets committed inside the Iceberg snapshot summary survive `kill -9` at
  arbitrary points; recovery resumes from the last committed offset with neither
  loss nor duplicate rows.
- The seam splits on a single T taken once; combined count/GMV equals an
  independent ground truth computed straight from the generator's JSONL.

### Interpretation

> TODO(Alan): why is "no duplicate rows after N hard kills" evidence of atomicity
> specifically (not just idempotency)? Name the production system this mechanism
> mirrors (Flink Iceberg sink / Kafka Connect Iceberg sink) and state what you now
> understand about why they commit offsets into the table, not the consumer group.

### Known limitations of the Phase 1 checks (tracked, not hidden)

Writing the boundaries down is the point of a correctness lab, not an admission.

- **L1 — count/GMV equality is aggregate, not a per-event set check.** The seam
  asserts `count == ground_truth.count` and `Decimal GMV == ground_truth.gmv`,
  not row-by-row set equality across the whole seam. In principle a compensating
  pair of errors (one extra + one missing, or +x/-x GMV) could net to equal
  aggregates. Mitigations already in place: the lake side is additionally checked
  as a set below T (`test_watermark_safety`) and for `total == distinct == count`
  (no dup + completeness). A full per-event seam reconciliation is a Phase 2 item.
- **L2 — ClickHouse is at-least-once on the >=T side.** CH's Kafka engine gives
  at-least-once delivery; a duplicate and a drop in the `>= T` region could in
  theory mutually cancel in the aggregate. Phase 1 does not dedup the CH side
  (append-only + algebraic GMV makes exact-once less critical for the metric), and
  the reconciliation job + CH-side dedup strategy are explicit Phase 2/roadmap work.

## Phase 2 — Chaos + naive comparison (placeholder)

> The killer table lives here: scenario × naive/correct × error rate.

| scenario | correct-mode error | naive-mode error | when it diverges |
|---|---|---|---|
| late events | _TBD_ | _TBD_ | _TBD_ |
| duplicate (producer retry) | _TBD_ | _TBD_ | _TBD_ |
| out-of-order (partition skew) | _TBD_ | _TBD_ | _TBD_ |
| commit-time kill (multi-point) | _TBD_ | _TBD_ | _TBD_ |
| drift injection (CH-only MV change) | _TBD_ | _TBD_ | _TBD_ |

> TODO(Alan): interpretation + the one production incident pattern each row explains.

## Phase 3 — Load + bottleneck (placeholder)

> k6 concurrency vs p50/p99 for ingest and seam query; before/after one fix.
> TODO(Alan): which result was unexpected, and which system's design it explains.

## Retrospective (for "if you redid it, what would change?")

> TODO(Alan): keep a running list as phases complete.
