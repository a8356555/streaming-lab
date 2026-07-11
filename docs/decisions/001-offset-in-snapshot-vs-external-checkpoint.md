# ADR-001: Where do Kafka offsets live — Iceberg snapshot vs external checkpoint

> Status: **PROPOSAL** — decision pending Alan
> This is the load-bearing decision of the whole repo.

## Context

Exactly-once for the lake side means the consumer's progress (Kafka offsets) and
the data it produced (Parquet files) must advance together, atomically. If they
can diverge, a crash produces either loss (offset ahead of data) or duplication
(data ahead of offset). The question is *where* the offsets are committed and
*whether that commit is the same transaction as the data commit.*

## Options

**A. Offsets in the Iceberg snapshot summary (chosen default).**
Each micro-batch writes Parquet and, in the *same* Iceberg commit, stamps the
snapshot summary with the next offsets + watermark T. The SqlCatalog pointer swap
is atomic (a SQLite transaction), so data and progress are one transaction.
Recovery reads offsets from the latest snapshot and seeks.
- For: single source of truth; crash-atomic by construction; no second store to
  keep consistent; identical in spirit to what Flink's Iceberg sink and Kafka
  Connect's Iceberg sink do internally.
- Against: couples offset bookkeeping to the table format; the summary carries
  operational metadata (some consider snapshots "data only"); SqlCatalog is a
  single-writer bottleneck (fine for one landing job).

**B. External checkpoint store (offsets in a separate file/KV/DB).**
Write Parquet, commit Iceberg, then write offsets to a side store.
- For: separation of concerns; offsets queryable without opening table metadata.
- Against: **two commits cannot be atomic** — a crash between them reintroduces
  exactly the loss/dup window we are trying to close. Requires idempotent replay
  or a 2PC-ish protocol to paper over. This is the trap the repo exists to expose.

**C. Kafka consumer-group offsets (the naive default).**
Let the broker track offsets via `enable.auto.commit` or manual `commit()`.
- For: zero code; the "obvious" approach.
- Against: the consumer-group commit is a *different system* from the Iceberg
  commit — never atomic with the data. Auto-commit can commit offsets for records
  not yet durably written to the lake → silent loss on crash. This is precisely
  the naive path Phase 2 attacks (see `naive_mode/`).

## Recommendation (proposal only)

Default to **A**. It is the only option where "progress and data" is literally one
transaction, which is the definition of exactly-once we are demonstrating. B and C
are kept as the Phase 2 antagonists to *measure* how they fail.

## Decision

> TODO(Alan): confirm A, in your own words, and state the one sentence you'd give
> an interviewer for "why not just commit the consumer-group offset?"

## Reversal trigger

Move to a REST/JDBC catalog (still option A, offsets-in-snapshot) when more than
one landing writer is needed or SqlCatalog's single-writer commit becomes a
bottleneck. Reconsider B only if a downstream system must read offsets without
Iceberg access — and even then, derive it from the snapshot, never write it
independently.
