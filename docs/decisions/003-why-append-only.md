# ADR-003: Append-only event model (why no CDC upsert across the seam)

> Status: **ACCEPTED** — ratified by Alan 2026-07-11

## Context

GMV = money of created-but-not-cancelled orders. There are two ways to model it:
mutate a current-state row per order, or append immutable events and derive state.
The choice determines whether the seam (lake `<T` ∪ CH `>=T`) can stay correct.

## Options

**A. Append-only events, algebraic GMV (chosen default).**
`order_created` contributes `+amount`, `order_cancelled` contributes `-amount`; a
cancellation is a NEW row, never an UPDATE. GMV is a sum over both sides of the
seam.
- For: the seam only ever splits *immutable* rows on T, so each event is counted
  exactly once and a late cancellation is just a newer row that lands on its
  correct side — the algebra self-corrects. State correctness is pushed off the
  seam entirely (SPEC db-warehouse §3.2 solution C).
- Against: no first-class "current order state" in the hot path; per-order status
  is a lake-side MERGE, computed separately.

**B. Mutable current-state table + CDC upsert.**
Each order is one row; cancel = UPDATE/DELETE; CH `ReplacingMergeTree` dedupes,
Iceberg does `MERGE`.
- For: intuitive "one row per order"; direct point lookups of order status.
- Against: **T cannot reconcile row *versions* across the seam.** T governs "which
  rows have passed", not "which version of a row each side holds". Near the seam,
  the same order can have a created-version in the lake and a cancelled-version in
  CH (or vice versa) → the two sides disagree and T does not save you. This is the
  deep-water case (SPEC db-warehouse §3.2 solution C, red-team finding 2).

## Recommendation (proposal only)

Default to **A** for the entire lab. Append-only is what makes the seam provably
correct with a single T, which is the thesis. Mutable-CDC upsert is explicitly a
backlog item (SPEC Non-goals) precisely because it breaks the clean seam and needs
a different mechanism.

## Decision

**Append-only events, algebraic GMV** (ratified by Alan 2026-07-11).

- Append-only is what makes the seam provably correct with a single T — the thesis.
- The seam only ever splits immutable rows on T, so a late `order_cancelled` is just
  a newer row on its correct side and the algebra self-corrects; no special handling.
- Mutable current-state + CDC upsert is explicitly backlog (SPEC Non-goals) because
  T governs which rows have passed, not which version each side holds.

> TODO(Alan): 發布前用自己的話改寫本段。

## Reversal trigger

Open the CDC-upsert backlog (Paimon / equality-deletes / temporal join) only after
Phase 3, and only if a target use case genuinely needs mutable current-state at
the seam rather than a lake-side derived table.
