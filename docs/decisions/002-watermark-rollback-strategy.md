# ADR-002: Watermark T strategy — lateness window, idle partitions, rollback

> Status: **ACCEPTED** — ratified by Alan 2026-07-11

## Context

T is the safe watermark that splits the seam: lake authoritative for
`event_time < T`, ClickHouse for `event_time >= T`. Safety requires: every event
with `event_time < T` is already in the lake. If T advances too aggressively, a
still-in-flight late event with `event_time < T` is dropped by the seam (loss). If
too conservatively, fresh data sits below T and the "seconds-fresh" promise erodes.

Two failure modes to defend against:
1. **Out-of-order events** — event_time not monotonic with offset.
2. **Idle partitions** — a partition delivers nothing, so its contribution to the
   watermark is unknown; advancing T past it risks dropping its future old events.

## Options

**Lateness handling**
- A. **Static window W**, `T = min_p(max_event_time_p) - W` (chosen default).
  Per-partition max, then min across partitions, minus a fixed W. Safe under the
  bounded-out-of-orderness ≤ W assumption. Simple, predictable, testable.
- B. Adaptive/percentile window (learn W from observed lateness).
  Better freshness, but T becomes data-dependent and hard to reason about; a
  mis-estimate silently drops data.
- C. No window, `T = min offset frontier's event_time`. Maximally safe but T
  crawls and freshness collapses.

**Idle partitions**
- D. **Hold**: an unseen/idle partition prevents T from advancing (chosen default).
  Correct but not live — one idle partition stalls the whole watermark.
- E. **Idle timeout**: after N seconds of silence, mark a partition idle and
  exclude it from the min. Restores liveness at the cost of a safety assumption
  ("an idle partition won't suddenly emit old data").

**When the assumption breaks (a truly late event arrives with event_time < T)**
- F. **Do nothing** — the seam has already dropped it (the failure Phase 2 will
  measure and visualize).
- G. **Roll T back** — not allowed if T is published/monotonic; would double-count.
- H. **Side-path**: route late-but-below-T events to a correction/late table and
  reconcile (Phase 2/3 territory, ties into the reconciliation job).

## Recommendation (proposal only)

Phase 1 default: **A + D + F** — static W, per-partition min, hold on idle, and
*let* the assumption-violation be a visible failure. This keeps the safety
property provable for the walking skeleton and sets up Phase 2 to *inject*
lateness that breaks the ≤ W assumption and quantify the resulting seam loss,
motivating **E** (idle timeout) and **H** (late side-path) as measured upgrades.

## Decision

**Phase 1 default A + D + F** — static window W, per-partition min, hold on idle,
and let an assumption-violation be a visible failure (ratified by Alan 2026-07-11).

- Per-partition min is the correct watermark, not `max - W`: the repo's own unit
  test (`test_watermark_unit.py::test_min_across_partitions_not_max`) demonstrates
  that a global max drops data when partitions drain unevenly.
- Static W keeps the safety property provable for the walking skeleton; Phase 2
  breaks the ≤ W assumption on purpose to measure the seam loss.

### Open questions (for Phase 2 kickoff)

Late-event policy is deferred to Phase 2, with a recorded leaning (Alan): first
make the failure **visible** (option F), then implement **T rollback as the fix**;
a dedicated late side-path (option H) goes to backlog, considered only if a seam
consumer drives a money-affecting action where dropped late events are
unacceptable. This is the first thing to ratify when Phase 2 opens.

> TODO(Alan): 發布前用自己的話改寫本段。

## Reversal trigger

Adopt E (idle timeout) once multi-partition skew stalls freshness in a demo;
adopt H (late side-path) if any consumer of the seam drives a money-affecting
action where dropped late events are unacceptable.
