# ADR-004: Landing job — pure-Python micro-batch consumer vs Flink/Connect/Spark

> Status: **ACCEPTED** — ratified by Alan 2026-07-11

## Context

Something must move events from Redpanda into Iceberg and commit offsets+T
atomically. The heavyweight streaming frameworks do this for you; the question is
whether pulling one in is worth it for this lab, whose point is to *understand and
demonstrate* the atomic-commit mechanism, not to hide it.

## Options

| Option | For | Against |
|---|---|---|
| **A. Pure-Python consumer + pyiceberg (chosen default)** | fewest components (SPEC "元件最少化"); the offset-in-snapshot mechanism is written explicitly and is *readable* — the whole teaching value; trivial to SIGKILL for the crash test | hand-rolled; no built-in backpressure/scaling; single writer |
| B. Kafka Connect Iceberg sink | lightest managed option; production-grade | the atomic-commit mechanism is hidden inside the connector — the exact thing we want to show is a black box |
| C. Spark Structured Streaming | mature Iceberg integration; scales | a JVM cluster to move two event types; heavy for a walking skeleton |
| D. Flink | the "correct" answer for stateful streaming; its Iceberg sink *is* offset-in-snapshot |航母送外賣 (aircraft carrier to deliver takeout) for append-only ingest with no stateful windows; the Flink materials are kept as reference only (`_materials/flink-reference/`) |

## Recommendation (proposal only)

Default to **A**. The repo's value is showing *how* exactly-once ingest works, so
implementing offset-in-snapshot by hand (≈120 lines) is the feature, not a
shortcut. B/C/D are the production answers, and the README explicitly connects our
hand-rolled mechanism to "this is what Flink's / Kafka Connect's Iceberg sink does
internally" — earned by having built it.

## Decision

**Option A — pure-Python micro-batch consumer + pyiceberg** (ratified by Alan 2026-07-11).

- The repo's purpose is to *expose* the exactly-once mechanism; Flink is exactly the
  tool that hides it inside a sink. Hand-rolling ~120 lines makes the mechanism the
  readable feature, not a black box.
- The README earns the line "this is what Flink's / Kafka Connect's Iceberg sink
  does internally" precisely by having built it.

> TODO(Alan): 發布前用自己的話改寫本段。

## Reversal trigger

Reach for D (Flink) the moment the workload needs real stateful streaming
(event-time windows, dual-stream joins, CEP) rather than append-and-commit; reach
for B (Kafka Connect) if the goal shifts from "demonstrate" to "operate cheaply".

Special case — Phase 4 throughput: if the single-threaded Python landing job hits a
GIL/throughput ceiling, **that wall is itself a finding** (measure and report it);
a Flink comparison goes to backlog, not a rewrite of this decision.
