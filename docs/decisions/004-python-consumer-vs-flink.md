# ADR-004: Landing job — pure-Python micro-batch consumer vs Flink/Connect/Spark

> Status: **PROPOSAL** — decision pending Alan

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

> TODO(Alan): confirm A for the lab. In your own words: what does Flink's Iceberg
> sink give you that this hand-rolled job does not (stateful windows, backpressure,
> rescaling), and when would you actually reach for it in production?

## Reversal trigger

Reach for D (Flink) the moment the workload needs real stateful streaming
(event-time windows, dual-stream joins, CEP) rather than append-and-commit; reach
for B (Kafka Connect) if the goal shifts from "demonstrate" to "operate cheaply".
