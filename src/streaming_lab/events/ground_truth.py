"""Independent ground-truth reader.

Computes count / GMV / per-watermark sets directly from the generator's JSONL,
NEVER through Kafka, ClickHouse, or Iceberg. This is the anti-fraud oracle: the
system under test cannot influence these numbers.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from streaming_lab.events.schema import OrderEvent


@dataclass(frozen=True)
class GroundTruth:
    count: int
    gmv: Decimal
    event_ids_below_t: frozenset[str]
    count_below_t: int
    gmv_below_t: Decimal


def read_events(path: str) -> list[OrderEvent]:
    events = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(OrderEvent.from_json(line))
    return events


def compute(path: str, t_ms: int) -> GroundTruth:
    events = read_events(path)
    gmv = sum((e.gmv_delta() for e in events), Decimal(0))
    below = [e for e in events if e.event_time_ms < t_ms]
    gmv_below = sum((e.gmv_delta() for e in below), Decimal(0))
    return GroundTruth(
        count=len(events),
        gmv=gmv,
        event_ids_below_t=frozenset(e.event_id for e in below),
        count_below_t=len(below),
        gmv_below_t=gmv_below,
    )
