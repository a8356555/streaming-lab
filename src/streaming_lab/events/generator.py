"""Event generator: bounded-disorder order events -> Redpanda + ground-truth JSONL.

Two outputs from one deterministic (seeded) run:
  1. Every event is appended to ``ground_truth_path`` (JSONL). Tests read THIS
     file directly to compute expected count/GMV -- never through the system
     under test. This is the anti-fraud independent verification path.
  2. Every event is produced to the Redpanda topic, keyed by order_id so a
     created/cancelled pair lands on the same partition (per-order ordering).

Bounded disorder guarantee: events are emitted in index order i with
``event_time_ms = start_ms + i*STEP_MS + jitter``, ``jitter in [0, W)``. So the
max out-of-orderness in event_time is < W = lateness window -- the precondition
that makes watermark T safe (SPEC db-warehouse §3.1). Cancellations always
follow their creation in emission order.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from decimal import Decimal

from confluent_kafka import Producer

from streaming_lab.config import CONFIG, Config
from streaming_lab.events.schema import ORDER_CANCELLED, ORDER_CREATED, OrderEvent

STEP_MS = 1  # event-time advance per emission


@dataclass(frozen=True)
class GenSummary:
    total: int
    created: int
    cancelled: int
    gmv: Decimal
    ground_truth_path: str


def build_events(
    n: int,
    seed: int,
    cfg: Config = CONFIG,
    start_ms: int = 1_700_000_000_000,
    cancel_ratio: float = 0.3,
    max_amount: int = 500,
) -> list[OrderEvent]:
    """Deterministically build exactly ``n`` events with bounded disorder.

    A cancellation references a previously created order (same order_id and
    amount) and is always emitted at a later index than its creation.
    """
    rng = random.Random(seed)
    events: list[OrderEvent] = []
    created_pool: list[tuple[str, Decimal, int]] = []  # (order_id, amount, user_id)
    i = 0
    while len(events) < n:
        jitter = rng.randint(0, cfg.lateness_window_ms - 1)
        event_time_ms = start_ms + i * STEP_MS + jitter
        if created_pool and rng.random() < cancel_ratio:
            idx = rng.randrange(len(created_pool))
            order_id, amount, user_id = created_pool.pop(idx)
            events.append(
                OrderEvent(
                    event_id=str(uuid.UUID(int=rng.getrandbits(128))),
                    order_id=order_id,
                    event_type=ORDER_CANCELLED,
                    amount=amount,
                    user_id=user_id,
                    event_time_ms=event_time_ms,
                )
            )
        else:
            order_id = str(uuid.UUID(int=rng.getrandbits(128)))
            amount = (Decimal(rng.randint(1, max_amount * 100)) / Decimal(100)).quantize(Decimal("0.01"))
            user_id = rng.randint(1, 1000)
            created_pool.append((order_id, amount, user_id))
            events.append(
                OrderEvent(
                    event_id=str(uuid.UUID(int=rng.getrandbits(128))),
                    order_id=order_id,
                    event_type=ORDER_CREATED,
                    amount=amount,
                    user_id=user_id,
                    event_time_ms=event_time_ms,
                )
            )
        i += 1
    return events


def write_ground_truth(events: list[OrderEvent], path: str) -> None:
    with open(path, "w") as f:
        for e in events:
            f.write(e.to_json() + "\n")


def produce(events: list[OrderEvent], cfg: Config = CONFIG) -> None:
    producer = Producer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
        }
    )
    for e in events:
        producer.produce(cfg.topic, key=e.order_id.encode(), value=e.to_json().encode())
        producer.poll(0)
    producer.flush()


def generate_and_produce(
    n: int,
    seed: int = 42,
    cfg: Config = CONFIG,
    do_produce: bool = True,
) -> GenSummary:
    """Build events, write the ground-truth JSONL, and (optionally) produce them."""
    events = build_events(n, seed, cfg)
    write_ground_truth(events, cfg.ground_truth_path)
    if do_produce:
        produce(events, cfg)
    created = sum(1 for e in events if e.event_type == ORDER_CREATED)
    gmv = sum((e.gmv_delta() for e in events), Decimal(0))
    return GenSummary(
        total=len(events),
        created=created,
        cancelled=len(events) - created,
        gmv=gmv,
        ground_truth_path=cfg.ground_truth_path,
    )


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=100_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--no-produce", action="store_true")
    args = ap.parse_args()
    summary = generate_and_produce(args.n, args.seed, do_produce=not args.no_produce)
    print(summary)
