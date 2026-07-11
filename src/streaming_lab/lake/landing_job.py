"""Landing job: Redpanda -> Parquet -> Iceberg, with offsets in the snapshot.

This is the heart of the repo. It proves exactly-once = "progress and data in
one transaction":

  * Kafka offsets are managed MANUALLY (enable.auto.commit=False, we assign+seek
    ourselves). We never trust the consumer-group offset. On start we read the
    resume offsets from the latest Iceberg snapshot summary and seek to them.
  * Each micro-batch: write a Parquet data file and, in the SAME Iceberg commit,
    stamp the snapshot summary with the next offsets + watermark T + per-partition
    max event_time + a commit sequence number.
  * The Iceberg commit (SqlCatalog pointer swap) is atomic. Crash before it ->
    the just-written Parquet is an unreferenced orphan (invisible, no double
    count). Crash after it -> offsets are already durable, restart resumes past
    them (no loss, no reprocessing).

Run as a process (``python -m streaming_lab.lake.landing_job``) so the crash test
can SIGKILL a real pid.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field

import pyarrow as pa
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from streaming_lab.config import CONFIG, Config
from streaming_lab.events.schema import OrderEvent
from streaming_lab.lake.catalog import get_catalog
from streaming_lab.lake.watermark import compute_watermark

# Snapshot summary keys (contract, see phase-1-plan §2.1)
K_OFFSETS = "streaming-lab.kafka.offsets"
K_WATERMARK = "streaming-lab.watermark.event_time_ms"
K_LATENESS = "streaming-lab.watermark.lateness_window_ms"
K_PART_MAX = "streaming-lab.watermark.per_partition_max_ms"
K_BATCH_N = "streaming-lab.batch.event_count"
K_SEQ = "streaming-lab.commit.seq"


def _summary_props(snapshot) -> dict[str, str]:
    """Extract custom properties from a snapshot summary across pyiceberg versions."""
    if snapshot is None:
        return {}
    summary = snapshot.summary
    props = getattr(summary, "additional_properties", None)
    if props is not None:
        return dict(props)
    try:
        return dict(summary)
    except Exception:  # pragma: no cover - defensive
        return {}


@dataclass
class ResumeState:
    offsets: dict[int, int] = field(default_factory=dict)  # partition -> next offset to consume
    per_partition_max_ms: dict[int, int] = field(default_factory=dict)
    watermark_t_ms: int = 0
    commit_seq: int = 0


def read_resume_state(cfg: Config = CONFIG) -> ResumeState:
    catalog = get_catalog(cfg)
    tbl = catalog.load_table(cfg.orders_identifier)
    props = _summary_props(tbl.current_snapshot())
    st = ResumeState()
    if K_OFFSETS in props:
        st.offsets = {int(k): int(v) for k, v in json.loads(props[K_OFFSETS]).items()}
    if K_PART_MAX in props:
        st.per_partition_max_ms = {int(k): int(v) for k, v in json.loads(props[K_PART_MAX]).items()}
    st.watermark_t_ms = int(props.get(K_WATERMARK, "0"))
    st.commit_seq = int(props.get(K_SEQ, "0"))
    return st


@dataclass
class Record:
    partition: int
    offset: int
    event: OrderEvent


def _arrow_batch(arrow_schema: pa.Schema, events: list[OrderEvent]) -> pa.Table:
    event_time = pa.array(
        [e.event_time_ms for e in events], type=pa.timestamp("ms", tz="UTC")
    ).cast(pa.timestamp("us", tz="UTC"))
    columns = {
        "event_id": pa.array([e.event_id for e in events], type=pa.string()),
        "order_id": pa.array([e.order_id for e in events], type=pa.string()),
        "event_type": pa.array([e.event_type for e in events], type=pa.string()),
        "amount": pa.array([e.amount for e in events], type=pa.decimal128(18, 2)),
        "user_id": pa.array([e.user_id for e in events], type=pa.int64()),
        "event_time": event_time,
    }
    return pa.table({f.name: columns[f.name] for f in arrow_schema}, schema=arrow_schema)


class LandingJob:
    def __init__(self, cfg: Config = CONFIG):
        self.cfg = cfg
        self.catalog = get_catalog(cfg)
        self.tbl = self.catalog.load_table(cfg.orders_identifier)
        self.arrow_schema = self.tbl.schema().as_arrow()
        self.state = read_resume_state(cfg)
        self.consumer = Consumer(
            {
                "bootstrap.servers": cfg.kafka_bootstrap,
                "group.id": "landing_job",  # present, but offsets come from the snapshot
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._assign_from_state()

    def _assign_from_state(self) -> None:
        partitions = [
            TopicPartition(self.cfg.topic, p, self.state.offsets.get(p, OFFSET_BEGINNING))
            for p in range(self.cfg.num_partitions)
        ]
        self.consumer.assign(partitions)

    def _commit_batch(self, batch: list[Record]) -> None:
        """Write Parquet + stamp offsets/watermark into ONE Iceberg snapshot."""
        events = [r.event for r in batch]

        # next offset to consume per partition = max seen offset in batch + 1
        next_offsets = dict(self.state.offsets)
        for r in batch:
            next_offsets[r.partition] = max(next_offsets.get(r.partition, -1), r.offset + 1)

        # per-partition max event_time (monotonic merge with prior state)
        part_max = dict(self.state.per_partition_max_ms)
        for r in batch:
            part_max[r.partition] = max(part_max.get(r.partition, 0), r.event.event_time_ms)

        t_new = compute_watermark(
            part_max,
            self.cfg.lateness_window_ms,
            previous_t_ms=self.state.watermark_t_ms,
            expected_partitions=self.cfg.num_partitions,
        )
        seq = self.state.commit_seq + 1

        snapshot_properties = {
            K_OFFSETS: json.dumps({str(k): v for k, v in next_offsets.items()}),
            K_WATERMARK: str(t_new),
            K_LATENESS: str(self.cfg.lateness_window_ms),
            K_PART_MAX: json.dumps({str(k): v for k, v in part_max.items()}),
            K_BATCH_N: str(len(events)),
            K_SEQ: str(seq),
        }

        arrow_tbl = _arrow_batch(self.arrow_schema, events)
        # append() writes the data file then atomically swaps the catalog pointer
        # to the new snapshot carrying snapshot_properties. Offsets + data commit together.
        self.tbl.append(arrow_tbl, snapshot_properties=snapshot_properties)

        # only advance in-memory state AFTER the commit returns
        self.state.offsets = next_offsets
        self.state.per_partition_max_ms = part_max
        self.state.watermark_t_ms = t_new
        self.state.commit_seq = seq

    def run(self, idle_exit_ms: int = 0, max_batches: int = 0) -> int:
        """Consume and commit micro-batches. Returns number of batches committed.

        ``idle_exit_ms`` > 0: exit cleanly after this long with no new messages
        (used by tests to run the job to completion). ``max_batches`` > 0: stop
        after that many commits.
        """
        batch: list[Record] = []
        last_flush = time.time()
        last_msg = time.time()
        committed = 0
        try:
            while True:
                msg = self.consumer.poll(0.2)
                now = time.time()
                if msg is None:
                    if batch and (now - last_flush) >= self.cfg.batch_max_seconds:
                        self._commit_batch(batch)
                        committed += 1
                        batch = []
                        last_flush = now
                        if max_batches and committed >= max_batches:
                            break
                    if idle_exit_ms and (now - last_msg) * 1000 >= idle_exit_ms:
                        if batch:
                            self._commit_batch(batch)
                            committed += 1
                            batch = []
                        break
                    continue
                if msg.error():
                    continue
                last_msg = now
                event = OrderEvent.from_json(msg.value().decode())
                batch.append(Record(msg.partition(), msg.offset(), event))
                if len(batch) >= self.cfg.batch_max_events or (now - last_flush) >= self.cfg.batch_max_seconds:
                    self._commit_batch(batch)
                    committed += 1
                    batch = []
                    last_flush = now
                    if max_batches and committed >= max_batches:
                        break
        finally:
            self.consumer.close()
        return committed


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--idle-exit-ms", type=int, default=0)
    ap.add_argument("--max-batches", type=int, default=0)
    args = ap.parse_args()
    job = LandingJob()
    n = job.run(idle_exit_ms=args.idle_exit_ms, max_batches=args.max_batches)
    print(f"committed_batches={n} watermark_t_ms={job.state.watermark_t_ms} commit_seq={job.state.commit_seq}")
