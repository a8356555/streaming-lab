"""Kafka topic admin (create/delete with an explicit partition count).

Topics are created explicitly (broker auto-create is disabled) so the partition
count is deterministic -- multi-partition offset tracking is the whole point of
the landing job.
"""
from __future__ import annotations

import time

from confluent_kafka.admin import AdminClient, NewTopic

from streaming_lab.config import CONFIG, Config


def _admin(cfg: Config) -> AdminClient:
    return AdminClient({"bootstrap.servers": cfg.kafka_bootstrap})


def create_topic(cfg: Config = CONFIG, partitions: int | None = None) -> None:
    admin = _admin(cfg)
    n = partitions or cfg.num_partitions
    fs = admin.create_topics([NewTopic(cfg.topic, num_partitions=n, replication_factor=1)])
    for topic, f in fs.items():
        try:
            f.result()
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise


def delete_topic(cfg: Config = CONFIG, wait_s: float = 5.0) -> None:
    admin = _admin(cfg)
    fs = admin.delete_topics([cfg.topic], operation_timeout=wait_s)
    for topic, f in fs.items():
        try:
            f.result()
        except Exception:
            pass
    time.sleep(1.0)  # let metadata propagate before a same-name recreate


if __name__ == "__main__":
    create_topic()
    print(f"topic {CONFIG.topic} ready ({CONFIG.num_partitions} partitions)")
