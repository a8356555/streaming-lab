"""Bootstrap the canonical demo pipeline: topic + Iceberg table.

CH's canonical tables come from docker/clickhouse/init.sql at container start.
This creates the canonical `orders` topic and the `lake.orders` Iceberg table
so a live demo (event-gen -> landing job -> seam query on canonical names) works.
Tests do NOT use these; they provision isolated resources per test.
"""
from __future__ import annotations

from streaming_lab.config import CONFIG
from streaming_lab.events.admin import create_topic
from streaming_lab.lake.catalog import create_orders_table


def main() -> None:
    create_topic(CONFIG)
    print(f"topic {CONFIG.topic} ready ({CONFIG.num_partitions} partitions)")
    tbl = create_orders_table(CONFIG)
    print(f"iceberg table {tbl.name()} ready at {tbl.location()}")


if __name__ == "__main__":
    main()
