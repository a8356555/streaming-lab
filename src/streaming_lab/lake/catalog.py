"""Iceberg catalog wiring (pyiceberg SqlCatalog on MinIO/S3).

SqlCatalog keeps the current-metadata pointer in SQLite and the data + metadata
files in MinIO. The commit that swaps the pointer is a single SQLite transaction
-> atomic. That atomic pointer swap is what makes "offsets + data in one
snapshot" a real transaction (see ADR-001).
"""
from __future__ import annotations

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    DecimalType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from streaming_lab.config import CONFIG, Config

# Field ids are stable identifiers; keep them fixed across the repo's life.
ORDERS_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "order_id", StringType(), required=True),
    NestedField(3, "event_type", StringType(), required=True),
    NestedField(4, "amount", DecimalType(18, 2), required=True),
    NestedField(5, "user_id", LongType(), required=False),
    NestedField(6, "event_time", TimestamptzType(), required=True),
)

ORDERS_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=6, field_id=1000, transform=DayTransform(), name="event_time_day")
)


def get_catalog(cfg: Config = CONFIG) -> SqlCatalog:
    return SqlCatalog(
        cfg.catalog_name,
        **{
            "uri": cfg.catalog_uri,
            "warehouse": cfg.warehouse,
            "s3.endpoint": cfg.s3_endpoint,
            "s3.access-key-id": cfg.s3_access_key,
            "s3.secret-access-key": cfg.s3_secret_key,
            "s3.region": cfg.s3_region,
            "s3.path-style-access": "true",
        },
    )


def create_orders_table(cfg: Config = CONFIG, drop_existing: bool = False) -> Table:
    catalog = get_catalog(cfg)
    try:
        catalog.create_namespace(cfg.lake_namespace)
    except Exception:
        pass  # already exists
    if drop_existing:
        try:
            catalog.drop_table(cfg.orders_identifier)
        except Exception:
            pass
    try:
        return catalog.create_table(
            cfg.orders_identifier,
            schema=ORDERS_SCHEMA,
            partition_spec=ORDERS_PARTITION_SPEC,
        )
    except Exception:
        return catalog.load_table(cfg.orders_identifier)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--drop", action="store_true", help="drop and recreate the table")
    args = ap.parse_args()
    tbl = create_orders_table(drop_existing=args.drop)
    print(f"orders table ready: {tbl.name()}  location={tbl.location()}")
