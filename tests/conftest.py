"""Integration test harness. Runs INSIDE the app container (services by name).

Each test gets fully isolated resources (unique topic, Iceberg table, ClickHouse
pipeline, ground-truth file) so tests never share global state -- the usual cause
of integration-test flakiness.
"""
from __future__ import annotations

import dataclasses
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest
from pyiceberg.expressions import LessThan

from streaming_lab.config import CONFIG, Config
from streaming_lab.events import admin
from streaming_lab.lake.catalog import create_orders_table, get_catalog
from streaming_lab.realtime import clickhouse as ch


@dataclass
class Env:
    cfg: Config
    catalog: object
    ch_client: object


@pytest.fixture
def env():
    suffix = uuid.uuid4().hex[:8]
    cfg = dataclasses.replace(
        CONFIG,
        topic=f"orders_{suffix}",
        orders_table=f"orders_{suffix}",
        ch_orders_table=f"orders_ch_{suffix}",
        ground_truth_path=f"/data/gt_{suffix}.jsonl",
        batch_max_events=3000,
        batch_max_seconds=1.0,
    )
    admin.create_topic(cfg)
    create_orders_table(cfg, drop_existing=True)
    ch_client = ch.get_ch_client(cfg)
    ch.create_pipeline(ch_client, cfg)

    yield Env(cfg=cfg, catalog=get_catalog(cfg), ch_client=ch_client)

    ch.drop_pipeline(ch_client, cfg)
    try:
        get_catalog(cfg).drop_table(cfg.orders_identifier)
    except Exception:
        pass
    admin.delete_topic(cfg)
    try:
        os.remove(cfg.ground_truth_path)
    except OSError:
        pass


# --- lake helpers (query Iceberg directly for verification) ---

def lake_event_ids_below_t(catalog, cfg: Config, t_ms: int) -> set[str]:
    tbl = catalog.load_table(cfg.orders_identifier)
    t_iso = datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc).isoformat()
    arrow = tbl.scan(row_filter=LessThan("event_time", t_iso)).to_arrow()
    return set(arrow.column("event_id").to_pylist())


def lake_dup_stats(catalog, cfg: Config) -> tuple[int, int]:
    """Return (total_rows, distinct_event_ids) in the lake table."""
    tbl = catalog.load_table(cfg.orders_identifier)
    arrow = tbl.scan().to_arrow()
    ids = arrow.column("event_id").to_pylist()
    return len(ids), len(set(ids))


def landing_subprocess_env(cfg: Config) -> dict:
    """Env for a landing-job subprocess so its CONFIG matches the test's cfg."""
    e = dict(os.environ)
    e.update(
        {
            "ORDERS_TOPIC": cfg.topic,
            "ORDERS_TABLE": cfg.orders_table,
            "CH_ORDERS_TABLE": cfg.ch_orders_table,
            "GROUND_TRUTH_PATH": cfg.ground_truth_path,
            "BATCH_MAX_EVENTS": str(cfg.batch_max_events),
            "BATCH_MAX_SECONDS": str(cfg.batch_max_seconds),
        }
    )
    return e
