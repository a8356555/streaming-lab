"""Seam query: Iceberg(event_time < T) UNION ClickHouse(event_time >= T).

The whole point of the seam: read the safe watermark T from the latest Iceberg
snapshot ONCE, then split the two layers on that single T so every event is
counted exactly once -- the lake is authoritative below T, ClickHouse at/above T
(SPEC db-warehouse §3.1: "same query must use the same T; take it first, then
compose").
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

import duckdb
from pyiceberg.expressions import LessThan

from streaming_lab.config import CONFIG, Config
from streaming_lab.lake.landing_job import K_WATERMARK, _summary_props
from streaming_lab.realtime.clickhouse import seam_side as ch_seam_side


@dataclass(frozen=True)
class SeamResult:
    watermark_t_ms: int
    count: int
    gmv: Decimal
    lake_count: int
    ch_count: int
    lake_gmv: Decimal
    ch_gmv: Decimal


def read_watermark_t(catalog, cfg: Config = CONFIG) -> int:
    tbl = catalog.load_table(cfg.orders_identifier)
    props = _summary_props(tbl.current_snapshot())
    return int(props.get(K_WATERMARK, "0"))


def lake_side(catalog, t_ms: int, cfg: Config = CONFIG) -> tuple[int, Decimal]:
    """Return (count, gmv) for the lake-authoritative side: event_time < T."""
    tbl = catalog.load_table(cfg.orders_identifier)
    t_iso = datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc).isoformat()
    arrow_tbl = tbl.scan(row_filter=LessThan("event_time", t_iso)).to_arrow()
    con = duckdb.connect()
    con.register("lake_arrow", arrow_tbl)
    row = con.execute(
        "SELECT count(*), "
        "sum(CASE WHEN event_type='order_created' THEN amount ELSE -amount END) "
        "FROM lake_arrow"
    ).fetchone()
    con.close()
    count = int(row[0])
    gmv = Decimal(str(row[1])) if row[1] is not None else Decimal(0)
    return count, gmv


def seam_query(catalog, ch_client, cfg: Config = CONFIG) -> SeamResult:
    t_ms = read_watermark_t(catalog, cfg)  # take T once
    lake_count, lake_gmv = lake_side(catalog, t_ms, cfg)
    ch_count, ch_gmv = ch_seam_side(ch_client, t_ms, cfg)
    return SeamResult(
        watermark_t_ms=t_ms,
        count=lake_count + ch_count,
        gmv=lake_gmv + ch_gmv,
        lake_count=lake_count,
        ch_count=ch_count,
        lake_gmv=lake_gmv,
        ch_gmv=ch_gmv,
    )
