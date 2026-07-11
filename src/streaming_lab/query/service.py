"""Seam query CLI. ``--once`` prints the current seam GMV as JSON.

An HTTP endpoint (GET /gmv) is deferred to Phase 3 (it is needed for the k6 load
test, not for Phase 1 correctness). Keeping this CLI dependency-free means the
core requirements stay minimal.
"""
from __future__ import annotations

import json

from streaming_lab.config import CONFIG
from streaming_lab.lake.catalog import get_catalog
from streaming_lab.realtime.clickhouse import get_ch_client
from streaming_lab.query.seam import seam_query


def run_once() -> dict:
    catalog = get_catalog(CONFIG)
    ch = get_ch_client(CONFIG)
    r = seam_query(catalog, ch, CONFIG)
    return {
        "watermark_t_ms": r.watermark_t_ms,
        "count": r.count,
        "gmv": str(r.gmv),
        "lake_count": r.lake_count,
        "ch_count": r.ch_count,
        "lake_gmv": str(r.lake_gmv),
        "ch_gmv": str(r.ch_gmv),
    }


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="print current seam result and exit")
    ap.parse_args()
    print(json.dumps(run_once(), indent=2))
