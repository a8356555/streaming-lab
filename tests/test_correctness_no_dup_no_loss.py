"""Correctness test 1: no duplication, no loss (baseline 100k events).

The seam-combined count and GMV must EXACTLY equal an independent ground truth
computed by reading the generator's JSONL directly -- never through the system
under test.
"""
from __future__ import annotations

from streaming_lab.events import generator, ground_truth
from streaming_lab.lake.landing_job import LandingJob
from streaming_lab.realtime import clickhouse as ch
from streaming_lab.query.seam import seam_query

from conftest import lake_dup_stats


def test_no_dup_no_loss(env):
    cfg = env.cfg
    gen = generator.generate_and_produce(100_000, seed=42, cfg=cfg)

    # land everything into the lake (multiple micro-batches => multiple snapshots)
    committed = LandingJob(cfg).run(idle_exit_ms=4000)
    assert committed > 1, "expected multiple micro-batch commits"

    # wait for ClickHouse (independent consumer) to catch up to all events
    seen = ch.wait_for_count(env.ch_client, gen.total, cfg, timeout_s=120)
    assert seen == gen.total

    result = seam_query(env.catalog, env.ch_client, cfg)
    gt = ground_truth.compute(cfg.ground_truth_path, result.watermark_t_ms)

    # anti-fraud: the seam genuinely split into two non-empty sides (not a
    # degenerate all-CH or all-lake query that would pass trivially).
    assert result.watermark_t_ms > 0
    assert result.lake_count > 0
    assert result.ch_count > 0

    # no loss + no duplication: exact equality on count and (Decimal) GMV.
    assert result.count == gt.count
    assert result.gmv == gt.gmv

    # Lake completeness (independent of the seam): after a full land the lake must
    # hold EVERY event, including the >=T tail. The seam alone cannot catch a lake
    # that silently drops its latest batch -- ClickHouse would backfill the >=T
    # side and the counts would still match. Assert the lake directly.
    lake_total, lake_distinct = lake_dup_stats(env.catalog, cfg)
    assert lake_total == lake_distinct  # no duplicate rows in the lake
    assert lake_total == gt.count       # lake dropped nothing, incl. the >=T tail
