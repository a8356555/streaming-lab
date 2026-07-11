"""Correctness test 3: watermark T safety.

Invariant: every event with event_time < T is guaranteed already in the lake.
We assert set equality of event_ids below T between the independent ground truth
and the lake -- the lake has all of them, and none extra.
"""
from __future__ import annotations

from streaming_lab.events import generator, ground_truth
from streaming_lab.lake.landing_job import LandingJob
from streaming_lab.query.seam import read_watermark_t

from conftest import lake_event_ids_below_t


def test_watermark_below_t_is_in_lake(env):
    cfg = env.cfg
    generator.generate_and_produce(50_000, seed=99, cfg=cfg)

    LandingJob(cfg).run(idle_exit_ms=4000)

    t_ms = read_watermark_t(env.catalog, cfg)
    assert t_ms > 0

    gt = ground_truth.compute(cfg.ground_truth_path, t_ms)
    # anti-fraud: the below-T set is non-trivial, so equality is not vacuous
    # (a lazy T near 0 would make the claim empty and meaningless).
    assert gt.count_below_t > 1000

    lake_ids = lake_event_ids_below_t(env.catalog, cfg, t_ms)

    # every event with event_time < T is in the lake, and the lake adds nothing
    # spurious in that range.
    assert lake_ids == gt.event_ids_below_t
