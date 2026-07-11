"""Correctness test 3: watermark T safety (steady-state).

Scope, stated honestly: this checks the STEADY-STATE property after the landing
job has fully drained the topic -- the set of events with event_time < T equals
exactly the set of events with event_time < T in the lake (all present, none
extra). It is a no-loss-below-T check at rest.

It does NOT exercise the *dynamic* invariant "T never runs ahead of what is
already landed" under live partition skew / late arrivals -- at full drain the
lake holds everything, so any T is trivially safe against the lake. The dynamic
property is currently covered only by the unit tests
(test_watermark_unit.py, per-partition-min vs global-max), and is scheduled for
end-to-end coverage by the Phase 2 chaos suite (skew + late injection). See
ADR-002 open questions.
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
