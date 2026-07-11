"""Correctness test 2: crash-restart with a REAL SIGKILL of the landing process.

The landing job runs as an actual subprocess. We kill -9 it at random points
(before/after commits), several times, then let a final run finish. Because
offsets live inside the Iceberg snapshot, recovery resumes from the last
committed offset -- no loss, no reprocessing, no duplicate rows.
"""
from __future__ import annotations

import os
import random
import signal
import subprocess
import sys
import time

from streaming_lab.events import generator, ground_truth
from streaming_lab.lake.landing_job import read_resume_state
from streaming_lab.realtime import clickhouse as ch
from streaming_lab.query.seam import seam_query

from conftest import lake_dup_stats, landing_subprocess_env


def _spawn(cfg):
    return subprocess.Popen(
        [sys.executable, "-m", "streaming_lab.lake.landing_job", "--idle-exit-ms", "600000"],
        env=landing_subprocess_env(cfg),
    )


def test_crash_restart_no_dup_no_loss(env):
    cfg = env.cfg
    gen = generator.generate_and_produce(100_000, seed=7, cfg=cfg)
    rng = random.Random(7)

    # burst 1: run briefly, hard-kill, capture progress
    p = _spawn(cfg)
    time.sleep(rng.uniform(2.0, 3.0))
    os.kill(p.pid, signal.SIGKILL)
    p.wait()
    st_after_first = read_resume_state(cfg)

    # bursts 2..4: keep hard-killing at random points
    for _ in range(3):
        p = _spawn(cfg)
        time.sleep(rng.uniform(1.5, 3.0))
        os.kill(p.pid, signal.SIGKILL)
        p.wait()

    # final run to completion (clean idle exit)
    p = subprocess.Popen(
        [sys.executable, "-m", "streaming_lab.lake.landing_job", "--idle-exit-ms", "5000"],
        env=landing_subprocess_env(cfg),
    )
    assert p.wait() == 0

    st_final = read_resume_state(cfg)

    # recovery actually happened: commits kept advancing across restarts.
    assert st_final.commit_seq > st_after_first.commit_seq

    # no duplication: every lake row is a distinct event (atomic commit means a
    # crash between Parquet-write and commit leaves an invisible orphan, not a dup).
    total, distinct = lake_dup_stats(env.catalog, cfg)
    assert total > 0
    assert total == distinct

    # no loss: seam == independent ground truth.
    ch.wait_for_count(env.ch_client, gen.total, cfg, timeout_s=120)
    result = seam_query(env.catalog, env.ch_client, cfg)
    gt = ground_truth.compute(cfg.ground_truth_path, result.watermark_t_ms)
    assert result.lake_count > 0 and result.ch_count > 0
    assert result.count == gt.count
    assert result.gmv == gt.gmv

    # Lake completeness (independent of the seam): the final clean run drained the
    # topic, so the lake must hold every event including the >=T tail. Without this
    # a landing job that lost its last batch after the kills would still pass the
    # seam checks (ClickHouse backfills the >=T side).
    assert total == gt.count
