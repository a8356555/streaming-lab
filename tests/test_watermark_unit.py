"""Pure unit tests for the watermark function (no infrastructure)."""
from __future__ import annotations

from streaming_lab.lake.watermark import compute_watermark

W = 1000


def test_min_across_partitions_not_max():
    # p0 far ahead, p1 behind. A global-max watermark would wrongly jump to
    # 10000-W; the correct per-partition-min watermark is bounded by the slow p1.
    t = compute_watermark({0: 10_000, 1: 2_000}, W, expected_partitions=2)
    assert t == 2_000 - W


def test_monotonic_non_decreasing():
    t = compute_watermark({0: 5_000, 1: 5_000}, W, previous_t_ms=9_999, expected_partitions=2)
    assert t == 9_999  # never goes backward


def test_unseen_partition_holds_watermark():
    # only 1 of 2 expected partitions has reported -> cannot advance past previous
    t = compute_watermark({0: 10_000}, W, previous_t_ms=500, expected_partitions=2)
    assert t == 500


def test_advances_when_all_partitions_present():
    t = compute_watermark({0: 8_000, 1: 9_000, 2: 8_500}, W, expected_partitions=3)
    assert t == 8_000 - W
