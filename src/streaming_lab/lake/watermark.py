"""Safe watermark T (pure functions, unit-testable without infrastructure).

T marks the boundary: "every event with event_time < T is guaranteed already
in the lake." The seam query trusts the lake below T and ClickHouse at/above T.

Per-partition watermark, then take the min (the Flink approach). A single global
``max(event_time) - W`` is WRONG for a multi-partition source: if one partition
is consumed far ahead of another, the global max comes from the fast partition
while the slow partition still holds unconsumed low-event_time rows below that T
-> the seam would drop them. So:

    wm_p = max_event_time_consumed_in_p - W      (per partition)
    T    = max( T_prev, min_p wm_p )             (min across partitions, monotonic)

Under bounded out-of-orderness <= W *within each partition* (offset order vs
event_time), wm_p is a valid per-partition watermark, and the min is globally
safe: any unconsumed event in partition p has event_time >= max_consumed_p - W
>= wm_p >= T.

A partition with no consumed events yet contributes no bound -> T cannot advance
past T_prev (correct conservative behaviour; idle-partition liveness is an
explicit Phase 2 / ADR-002 concern).
"""
from __future__ import annotations

from typing import Mapping


def compute_watermark(
    per_partition_max_ms: Mapping[int, int],
    lateness_window_ms: int,
    previous_t_ms: int = 0,
    expected_partitions: int | None = None,
) -> int:
    """Return the new safe watermark T (epoch millis), monotonic vs previous.

    ``per_partition_max_ms`` maps partition -> max event_time committed for that
    partition. ``expected_partitions``: if given and any expected partition has
    not yet contributed a max, T cannot advance past ``previous_t_ms`` (an
    unseen partition may still deliver old events).
    """
    if expected_partitions is not None and len(per_partition_max_ms) < expected_partitions:
        return previous_t_ms
    if not per_partition_max_ms:
        return previous_t_ms
    min_max = min(per_partition_max_ms.values())
    candidate = min_max - lateness_window_ms
    return max(previous_t_ms, candidate)
