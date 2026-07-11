# naive_mode — the Phase 2 antagonist (do NOT fix)

`hybrid_source_of_truth.py` and `multi_tier_storage.py` are copied **unmodified**
from the 2023 `system-design/data-intensive-app` repo.

`HybridSourceOfTruth.store_event_with_durability` writes Kafka, then cold
storage, then relies on stream processing — three sequential writes with **no
atomic coordination**, and it swallows errors into a partial-success dict. That
is the dual-write bug this lab exists to expose.

Phase 2 runs the same chaos suite against this naive path (offsets in the
consumer group, dual-write) and against the correct path (offsets in the Iceberg
snapshot) to show *where* and *by how much* the naive version produces wrong
numbers. Nothing here should be "fixed" — it is preserved as a foil.
