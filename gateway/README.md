# gateway — optional HTTP ingest (reused, not on the Phase 1 correctness path)

`async_kafka_producer.py` and `events_api_original.py` are the FastAPI gateway +
idempotent async Kafka producer reused from the 2023 system-design repo. Phase 1
correctness tests drive `event-gen -> Redpanda` directly (deterministic, no HTTP
flakiness). This gateway is kept for the Phase 3 k6 load test of the ingest path.
