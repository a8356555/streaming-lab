"""Central configuration, env-driven.

All service endpoints, the event domain constants, and the watermark window
live here so the same code runs identically inside the ``app`` container and
(where wheels permit) on a host. No value is hardcoded at a call site.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


@dataclass(frozen=True)
class Config:
    # --- Redpanda / Kafka ---
    kafka_bootstrap: str = _env("KAFKA_BOOTSTRAP", "localhost:19092")
    topic: str = _env("ORDERS_TOPIC", "orders")
    num_partitions: int = int(_env("ORDERS_PARTITIONS", "4"))

    # --- ClickHouse ---
    ch_host: str = _env("CH_HOST", "localhost")
    ch_port: int = int(_env("CH_PORT", "18123"))
    ch_database: str = _env("CH_DATABASE", "lake")
    ch_orders_table: str = _env("CH_ORDERS_TABLE", "orders_ch")

    # --- MinIO / S3 (Iceberg warehouse) ---
    s3_endpoint: str = _env("S3_ENDPOINT", "http://localhost:19100")
    s3_access_key: str = _env("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key: str = _env("S3_SECRET_KEY", "minioadmin")
    s3_region: str = _env("S3_REGION", "us-east-1")
    bucket: str = _env("LAKE_BUCKET", "lakehouse")

    # --- Iceberg catalog (pyiceberg SqlCatalog) ---
    catalog_name: str = _env("CATALOG_NAME", "streaming_lab")
    catalog_uri: str = _env("CATALOG_URI", "sqlite:////data/catalog.db")
    lake_namespace: str = _env("LAKE_NAMESPACE", "lake")
    orders_table: str = _env("ORDERS_TABLE", "orders")

    # --- Watermark ---
    # Max out-of-orderness the generator injects; T = max_event_time - this.
    lateness_window_ms: int = int(_env("LATENESS_WINDOW_MS", "1000"))

    # --- Ground truth (independent verification path) ---
    ground_truth_path: str = _env("GROUND_TRUTH_PATH", "/data/ground_truth.jsonl")

    # --- Landing job micro-batch ---
    batch_max_events: int = int(_env("BATCH_MAX_EVENTS", "5000"))
    batch_max_seconds: float = float(_env("BATCH_MAX_SECONDS", "2.0"))

    @property
    def orders_identifier(self) -> str:
        return f"{self.lake_namespace}.{self.orders_table}"

    @property
    def warehouse(self) -> str:
        return f"s3://{self.bucket}/warehouse"


CONFIG = Config()
