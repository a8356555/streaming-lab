"""ClickHouse real-time layer client helpers.

ClickHouse consumes the same ``orders`` topic via a Kafka engine table + MV
(see docker/clickhouse/init.sql) using its OWN consumer group, fully independent
of the landing job. It is the "seconds-fresh" side of the seam: authoritative for
event_time >= T.
"""
from __future__ import annotations

import time
from decimal import Decimal

import clickhouse_connect

from streaming_lab.config import CONFIG, Config

_GMV_EXPR = "sum(if(event_type='order_created', amount, -amount))"


def get_ch_client(cfg: Config = CONFIG):
    return clickhouse_connect.get_client(
        host=cfg.ch_host, port=cfg.ch_port, database=cfg.ch_database
    )


def create_pipeline(client, cfg: Config = CONFIG) -> None:
    """Create the Kafka-engine -> MV -> MergeTree pipeline for cfg's topic/table.

    Mirrors docker/clickhouse/init.sql but with configurable names so each test
    gets an isolated pipeline (own topic, own consumer group, own target table).
    """
    target = cfg.ch_orders_table
    queue = f"{target}_queue"
    mv = f"{target}_mv"
    group = f"ch_{target}_consumer"
    client.command(f"DROP VIEW IF EXISTS {mv}")
    client.command(f"DROP TABLE IF EXISTS {queue}")
    client.command(f"DROP TABLE IF EXISTS {target}")
    client.command(
        f"""
        CREATE TABLE {queue}
        (event_id String, order_id String, event_type String,
         amount Decimal(18,2), user_id Int64, event_time_ms Int64)
        ENGINE = Kafka SETTINGS
          kafka_broker_list = '{cfg.kafka_bootstrap}',
          kafka_topic_list = '{cfg.topic}',
          kafka_group_name = '{group}',
          kafka_format = 'JSONEachRow',
          kafka_num_consumers = 1
        """
    )
    client.command(
        f"""
        CREATE TABLE {target}
        (event_id String, order_id String, event_type String,
         amount Decimal(18,2), user_id Int64, event_time DateTime64(3,'UTC'))
        ENGINE = MergeTree ORDER BY (event_time, event_id)
        """
    )
    client.command(
        f"""
        CREATE MATERIALIZED VIEW {mv} TO {target} AS
        SELECT event_id, order_id, event_type, amount, user_id,
               toDateTime64(event_time_ms/1000.0, 3, 'UTC') AS event_time
        FROM {queue}
        """
    )


def drop_pipeline(client, cfg: Config = CONFIG) -> None:
    target = cfg.ch_orders_table
    client.command(f"DROP VIEW IF EXISTS {target}_mv")
    client.command(f"DROP TABLE IF EXISTS {target}_queue")
    client.command(f"DROP TABLE IF EXISTS {target}")


def total_count(client, cfg: Config = CONFIG) -> int:
    return int(client.query(f"SELECT count() FROM {cfg.ch_orders_table}").result_rows[0][0])


def wait_for_count(
    client, expected: int, cfg: Config = CONFIG, timeout_s: float = 90.0, interval_s: float = 0.5
) -> int:
    """Poll until the target table has >= expected rows (CH caught up), or timeout."""
    deadline = time.time() + timeout_s
    seen = 0
    while time.time() < deadline:
        seen = total_count(client, cfg)
        if seen >= expected:
            return seen
        time.sleep(interval_s)
    return seen


def seam_side(client, t_ms: int, cfg: Config = CONFIG) -> tuple[int, Decimal]:
    """Return (count, gmv) for the CH-authoritative side: event_time >= T."""
    t_expr = f"toDateTime64({t_ms}/1000.0, 3, 'UTC')"
    row = client.query(
        f"SELECT count(), {_GMV_EXPR} FROM {cfg.ch_orders_table} WHERE event_time >= {t_expr}"
    ).result_rows[0]
    count = int(row[0])
    gmv = Decimal(str(row[1])) if row[1] is not None else Decimal(0)
    return count, gmv
