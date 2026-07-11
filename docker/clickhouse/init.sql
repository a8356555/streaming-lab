-- ClickHouse real-time layer: Kafka engine (streaming consumer) + MV trigger.
-- Auto-run on first container start via docker-entrypoint-initdb.d.
-- Mechanism (SPEC db-warehouse §2.3): the Kafka table is a resident consumer,
-- not a poller; each accumulated block triggers the MV SELECT into MergeTree.

CREATE DATABASE IF NOT EXISTS lake;

-- The pipe (not storage): resident consumer pulling JSONEachRow from Redpanda.
CREATE TABLE IF NOT EXISTS lake.orders_queue
(
    event_id      String,
    order_id      String,
    event_type    String,
    amount        Decimal(18, 2),
    user_id       Int64,
    event_time_ms Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'orders',
    kafka_group_name = 'ch_orders_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

-- Target storage table.
CREATE TABLE IF NOT EXISTS lake.orders_ch
(
    event_id   String,
    order_id   String,
    event_type String,
    amount     Decimal(18, 2),
    user_id    Int64,
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (event_time, event_id);

-- Trigger (not a schedule): each Kafka block flush runs this SELECT into the target.
CREATE MATERIALIZED VIEW IF NOT EXISTS lake.orders_mv TO lake.orders_ch AS
SELECT
    event_id,
    order_id,
    event_type,
    amount,
    user_id,
    toDateTime64(event_time_ms / 1000.0, 3, 'UTC') AS event_time
FROM lake.orders_queue;
