"""
Analytics aggregation job using PyFlink Table API for complex SQL-based analytics

Demonstrates unified batch/stream processing:
- Same code works for both streaming and bounded batch data
- SQL-based complex analytics  
- Pre-aggregations for ClickHouse analytical queries

Output: ClickHouse (warm storage) for business intelligence
"""
import argparse
import logging
from datetime import datetime

from pyflink.common import WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

from src.models.event import Event
from src.sources.kafka_source import KafkaEventSource, create_kafka_table_source
from src.sinks.clickhouse_sink import ClickHouseSink
from src.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


def create_analytics_aggregation_job(env: StreamExecutionEnvironment, args):
    """Create and configure the analytics aggregation job"""
    
    # Create Table Environment for SQL processing
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, settings)
    
    # Configure environment for exactly-once processing
    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.mode", "EXACTLY_ONCE"
    )
    table_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval", "30s"
    )
    
    # Support both streaming and batch mode
    processing_mode = args.processing_mode
    
    if processing_mode == "batch":
        # Batch processing mode - process historical data
        logger.info("Running in BATCH mode")
        start_timestamp = args.batch_start_timestamp or 0
        end_timestamp = args.batch_end_timestamp or int(datetime.now().timestamp() * 1000)
        
        # Create bounded Kafka source
        kafka_source = KafkaEventSource.create_bounded(
            bootstrap_servers=args.kafka_servers,
            topics=['raw-events'],
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )
        
        # Convert to table
        events_table = create_kafka_table_source(
            table_env, args.kafka_servers, 'raw-events', f'analytics-batch-{start_timestamp}'
        )
    else:
        # Streaming processing mode - process real-time data
        logger.info("Running in STREAMING mode")
        
        # Create Kafka table source for SQL processing
        events_table = create_kafka_table_source(
            table_env, args.kafka_servers, 'raw-events', 'analytics-aggregation'
        )
    
    # Setup ClickHouse sink
    clickhouse_sink = ClickHouseSink(host=args.clickhouse_host)
    
    # Execute various analytical aggregations
    execute_hourly_aggregations(table_env, events_table, clickhouse_sink)
    execute_daily_aggregations(table_env, events_table, clickhouse_sink)
    execute_user_behavior_analytics(table_env, events_table, clickhouse_sink)
    execute_conversion_funnel_analysis(table_env, events_table, clickhouse_sink)
    execute_geographic_analysis(table_env, events_table, clickhouse_sink)
    
    logger.info("Analytics Aggregation Job topology configured")


def execute_hourly_aggregations(table_env, events_table, clickhouse_sink):
    """Execute hourly aggregations using SQL"""
    logger.info("Setting up hourly aggregations")
    
    # Hourly event counts by type and country
    hourly_stats_query = """
        SELECT 
            DATE_FORMAT(TUMBLE_START(event_time, INTERVAL '1' HOUR), 'yyyy-MM-dd HH:00:00') as hour_window,
            event_type,
            country,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(DISTINCT session_id) as unique_sessions,
            TUMBLE_END(event_time, INTERVAL '1' HOUR) as window_end
        FROM kafka_events 
        WHERE country IS NOT NULL
        GROUP BY 
            TUMBLE(event_time, INTERVAL '1' HOUR),
            event_type, 
            country
    """
    
    hourly_stats = table_env.sql_query(hourly_stats_query)
    
    # Convert to DataStream and sink to ClickHouse
    hourly_stats_stream = table_env.to_append_stream(hourly_stats)
    hourly_stats_stream.add_sink(clickhouse_sink.create_hourly_stats_sink()) \
        .name("clickhouse-hourly-stats-sink") \
        .uid("clickhouse-hourly-stats-sink")
    
    # Hourly page view analytics
    hourly_pageviews_query = """
        SELECT 
            DATE_FORMAT(TUMBLE_START(event_time, INTERVAL '1' HOUR), 'yyyy-MM-dd HH:00:00') as hour_window,
            url,
            COUNT(*) as page_views,
            COUNT(DISTINCT user_id) as unique_visitors,
            TUMBLE_END(event_time, INTERVAL '1' HOUR) as window_end
        FROM kafka_events 
        WHERE event_type = 'page_view' AND url IS NOT NULL
        GROUP BY 
            TUMBLE(event_time, INTERVAL '1' HOUR),
            url
        HAVING COUNT(*) >= 10
    """
    
    hourly_pageviews = table_env.sql_query(hourly_pageviews_query)
    hourly_pageviews_stream = table_env.to_append_stream(hourly_pageviews)
    hourly_pageviews_stream.add_sink(clickhouse_sink.create_pageview_stats_sink()) \
        .name("clickhouse-pageview-stats-sink") \
        .uid("clickhouse-pageview-stats-sink")


def execute_daily_aggregations(table_env, events_table, clickhouse_sink):
    """Execute daily aggregations"""
    logger.info("Setting up daily aggregations")
    
    # Daily user engagement metrics
    daily_user_metrics_query = """
        SELECT 
            DATE_FORMAT(TUMBLE_START(event_time, INTERVAL '1' DAY), 'yyyy-MM-dd') as date_partition,
            user_id,
            COUNT(*) as total_events,
            SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as page_views,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
            COUNT(DISTINCT session_id) as sessions,
            COUNT(DISTINCT url) as unique_pages_visited,
            MIN(event_time) as first_event_time,
            MAX(event_time) as last_event_time,
            TUMBLE_END(event_time, INTERVAL '1' DAY) as window_end
        FROM kafka_events 
        WHERE user_id IS NOT NULL
        GROUP BY 
            TUMBLE(event_time, INTERVAL '1' DAY),
            user_id
    """
    
    daily_user_metrics = table_env.sql_query(daily_user_metrics_query)
    daily_user_metrics_stream = table_env.to_append_stream(daily_user_metrics)
    daily_user_metrics_stream.add_sink(clickhouse_sink.create_user_metrics_sink()) \
        .name("clickhouse-daily-user-metrics-sink") \
        .uid("clickhouse-daily-user-metrics-sink")
    
    # Daily cohort analysis
    daily_cohorts_query = """
        SELECT 
            DATE_FORMAT(TUMBLE_START(event_time, INTERVAL '1' DAY), 'yyyy-MM-dd') as date_partition,
            country,
            event_type,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(*) as total_events,
            AVG(COUNT(*)) OVER (
                PARTITION BY country, event_type 
                ORDER BY TUMBLE_START(event_time, INTERVAL '1' DAY) 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as seven_day_avg_events,
            TUMBLE_END(event_time, INTERVAL '1' DAY) as window_end
        FROM kafka_events 
        WHERE country IS NOT NULL AND event_type IS NOT NULL
        GROUP BY 
            TUMBLE(event_time, INTERVAL '1' DAY),
            country,
            event_type
    """
    
    daily_cohorts = table_env.sql_query(daily_cohorts_query)
    daily_cohorts_stream = table_env.to_append_stream(daily_cohorts)
    daily_cohorts_stream.add_sink(clickhouse_sink.create_cohort_stats_sink()) \
        .name("clickhouse-cohort-stats-sink") \
        .uid("clickhouse-cohort-stats-sink")


def execute_user_behavior_analytics(table_env, events_table, clickhouse_sink):
    """Execute user behavior analytics"""
    logger.info("Setting up user behavior analytics")
    
    # User journey analysis with session-like grouping
    user_journeys_query = """
        SELECT 
            session_id,
            user_id,
            COUNT(*) as events_in_session,
            SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as page_views,
            MIN(event_time) as session_start,
            MAX(event_time) as session_end,
            EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) as session_duration_seconds,
            FIRST_VALUE(url IGNORE NULLS) OVER (
                PARTITION BY session_id 
                ORDER BY event_time 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as landing_page,
            LAST_VALUE(url IGNORE NULLS) OVER (
                PARTITION BY session_id 
                ORDER BY event_time 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as exit_page,
            CASE WHEN SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) <= 1 
                 THEN TRUE ELSE FALSE END as is_bounce
        FROM kafka_events 
        WHERE session_id IS NOT NULL
        GROUP BY session_id, user_id
        HAVING COUNT(*) >= 2
    """
    
    user_journeys = table_env.sql_query(user_journeys_query)
    user_journeys_stream = table_env.to_append_stream(user_journeys)
    user_journeys_stream.add_sink(clickhouse_sink.create_user_journeys_sink()) \
        .name("clickhouse-user-journeys-sink") \
        .uid("clickhouse-user-journeys-sink")


def execute_conversion_funnel_analysis(table_env, events_table, clickhouse_sink):
    """Execute conversion funnel analysis"""
    logger.info("Setting up conversion funnel analysis")
    
    # E-commerce funnel analysis
    conversion_funnel_query = """
        WITH user_funnel_steps AS (
            SELECT 
                user_id,
                DATE_FORMAT(event_time, 'yyyy-MM-dd') as date_partition,
                MAX(CASE WHEN event_type = 'page_view' AND url LIKE '%/product/%' THEN 1 ELSE 0 END) as viewed_product,
                MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as added_to_cart,
                MAX(CASE WHEN event_type = 'checkout' THEN 1 ELSE 0 END) as started_checkout,
                MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as completed_purchase
            FROM kafka_events 
            WHERE user_id IS NOT NULL
            GROUP BY user_id, DATE_FORMAT(event_time, 'yyyy-MM-dd')
        )
        SELECT 
            date_partition,
            COUNT(DISTINCT user_id) as total_users,
            SUM(viewed_product) as users_viewed_product,
            SUM(added_to_cart) as users_added_to_cart,
            SUM(started_checkout) as users_started_checkout,
            SUM(completed_purchase) as users_completed_purchase,
            CASE WHEN SUM(viewed_product) > 0 
                 THEN CAST(SUM(added_to_cart) AS DOUBLE) / SUM(viewed_product) 
                 ELSE 0 END as product_to_cart_rate,
            CASE WHEN SUM(added_to_cart) > 0 
                 THEN CAST(SUM(started_checkout) AS DOUBLE) / SUM(added_to_cart) 
                 ELSE 0 END as cart_to_checkout_rate,
            CASE WHEN SUM(started_checkout) > 0 
                 THEN CAST(SUM(completed_purchase) AS DOUBLE) / SUM(started_checkout) 
                 ELSE 0 END as checkout_to_purchase_rate
        FROM user_funnel_steps
        GROUP BY date_partition
    """
    
    conversion_funnel = table_env.sql_query(conversion_funnel_query)
    conversion_funnel_stream = table_env.to_append_stream(conversion_funnel)
    conversion_funnel_stream.add_sink(clickhouse_sink.create_conversion_funnel_sink()) \
        .name("clickhouse-conversion-funnel-sink") \
        .uid("clickhouse-conversion-funnel-sink")


def execute_geographic_analysis(table_env, events_table, clickhouse_sink):
    """Execute geographic analysis"""
    logger.info("Setting up geographic analysis")
    
    # Geographic performance analysis with sliding windows
    geo_analysis_query = """
        SELECT 
            country,
            city,
            DATE_FORMAT(HOP_START(event_time, INTERVAL '1' HOUR, INTERVAL '24' HOUR), 'yyyy-MM-dd HH:00:00') as window_start,
            COUNT(*) as total_events,
            COUNT(DISTINCT user_id) as unique_users,
            AVG(CASE WHEN event_type = 'purchase' THEN 1.0 ELSE 0.0 END) as purchase_rate,
            SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) as error_count,
            CASE WHEN COUNT(*) > 0 
                 THEN CAST(SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) 
                 ELSE 0 END as error_rate,
            HOP_END(event_time, INTERVAL '1' HOUR, INTERVAL '24' HOUR) as window_end
        FROM kafka_events 
        WHERE country IS NOT NULL
        GROUP BY 
            HOP(event_time, INTERVAL '1' HOUR, INTERVAL '24' HOUR),
            country,
            city
        HAVING COUNT(*) >= 100
    """
    
    geo_analysis = table_env.sql_query(geo_analysis_query)
    geo_analysis_stream = table_env.to_append_stream(geo_analysis)
    geo_analysis_stream.add_sink(clickhouse_sink.create_geo_analytics_sink()) \
        .name("clickhouse-geo-analytics-sink") \
        .uid("clickhouse-geo-analytics-sink")


def main():
    """Main entry point for analytics aggregation job"""
    setup_logging()
    
    parser = argparse.ArgumentParser(description='Analytics Aggregation Job')
    parser.add_argument('--kafka-servers', required=True,
                       help='Kafka bootstrap servers')
    parser.add_argument('--clickhouse-host', required=True,
                       help='ClickHouse host')
    parser.add_argument('--processing-mode', choices=['streaming', 'batch'], default='streaming',
                       help='Processing mode: streaming or batch')
    parser.add_argument('--batch-start-timestamp', type=int,
                       help='Batch processing start timestamp (milliseconds)')
    parser.add_argument('--batch-end-timestamp', type=int,
                       help='Batch processing end timestamp (milliseconds)')
    parser.add_argument('--parallelism', type=int, default=6,
                       help='Job parallelism')
    parser.add_argument('--checkpoint-interval', type=int, default=30000,
                       help='Checkpoint interval in milliseconds')
    
    args = parser.parse_args()
    
    # Create Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Configure exactly-once processing
    env.enable_checkpointing(args.checkpoint_interval)
    env.get_checkpoint_config().set_checkpointing_mode(
        CheckpointingMode.EXACTLY_ONCE
    )
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10000)
    env.get_checkpoint_config().set_checkpoint_timeout(120000)
    
    # Set parallelism
    env.set_parallelism(args.parallelism)
    
    # Create job
    create_analytics_aggregation_job(env, args)
    
    # Execute job
    logger.info(f"Starting Analytics Aggregation Job ({args.processing_mode} mode) "
                f"with parallelism {args.parallelism}")
    env.execute(f"Analytics Aggregation Job - PyFlink ({args.processing_mode})")


if __name__ == "__main__":
    main()