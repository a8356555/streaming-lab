"""
Real-time metrics processing job using PyFlink

Processes events in 1-second windows to provide:
- Event counts by type, country, user
- Real-time dashboards  
- Live counters and gauges

Output: Redis (hot storage) for dashboard consumption
"""
import argparse
import logging
from datetime import datetime, timedelta

from pyflink.common import WatermarkStrategy, Time, Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction, TimeWindow
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.util import Collector

from src.models.event import Event
from src.sources.kafka_source import KafkaEventSource
from src.sinks.redis_sink import RedisSink
from src.utils.metrics import MetricsCollector
from src.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


class GlobalMetricsProcessor(ProcessWindowFunction):
    """Processes global event counts and rates"""
    
    def __init__(self):
        self.last_window_count = 0
    
    def process(self, key: str, context: ProcessWindowFunction.Context, 
                elements, out: Collector):
        """Process window of events for global metrics"""
        events = list(elements)
        
        count = len(events)
        error_count = sum(1 for e in events if e.is_error())
        page_view_count = sum(1 for e in events if e.is_page_view())
        purchase_count = sum(1 for e in events if e.is_purchase())
        
        # Calculate rate change compared to previous window
        rate_change = 0.0
        if self.last_window_count > 0:
            rate_change = ((count - self.last_window_count) / self.last_window_count) * 100
        
        self.last_window_count = count
        
        metric = {
            'window_start': context.window().start,
            'window_end': context.window().end,
            'event_count': count,
            'error_count': error_count,
            'page_view_count': page_view_count,
            'purchase_count': purchase_count,
            'rate_change_percent': rate_change,
            'processing_time': datetime.now().isoformat()
        }
        
        out.collect(metric)
        
        logger.debug(f"Global metrics: {count} events, {error_count} errors, "
                    f"rate change: {rate_change:.2f}%")


class EventTypeMetricsProcessor(ProcessWindowFunction):
    """Processes event counts by event type"""
    
    def process(self, key: str, context: ProcessWindowFunction.Context,
                elements, out: Collector):
        """Process window of events by event type"""
        events = list(elements)
        
        unique_users = set()
        unique_countries = set()
        
        for event in events:
            if event.has_user():
                unique_users.add(event.user_id)
            if event.country:
                unique_countries.add(event.country)
        
        metric = {
            'event_type': key,
            'window_start': context.window().start,
            'window_end': context.window().end,
            'event_count': len(events),
            'unique_users': len(unique_users),
            'unique_countries': len(unique_countries),
            'processing_time': datetime.now().isoformat()
        }
        
        out.collect(metric)


class CountryMetricsProcessor(ProcessWindowFunction):
    """Processes event counts by country"""
    
    def process(self, key: str, context: ProcessWindowFunction.Context,
                elements, out: Collector):
        """Process window of events by country"""
        events = list(events)
        
        unique_users = set()
        event_type_breakdown = {}
        
        for event in events:
            if event.has_user():
                unique_users.add(event.user_id)
            
            event_type = event.event_type
            event_type_breakdown[event_type] = event_type_breakdown.get(event_type, 0) + 1
        
        metric = {
            'country': key,
            'window_start': context.window().start,
            'window_end': context.window().end,
            'event_count': len(events),
            'unique_users': len(unique_users),
            'event_type_breakdown': event_type_breakdown,
            'processing_time': datetime.now().isoformat()
        }
        
        out.collect(metric)


class ActiveUsersProcessor(ProcessWindowFunction):
    """Tracks active users in 1-minute windows"""
    
    def process(self, key: str, context: ProcessWindowFunction.Context,
                elements, out: Collector):
        """Process window for active users metrics"""
        events = list(elements)
        
        active_users = set()
        active_sessions = set()
        
        for event in events:
            if event.has_user():
                active_users.add(event.user_id)
            if event.has_session():
                active_sessions.add(event.session_id)
        
        metric = {
            'window_start': context.window().start,
            'window_end': context.window().end,
            'active_users': len(active_users),
            'active_sessions': len(active_sessions),
            'processing_time': datetime.now().isoformat()
        }
        
        out.collect(metric)


class PageViewMetricsProcessor(ProcessWindowFunction):
    """Processes page view metrics and popular URLs"""
    
    def process(self, key: str, context: ProcessWindowFunction.Context,
                elements, out: Collector):
        """Process window for page view metrics"""
        events = list(elements)
        
        url_counts = {}
        unique_viewers = set()
        
        for event in events:
            if event.url:
                url_counts[event.url] = url_counts.get(event.url, 0) + 1
            if event.has_user():
                unique_viewers.add(event.user_id)
        
        # Get top 10 URLs
        top_urls = dict(sorted(url_counts.items(), 
                              key=lambda x: x[1], reverse=True)[:10])
        
        metric = {
            'window_start': context.window().start,
            'window_end': context.window().end,
            'total_page_views': len(events),
            'unique_viewers': len(unique_viewers),
            'top_urls': top_urls,
            'processing_time': datetime.now().isoformat()
        }
        
        out.collect(metric)


class ErrorRateProcessor(ProcessWindowFunction):
    """Monitors error rates and patterns"""
    
    def process(self, key: str, context: ProcessWindowFunction.Context,
                elements, out: Collector):
        """Process window for error metrics"""
        events = list(elements)
        
        error_types = {}
        affected_users = set()
        
        for event in events:
            error_name = event.event_name or 'unknown'
            error_types[error_name] = error_types.get(error_name, 0) + 1
            
            if event.has_user():
                affected_users.add(event.user_id)
        
        metric = {
            'window_start': context.window().start,
            'window_end': context.window().end,
            'error_count': len(events),
            'affected_users': len(affected_users),
            'error_types': error_types,
            'processing_time': datetime.now().isoformat()
        }
        
        out.collect(metric)


def create_realtime_metrics_job(env: StreamExecutionEnvironment, args):
    """Create and configure the realtime metrics job"""
    
    # Configure watermark strategy for event time processing
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(lambda event, timestamp: event.get_event_time_millis()) \
        .with_idleness(Duration.of_minutes(1))
    
    # Source: Kafka events
    kafka_source = KafkaEventSource(
        bootstrap_servers=args.kafka_servers,
        topics=['raw-events'],
        group_id='realtime-metrics',
        value_deserializer='json'
    )
    
    events = env.add_source(kafka_source) \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .name("kafka-events-source") \
        .uid("kafka-events-source")
    
    # Setup Redis sinks
    redis_sink = RedisSink(cluster_nodes=args.redis_nodes.split(','))
    
    # Overall event counts per second
    events \
        .key_by(lambda event: "global") \
        .window(TumblingEventTimeWindows.of(Time.seconds(1))) \
        .process(GlobalMetricsProcessor()) \
        .name("global-metrics-processor") \
        .uid("global-metrics-processor") \
        .add_sink(redis_sink.create_global_metrics_sink()) \
        .name("redis-global-metrics-sink") \
        .uid("redis-global-metrics-sink")
    
    # Event counts by type
    events \
        .key_by(lambda event: event.event_type) \
        .window(TumblingEventTimeWindows.of(Time.seconds(1))) \
        .process(EventTypeMetricsProcessor()) \
        .name("event-type-metrics-processor") \
        .uid("event-type-metrics-processor") \
        .add_sink(redis_sink.create_event_type_metrics_sink()) \
        .name("redis-event-type-metrics-sink") \
        .uid("redis-event-type-metrics-sink")
    
    # Event counts by country
    events \
        .filter(lambda event: event.country is not None) \
        .key_by(lambda event: event.country) \
        .window(TumblingEventTimeWindows.of(Time.seconds(1))) \
        .process(CountryMetricsProcessor()) \
        .name("country-metrics-processor") \
        .uid("country-metrics-processor") \
        .add_sink(redis_sink.create_country_metrics_sink()) \
        .name("redis-country-metrics-sink") \
        .uid("redis-country-metrics-sink")
    
    # Active users per minute
    events \
        .filter(lambda event: event.has_user()) \
        .key_by(lambda event: "active_users") \
        .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
        .process(ActiveUsersProcessor()) \
        .name("active-users-processor") \
        .uid("active-users-processor") \
        .add_sink(redis_sink.create_active_users_sink()) \
        .name("redis-active-users-sink") \
        .uid("redis-active-users-sink")
    
    # Page view metrics
    events \
        .filter(lambda event: event.is_page_view() and event.url is not None) \
        .key_by(lambda event: "page_views") \
        .window(TumblingEventTimeWindows.of(Time.seconds(1))) \
        .process(PageViewMetricsProcessor()) \
        .name("page-view-metrics-processor") \
        .uid("page-view-metrics-processor") \
        .add_sink(redis_sink.create_page_view_metrics_sink()) \
        .name("redis-page-view-metrics-sink") \
        .uid("redis-page-view-metrics-sink")
    
    # Error rate monitoring
    events \
        .filter(lambda event: event.is_error()) \
        .key_by(lambda event: "error_rate") \
        .window(TumblingEventTimeWindows.of(Time.seconds(1))) \
        .process(ErrorRateProcessor()) \
        .name("error-rate-processor") \
        .uid("error-rate-processor") \
        .add_sink(redis_sink.create_error_metrics_sink()) \
        .name("redis-error-metrics-sink") \
        .uid("redis-error-metrics-sink")
    
    logger.info("Realtime Metrics Job topology configured")


def main():
    """Main entry point for realtime metrics job"""
    setup_logging()
    
    parser = argparse.ArgumentParser(description='Realtime Metrics Processing Job')
    parser.add_argument('--kafka-servers', required=True, 
                       help='Kafka bootstrap servers')
    parser.add_argument('--redis-nodes', required=True,
                       help='Redis cluster nodes (comma-separated)')
    parser.add_argument('--parallelism', type=int, default=4,
                       help='Job parallelism')
    parser.add_argument('--checkpoint-interval', type=int, default=10000,
                       help='Checkpoint interval in milliseconds')
    
    args = parser.parse_args()
    
    # Create Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Configure exactly-once processing
    env.enable_checkpointing(args.checkpoint_interval)
    env.get_checkpoint_config().set_checkpointing_mode(
        CheckpointingMode.EXACTLY_ONCE
    )
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    
    # Set parallelism
    env.set_parallelism(args.parallelism)
    
    # Initialize metrics collection
    metrics_collector = MetricsCollector()
    
    # Create job
    create_realtime_metrics_job(env, args)
    
    # Execute job
    logger.info(f"Starting Realtime Metrics Job with parallelism {args.parallelism}")
    env.execute("Realtime Metrics Job - PyFlink")


if __name__ == "__main__":
    main()