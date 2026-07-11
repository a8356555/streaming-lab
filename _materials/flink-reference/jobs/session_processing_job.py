"""
User session processing job using PyFlink

Tracks user behavior across sessions using session windows:
- Session duration and page views
- User journey analysis
- Bounce rate calculation
- Cross-session behavior patterns

Output: ClickHouse (warm storage) for analytics, Redis for active sessions
"""
import argparse
import logging
from datetime import datetime, timedelta
from typing import Iterable

from pyflink.common import WatermarkStrategy, Time, Duration
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction, KeyedProcessFunction
from pyflink.datastream.window import EventTimeSessionWindows, TumblingEventTimeWindows
from pyflink.util import Collector
from pyflink.common.state import ValueStateDescriptor
from pyflink.common.typeinfo import Types

from src.models.event import Event, UserSession
from src.sources.kafka_source import KafkaEventSource
from src.sinks.redis_sink import RedisSink
from src.sinks.clickhouse_sink import ClickHouseSink
from src.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


class SessionAggregator(ProcessWindowFunction):
    """Aggregates events into user sessions"""
    
    def process(self, key: int, context: ProcessWindowFunction.Context,
                elements, out: Collector):
        """Process session window to create UserSession"""
        events = list(elements)
        
        if not events:
            return
        
        # Sort events by timestamp
        events.sort(key=lambda e: e.timestamp)
        
        # Create session from events
        session = self._aggregate_events_to_session(key, events)
        
        if session:
            out.collect(session)
            logger.debug(f"Created session: {session}")
    
    def _aggregate_events_to_session(self, user_id: int, events: list) -> UserSession:
        """Aggregate events into a session"""
        if not events:
            return None
        
        start_time = events[0].timestamp
        end_time = events[-1].timestamp
        
        # Generate session_id if not present
        session_id = events[0].session_id
        if not session_id:
            session_id = f"session_{user_id}_{int(start_time.timestamp())}"
        
        # Count different event types
        page_views = [e for e in events if e.is_page_view()]
        unique_urls = set(e.url for e in page_views if e.url)
        countries = set(e.country for e in events if e.country)
        
        # Extract page sequence
        page_sequence = [e.url for e in page_views if e.url]
        
        # Determine landing and exit pages
        landing_page = page_sequence[0] if page_sequence else None
        exit_page = page_sequence[-1] if page_sequence else None
        
        # Device info extraction
        device_type, browser = self._extract_device_info(events)
        
        # Calculate duration
        duration_seconds = int((end_time - start_time).total_seconds())
        
        return UserSession(
            session_id=session_id,
            user_id=user_id,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration_seconds,
            event_count=len(events),
            page_view_count=len(page_views),
            unique_urls_count=len(unique_urls),
            countries=countries,
            landing_page=landing_page,
            exit_page=exit_page,
            page_view_sequence=page_sequence,
            device_type=device_type,
            browser=browser
        )
    
    def _extract_device_info(self, events: list) -> tuple:
        """Extract device info from events"""
        for event in events:
            if event.properties and 'user_agent' in event.properties:
                ua = event.properties['user_agent'].lower()
                
                device_type = 'desktop'
                if 'mobile' in ua or 'android' in ua or 'iphone' in ua:
                    device_type = 'mobile'
                elif 'tablet' in ua or 'ipad' in ua:
                    device_type = 'tablet'
                
                browser = 'other'
                if 'chrome' in ua:
                    browser = 'chrome'
                elif 'firefox' in ua:
                    browser = 'firefox'
                elif 'safari' in ua:
                    browser = 'safari'
                elif 'edge' in ua:
                    browser = 'edge'
                
                return device_type, browser
        
        return None, None


class DailySessionMetricsProcessor(ProcessWindowFunction):
    """Process daily session metrics per user"""
    
    def process(self, key: int, context: ProcessWindowFunction.Context,
                elements, out: Collector):
        """Process daily window of sessions for user metrics"""
        sessions = list(elements)
        
        if not sessions:
            return
        
        total_sessions = len(sessions)
        total_duration = sum(s.duration_seconds for s in sessions)
        total_page_views = sum(s.page_view_count for s in sessions)
        bounce_sessions = sum(1 for s in sessions if s.is_bounce)
        
        # Calculate averages
        avg_duration = total_duration / total_sessions if total_sessions > 0 else 0
        avg_page_views = total_page_views / total_sessions if total_sessions > 0 else 0
        bounce_rate = bounce_sessions / total_sessions if total_sessions > 0 else 0
        
        # Find first and last session times
        first_session = min(sessions, key=lambda s: s.start_time)
        last_session = max(sessions, key=lambda s: s.end_time)
        
        metric = {
            'date_partition': first_session.date_partition,
            'user_id': key,
            'total_sessions': total_sessions,
            'total_duration_seconds': total_duration,
            'total_page_views': total_page_views,
            'bounce_sessions': bounce_sessions,
            'avg_session_duration': avg_duration,
            'avg_page_views_per_session': avg_page_views,
            'bounce_rate': bounce_rate,
            'first_session_time': first_session.start_time.isoformat(),
            'last_session_time': last_session.end_time.isoformat(),
            'unique_countries': len(set().union(*(s.countries for s in sessions))),
            'processing_time': datetime.now().isoformat()
        }
        
        out.collect(metric)


class ActiveSessionTracker(KeyedProcessFunction):
    """Track active sessions in real-time"""
    
    def __init__(self):
        self.session_state = None
    
    def open(self, runtime_context):
        """Initialize state"""
        self.session_state = runtime_context.get_state(
            ValueStateDescriptor("active_session", Types.STRING())
        )
    
    def process_element(self, value: Event, ctx: KeyedProcessFunction.Context, out: Collector):
        """Process event for active session tracking"""
        user_id = value.user_id
        current_session = self.session_state.value()
        
        # Update active session info
        session_info = {
            'user_id': user_id,
            'session_id': value.session_id,
            'last_activity': value.timestamp.isoformat(),
            'event_count': 1,
            'countries': [value.country] if value.country else [],
            'device_type': self._extract_device_type(value),
            'updated_at': datetime.now().isoformat()
        }
        
        if current_session:
            import json
            try:
                existing = json.loads(current_session)
                if existing['session_id'] == value.session_id:
                    # Update existing session
                    session_info['event_count'] = existing.get('event_count', 0) + 1
                    if value.country and value.country not in existing.get('countries', []):
                        session_info['countries'] = existing.get('countries', []) + [value.country]
                    else:
                        session_info['countries'] = existing.get('countries', [])
            except:
                pass  # Use new session info
        
        # Store updated session info
        self.session_state.update(json.dumps(session_info))
        
        # Output active session info
        out.collect(session_info)
        
        # Set timer for session timeout (30 minutes)
        timer_time = ctx.timestamp() + (30 * 60 * 1000)  # 30 minutes in ms
        ctx.timer_service().register_event_time_timer(timer_time)
    
    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext, out: Collector):
        """Handle session timeout"""
        current_session = self.session_state.value()
        if current_session:
            import json
            session_info = json.loads(current_session)
            session_info['status'] = 'timeout'
            session_info['timeout_at'] = datetime.fromtimestamp(timestamp / 1000).isoformat()
            
            out.collect(session_info)
            self.session_state.clear()
    
    def _extract_device_type(self, event: Event) -> str:
        """Extract device type from event"""
        if event.properties and 'user_agent' in event.properties:
            ua = event.properties['user_agent'].lower()
            if 'mobile' in ua or 'android' in ua or 'iphone' in ua:
                return 'mobile'
            elif 'tablet' in ua or 'ipad' in ua:
                return 'tablet'
        return 'desktop'


class UserJourneyTracker(KeyedProcessFunction):
    """Track user page view journeys in real-time"""
    
    def __init__(self):
        self.journey_state = None
        self.max_journey_length = 50  # Prevent memory issues
    
    def open(self, runtime_context):
        """Initialize state"""
        self.journey_state = runtime_context.get_state(
            ValueStateDescriptor("user_journey", Types.STRING())
        )
    
    def process_element(self, value: Event, ctx: KeyedProcessFunction.Context, out: Collector):
        """Track page view journey"""
        if not value.is_page_view() or not value.url:
            return
        
        journey_key = f"{value.user_id}:{value.session_id}"
        current_journey = self.journey_state.value()
        
        if current_journey:
            import json
            try:
                journey_data = json.loads(current_journey)
                pages = journey_data.get('pages', [])
                
                # Add current page
                pages.append({
                    'url': value.url,
                    'timestamp': value.timestamp.isoformat(),
                    'country': value.country
                })
                
                # Limit journey length
                if len(pages) > self.max_journey_length:
                    pages = pages[-self.max_journey_length:]
                
                journey_data['pages'] = pages
                journey_data['last_updated'] = datetime.now().isoformat()
                journey_data['page_count'] = len(pages)
                
            except:
                # Create new journey
                journey_data = {
                    'user_id': value.user_id,
                    'session_id': value.session_id,
                    'pages': [{
                        'url': value.url,
                        'timestamp': value.timestamp.isoformat(),
                        'country': value.country
                    }],
                    'page_count': 1,
                    'started_at': value.timestamp.isoformat(),
                    'last_updated': datetime.now().isoformat()
                }
        else:
            # New journey
            journey_data = {
                'user_id': value.user_id,
                'session_id': value.session_id,
                'pages': [{
                    'url': value.url,
                    'timestamp': value.timestamp.isoformat(),
                    'country': value.country
                }],
                'page_count': 1,
                'started_at': value.timestamp.isoformat(),
                'last_updated': datetime.now().isoformat()
            }
        
        # Store updated journey
        self.journey_state.update(json.dumps(journey_data))
        
        # Output journey update
        out.collect(journey_data)


def create_session_processing_job(env: StreamExecutionEnvironment, args):
    """Create and configure the session processing job"""
    
    # Configure watermark strategy
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(10)) \
        .with_timestamp_assigner(lambda event, timestamp: event.get_event_time_millis()) \
        .with_idleness(Duration.of_minutes(2))
    
    # Source: Kafka events
    kafka_source = KafkaEventSource(
        bootstrap_servers=args.kafka_servers,
        topics=['raw-events'],
        group_id='session-processing',
        value_deserializer='json'
    )
    
    events = env.add_source(kafka_source.create_source()) \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .name("kafka-events-source") \
        .uid("kafka-events-source")
    
    # Filter events with user information
    user_events = events.filter(lambda event: event.has_user()) \
        .name("user-events-filter") \
        .uid("user-events-filter")
    
    # Setup sinks
    redis_sink = RedisSink(cluster_nodes=args.redis_nodes.split(','))
    clickhouse_sink = ClickHouseSink(host=args.clickhouse_host)
    
    # Main session processing with session windows
    session_timeout_minutes = args.session_timeout
    completed_sessions = user_events \
        .key_by(lambda event: event.user_id) \
        .window(EventTimeSessionWindows.with_gap(Time.minutes(session_timeout_minutes))) \
        .process(SessionAggregator()) \
        .name("session-aggregator") \
        .uid("session-aggregator")
    
    # Store completed sessions in ClickHouse
    completed_sessions.add_sink(clickhouse_sink.create_sessions_sink()) \
        .name("clickhouse-sessions-sink") \
        .uid("clickhouse-sessions-sink")
    
    # Store session summaries in Redis
    completed_sessions.add_sink(redis_sink.create_session_summary_sink()) \
        .name("redis-session-summary-sink") \
        .uid("redis-session-summary-sink")
    
    # Daily session metrics per user
    completed_sessions \
        .key_by(lambda session: session.user_id) \
        .window(TumblingEventTimeWindows.of(Time.days(1))) \
        .process(DailySessionMetricsProcessor()) \
        .name("daily-session-metrics-processor") \
        .uid("daily-session-metrics-processor") \
        .add_sink(clickhouse_sink.create_user_metrics_sink()) \
        .name("clickhouse-user-metrics-sink") \
        .uid("clickhouse-user-metrics-sink")
    
    # Real-time active session tracking
    user_events \
        .key_by(lambda event: event.user_id) \
        .process(ActiveSessionTracker()) \
        .name("active-session-tracker") \
        .uid("active-session-tracker") \
        .add_sink(redis_sink.create_active_sessions_sink()) \
        .name("redis-active-sessions-sink") \
        .uid("redis-active-sessions-sink")
    
    # User journey tracking
    user_events \
        .filter(lambda event: event.is_page_view()) \
        .key_by(lambda event: f"{event.user_id}:{event.session_id}") \
        .process(UserJourneyTracker()) \
        .name("user-journey-tracker") \
        .uid("user-journey-tracker") \
        .add_sink(redis_sink.create_user_journey_sink()) \
        .name("redis-user-journey-sink") \
        .uid("redis-user-journey-sink")
    
    logger.info("Session Processing Job topology configured")


def main():
    """Main entry point for session processing job"""
    setup_logging()
    
    parser = argparse.ArgumentParser(description='Session Processing Job')
    parser.add_argument('--kafka-servers', required=True,
                       help='Kafka bootstrap servers')
    parser.add_argument('--redis-nodes', required=True,
                       help='Redis cluster nodes (comma-separated)')
    parser.add_argument('--clickhouse-host', required=True,
                       help='ClickHouse host')
    parser.add_argument('--session-timeout', type=int, default=30,
                       help='Session timeout in minutes')
    parser.add_argument('--parallelism', type=int, default=8,
                       help='Job parallelism')
    parser.add_argument('--checkpoint-interval', type=int, default=15000,
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
    
    # Create job
    create_session_processing_job(env, args)
    
    # Execute job
    logger.info(f"Starting Session Processing Job with parallelism {args.parallelism}")
    env.execute("Session Processing Job - PyFlink")


if __name__ == "__main__":
    main()