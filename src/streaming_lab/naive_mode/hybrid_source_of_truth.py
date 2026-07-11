import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import structlog

logger = structlog.get_logger(__name__)

@dataclass
class DataRetentionPolicy:
    """Defines retention policies across storage tiers"""
    kafka_days: int = 7           # Hot data for stream processing
    clickhouse_months: int = 12   # Warm data for analytics
    cold_storage_years: int = 7   # Cold data for compliance/archive

class HybridSourceOfTruth:
    """
    Implements hybrid source of truth with multiple retention tiers
    
    Data Flow:
    Events → Kafka (7 days) + Cold Storage (7 years) + ClickHouse (12 months)
    
    Recovery Strategy:
    - Recent data (0-7 days): Replay from Kafka
    - Historical data (7 days - 12 months): Query ClickHouse  
    - Archived data (12+ months): Restore from Cold Storage
    """
    
    def __init__(self, storage: 'MultiTierStorage', retention_policy: DataRetentionPolicy = None):
        self.storage = storage
        self.policy = retention_policy or DataRetentionPolicy()
        
        # Track what's been archived
        self.archival_status = {}
        
    async def store_event_with_durability(self, event: Dict[str, Any]) -> Dict[str, bool]:
        """Store event across all durability tiers"""
        results = {}
        
        try:
            # 1. Kafka (immediate processing, short retention)
            kafka_success = await self._store_to_kafka(event)
            results['kafka'] = kafka_success
            
            # 2. Cold Storage (permanent archive, immediate)
            cold_success = await self._store_to_cold_storage(event)
            results['cold_storage'] = cold_success
            
            # 3. ClickHouse (analytics, via stream processing)
            # This happens asynchronously via stream processors
            results['stream_processing'] = kafka_success
            
            logger.info("Event stored with durability", 
                       event_id=event.get('event_id'),
                       success_rate=f"{sum(results.values())}/{len(results)}")
            
            return results
            
        except Exception as e:
            logger.error("Error in hybrid storage", error=str(e))
            return results
    
    async def _store_to_kafka(self, event: Dict[str, Any]) -> bool:
        """Store to Kafka for immediate processing"""
        try:
            if hasattr(self.storage, 'kafka_producer'):
                success = await self.storage.kafka_producer.send_event(
                    topic='raw-events',
                    event=event,
                    key=str(event.get('user_id', event.get('event_id')))
                )
                return success
            return False
        except Exception as e:
            logger.error("Kafka storage failed", error=str(e))
            return False
    
    async def _store_to_cold_storage(self, event: Dict[str, Any]) -> bool:
        """Store to cold storage for permanent retention"""
        try:
            if not self.storage.cold_storage:
                return False
            
            # Partition by date for efficient querying
            date_partition = event.get('date_partition', datetime.utcnow().strftime('%Y-%m-%d'))
            hour_partition = datetime.utcnow().strftime('%H')
            
            # Create hierarchical path: events/2024/01/15/12/event_id.json
            object_key = f"events/{date_partition.replace('-', '/')}/{hour_partition}/{event['event_id']}.json"
            
            # Add archival metadata
            archive_event = {
                **event,
                'archived_at': datetime.utcnow().isoformat(),
                'archive_version': '1.0',
                'retention_policy': {
                    'tier': 'cold',
                    'retention_years': self.policy.cold_storage_years
                }
            }
            
            success = await self.storage.cold_storage.put_object(
                object_key,
                json.dumps(archive_event).encode('utf-8'),
                metadata={
                    'event_type': event.get('event_type', ''),
                    'date_partition': date_partition,
                    'user_id': str(event.get('user_id', '')),
                    'content_type': 'application/json'
                }
            )
            
            return success
            
        except Exception as e:
            logger.error("Cold storage failed", error=str(e))
            return False
    
    async def replay_events_from_source(
        self, 
        start_date: str, 
        end_date: str,
        target_kafka_topic: str = 'raw-events-replay'
    ) -> Dict[str, Any]:
        """
        Replay events from appropriate source of truth based on date range
        
        Strategy:
        - Recent (0-7 days): Replay from Kafka
        - Medium (7 days - 12 months): Query from ClickHouse  
        - Historical (12+ months): Restore from Cold Storage
        """
        
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        current_dt = datetime.utcnow()
        
        results = {
            'total_events': 0,
            'sources_used': [],
            'errors': []
        }
        
        # Determine data sources based on age
        kafka_cutoff = current_dt - timedelta(days=self.policy.kafka_days)
        clickhouse_cutoff = current_dt - timedelta(days=self.policy.clickhouse_months * 30)
        
        try:
            # Recent data from Kafka
            if end_dt > kafka_cutoff:
                kafka_start = max(start_dt, kafka_cutoff)
                kafka_events = await self._replay_from_kafka(kafka_start, end_dt, target_kafka_topic)
                results['total_events'] += kafka_events
                results['sources_used'].append(f'kafka:{kafka_events}')
            
            # Medium-term data from ClickHouse
            if start_dt < kafka_cutoff and end_dt > clickhouse_cutoff:
                ch_start = max(start_dt, clickhouse_cutoff)
                ch_end = min(end_dt, kafka_cutoff)
                ch_events = await self._replay_from_clickhouse(ch_start, ch_end, target_kafka_topic)
                results['total_events'] += ch_events
                results['sources_used'].append(f'clickhouse:{ch_events}')
            
            # Historical data from Cold Storage
            if start_dt < clickhouse_cutoff:
                cold_start = start_dt
                cold_end = min(end_dt, clickhouse_cutoff)
                cold_events = await self._replay_from_cold_storage(cold_start, cold_end, target_kafka_topic)
                results['total_events'] += cold_events
                results['sources_used'].append(f'cold_storage:{cold_events}')
            
            logger.info("Event replay completed", **results)
            return results
            
        except Exception as e:
            logger.error("Event replay failed", error=str(e))
            results['errors'].append(str(e))
            return results
    
    async def _replay_from_kafka(self, start_dt: datetime, end_dt: datetime, target_topic: str) -> int:
        """Replay events from Kafka (recent data)"""
        try:
            # This would need a custom Kafka consumer that filters by timestamp
            logger.info("Replaying from Kafka", start=start_dt.isoformat(), end=end_dt.isoformat())
            
            # Implementation would:
            # 1. Create consumer with auto_offset_reset='earliest'
            # 2. Filter messages by timestamp range
            # 3. Republish to target topic
            
            # Placeholder implementation
            return 0
            
        except Exception as e:
            logger.error("Kafka replay failed", error=str(e))
            return 0
    
    async def _replay_from_clickhouse(self, start_dt: datetime, end_dt: datetime, target_topic: str) -> int:
        """Replay events from ClickHouse (medium-term data)"""
        try:
            if not self.storage.warm_storage:
                return 0
            
            # Query ClickHouse for events in date range
            query = """
                SELECT * FROM events 
                WHERE timestamp >= %(start_date)s 
                AND timestamp < %(end_date)s 
                ORDER BY timestamp
            """
            
            events = await self.storage.warm_storage.execute_query(query, {
                'start_date': start_dt.isoformat(),
                'end_date': end_dt.isoformat()
            })
            
            # Republish events to Kafka
            event_count = 0
            for event_row in events:
                # Convert ClickHouse row back to event format
                event = self._clickhouse_row_to_event(event_row)
                
                # Send to replay topic
                if hasattr(self.storage, 'kafka_producer'):
                    await self.storage.kafka_producer.send_event(target_topic, event)
                    event_count += 1
            
            logger.info("ClickHouse replay completed", events=event_count)
            return event_count
            
        except Exception as e:
            logger.error("ClickHouse replay failed", error=str(e))
            return 0
    
    async def _replay_from_cold_storage(self, start_dt: datetime, end_dt: datetime, target_topic: str) -> int:
        """Replay events from Cold Storage (historical data)"""
        try:
            if not self.storage.cold_storage:
                return 0
            
            event_count = 0
            current_date = start_dt.date()
            end_date = end_dt.date()
            
            # Iterate through each day in the range
            while current_date <= end_date:
                date_str = current_date.strftime('%Y/%m/%d')
                
                # List all objects for this date
                # This would need implementation in cold storage class
                daily_events = await self._get_daily_events_from_cold_storage(date_str)
                
                for event in daily_events:
                    # Filter by time range
                    event_time = datetime.fromisoformat(event['timestamp'])
                    if start_dt <= event_time < end_dt:
                        # Send to replay topic
                        if hasattr(self.storage, 'kafka_producer'):
                            await self.storage.kafka_producer.send_event(target_topic, event)
                            event_count += 1
                
                current_date += timedelta(days=1)
            
            logger.info("Cold storage replay completed", events=event_count)
            return event_count
            
        except Exception as e:
            logger.error("Cold storage replay failed", error=str(e))
            return 0
    
    async def _get_daily_events_from_cold_storage(self, date_path: str) -> List[Dict[str, Any]]:
        """Get all events for a specific date from cold storage"""
        # This would need to be implemented based on your S3/MinIO client
        # Placeholder implementation
        return []
    
    def _clickhouse_row_to_event(self, row) -> Dict[str, Any]:
        """Convert ClickHouse row back to original event format"""
        # Implementation depends on your ClickHouse schema
        # Placeholder implementation
        return {}
    
    async def get_retention_status(self) -> Dict[str, Any]:
        """Get current retention status across all storage tiers"""
        current_time = datetime.utcnow()
        
        kafka_cutoff = current_time - timedelta(days=self.policy.kafka_days)
        clickhouse_cutoff = current_time - timedelta(days=self.policy.clickhouse_months * 30)
        
        return {
            'policy': {
                'kafka_retention_days': self.policy.kafka_days,
                'clickhouse_retention_months': self.policy.clickhouse_months,
                'cold_storage_retention_years': self.policy.cold_storage_years
            },
            'current_coverage': {
                'kafka_covers': f"Recent {self.policy.kafka_days} days",
                'clickhouse_covers': f"Last {self.policy.clickhouse_months} months", 
                'cold_storage_covers': f"Last {self.policy.cold_storage_years} years"
            },
            'cutoff_dates': {
                'kafka_cutoff': kafka_cutoff.isoformat(),
                'clickhouse_cutoff': clickhouse_cutoff.isoformat()
            }
        }
    
    async def cleanup_expired_data(self):
        """Clean up expired data based on retention policies"""
        try:
            # Kafka cleanup happens automatically via retention settings
            
            # ClickHouse cleanup
            if self.storage.warm_storage:
                cutoff_date = datetime.utcnow() - timedelta(days=self.policy.clickhouse_months * 30)
                
                cleanup_query = """
                    ALTER TABLE events DELETE 
                    WHERE timestamp < %(cutoff_date)s
                """
                
                await self.storage.warm_storage.execute_query(cleanup_query, {
                    'cutoff_date': cutoff_date.isoformat()
                })
            
            # Cold storage cleanup (if needed for compliance)
            # Would implement S3 lifecycle policies instead
            
            logger.info("Data cleanup completed")
            
        except Exception as e:
            logger.error("Data cleanup failed", error=str(e))