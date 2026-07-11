import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional, List
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
import orjson
from prometheus_client import Counter, Histogram, Gauge
import structlog
from contextlib import asynccontextmanager

logger = structlog.get_logger(__name__)

# Prometheus metrics
EVENTS_PRODUCED_TOTAL = Counter('events_produced_total', 'Total events produced to Kafka', ['topic', 'partition'])
PRODUCE_DURATION = Histogram('kafka_produce_duration_seconds', 'Time spent producing to Kafka')
PRODUCER_ERRORS = Counter('kafka_producer_errors_total', 'Total Kafka producer errors', ['error_type'])
ACTIVE_PRODUCERS = Gauge('active_kafka_producers', 'Number of active Kafka producers')

class AsyncKafkaProducerPool:
    """High-performance async Kafka producer pool with connection pooling and batching"""
    
    def __init__(
        self,
        bootstrap_servers: List[str],
        pool_size: int = 5,
        batch_size: int = 1000,
        linger_ms: int = 100,
        compression_type: str = 'snappy',
        enable_idempotence: bool = True,
        acks: str = 'all'
    ):
        self.bootstrap_servers = bootstrap_servers
        self.pool_size = pool_size
        self.batch_size = batch_size
        self.linger_ms = linger_ms
        self.compression_type = compression_type
        self.enable_idempotence = enable_idempotence
        self.acks = acks
        
        self.producers: List[AIOKafkaProducer] = []
        self.producer_index = 0
        self.lock = asyncio.Lock()
        
        # Batching mechanism
        self.batch_queue = asyncio.Queue(maxsize=10000)
        self.batch_worker_task = None
        
        # Circuit breaker
        self.failure_count = 0
        self.failure_threshold = 10
        self.circuit_open = False
        self.last_failure_time = 0
        self.circuit_timeout = 60  # seconds
        
        logger.info("Initialized AsyncKafkaProducerPool", 
                   pool_size=pool_size, batch_size=batch_size)
    
    async def start(self):
        """Initialize the producer pool"""
        try:
            # Create producer pool
            for i in range(self.pool_size):
                producer = AIOKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=self._serialize_value,
                    key_serializer=self._serialize_key,
                    batch_size=self.batch_size,
                    linger_ms=self.linger_ms,
                    compression_type=self.compression_type,
                    enable_idempotence=self.enable_idempotence,
                    acks=self.acks,
                    max_in_flight_requests_per_connection=5,
                    request_timeout_ms=30000,
                    retry_backoff_ms=100,
                    max_block_ms=5000,
                )
                
                await producer.start()
                self.producers.append(producer)
                ACTIVE_PRODUCERS.inc()
                
                logger.info("Started Kafka producer", producer_id=i)
            
            # Start batch processing worker
            self.batch_worker_task = asyncio.create_task(self._batch_worker())
            
            logger.info("AsyncKafkaProducerPool started successfully", 
                       active_producers=len(self.producers))
            
        except Exception as e:
            logger.error("Failed to start AsyncKafkaProducerPool", error=str(e))
            await self.stop()
            raise
    
    async def stop(self):
        """Gracefully shutdown the producer pool"""
        logger.info("Stopping AsyncKafkaProducerPool...")
        
        # Cancel batch worker
        if self.batch_worker_task:
            self.batch_worker_task.cancel()
            try:
                await self.batch_worker_task
            except asyncio.CancelledError:
                pass
        
        # Stop all producers
        for producer in self.producers:
            try:
                await producer.stop()
                ACTIVE_PRODUCERS.dec()
            except Exception as e:
                logger.warning("Error stopping producer", error=str(e))
        
        self.producers.clear()
        logger.info("AsyncKafkaProducerPool stopped")
    
    async def send_event(
        self, 
        topic: str, 
        event: Dict[str, Any], 
        key: Optional[str] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None
    ) -> bool:
        """Send single event to Kafka"""
        
        # Check circuit breaker
        if self._is_circuit_open():
            PRODUCER_ERRORS.labels(error_type='circuit_open').inc()
            logger.warning("Circuit breaker is open, dropping event")
            return False
        
        try:
            # Add metadata
            enriched_event = {
                **event,
                'producer_timestamp': int(time.time() * 1000),
                'producer_id': f"producer_{self.producer_index % self.pool_size}"
            }
            
            # Get producer from pool (round-robin)
            producer = await self._get_producer()
            
            # Send to Kafka
            with PRODUCE_DURATION.time():
                future = await producer.send(
                    topic=topic,
                    value=enriched_event,
                    key=key,
                    partition=partition,
                    timestamp_ms=timestamp
                )
                
                # Get metadata
                record_metadata = await future
                
                EVENTS_PRODUCED_TOTAL.labels(
                    topic=record_metadata.topic,
                    partition=record_metadata.partition
                ).inc()
                
                logger.debug("Event sent to Kafka", 
                           topic=topic, partition=record_metadata.partition,
                           offset=record_metadata.offset)
                
                # Reset circuit breaker on success
                self.failure_count = 0
                self.circuit_open = False
                
                return True
                
        except KafkaError as e:
            await self._handle_kafka_error(e)
            return False
        except Exception as e:
            logger.error("Unexpected error sending event", error=str(e))
            PRODUCER_ERRORS.labels(error_type='unexpected').inc()
            return False
    
    async def send_batch(
        self, 
        topic: str, 
        events: List[Dict[str, Any]], 
        key_extractor: Optional[callable] = None
    ) -> int:
        """Send batch of events to Kafka"""
        
        if self._is_circuit_open():
            logger.warning("Circuit breaker open, dropping batch", size=len(events))
            return 0
        
        successful_sends = 0
        
        try:
            producer = await self._get_producer()
            
            # Create batch of futures
            futures = []
            for event in events:
                key = key_extractor(event) if key_extractor else None
                
                enriched_event = {
                    **event,
                    'producer_timestamp': int(time.time() * 1000),
                    'batch_id': f"batch_{int(time.time())}"
                }
                
                future = producer.send(
                    topic=topic,
                    value=enriched_event,
                    key=key
                )
                futures.append(future)
            
            # Wait for all sends to complete
            with PRODUCE_DURATION.time():
                results = await asyncio.gather(*futures, return_exceptions=True)
                
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.warning("Failed to send event in batch", 
                                     event_index=i, error=str(result))
                        PRODUCER_ERRORS.labels(error_type='batch_item_failed').inc()
                    else:
                        successful_sends += 1
                        EVENTS_PRODUCED_TOTAL.labels(
                            topic=result.topic,
                            partition=result.partition
                        ).inc()
            
            logger.info("Batch sent to Kafka", 
                       total=len(events), successful=successful_sends)
            
            return successful_sends
            
        except Exception as e:
            logger.error("Failed to send batch", error=str(e))
            PRODUCER_ERRORS.labels(error_type='batch_failed').inc()
            return 0
    
    async def send_async_batch(self, topic: str, event: Dict[str, Any]):
        """Add event to async batch queue for background processing"""
        try:
            await self.batch_queue.put({
                'topic': topic,
                'event': event,
                'timestamp': time.time()
            })
        except asyncio.QueueFull:
            logger.warning("Batch queue full, dropping event")
            PRODUCER_ERRORS.labels(error_type='queue_full').inc()
    
    async def _batch_worker(self):
        """Background worker that processes batched events"""
        batch = []
        last_send = time.time()
        
        while True:
            try:
                # Wait for event or timeout
                try:
                    item = await asyncio.wait_for(
                        self.batch_queue.get(), 
                        timeout=self.linger_ms / 1000
                    )
                    batch.append(item)
                except asyncio.TimeoutError:
                    pass
                
                current_time = time.time()
                should_send = (
                    len(batch) >= self.batch_size or
                    (batch and current_time - last_send > self.linger_ms / 1000)
                )
                
                if should_send and batch:
                    # Group by topic
                    topic_batches = {}
                    for item in batch:
                        topic = item['topic']
                        if topic not in topic_batches:
                            topic_batches[topic] = []
                        topic_batches[topic].append(item['event'])
                    
                    # Send each topic batch
                    for topic, events in topic_batches.items():
                        await self.send_batch(topic, events)
                    
                    batch.clear()
                    last_send = current_time
                    
                    logger.debug("Processed batch", 
                               topics=list(topic_batches.keys()),
                               total_events=sum(len(events) for events in topic_batches.values()))
                
            except asyncio.CancelledError:
                logger.info("Batch worker cancelled")
                break
            except Exception as e:
                logger.error("Error in batch worker", error=str(e))
                await asyncio.sleep(1)
    
    async def _get_producer(self) -> AIOKafkaProducer:
        """Get producer from pool using round-robin"""
        async with self.lock:
            producer = self.producers[self.producer_index % len(self.producers)]
            self.producer_index += 1
            return producer
    
    def _serialize_value(self, value: Any) -> bytes:
        """Fast JSON serialization using orjson"""
        if isinstance(value, (dict, list)):
            return orjson.dumps(value)
        elif isinstance(value, str):
            return value.encode('utf-8')
        else:
            return str(value).encode('utf-8')
    
    def _serialize_key(self, key: Any) -> Optional[bytes]:
        """Serialize key"""
        if key is None:
            return None
        elif isinstance(key, str):
            return key.encode('utf-8')
        else:
            return str(key).encode('utf-8')
    
    async def _handle_kafka_error(self, error: KafkaError):
        """Handle Kafka errors with circuit breaker logic"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.circuit_open = True
            logger.warning("Circuit breaker opened", 
                         failure_count=self.failure_count)
        
        error_type = type(error).__name__
        PRODUCER_ERRORS.labels(error_type=error_type).inc()
        
        logger.error("Kafka error", error=str(error), error_type=error_type,
                    failure_count=self.failure_count)
    
    def _is_circuit_open(self) -> bool:
        """Check if circuit breaker is open"""
        if not self.circuit_open:
            return False
        
        # Check if timeout has passed
        if time.time() - self.last_failure_time > self.circuit_timeout:
            self.circuit_open = False
            self.failure_count = 0
            logger.info("Circuit breaker reset")
            return False
        
        return True
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of producer pool"""
        return {
            'active_producers': len(self.producers),
            'circuit_open': self.circuit_open,
            'failure_count': self.failure_count,
            'queue_size': self.batch_queue.qsize(),
            'last_failure_time': self.last_failure_time
        }

# Global producer pool instance
producer_pool: Optional[AsyncKafkaProducerPool] = None

@asynccontextmanager
async def get_kafka_producer_pool():
    """Context manager for Kafka producer pool"""
    global producer_pool
    
    if producer_pool is None:
        # Initialize from config
        bootstrap_servers = ['localhost:9092']  # From env config
        producer_pool = AsyncKafkaProducerPool(bootstrap_servers)
        await producer_pool.start()
    
    try:
        yield producer_pool
    finally:
        # Pool is shared, don't stop here
        pass

async def cleanup_producer_pool():
    """Cleanup producer pool on app shutdown"""
    global producer_pool
    if producer_pool:
        await producer_pool.stop()
        producer_pool = None