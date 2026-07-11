import asyncio
import time
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
import orjson
import structlog
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

from app.core.async_kafka_producer import AsyncKafkaProducerPool, get_kafka_producer_pool
from app.storage.multi_tier_storage import MultiTierStorage

logger = structlog.get_logger(__name__)

# Prometheus metrics
HTTP_REQUESTS_TOTAL = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status_code'])
HTTP_REQUEST_DURATION = Histogram('http_request_duration_seconds', 'HTTP request duration')
EVENTS_INGESTED_TOTAL = Counter('events_ingested_total', 'Total events ingested', ['source'])
ACTIVE_CONNECTIONS = Gauge('active_connections', 'Active connections')

# Request/Response Models
class EventCreate(BaseModel):
    user_id: Optional[int] = None
    session_id: Optional[str] = None
    event_type: str = Field(..., min_length=1, max_length=100)
    event_name: Optional[str] = Field(None, max_length=100)
    properties: Dict[str, Any] = Field(default_factory=dict)
    url: Optional[str] = Field(None, max_length=2048)
    referrer: Optional[str] = Field(None, max_length=2048)
    user_agent: Optional[str] = Field(None, max_length=512)
    ip_address: Optional[str] = Field(None, max_length=45)  # IPv6 compatible
    country: Optional[str] = Field(None, max_length=2)  # ISO 3166-1 alpha-2
    city: Optional[str] = Field(None, max_length=100)

class BatchEventCreate(BaseModel):
    events: List[EventCreate] = Field(..., min_items=1, max_items=1000)
    batch_id: Optional[str] = None

class EventResponse(BaseModel):
    status: str
    event_id: str
    timestamp: str
    processing_time_ms: float

class BatchEventResponse(BaseModel):
    status: str
    batch_id: str
    processed_count: int
    failed_count: int
    processing_time_ms: float

class AnalyticsQuery(BaseModel):
    query: str
    parameters: Optional[Dict[str, Any]] = None
    time_range: Optional[Dict[str, str]] = None

# Global storage instance
storage: Optional[MultiTierStorage] = None
producer_pool: Optional[AsyncKafkaProducerPool] = None

async def get_storage() -> MultiTierStorage:
    """Dependency to get storage instance"""
    if storage is None:
        raise HTTPException(status_code=500, detail="Storage not initialized")
    return storage

async def get_producer() -> AsyncKafkaProducerPool:
    """Dependency to get Kafka producer pool"""
    if producer_pool is None:
        raise HTTPException(status_code=500, detail="Kafka producer not initialized")
    return producer_pool

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    global storage, producer_pool
    
    # Startup
    logger.info("Starting Data-Intensive Analytics Platform")
    
    try:
        # Initialize Kafka producer pool
        producer_pool = AsyncKafkaProducerPool(
            bootstrap_servers=['localhost:9092'],
            pool_size=5,
            batch_size=500,
            linger_ms=50  # Lower latency
        )
        await producer_pool.start()
        
        # Initialize multi-tier storage
        storage = MultiTierStorage()
        storage_config = {
            'hot_storage': {
                'cluster_nodes': ['localhost:7000', 'localhost:7001', 'localhost:7002']
            },
            'warm_storage': {
                'hosts': ['localhost:9000', 'localhost:9001'],
                'database': 'analytics'
            },
            'search_storage': {
                'hosts': ['localhost:9200']
            }
        }
        await storage.initialize(storage_config)
        
        logger.info("Application started successfully")
        yield
        
    except Exception as e:
        logger.error("Failed to start application", error=str(e))
        raise
    finally:
        # Shutdown
        logger.info("Shutting down application")
        
        if producer_pool:
            await producer_pool.stop()
        
        logger.info("Application shutdown complete")

# Create FastAPI app with lifespan
app = FastAPI(
    title="Data-Intensive Analytics Platform",
    version="2.0.0",
    description="High-performance event processing with multi-tier storage",
    lifespan=lifespan
)

@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Middleware for collecting HTTP metrics"""
    start_time = time.time()
    
    # Increment active connections
    ACTIVE_CONNECTIONS.inc()
    
    try:
        response = await call_next(request)
        
        # Record metrics
        processing_time = time.time() - start_time
        
        HTTP_REQUESTS_TOTAL.labels(
            method=request.method,
            endpoint=request.url.path,
            status_code=response.status_code
        ).inc()
        
        HTTP_REQUEST_DURATION.observe(processing_time)
        
        return response
    finally:
        ACTIVE_CONNECTIONS.dec()

@app.post("/events", response_model=EventResponse)
async def ingest_single_event(
    event: EventCreate,
    background_tasks: BackgroundTasks,
    producer: AsyncKafkaProducerPool = Depends(get_producer),
    storage: MultiTierStorage = Depends(get_storage)
):
    """Ingest a single event with async processing"""
    start_time = time.time()
    
    try:
        # Generate event ID and timestamp
        event_id = str(uuid.uuid4())
        timestamp = datetime.utcnow()
        
        # Create enriched event
        enriched_event = {
            'event_id': event_id,
            'user_id': event.user_id,
            'session_id': event.session_id or f"session_{int(timestamp.timestamp())}",
            'event_type': event.event_type,
            'event_name': event.event_name or event.event_type,
            'properties': event.properties,
            'url': event.url,
            'referrer': event.referrer,
            'user_agent': event.user_agent,
            'ip_address': event.ip_address,
            'country': event.country,
            'city': event.city,
            'timestamp': timestamp.isoformat(),
            'date_partition': timestamp.strftime('%Y-%m-%d'),
            'ingestion_timestamp': timestamp.isoformat(),
            'source': 'api'
        }
        
        # Send to Kafka asynchronously (fire and forget for lowest latency)
        kafka_success = await producer.send_event(
            topic='raw-events',
            event=enriched_event,
            key=str(event.user_id) if event.user_id else event_id
        )
        
        if not kafka_success:
            logger.warning("Failed to send event to Kafka, storing directly", event_id=event_id)
            # Fallback: store directly in storage tiers
            background_tasks.add_task(storage.store_event, enriched_event)
        
        processing_time = (time.time() - start_time) * 1000
        
        EVENTS_INGESTED_TOTAL.labels(source='single_event').inc()
        
        return EventResponse(
            status="accepted",
            event_id=event_id,
            timestamp=timestamp.isoformat(),
            processing_time_ms=round(processing_time, 2)
        )
        
    except Exception as e:
        logger.error("Error ingesting single event", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/events/batch", response_model=BatchEventResponse)
async def ingest_batch_events(
    batch: BatchEventCreate,
    background_tasks: BackgroundTasks,
    producer: AsyncKafkaProducerPool = Depends(get_producer),
    storage: MultiTierStorage = Depends(get_storage)
):
    """Ingest batch of events with optimized processing"""
    start_time = time.time()
    
    try:
        batch_id = batch.batch_id or str(uuid.uuid4())
        timestamp = datetime.utcnow()
        date_partition = timestamp.strftime('%Y-%m-%d')
        
        # Process events in batch
        enriched_events = []
        
        for i, event in enumerate(batch.events):
            event_id = str(uuid.uuid4())
            
            enriched_event = {
                'event_id': event_id,
                'batch_id': batch_id,
                'batch_index': i,
                'user_id': event.user_id,
                'session_id': event.session_id or f"session_{int(timestamp.timestamp())}_{event.user_id}",
                'event_type': event.event_type,
                'event_name': event.event_name or event.event_type,
                'properties': event.properties,
                'url': event.url,
                'referrer': event.referrer,
                'user_agent': event.user_agent,
                'ip_address': event.ip_address,
                'country': event.country,
                'city': event.city,
                'timestamp': timestamp.isoformat(),
                'date_partition': date_partition,
                'ingestion_timestamp': timestamp.isoformat(),
                'source': 'batch_api'
            }
            
            enriched_events.append(enriched_event)
        
        # Send batch to Kafka
        successful_sends = await producer.send_batch(
            topic='raw-events',
            events=enriched_events,
            key_extractor=lambda e: str(e['user_id']) if e['user_id'] else e['event_id']
        )
        
        failed_count = len(enriched_events) - successful_sends
        
        # Handle failed events
        if failed_count > 0:
            logger.warning("Some events failed to send to Kafka", 
                         failed_count=failed_count, batch_id=batch_id)
            # Store failed events directly (could implement retry logic)
            for event in enriched_events[successful_sends:]:
                background_tasks.add_task(storage.store_event, event)
        
        processing_time = (time.time() - start_time) * 1000
        
        EVENTS_INGESTED_TOTAL.labels(source='batch_events').inc()
        
        return BatchEventResponse(
            status="accepted",
            batch_id=batch_id,
            processed_count=successful_sends,
            failed_count=failed_count,
            processing_time_ms=round(processing_time, 2)
        )
        
    except Exception as e:
        logger.error("Error ingesting batch events", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/realtime/metrics")
async def get_realtime_metrics(
    time_window: str = "1m",
    storage: MultiTierStorage = Depends(get_storage)
):
    """Get real-time metrics from hot storage"""
    try:
        metrics = await storage.get_realtime_metrics(time_window)
        return metrics
    except Exception as e:
        logger.error("Error getting real-time metrics", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/analytics/query")
async def execute_analytics_query(
    query: AnalyticsQuery,
    storage: MultiTierStorage = Depends(get_storage)
):
    """Execute analytical query on warm storage"""
    try:
        results = await storage.query_analytics(query.query, query.parameters)
        
        return {
            "results": results,
            "execution_time": time.time(),
            "row_count": len(results) if isinstance(results, list) else 0
        }
        
    except Exception as e:
        logger.error("Error executing analytics query", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/search/events")
async def search_events(
    q: str,
    size: int = 10,
    from_: int = 0,
    storage: MultiTierStorage = Depends(get_storage)
):
    """Search events using full-text search"""
    try:
        search_query = {
            "query": {
                "multi_match": {
                    "query": q,
                    "fields": ["event_type", "event_name", "properties.*", "url"]
                }
            },
            "size": size,
            "from": from_,
            "sort": [{"timestamp": {"order": "desc"}}]
        }
        
        results = await storage.search_events(search_query)
        
        return {
            "total": results.get('hits', {}).get('total', {}).get('value', 0),
            "events": [hit['_source'] for hit in results.get('hits', {}).get('hits', [])],
            "took": results.get('took', 0)
        }
        
    except Exception as e:
        logger.error("Error searching events", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health")
async def health_check(
    producer: AsyncKafkaProducerPool = Depends(get_producer)
):
    """Health check endpoint"""
    try:
        producer_health = producer.get_health_status()
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "services": {
                "api": "up",
                "kafka_producer": "up" if producer_health['active_producers'] > 0 else "down",
                "circuit_breaker": "open" if producer_health['circuit_open'] else "closed"
            },
            "metrics": {
                "active_producers": producer_health['active_producers'],
                "queue_size": producer_health['queue_size'],
                "failure_count": producer_health['failure_count']
            }
        }
        
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/metrics")
async def prometheus_metrics():
    """Prometheus metrics endpoint"""
    return StreamingResponse(
        iter([generate_latest()]),
        media_type=CONTENT_TYPE_LATEST,
        headers={"Content-Type": CONTENT_TYPE_LATEST}
    )

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Data-Intensive Analytics Platform",
        "version": "2.0.0",
        "description": "High-performance event processing with Python-first architecture",
        "features": [
            "Async Kafka producer pool with batching",
            "Multi-tier storage (Hot/Warm/Cold/Search)",
            "Circuit breaker and fault tolerance",
            "Prometheus metrics integration",
            "Sub-millisecond event ingestion",
            "Horizontal scalability"
        ],
        "endpoints": {
            "ingest": "/events, /events/batch",
            "analytics": "/analytics/query, /realtime/metrics",
            "search": "/search/events",
            "monitoring": "/health, /metrics"
        }
    }

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.api.events:app",
        host="0.0.0.0",
        port=8000,
        workers=1,  # Use multiple processes in production
        loop="uvloop",  # High-performance event loop
        http="httptools",  # High-performance HTTP parser
        log_config={
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "handlers": {
                "default": {
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                },
            },
            "root": {
                "level": "INFO",
                "handlers": ["default"],
            },
        }
    )