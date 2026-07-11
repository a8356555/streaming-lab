import asyncio
import json
import time
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

# Hot Storage - Redis Cluster
import redis.asyncio as aioredis
from redis.asyncio import Redis
from redis.exceptions import RedisError

# Warm Storage - ClickHouse
from clickhouse_driver import Client as ClickHouseClient
from clickhouse_driver.errors import Error as ClickHouseError
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Cold Storage - MinIO/S3
import boto3
from botocore.exceptions import ClientError
import aiofiles
import orjson

# Search - Elasticsearch
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchException

import structlog
from prometheus_client import Counter, Histogram, Gauge

logger = structlog.get_logger(__name__)

# Metrics
STORAGE_OPERATIONS_TOTAL = Counter('storage_operations_total', 'Storage operations by tier', ['tier', 'operation'])
STORAGE_OPERATION_DURATION = Histogram('storage_operation_duration_seconds', 'Storage operation duration', ['tier', 'operation'])
STORAGE_ERRORS = Counter('storage_errors_total', 'Storage errors by tier', ['tier', 'error_type'])
HOT_STORAGE_SIZE = Gauge('hot_storage_size_bytes', 'Hot storage size in bytes')

class StorageTier(ABC):
    """Abstract base class for storage tiers"""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        pass
    
    @abstractmethod
    async def put(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> bool:
        pass
    
    @abstractmethod
    async def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def batch_put(self, items: Dict[str, Any], ttl: Optional[int] = None) -> int:
        pass

class HotStorage(StorageTier):
    """Redis Cluster for hot data (sub-millisecond access)"""
    
    def __init__(self, redis_cluster_nodes: List[str], pool_size: int = 10):
        self.cluster_nodes = redis_cluster_nodes
        self.pool_size = pool_size
        self.redis_cluster = None
        
    async def initialize(self):
        """Initialize Redis cluster connection"""
        try:
            self.redis_cluster = aioredis.RedisCluster(
                host=self.cluster_nodes[0].split(':')[0],
                port=int(self.cluster_nodes[0].split(':')[1]),
                max_connections_per_node=self.pool_size,
                skip_full_coverage_check=True,
                health_check_interval=30,
                socket_keepalive=True,
                socket_keepalive_options={},
                retry_on_timeout=True,
                decode_responses=False
            )
            
            # Test connection
            await self.redis_cluster.ping()
            logger.info("Redis cluster initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize Redis cluster", error=str(e))
            raise
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from hot storage"""
        try:
            with STORAGE_OPERATION_DURATION.labels(tier='hot', operation='get').time():
                value = await self.redis_cluster.get(key)
                
                STORAGE_OPERATIONS_TOTAL.labels(tier='hot', operation='get').inc()
                
                if value:
                    return orjson.loads(value)
                return None
                
        except RedisError as e:
            STORAGE_ERRORS.labels(tier='hot', error_type='redis_error').inc()
            logger.error("Redis get error", key=key, error=str(e))
            return None
        except Exception as e:
            STORAGE_ERRORS.labels(tier='hot', error_type='unexpected').inc()
            logger.error("Unexpected error in hot storage get", key=key, error=str(e))
            return None
    
    async def put(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Put value to hot storage"""
        try:
            with STORAGE_OPERATION_DURATION.labels(tier='hot', operation='put').time():
                serialized_value = orjson.dumps(value)
                
                if ttl:
                    await self.redis_cluster.setex(key, ttl, serialized_value)
                else:
                    await self.redis_cluster.set(key, serialized_value)
                
                STORAGE_OPERATIONS_TOTAL.labels(tier='hot', operation='put').inc()
                return True
                
        except RedisError as e:
            STORAGE_ERRORS.labels(tier='hot', error_type='redis_error').inc()
            logger.error("Redis put error", key=key, error=str(e))
            return False
        except Exception as e:
            STORAGE_ERRORS.labels(tier='hot', error_type='unexpected').inc()
            logger.error("Unexpected error in hot storage put", key=key, error=str(e))
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete value from hot storage"""
        try:
            result = await self.redis_cluster.delete(key)
            STORAGE_OPERATIONS_TOTAL.labels(tier='hot', operation='delete').inc()
            return result > 0
        except RedisError as e:
            STORAGE_ERRORS.labels(tier='hot', error_type='redis_error').inc()
            logger.error("Redis delete error", key=key, error=str(e))
            return False
    
    async def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        """Batch get from hot storage"""
        try:
            with STORAGE_OPERATION_DURATION.labels(tier='hot', operation='batch_get').time():
                values = await self.redis_cluster.mget(keys)
                
                result = {}
                for key, value in zip(keys, values):
                    if value:
                        try:
                            result[key] = orjson.loads(value)
                        except orjson.JSONDecodeError:
                            result[key] = value.decode('utf-8') if isinstance(value, bytes) else value
                
                STORAGE_OPERATIONS_TOTAL.labels(tier='hot', operation='batch_get').inc()
                return result
                
        except RedisError as e:
            STORAGE_ERRORS.labels(tier='hot', error_type='redis_error').inc()
            logger.error("Redis batch get error", keys_count=len(keys), error=str(e))
            return {}
    
    async def batch_put(self, items: Dict[str, Any], ttl: Optional[int] = None) -> int:
        """Batch put to hot storage"""
        try:
            with STORAGE_OPERATION_DURATION.labels(tier='hot', operation='batch_put').time():
                pipe = self.redis_cluster.pipeline()
                
                for key, value in items.items():
                    serialized_value = orjson.dumps(value)
                    if ttl:
                        pipe.setex(key, ttl, serialized_value)
                    else:
                        pipe.set(key, serialized_value)
                
                results = await pipe.execute()
                successful = sum(1 for result in results if result)
                
                STORAGE_OPERATIONS_TOTAL.labels(tier='hot', operation='batch_put').inc()
                return successful
                
        except RedisError as e:
            STORAGE_ERRORS.labels(tier='hot', error_type='redis_error').inc()
            logger.error("Redis batch put error", items_count=len(items), error=str(e))
            return 0
    
    async def increment_counter(self, key: str, amount: int = 1, ttl: Optional[int] = None) -> int:
        """Increment counter in hot storage"""
        try:
            result = await self.redis_cluster.incr(key, amount)
            if ttl:
                await self.redis_cluster.expire(key, ttl)
            return result
        except RedisError as e:
            logger.error("Redis increment error", key=key, error=str(e))
            return 0
    
    async def add_to_sorted_set(self, key: str, score: float, member: str, ttl: Optional[int] = None) -> bool:
        """Add to sorted set in hot storage"""
        try:
            result = await self.redis_cluster.zadd(key, {member: score})
            if ttl:
                await self.redis_cluster.expire(key, ttl)
            return result > 0
        except RedisError as e:
            logger.error("Redis sorted set error", key=key, error=str(e))
            return False

class WarmStorage(StorageTier):
    """ClickHouse for warm data (analytical queries, 1-second access)"""
    
    def __init__(self, clickhouse_hosts: List[str], database: str = 'analytics'):
        self.hosts = clickhouse_hosts
        self.database = database
        self.clients = []
        self.executor = ThreadPoolExecutor(max_workers=5)
        
    async def initialize(self):
        """Initialize ClickHouse connections"""
        try:
            for host in self.hosts:
                client = ClickHouseClient(
                    host=host,
                    database=self.database,
                    settings={'use_numpy': True}
                )
                
                # Test connection
                await asyncio.get_event_loop().run_in_executor(
                    self.executor, client.execute, 'SELECT 1'
                )
                
                self.clients.append(client)
            
            logger.info("ClickHouse clients initialized", hosts=self.hosts)
            
        except Exception as e:
            logger.error("Failed to initialize ClickHouse", error=str(e))
            raise
    
    async def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Any]:
        """Execute query on ClickHouse"""
        try:
            with STORAGE_OPERATION_DURATION.labels(tier='warm', operation='query').time():
                # Use first available client (can implement load balancing)
                client = self.clients[0]
                
                result = await asyncio.get_event_loop().run_in_executor(
                    self.executor, client.execute, query, params or {}
                )
                
                STORAGE_OPERATIONS_TOTAL.labels(tier='warm', operation='query').inc()
                return result
                
        except ClickHouseError as e:
            STORAGE_ERRORS.labels(tier='warm', error_type='clickhouse_error').inc()
            logger.error("ClickHouse query error", query=query[:100], error=str(e))
            return []
        except Exception as e:
            STORAGE_ERRORS.labels(tier='warm', error_type='unexpected').inc()
            logger.error("Unexpected error in warm storage query", error=str(e))
            return []
    
    async def insert_events(self, events: List[Dict[str, Any]], table: str = 'events') -> bool:
        """Insert events into ClickHouse"""
        try:
            with STORAGE_OPERATION_DURATION.labels(tier='warm', operation='insert').time():
                client = self.clients[0]
                
                # Prepare data for insertion
                columns = ['event_id', 'user_id', 'session_id', 'event_type', 'event_name',
                          'properties', 'url', 'country', 'city', 'timestamp', 'date_partition']
                
                data = []
                for event in events:
                    row = [
                        event.get('event_id', ''),
                        event.get('user_id'),
                        event.get('session_id', ''),
                        event.get('event_type', ''),
                        event.get('event_name', ''),
                        json.dumps(event.get('properties', {})),
                        event.get('url', ''),
                        event.get('country', ''),
                        event.get('city', ''),
                        event.get('timestamp', datetime.utcnow()),
                        event.get('date_partition', datetime.utcnow().strftime('%Y-%m-%d'))
                    ]
                    data.append(row)
                
                await asyncio.get_event_loop().run_in_executor(
                    self.executor,
                    client.execute,
                    f'INSERT INTO {table} ({",".join(columns)}) VALUES',
                    data
                )
                
                STORAGE_OPERATIONS_TOTAL.labels(tier='warm', operation='insert').inc()
                return True
                
        except Exception as e:
            STORAGE_ERRORS.labels(tier='warm', error_type='insert_error').inc()
            logger.error("ClickHouse insert error", events_count=len(events), error=str(e))
            return False
    
    # Implement abstract methods (not typically used directly for ClickHouse)
    async def get(self, key: str) -> Optional[Any]:
        return None
    
    async def put(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        return False
    
    async def delete(self, key: str) -> bool:
        return False
    
    async def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        return {}
    
    async def batch_put(self, items: Dict[str, Any], ttl: Optional[int] = None) -> int:
        return 0

class ColdStorage(StorageTier):
    """MinIO/S3 for cold data (long-term storage, high latency)"""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.s3_client = None
        
    async def initialize(self):
        """Initialize S3/MinIO client"""
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key
            )
            
            # Ensure bucket exists
            try:
                self.s3_client.head_bucket(Bucket=self.bucket)
            except ClientError:
                self.s3_client.create_bucket(Bucket=self.bucket)
            
            logger.info("S3/MinIO client initialized", bucket=self.bucket)
            
        except Exception as e:
            logger.error("Failed to initialize S3/MinIO", error=str(e))
            raise
    
    async def put_object(self, key: str, data: bytes, metadata: Optional[Dict] = None) -> bool:
        """Put object to cold storage"""
        try:
            with STORAGE_OPERATION_DURATION.labels(tier='cold', operation='put').time():
                loop = asyncio.get_event_loop()
                
                await loop.run_in_executor(
                    None,
                    self.s3_client.put_object,
                    {
                        'Bucket': self.bucket,
                        'Key': key,
                        'Body': data,
                        'Metadata': metadata or {}
                    }
                )
                
                STORAGE_OPERATIONS_TOTAL.labels(tier='cold', operation='put').inc()
                return True
                
        except ClientError as e:
            STORAGE_ERRORS.labels(tier='cold', error_type='s3_error').inc()
            logger.error("S3 put error", key=key, error=str(e))
            return False
    
    async def get_object(self, key: str) -> Optional[bytes]:
        """Get object from cold storage"""
        try:
            with STORAGE_OPERATION_DURATION.labels(tier='cold', operation='get').time():
                loop = asyncio.get_event_loop()
                
                response = await loop.run_in_executor(
                    None,
                    self.s3_client.get_object,
                    {'Bucket': self.bucket, 'Key': key}
                )
                
                STORAGE_OPERATIONS_TOTAL.labels(tier='cold', operation='get').inc()
                return response['Body'].read()
                
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchKey':
                STORAGE_ERRORS.labels(tier='cold', error_type='s3_error').inc()
                logger.error("S3 get error", key=key, error=str(e))
            return None
    
    # Implement abstract methods
    async def get(self, key: str) -> Optional[Any]:
        data = await self.get_object(key)
        if data:
            return orjson.loads(data)
        return None
    
    async def put(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        data = orjson.dumps(value)
        return await self.put_object(key, data)
    
    async def delete(self, key: str) -> bool:
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self.s3_client.delete_object,
                {'Bucket': self.bucket, 'Key': key}
            )
            return True
        except Exception:
            return False
    
    async def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        result = {}
        for key in keys:
            value = await self.get(key)
            if value:
                result[key] = value
        return result
    
    async def batch_put(self, items: Dict[str, Any], ttl: Optional[int] = None) -> int:
        successful = 0
        for key, value in items.items():
            if await self.put(key, value):
                successful += 1
        return successful

class SearchStorage:
    """Elasticsearch for search and text analytics"""
    
    def __init__(self, hosts: List[str], index_prefix: str = 'events'):
        self.hosts = hosts
        self.index_prefix = index_prefix
        self.es_client = None
        
    async def initialize(self):
        """Initialize Elasticsearch client"""
        try:
            self.es_client = AsyncElasticsearch(
                hosts=self.hosts,
                retry_on_timeout=True,
                max_retries=3
            )
            
            # Test connection
            await self.es_client.ping()
            logger.info("Elasticsearch client initialized", hosts=self.hosts)
            
        except Exception as e:
            logger.error("Failed to initialize Elasticsearch", error=str(e))
            raise
    
    async def index_events(self, events: List[Dict[str, Any]], index_suffix: str = None) -> int:
        """Index events for search"""
        try:
            index_name = f"{self.index_prefix}-{index_suffix or datetime.utcnow().strftime('%Y-%m')}"
            
            # Prepare bulk index operations
            operations = []
            for event in events:
                operations.append({
                    "index": {
                        "_index": index_name,
                        "_id": event.get('event_id')
                    }
                })
                operations.append(event)
            
            response = await self.es_client.bulk(operations=operations)
            
            # Count successful operations
            successful = 0
            for item in response['items']:
                if 'index' in item and item['index'].get('status') in [200, 201]:
                    successful += 1
            
            return successful
            
        except ElasticsearchException as e:
            logger.error("Elasticsearch index error", events_count=len(events), error=str(e))
            return 0
    
    async def search(self, query: Dict[str, Any], index_pattern: str = None) -> Dict[str, Any]:
        """Search events"""
        try:
            index_name = index_pattern or f"{self.index_prefix}-*"
            
            response = await self.es_client.search(
                index=index_name,
                body=query
            )
            
            return response
            
        except ElasticsearchException as e:
            logger.error("Elasticsearch search error", error=str(e))
            return {}

class MultiTierStorage:
    """Orchestrates access across storage tiers based on data lifecycle"""
    
    def __init__(self):
        self.hot_storage: Optional[HotStorage] = None
        self.warm_storage: Optional[WarmStorage] = None
        self.cold_storage: Optional[ColdStorage] = None
        self.search_storage: Optional[SearchStorage] = None
        
    async def initialize(self, config: Dict[str, Any]):
        """Initialize all storage tiers"""
        
        # Initialize hot storage (Redis)
        if 'hot_storage' in config:
            hot_config = config['hot_storage']
            self.hot_storage = HotStorage(hot_config['cluster_nodes'])
            await self.hot_storage.initialize()
        
        # Initialize warm storage (ClickHouse)
        if 'warm_storage' in config:
            warm_config = config['warm_storage']
            self.warm_storage = WarmStorage(warm_config['hosts'], warm_config.get('database', 'analytics'))
            await self.warm_storage.initialize()
        
        # Initialize cold storage (S3/MinIO)
        if 'cold_storage' in config:
            cold_config = config['cold_storage']
            self.cold_storage = ColdStorage(
                cold_config['endpoint'],
                cold_config['access_key'],
                cold_config['secret_key'],
                cold_config['bucket']
            )
            await self.cold_storage.initialize()
        
        # Initialize search storage (Elasticsearch)
        if 'search_storage' in config:
            search_config = config['search_storage']
            self.search_storage = SearchStorage(search_config['hosts'])
            await self.search_storage.initialize()
        
        logger.info("Multi-tier storage initialized successfully")
    
    async def store_event(self, event: Dict[str, Any]) -> bool:
        """Store event across appropriate tiers based on data lifecycle"""
        try:
            event_id = event.get('event_id')
            user_id = event.get('user_id')
            
            # Store in hot storage for real-time access (with TTL)
            if self.hot_storage:
                await self.hot_storage.put(f"event:{event_id}", event, ttl=3600)  # 1 hour
                
                # Update real-time counters
                current_minute = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
                await self.hot_storage.increment_counter(f"events:count:{current_minute}", ttl=3600)
                
                if user_id:
                    await self.hot_storage.increment_counter(f"events:user:{user_id}:{current_minute}", ttl=3600)
            
            # Store in warm storage for analytics
            if self.warm_storage:
                await self.warm_storage.insert_events([event])
            
            # Index in search storage
            if self.search_storage:
                await self.search_storage.index_events([event])
            
            return True
            
        except Exception as e:
            logger.error("Error storing event across tiers", event_id=event.get('event_id'), error=str(e))
            return False
    
    async def get_realtime_metrics(self, time_window: str = '1m') -> Dict[str, Any]:
        """Get real-time metrics from hot storage"""
        if not self.hot_storage:
            return {}
        
        current_time = datetime.utcnow()
        
        if time_window == '1m':
            key = current_time.strftime('%Y-%m-%d-%H-%M')
            events_count = await self.hot_storage.get(f"events:count:{key}") or 0
            
            return {
                'time_window': time_window,
                'events_count': events_count,
                'timestamp': current_time.isoformat()
            }
        
        return {}
    
    async def query_analytics(self, query: str, params: Optional[Dict] = None) -> List[Any]:
        """Query analytical data from warm storage"""
        if not self.warm_storage:
            return []
        
        return await self.warm_storage.execute_query(query, params)
    
    async def search_events(self, search_query: Dict[str, Any]) -> Dict[str, Any]:
        """Search events using search storage"""
        if not self.search_storage:
            return {}
        
        return await self.search_storage.search(search_query)