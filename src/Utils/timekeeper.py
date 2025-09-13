import asyncio
import redis.asyncio as redis
from typing import Dict, Optional, List, Set, Any, Tuple, Union
import logging
from datetime import datetime, timedelta
import json
import os
import time
import hashlib
import numpy as np
import msgpack
import zlib
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from cachetools import TTLCache, LRUCache
from dotenv import load_dotenv
import weakref

# Import the permission system
from .permissions import PermissionMixin

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# EXCEPTIONS
# ============================================================================

class TimeTrackerError(Exception):
    """Base exception for TimeTracker errors"""
    pass


class ConnectionError(TimeTrackerError):
    """Raised when Redis connection fails"""
    pass


class CategoryError(TimeTrackerError):
    """Raised for category related errors"""
    pass


class ValidationError(TimeTrackerError):
    """Raised for input validation errors"""
    pass


class CircuitBreakerOpenError(TimeTrackerError):
    """Raised when circuit breaker is open"""
    pass


class PermissionError(TimeTrackerError):
    """Raised when user lacks permissions"""
    pass


# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class TimeEntry:
    """Structured time entry"""
    server_id: int
    user_id: int
    category: str
    seconds: int
    timestamp: datetime
    session_id: Optional[str] = None


@dataclass
class UserStats:
    """User statistics"""
    total_time: int
    categories: Dict[str, int]
    productivity_score: float
    streak_days: int
    last_activity: datetime
    session_count: int
    is_suspended: bool = False


@dataclass
class ServerStats:
    """Server statistics"""
    total_users: int
    active_users: int
    total_time: int
    categories: List[str]
    category_totals: Dict[str, int]
    daily_average: float
    peak_hours: Dict[int, int]
    system_enabled: bool = True
    suspended_users: int = 0


@dataclass
class Prediction:
    """Prediction result"""
    value: float
    confidence: float
    trend: str
    trend_strength: float


@dataclass
class PermissionCheck:
    """Permission check result"""
    allowed: bool
    reason: str
    required_roles: List[int] = None
    is_suspended: bool = False
    system_enabled: bool = True


# ============================================================================
# CIRCUIT BREAKER
# ============================================================================

class CircuitBreaker:
    """Circuit breaker for fault tolerance"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60, half_open_limit: int = 3):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_limit = half_open_limit
        
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = asyncio.Lock()
    
    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        async with self._lock:
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = "HALF_OPEN"
                    self.success_count = 0
                else:
                    raise CircuitBreakerOpenError("Circuit breaker is OPEN")
            
            elif self.state == "HALF_OPEN":
                if self.success_count >= self.half_open_limit:
                    self.state = "CLOSED"
                    self.failure_count = 0
        
        try:
            result = await func(*args, **kwargs)
            
            async with self._lock:
                if self.state == "HALF_OPEN":
                    self.success_count += 1
                elif self.state == "CLOSED":
                    self.failure_count = 0
            
            return result
            
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = "OPEN"
            
            raise e
    
    @property
    def status(self) -> Dict[str, Any]:
        """Get circuit breaker status"""
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time
        }


# ============================================================================
# BATCH PROCESSOR
# ============================================================================

class BatchProcessor:
    """High-performance batch operation processor"""
    
    def __init__(self, redis_client: redis.Redis, batch_size: int = 100, flush_interval: float = 1.0):
        self.redis = redis_client
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        
        self.pending_operations = deque()
        self.last_flush = time.time()
        self._flush_lock = asyncio.Lock()
        self._background_task = None
    
    async def start(self):
        """Start background batch processor"""
        if self._background_task is None:
            self._background_task = asyncio.create_task(self._background_flush())
    
    async def stop(self):
        """Stop background processor and flush remaining operations"""
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
            self._background_task = None
        
        await self.flush()
    
    async def queue_operation(self, operation_type: str, **kwargs):
        """Queue operation for batch processing"""
        self.pending_operations.append({
            "type": operation_type,
            "timestamp": time.time(),
            **kwargs
        })
        
        if len(self.pending_operations) >= self.batch_size:
            await self.flush()
    
    async def flush(self):
        """Process all pending operations"""
        async with self._flush_lock:
            if not self.pending_operations:
                return
            
            operations = list(self.pending_operations)
            self.pending_operations.clear()
            self.last_flush = time.time()
            
            # Group by operation type for efficient processing
            grouped = defaultdict(list)
            for op in operations:
                grouped[op["type"]].append(op)
            
            # Process each type
            tasks = []
            for op_type, ops in grouped.items():
                if op_type == "update_leaderboard":
                    tasks.append(self._batch_update_leaderboards(ops))
                elif op_type == "update_aggregation":
                    tasks.append(self._batch_update_aggregations(ops))
                elif op_type == "update_timeseries":
                    tasks.append(self._batch_update_timeseries(ops))
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _background_flush(self):
        """Background task to flush operations periodically"""
        while True:
            try:
                await asyncio.sleep(self.flush_interval)
                if time.time() - self.last_flush >= self.flush_interval:
                    await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Background flush error: {e}")
    
    async def _batch_update_leaderboards(self, operations: List[Dict]):
        """Batch update leaderboard sorted sets"""
        pipe = self.redis.pipeline()
        
        # Group by leaderboard key
        leaderboard_updates = defaultdict(dict)
        for op in operations:
            key = f"leaderboard:{op['server_id']}:{op['category']}"
            leaderboard_updates[key][str(op['user_id'])] = op['total']
        
        # Batch update all leaderboards
        for key, updates in leaderboard_updates.items():
            pipe.zadd(key, updates)
            pipe.expire(key, 86400 * 30)  # 30-day expiry
        
        await pipe.execute()
    
    async def _batch_update_aggregations(self, operations: List[Dict]):
        """Batch update aggregation hashes"""
        pipe = self.redis.pipeline()
        
        # Group by aggregation key
        agg_updates = defaultdict(lambda: defaultdict(int))
        for op in operations:
            now = datetime.fromtimestamp(op['timestamp'])
            keys = [
                f"agg:server:{op['server_id']}:hourly:{now.strftime('%Y-%m-%d-%H')}",
                f"agg:server:{op['server_id']}:daily:{now.strftime('%Y-%m-%d')}",
                f"agg:user:{op['user_id']}:daily:{now.strftime('%Y-%m-%d')}",
                f"agg:category:{op['server_id']}:{op['category']}:daily:{now.strftime('%Y-%m-%d')}"
            ]
            
            for key in keys:
                agg_updates[key]["total"] += op['seconds']
                agg_updates[key][f"cat:{op['category']}"] += op['seconds']
        
        # Batch update all aggregations
        for key, updates in agg_updates.items():
            for field, value in updates.items():
                pipe.hincrby(key, field, value)
            pipe.expire(key, 86400 * 90)  # 90-day retention
        
        await pipe.execute()
    
    async def _batch_update_timeseries(self, operations: List[Dict]):
        """Batch update time series streams"""
        pipe = self.redis.pipeline()
        
        for op in operations:
            timeseries_key = f"timeseries:{op['server_id']}:{op['user_id']}"
            pipe.xadd(timeseries_key, {
                "category": op['category'],
                "seconds": op['seconds'],
                "total": op.get('total', 0)
            })
            pipe.xtrim(timeseries_key, maxlen=10000)  # Keep last 10k entries
        
        await pipe.execute()


# ============================================================================
# ANALYTICS ENGINE
# ============================================================================

class AnalyticsEngine:
    """Advanced analytics and predictive modeling"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self._prediction_cache = TTLCache(maxsize=1000, ttl=3600)  # 1-hour cache
    
    async def get_activity_patterns(self, server_id: int, user_id: int, days: int = 30) -> Dict[str, Any]:
        """Analyze user activity patterns"""
        timeseries_key = f"timeseries:{server_id}:{user_id}"
        
        # Get recent entries
        cutoff_timestamp = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        entries = await self.redis.xrevrange(
            timeseries_key, 
            min=cutoff_timestamp,
            count=5000
        )
        
        if not entries:
            return self._empty_patterns()
        
        # Process patterns
        hourly_activity = defaultdict(int)
        daily_activity = defaultdict(int)
        category_trends = defaultdict(list)
        session_durations = []
        
        for entry_id, fields in entries:
            timestamp = int(entry_id.split('-')[0]) / 1000
            dt = datetime.fromtimestamp(timestamp)
            
            category = fields.get('category', 'unknown')
            seconds = int(fields.get('seconds', 0))
            
            hourly_activity[dt.hour] += seconds
            daily_activity[dt.weekday()] += seconds
            category_trends[category].append((timestamp, seconds))
            session_durations.append(seconds)
        
        return {
            "peak_hours": dict(hourly_activity),
            "peak_days": dict(daily_activity),
            "category_trends": dict(category_trends),
            "total_sessions": len(entries),
            "average_session": np.mean(session_durations) if session_durations else 0,
            "session_variance": np.var(session_durations) if session_durations else 0
        }
    
    async def calculate_productivity_score(self, server_id: int, user_id: int) -> float:
        """Calculate productivity score (0-100)"""
        cache_key = f"productivity:{server_id}:{user_id}"
        if cache_key in self._prediction_cache:
            return self._prediction_cache[cache_key]
        
        patterns = await self.get_activity_patterns(server_id, user_id, days=14)
        
        if patterns["total_sessions"] == 0:
            return 0.0
        
        score = 0.0
        
        # Consistency score (30 points) - regular daily activity
        daily_values = list(patterns["peak_days"].values())
        if daily_values:
            consistency = 1 - (np.var(daily_values) / (np.mean(daily_values) + 1))
            score += max(0, consistency) * 30
        
        # Peak hours score (25 points) - productive time slots
        productive_hours = [9, 10, 11, 14, 15, 16, 17]
        total_productive = sum(patterns["peak_hours"].get(h, 0) for h in productive_hours)
        total_time = sum(patterns["peak_hours"].values())
        if total_time > 0:
            score += (total_productive / total_time) * 25
        
        # Activity level (25 points) - session frequency
        sessions_per_day = patterns["total_sessions"] / 14
        activity_normalized = min(sessions_per_day / 5, 1.0)  # Normalize to 5 sessions/day
        score += activity_normalized * 25
        
        # Session quality (20 points) - consistent session lengths
        if patterns["average_session"] > 0:
            session_quality = 1 - min(patterns["session_variance"] / (patterns["average_session"] ** 2), 1)
            score += session_quality * 20
        
        final_score = min(score, 100.0)
        self._prediction_cache[cache_key] = final_score
        return final_score
    
    async def predict_weekly_total(self, server_id: int, user_id: int) -> Prediction:
        """Predict weekly total time using trend analysis"""
        cache_key = f"prediction:{server_id}:{user_id}"
        if cache_key in self._prediction_cache:
            return self._prediction_cache[cache_key]
        
        # Get 12 weeks of historical data
        historical = await self._get_weekly_totals(server_id, user_id, weeks=12)
        
        if len(historical) < 3:
            return Prediction(value=0, confidence=0, trend="insufficient_data", trend_strength=0)
        
        # Linear regression for trend analysis
        weeks = np.array(range(len(historical)))
        totals = np.array(list(historical.values()))
        
        # Calculate trend line
        A = np.vstack([weeks, np.ones(len(weeks))]).T
        slope, intercept = np.linalg.lstsq(A, totals, rcond=None)[0]
        
        # Predict next week
        next_week = len(weeks)
        prediction = slope * next_week + intercept
        
        # Calculate confidence based on R-squared
        y_pred = slope * weeks + intercept
        ss_res = np.sum((totals - y_pred) ** 2)
        ss_tot = np.sum((totals - np.mean(totals)) ** 2)
        r_squared = 1 - (ss_res / (ss_tot + 1e-8))
        confidence = max(0, min(1, r_squared))
        
        # Determine trend
        trend = "increasing" if slope > 0 else "decreasing" if slope < 0 else "stable"
        trend_strength = abs(slope) / (np.mean(totals) + 1)
        
        result = Prediction(
            value=max(0, prediction),
            confidence=confidence,
            trend=trend,
            trend_strength=trend_strength
        )
        
        self._prediction_cache[cache_key] = result
        return result
    
    async def get_insights(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Get comprehensive user insights"""
        patterns = await self.get_activity_patterns(server_id, user_id)
        productivity = await self.calculate_productivity_score(server_id, user_id)
        prediction = await self.predict_weekly_total(server_id, user_id)
        
        # Find peak productivity hour
        peak_hour = max(patterns["peak_hours"].items(), key=lambda x: x[1], default=(9, 0))[0]
        
        # Find most productive day
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        peak_day_idx = max(patterns["peak_days"].items(), key=lambda x: x[1], default=(0, 0))[0]
        peak_day = days[peak_day_idx]
        
        # Category insights
        category_ranking = sorted(
            patterns["category_trends"].items(),
            key=lambda x: sum(entry[1] for entry in x[1]),
            reverse=True
        )
        
        return {
            "productivity_score": productivity,
            "peak_hour": peak_hour,
            "peak_day": peak_day,
            "top_categories": [cat for cat, _ in category_ranking[:5]],
            "weekly_prediction": asdict(prediction),
            "session_quality": {
                "average_duration": patterns["average_session"],
                "consistency": 1 - min(patterns["session_variance"] / (patterns["average_session"] ** 2 + 1), 1)
            },
            "activity_level": patterns["total_sessions"] / 30  # Sessions per day
        }
    
    def _empty_patterns(self) -> Dict[str, Any]:
        """Return empty patterns structure"""
        return {
            "peak_hours": {},
            "peak_days": {},
            "category_trends": {},
            "total_sessions": 0,
            "average_session": 0,
            "session_variance": 0
        }
    
    async def _get_weekly_totals(self, server_id: int, user_id: int, weeks: int) -> Dict[str, int]:
        """Get weekly totals for trend analysis"""
        pipe = self.redis.pipeline()
        weekly_totals = {}
        
        for i in range(weeks):
            week_start = datetime.now() - timedelta(weeks=i+1)
            week_key = week_start.strftime('%Y-%W')
            daily_keys = []
            
            # Get all days in the week
            for day in range(7):
                day_date = (week_start + timedelta(days=day)).strftime('%Y-%m-%d')
                daily_keys.append(f"agg:user:{user_id}:daily:{day_date}")
            
            # Batch get all daily totals for the week
            for key in daily_keys:
                pipe.hget(key, "total")
        
        results = await pipe.execute()
        
        # Process results into weekly totals
        week_idx = 0
        for i in range(0, len(results), 7):
            week_total = 0
            for j in range(7):
                if i + j < len(results) and results[i + j]:
                    week_total += int(results[i + j])
            
            week_start = datetime.now() - timedelta(weeks=week_idx+1)
            week_key = week_start.strftime('%Y-%W')
            weekly_totals[week_key] = week_total
            week_idx += 1
        
        return weekly_totals


# ============================================================================
# MAIN TIME TRACKER WITH PERMISSION INTEGRATION
# ============================================================================

class UltimateTimeTracker(PermissionMixin):
    """
    Production-ready time tracker with integrated permission system
    """
    
    def __init__(self, redis_url: Optional[str] = None, pool_size: int = 50):
        """Initialize the ultimate time tracker with permission system"""
        self.redis_url = redis_url or os.getenv('REDIS_URL')
        if not self.redis_url:
            raise TimeTrackerError("Redis URL not provided")
        
        # Connection pool
        self.pool = redis.ConnectionPool.from_url(
            self.redis_url,
            max_connections=pool_size,
            retry_on_timeout=True,
            retry_on_error=[redis.ConnectionError, redis.TimeoutError],
            socket_keepalive=True,
            socket_keepalive_options={1: 1, 2: 3, 3: 5}
        )
        
        self.redis: Optional[redis.Redis] = None
        self._connected = False
        
        # Components
        self.circuit_breaker = CircuitBreaker()
        self.batch_processor: Optional[BatchProcessor] = None
        self.analytics: Optional[AnalyticsEngine] = None
        
        # Multi-level caching
        self.category_cache = TTLCache(maxsize=1000, ttl=300)  # 5 min
        self.user_cache = LRUCache(maxsize=5000)  # Most recent users
        self.stats_cache = TTLCache(maxsize=100, ttl=60)  # 1 min
        self.leaderboard_cache = TTLCache(maxsize=500, ttl=30)  # 30 sec
        
        # Permission system cache
        self.permission_cache = TTLCache(maxsize=1000, ttl=300)  # 5 min permission cache
        
        # Fallback storage for circuit breaker
        self.fallback_storage = {}
        self._fallback_lock = asyncio.Lock()
        
        # Cleanup tracking
        self._instances = weakref.WeakSet()
        self._instances.add(self)
    
    async def connect(self):
        """Connect to Redis and initialize components"""
        try:
            self.redis = redis.Redis(connection_pool=self.pool)
            await self.redis.ping()
            self._connected = True
            
            # Initialize components
            self.batch_processor = BatchProcessor(self.redis)
            await self.batch_processor.start()
            
            self.analytics = AnalyticsEngine(self.redis)
            
            logger.info("Connected to Redis and initialized components")
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Redis: {e}")
    
    async def close(self):
        """Close connections and cleanup"""
        if self.batch_processor:
            await self.batch_processor.stop()
        
        if self.redis:
            await self.redis.aclose()
            self._connected = False
        
        # Clear caches
        self.category_cache.clear()
        self.user_cache.clear()
        self.stats_cache.clear()
        self.leaderboard_cache.clear()
        self.permission_cache.clear()
        
        logger.info("Closed Redis connection and cleaned up resources")
    
    # ========================================================================
    # VALIDATION
    # ========================================================================
    
    def _validate_ids(self, server_id: int, user_id: int):
        """Validate server and user IDs"""
        if not isinstance(server_id, int) or server_id <= 0:
            raise ValidationError("Server ID must be a positive integer")
        if not isinstance(user_id, int) or user_id <= 0:
            raise ValidationError("User ID must be a positive integer")
    
    def _validate_category_name(self, category: str):
        """Validate category name"""
        if not isinstance(category, str):
            raise ValidationError("Category must be a string")
        if not category.strip():
            raise ValidationError("Category name cannot be empty")
        if len(category) > 50:
            raise ValidationError("Category name too long (max 50 characters)")
        if any(char in category for char in [':', '\n', '\r', '\t']):
            raise ValidationError("Category name contains invalid characters")
    
    def _validate_time_input(self, seconds: int):
        """Validate time input"""
        if not isinstance(seconds, int):
            raise ValidationError("Time must be an integer")
        if seconds < 0:
            raise ValidationError("Time must be non-negative")
        if seconds > 86400 * 7:  # More than a week
            raise ValidationError("Time value too large (max 7 days)")
    
    def _ensure_connected(self):
        """Ensure Redis is connected"""
        if not self._connected or not self.redis:
            raise ConnectionError("Not connected to Redis. Call connect() first.")
    
    # ========================================================================
    # KEY GENERATION
    # ========================================================================
    
    def _get_server_hash_key(self, server_id: int) -> str:
        """Get Redis hash key for server user data"""
        return f"server:{server_id}:users"
    
    def _get_server_metadata_key(self, server_id: int) -> str:
        """Get Redis key for server metadata"""
        return f"server:{server_id}:metadata"
    
    def _get_session_key(self, server_id: int, user_id: int) -> str:
        """Get Redis key for user session"""
        return f"session:{server_id}:{user_id}"
    
    def _get_leaderboard_key(self, server_id: int, category: str) -> str:
        """Get Redis key for category leaderboard"""
        return f"leaderboard:{server_id}:{category}"
    
    def _get_timeseries_key(self, server_id: int, user_id: int) -> str:
        """Get Redis key for user time series"""
        return f"timeseries:{server_id}:{user_id}"
    
    # ========================================================================
    # DATA COMPRESSION
    # ========================================================================
    
    def _compress_data(self, data: Dict) -> str:
        """Compress data using msgpack and zlib"""
        packed = msgpack.packb(data)
        if len(packed) > 100:  # Only compress larger payloads
            compressed = zlib.compress(packed)
            return f"compressed:{compressed.hex()}"
        return f"msgpack:{packed.hex()}"
    
    def _decompress_data(self, raw_data: str) -> Dict:
        """Decompress data"""
        if not raw_data:
            return {}
        
        try:
            if raw_data.startswith("compressed:"):
                compressed_hex = raw_data[11:]
                compressed_data = bytes.fromhex(compressed_hex)
                packed_data = zlib.decompress(compressed_data)
                return msgpack.unpackb(packed_data, strict_map_key=False)
            
            elif raw_data.startswith("msgpack:"):
                packed_hex = raw_data[8:]
                packed_data = bytes.fromhex(packed_hex)
                return msgpack.unpackb(packed_data, strict_map_key=False)
            
            else:
                # Legacy JSON format
                return json.loads(raw_data)
                
        except Exception as e:
            logger.error(f"Failed to decompress data: {e}")
            return {}
    
    # ========================================================================
    # CORE DATA OPERATIONS
    # ========================================================================
    
    async def _get_user_data(self, server_id: int, user_id: int) -> Dict[str, int]:
        """Get user's category data"""
        cache_key = f"{server_id}:{user_id}"
        if cache_key in self.user_cache:
            return self.user_cache[cache_key].copy()
        
        self._ensure_connected()
        server_key = self._get_server_hash_key(server_id)
        raw_data = await self.redis.hget(server_key, str(user_id))
        
        data = self._decompress_data(raw_data) if raw_data else {}
        self.user_cache[cache_key] = data
        return data.copy()
    
    async def _set_user_data(self, server_id: int, user_id: int, data: Dict[str, int]):
        """Set user's category data with compression"""
        self._ensure_connected()
        server_key = self._get_server_hash_key(server_id)
        compressed_data = self._compress_data(data)
        await self.redis.hset(server_key, str(user_id), compressed_data)
        
        # Update cache
        cache_key = f"{server_id}:{user_id}"
        self.user_cache[cache_key] = data.copy()
    
    async def _ensure_server_metadata(self, server_id: int, categories: Set[str] = None):
        """Ensure server metadata exists and update categories atomically"""
        cache_key = f"metadata:{server_id}"
        
        # Check cache first
        if categories and cache_key in self.category_cache:
            existing_categories = self.category_cache[cache_key]
            if categories.issubset(existing_categories):
                return  # All categories already exist
        
        self._ensure_connected()
        metadata_key = self._get_server_metadata_key(server_id)
        
        # Atomic update using pipeline
        async with self.redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(metadata_key)
                    
                    existing_data = await self.redis.hgetall(metadata_key)
                    if existing_data and 'categories' in existing_data:
                        try:
                            existing_categories = set(json.loads(existing_data['categories']))
                        except json.JSONDecodeError:
                            existing_categories = set()
                    else:
                        existing_categories = set()
                    
                    # Combine with new categories
                    if categories:
                        all_categories = existing_categories | categories
                    else:
                        all_categories = existing_categories or {'general'}
                    
                    # Update if changed
                    if all_categories != existing_categories:
                        pipe.multi()
                        metadata = {
                            'categories': json.dumps(list(all_categories)),
                            'created_at': existing_data.get('created_at', datetime.now().isoformat()),
                            'updated_at': datetime.now().isoformat()
                        }
                        await pipe.hset(metadata_key, mapping=metadata)
                        await pipe.execute()
                        
                        # Update cache
                        self.category_cache[cache_key] = all_categories
                    
                    break
                    
                except redis.WatchError:
                    continue
    
    # ========================================================================
    # ENHANCED PUBLIC API WITH PERMISSION CHECKS
    # ========================================================================
    
    async def add_time_with_permissions(self, server_id: int, user_id: int, category: str, seconds: int, 
                                       user_role_ids: List[int] = None) -> Tuple[int, PermissionCheck]:
        """
        Add time with permission checking
        
        Returns:
            (new_total, permission_check_result)
        """
        # Check permissions first
        permission_check = await self.check_user_permissions(server_id, user_id, user_role_ids or [])
        if not permission_check.allowed:
            return 0, permission_check
        
        # Proceed with time addition
        new_total = await self.add_time(server_id, user_id, category, seconds)
        return new_total, permission_check
    
    async def check_user_permissions(self, server_id: int, user_id: int, user_role_ids: List[int]) -> PermissionCheck:
        """
        Check if user has permission to use time tracking
        
        Returns:
            PermissionCheck with detailed information
        """
        try:
            can_access, reason = await self.check_user_access(server_id, user_id, user_role_ids)
            
            if can_access:
                return PermissionCheck(allowed=True, reason="Access granted", system_enabled=True)
            
            # Get detailed permission info for better error messages
            permissions = await self.get_server_permissions(server_id)
            
            return PermissionCheck(
                allowed=False,
                reason=reason,
                required_roles=permissions.get("required_roles", []),
                is_suspended=user_id in permissions.get("suspended_users", []),
                system_enabled=permissions.get("enabled", True)
            )
            
        except Exception as e:
            logger.error(f"Permission check failed: {e}")
            return PermissionCheck(
                allowed=False,
                reason="Permission check failed - contact administrator",
                system_enabled=False
            )
    
    async def add_time(self, server_id: int, user_id: int, category: str, seconds: int) -> int:
        """
        Add time to a category with full optimization
        
        Returns:
            New total time for the category
        """
        # Validate inputs
        self._validate_ids(server_id, user_id)
        self._validate_category_name(category)
        self._validate_time_input(seconds)
        
        try:
            return await self.circuit_breaker.call(
                self._add_time_internal, server_id, user_id, category, seconds
            )
        except CircuitBreakerOpenError:
            # Use fallback storage
            return await self._add_time_fallback(server_id, user_id, category, seconds)
    
    async def _add_time_internal(self, server_id: int, user_id: int, category: str, seconds: int) -> int:
        """Internal add time implementation"""
        await self._ensure_server_metadata(server_id, {category})
        
        server_key = self._get_server_hash_key(server_id)
        user_id_str = str(user_id)
        
        # Atomic update using optimistic locking
        async with self.redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(server_key)
                    
                    # Get current data
                    raw_data = await self.redis.hget(server_key, user_id_str)
                    user_data = self._decompress_data(raw_data) if raw_data else {}
                    
                    # Calculate new total
                    current_time = user_data.get(category, 0)
                    new_time = current_time + seconds
                    user_data[category] = new_time
                    
                    # Execute atomic update
                    pipe.multi()
                    compressed_data = self._compress_data(user_data)
                    await pipe.hset(server_key, user_id_str, compressed_data)
                    await pipe.execute()
                    
                    # Update cache
                    cache_key = f"{server_id}:{user_id}"
                    self.user_cache[cache_key] = user_data.copy()
                    
                    # Invalidate related caches
                    await self._invalidate_related_caches(server_id, user_id)
                    
                    # Queue background updates
                    if self.batch_processor:
                        await self.batch_processor.queue_operation(
                            "update_leaderboard",
                            server_id=server_id,
                            user_id=user_id,
                            category=category,
                            total=new_time
                        )
                        
                        await self.batch_processor.queue_operation(
                            "update_aggregation",
                            server_id=server_id,
                            user_id=user_id,
                            category=category,
                            seconds=seconds,
                            timestamp=time.time()
                        )
                        
                        await self.batch_processor.queue_operation(
                            "update_timeseries",
                            server_id=server_id,
                            user_id=user_id,
                            category=category,
                            seconds=seconds,
                            total=new_time
                        )
                    
                    return new_time
                    
                except redis.WatchError:
                    continue
    
    async def _add_time_fallback(self, server_id: int, user_id: int, category: str, seconds: int) -> int:
        """Fallback add time when Redis is unavailable"""
        async with self._fallback_lock:
            key = f"{server_id}:{user_id}:{category}"
            current = self.fallback_storage.get(key, 0)
            new_total = current + seconds
            self.fallback_storage[key] = new_total
            
            # Schedule sync when Redis recovers
            asyncio.create_task(self._sync_fallback_data())
            
            return new_total
    
    async def _sync_fallback_data(self):
        """Sync fallback data back to Redis"""
        await asyncio.sleep(60)  # Wait before retry
        
        try:
            if await self.redis.ping():
                async with self._fallback_lock:
                    for key, value in list(self.fallback_storage.items()):
                        parts = key.split(':')
                        if len(parts) == 3:
                            server_id, user_id, category = parts
                            await self._add_time_internal(
                                int(server_id), int(user_id), category, value
                            )
                            del self.fallback_storage[key]
                
                logger.info("Synced fallback data to Redis")
        except Exception as e:
            logger.error(f"Failed to sync fallback data: {e}")
    
    async def get_user_times(self, server_id: int, user_id: int) -> Dict[str, int]:
        """Get all category times for a user"""
        self._validate_ids(server_id, user_id)
        
        user_data = await self._get_user_data(server_id, user_id)
        categories = await self.get_server_categories(server_id)
        
        # Fill missing categories with 0
        result = {}
        for category in categories:
            result[category] = user_data.get(category, 0)
        
        return result
    
    async def set_user_time(self, server_id: int, user_id: int, category: str, seconds: int) -> None:
        """Set specific category time (atomic operation)"""
        self._validate_ids(server_id, user_id)
        self._validate_category_name(category)
        self._validate_time_input(seconds)
        
        await self._ensure_server_metadata(server_id, {category})
        
        server_key = self._get_server_hash_key(server_id)
        user_id_str = str(user_id)
        
        async with self.redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(server_key)
                    
                    raw_data = await self.redis.hget(server_key, user_id_str)
                    user_data = self._decompress_data(raw_data) if raw_data else {}
                    user_data[category] = seconds
                    
                    pipe.multi()
                    compressed_data = self._compress_data(user_data)
                    await pipe.hset(server_key, user_id_str, compressed_data)
                    await pipe.execute()
                    
                    # Update cache and invalidate related
                    cache_key = f"{server_id}:{user_id}"
                    self.user_cache[cache_key] = user_data.copy()
                    await self._invalidate_related_caches(server_id, user_id)
                    
                    break
                    
                except redis.WatchError:
                    continue
    
    async def get_user_total(self, server_id: int, user_id: int) -> int:
        """Get total time across all categories"""
        times = await self.get_user_times(server_id, user_id)
        return sum(times.values())
    
    async def get_server_categories(self, server_id: int) -> Set[str]:
        """Get all categories for a server (cached)"""
        cache_key = f"metadata:{server_id}"
        if cache_key in self.category_cache:
            return self.category_cache[cache_key].copy()
        
        self._ensure_connected()
        metadata_key = self._get_server_metadata_key(server_id)
        categories_json = await self.redis.hget(metadata_key, 'categories')
        
        if categories_json:
            try:
                categories = set(json.loads(categories_json))
                self.category_cache[cache_key] = categories
                return categories.copy()
            except json.JSONDecodeError:
                pass
        
        return set()
    
    async def get_fast_leaderboard(self, server_id: int, category: str = None, limit: int = 10) -> List[Dict]:
        """Get leaderboard using sorted sets (O(log N))"""
        cache_key = (server_id, category or "total", limit)
        if cache_key in self.leaderboard_cache:
            return self.leaderboard_cache[cache_key].copy()
        
        if category:
            # Category-specific leaderboard from sorted set
            leaderboard_key = self._get_leaderboard_key(server_id, category)
            top_users = await self.redis.zrevrange(
                leaderboard_key, 0, limit-1, withscores=True
            )
            
            result = [
                {"user_id": int(user_id), "time": int(score), "category": category}
                for user_id, score in top_users
            ]
        else:
            # Total time leaderboard (fallback to hash scan)
            result = await self._get_total_leaderboard(server_id, limit)
        
        self.leaderboard_cache[cache_key] = result
        return result.copy()
    
    async def _get_total_leaderboard(self, server_id: int, limit: int) -> List[Dict]:
        """Get total time leaderboard by scanning user data"""
        server_key = self._get_server_hash_key(server_id)
        all_data = await self.redis.hgetall(server_key)
        
        user_totals = []
        for user_id_str, raw_data in all_data.items():
            try:
                user_data = self._decompress_data(raw_data)
                total_time = sum(user_data.values())
                if total_time > 0:
                    user_totals.append({
                        "user_id": int(user_id_str),
                        "time": total_time
                    })
            except (ValueError, TypeError):
                continue
        
        user_totals.sort(key=lambda x: x["time"], reverse=True)
        return user_totals[:limit]
    
    async def get_server_stats(self, server_id: int) -> ServerStats:
        """Get comprehensive server statistics with permission info"""
        cache_key = f"stats:{server_id}"
        if cache_key in self.stats_cache:
            return self.stats_cache[cache_key]
        
        categories = await self.get_server_categories(server_id)
        server_key = self._get_server_hash_key(server_id)
        all_data = await self.redis.hgetall(server_key)
        
        # Get permission info
        permissions = await self.get_server_permissions(server_id)
        
        if not all_data:
            stats = ServerStats(
                total_users=0, active_users=0, total_time=0,
                categories=list(categories), category_totals={},
                daily_average=0.0, peak_hours={},
                system_enabled=permissions.get("enabled", True),
                suspended_users=len(permissions.get("suspended_users", []))
            )
            self.stats_cache[cache_key] = stats
            return stats
        
        # Process user data
        total_time = 0
        category_totals = defaultdict(int)
        active_users = 0
        
        for user_id_str, raw_data in all_data.items():
            try:
                user_data = self._decompress_data(raw_data)
                user_total = sum(user_data.values())
                
                if user_total > 0:
                    active_users += 1
                    total_time += user_total
                    
                    for category, time in user_data.items():
                        category_totals[category] += time
                        
            except (ValueError, TypeError):
                continue
        
        # Calculate daily average (assume 30-day period)
        daily_average = total_time / (86400 * 30) if total_time > 0 else 0.0
        
        # Get peak hours from aggregation data
        peak_hours = await self._get_peak_hours(server_id)
        
        stats = ServerStats(
            total_users=len(all_data),
            active_users=active_users,
            total_time=total_time,
            categories=list(categories),
            category_totals=dict(category_totals),
            daily_average=daily_average,
            peak_hours=peak_hours,
            system_enabled=permissions.get("enabled", True),
            suspended_users=len(permissions.get("suspended_users", []))
        )
        
        self.stats_cache[cache_key] = stats
        return stats
    
    async def _get_peak_hours(self, server_id: int) -> Dict[int, int]:
        """Get peak hours from aggregation data"""
        pipe = self.redis.pipeline()
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Get last 7 days of hourly data
        for i in range(7):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            for hour in range(24):
                key = f"agg:server:{server_id}:hourly:{date}-{hour:02d}"
                pipe.hget(key, "total")
        
        results = await pipe.execute()
        
        # Aggregate by hour
        hourly_totals = defaultdict(int)
        for i, result in enumerate(results):
            if result:
                hour = i % 24
                hourly_totals[hour] += int(result)
        
        return dict(hourly_totals)
    
    async def get_user_stats(self, server_id: int, user_id: int) -> UserStats:
        """Get comprehensive user statistics with permission info"""
        self._validate_ids(server_id, user_id)
        
        times = await self.get_user_times(server_id, user_id)
        total_time = sum(times.values())
        
        # Check if user is suspended
        is_suspended = await self.is_user_suspended(server_id, user_id)
        
        if self.analytics:
            productivity_score = await self.analytics.calculate_productivity_score(server_id, user_id)
            patterns = await self.analytics.get_activity_patterns(server_id, user_id)
            session_count = patterns["total_sessions"]
        else:
            productivity_score = 0.0
            session_count = 0
        
        # Calculate streak (simplified - consecutive days with activity)
        streak_days = await self._calculate_streak(server_id, user_id)
        
        return UserStats(
            total_time=total_time,
            categories=times,
            productivity_score=productivity_score,
            streak_days=streak_days,
            last_activity=datetime.now(),  # Would need to track this
            session_count=session_count,
            is_suspended=is_suspended
        )
    
    async def _calculate_streak(self, server_id: int, user_id: int) -> int:
        """Calculate consecutive days with activity"""
        pipe = self.redis.pipeline()
        
        # Check last 30 days
        streak = 0
        for i in range(30):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            key = f"agg:user:{user_id}:daily:{date}"
            pipe.hget(key, "total")
        
        results = await pipe.execute()
        
        # Count consecutive days from today
        for result in results:
            if result and int(result) > 0:
                streak += 1
            else:
                break
        
        return streak
    
    async def get_insights(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Get AI-powered insights"""
        if not self.analytics:
            return {}
        
        return await self.analytics.get_insights(server_id, user_id)
    
    async def predict_weekly_total(self, server_id: int, user_id: int) -> Prediction:
        """Get weekly prediction"""
        if not self.analytics:
            return Prediction(value=0, confidence=0, trend="no_analytics", trend_strength=0)
        
        return await self.analytics.predict_weekly_total(server_id, user_id)
    
    # ========================================================================
    # CATEGORY MANAGEMENT
    # ========================================================================
    
    async def create_category(self, server_id: int, category: str) -> None:
        """Create a new category"""
        self._validate_category_name(category)
        await self._ensure_server_metadata(server_id, {category})
    
    async def rename_category(self, server_id: int, old_name: str, new_name: str) -> None:
        """Rename a category atomically"""
        self._validate_category_name(old_name)
        self._validate_category_name(new_name)
        
        # Complex atomic operation across multiple data structures
        metadata_key = self._get_server_metadata_key(server_id)
        server_key = self._get_server_hash_key(server_id)
        
        async with self.redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(metadata_key, server_key)
                    
                    # Validate category exists
                    categories = await self.get_server_categories(server_id)
                    if old_name not in categories:
                        raise CategoryError(f"Category '{old_name}' does not exist")
                    if new_name in categories:
                        raise CategoryError(f"Category '{new_name}' already exists")
                    
                    # Update categories
                    categories.remove(old_name)
                    categories.add(new_name)
                    
                    # Get all user data
                    all_users = await self.redis.hgetall(server_key)
                    
                    pipe.multi()
                    
                    # Update metadata
                    metadata = {
                        'categories': json.dumps(list(categories)),
                        'created_at': datetime.now().isoformat(),
                        'updated_at': datetime.now().isoformat()
                    }
                    await pipe.hset(metadata_key, mapping=metadata)
                    
                    # Update all user data
                    for user_id_str, raw_data in all_users.items():
                        try:
                            user_data = self._decompress_data(raw_data)
                            if old_name in user_data:
                                user_data[new_name] = user_data.pop(old_name)
                                compressed_data = self._compress_data(user_data)
                                await pipe.hset(server_key, user_id_str, compressed_data)
                        except Exception:
                            continue
                    
                    # Rename leaderboard
                    old_lb_key = self._get_leaderboard_key(server_id, old_name)
                    new_lb_key = self._get_leaderboard_key(server_id, new_name)
                    await pipe.rename(old_lb_key, new_lb_key)
                    
                    await pipe.execute()
                    
                    # Clear caches
                    await self._invalidate_all_caches(server_id)
                    break
                    
                except redis.WatchError:
                    continue
    
    async def remove_category(self, server_id: int, category: str, remove_user_data: bool = False) -> None:
        """Remove a category atomically"""
        self._validate_category_name(category)
        
        metadata_key = self._get_server_metadata_key(server_id)
        server_key = self._get_server_hash_key(server_id)
        
        async with self.redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(metadata_key, server_key)
                    
                    # Validate category exists
                    categories = await self.get_server_categories(server_id)
                    if category not in categories:
                        raise CategoryError(f"Category '{category}' does not exist")
                    
                    categories.remove(category)
                    
                    pipe.multi()
                    
                    # Update metadata
                    metadata = {
                        'categories': json.dumps(list(categories)),
                        'updated_at': datetime.now().isoformat()
                    }
                    await pipe.hset(metadata_key, mapping=metadata)
                    
                    if remove_user_data:
                        # Remove from all users
                        all_users = await self.redis.hgetall(server_key)
                        for user_id_str, raw_data in all_users.items():
                            try:
                                user_data = self._decompress_data(raw_data)
                                if category in user_data:
                                    del user_data[category]
                                    compressed_data = self._compress_data(user_data)
                                    await pipe.hset(server_key, user_id_str, compressed_data)
                            except Exception:
                                continue
                    
                    # Remove leaderboard
                    lb_key = self._get_leaderboard_key(server_id, category)
                    await pipe.delete(lb_key)
                    
                    await pipe.execute()
                    
                    # Clear caches
                    await self._invalidate_all_caches(server_id)
                    break
                    
                except redis.WatchError:
                    continue
    
    # ========================================================================
    # BULK OPERATIONS
    # ========================================================================
    
    async def bulk_add_time(self, operations: List[Dict]) -> List[int]:
        """Bulk add time operations"""
        if not operations:
            return []
        
        # Validate all operations first
        for op in operations:
            if not all(key in op for key in ['server_id', 'user_id', 'category', 'seconds']):
                raise ValidationError("Invalid operation format")
            self._validate_ids(op['server_id'], op['user_id'])
            self._validate_category_name(op['category'])
            self._validate_time_input(op['seconds'])
        
        # Process operations
        results = []
        for op in operations:
            result = await self.add_time(
                op['server_id'], op['user_id'], op['category'], op['seconds']
            )
            results.append(result)
        
        return results
    
    # ========================================================================
    # DATA MANAGEMENT
    # ========================================================================
    
    async def delete_user(self, server_id: int, user_id: int) -> bool:
        """Delete all user data"""
        self._validate_ids(server_id, user_id)
        self._ensure_connected()
        
        server_key = self._get_server_hash_key(server_id)
        session_key = self._get_session_key(server_id, user_id)
        timeseries_key = self._get_timeseries_key(server_id, user_id)
        
        # Delete from multiple data structures
        pipe = self.redis.pipeline()
        await pipe.hdel(server_key, str(user_id))
        await pipe.delete(session_key, timeseries_key)
        
        # Remove from all category leaderboards
        categories = await self.get_server_categories(server_id)
        for category in categories:
            lb_key = self._get_leaderboard_key(server_id, category)
            await pipe.zrem(lb_key, str(user_id))
        
        results = await pipe.execute()
        
        # Clear cache
        cache_key = f"{server_id}:{user_id}"
        self.user_cache.pop(cache_key, None)
        
        return results[0] > 0
    
    async def delete_server_data(self, server_id: int) -> int:
        """Delete all server data"""
        self._ensure_connected()
        
        # Get count before deletion
        server_key = self._get_server_hash_key(server_id)
        user_count = await self.redis.hlen(server_key)
        
        # Delete all server-related keys
        pipe = self.redis.pipeline()
        
        # Main data structures
        metadata_key = self._get_server_metadata_key(server_id)
        permissions_key = self._get_permissions_key(server_id)
        await pipe.delete(server_key, metadata_key, permissions_key)
        
        # Sessions
        session_pattern = f"session:{server_id}:*"
        session_keys = await self.redis.keys(session_pattern)
        if session_keys:
            await pipe.delete(*session_keys)
        
        # Leaderboards
        lb_pattern = f"leaderboard:{server_id}:*"
        lb_keys = await self.redis.keys(lb_pattern)
        if lb_keys:
            await pipe.delete(*lb_keys)
        
        # Time series
        ts_pattern = f"timeseries:{server_id}:*"
        ts_keys = await self.redis.keys(ts_pattern)
        if ts_keys:
            await pipe.delete(*ts_keys)
        
        # Aggregations
        agg_pattern = f"agg:server:{server_id}:*"
        agg_keys = await self.redis.keys(agg_pattern)
        if agg_keys:
            await pipe.delete(*agg_keys)
        
        await pipe.execute()
        
        # Clear all caches for this server
        await self._invalidate_all_caches(server_id)
        
        logger.info(f"Deleted all data for server {server_id} ({user_count} users)")
        return user_count
    
    # ========================================================================
    # CACHE MANAGEMENT
    # ========================================================================
    
    async def _invalidate_related_caches(self, server_id: int, user_id: int):
        """Invalidate caches related to a user/server"""
        # User cache
        user_cache_key = f"{server_id}:{user_id}"
        self.user_cache.pop(user_cache_key, None)
        
        # Server-level caches
        self.stats_cache.pop(f"stats:{server_id}", None)
        
        # Permission cache
        self.permission_cache.pop(f"permissions:{server_id}", None)
        
        # Leaderboard caches for this server
        keys_to_remove = [
            key for key in self.leaderboard_cache.keys()
            if isinstance(key, tuple) and key[0] == server_id
        ]
        for key in keys_to_remove:
            self.leaderboard_cache.pop(key, None)
    
    async def _invalidate_all_caches(self, server_id: int):
        """Invalidate all caches for a server"""
        # Category cache
        self.category_cache.pop(f"metadata:{server_id}", None)
        
        # Permission cache
        self.permission_cache.pop(f"permissions:{server_id}", None)
        
        # User caches for this server
        user_keys_to_remove = [
            key for key in self.user_cache.keys()
            if key.startswith(f"{server_id}:")
        ]
        for key in user_keys_to_remove:
            self.user_cache.pop(key, None)
        
        # Server stats
        self.stats_cache.pop(f"stats:{server_id}", None)
        
        # Leaderboards
        lb_keys_to_remove = [
            key for key in self.leaderboard_cache.keys()
            if isinstance(key, tuple) and key[0] == server_id
        ]
        for key in lb_keys_to_remove:
            self.leaderboard_cache.pop(key, None)
    
    # ========================================================================
    # HEALTH & MONITORING
    # ========================================================================
    
    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        health = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "components": {}
        }
        
        try:
            # Redis connectivity
            start = time.time()
            await self.redis.ping()
            redis_latency = (time.time() - start) * 1000
            
            health["components"]["redis"] = {
                "status": "healthy",
                "latency_ms": redis_latency
            }
            
            # Connection pool
            pool_stats = {
                "created_connections": self.pool.created_connections,
                "available_connections": len(self.pool._available_connections),
                "in_use_connections": len(self.pool._in_use_connections),
                "max_connections": self.pool.max_connections
            }
            
            pool_utilization = (pool_stats["in_use_connections"] / pool_stats["max_connections"]) * 100
            
            health["components"]["connection_pool"] = {
                "status": "healthy" if pool_utilization < 80 else "warning",
                "utilization_percent": pool_utilization,
                **pool_stats
            }
            
            # Circuit breaker
            health["components"]["circuit_breaker"] = {
                "status": "healthy" if self.circuit_breaker.state == "CLOSED" else "warning",
                **self.circuit_breaker.status
            }
            
            # Caches
            health["components"]["caches"] = {
                "category_cache": {"size": len(self.category_cache), "maxsize": self.category_cache.maxsize},
                "user_cache": {"size": len(self.user_cache), "maxsize": self.user_cache.maxsize},
                "stats_cache": {"size": len(self.stats_cache), "maxsize": self.stats_cache.maxsize},
                "leaderboard_cache": {"size": len(self.leaderboard_cache), "maxsize": self.leaderboard_cache.maxsize},
                "permission_cache": {"size": len(self.permission_cache), "maxsize": self.permission_cache.maxsize}
            }
            
            # Fallback storage
            health["components"]["fallback"] = {
                "status": "ok",
                "pending_operations": len(self.fallback_storage)
            }
            
            # Overall status
            component_statuses = [comp["status"] for comp in health["components"].values()]
            if any(status == "error" for status in component_statuses):
                health["status"] = "error"
            elif any(status == "warning" for status in component_statuses):
                health["status"] = "warning"
            
        except Exception as e:
            health["status"] = "error"
            health["error"] = str(e)
        
        return health
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get performance metrics"""
        return {
            "cache_metrics": {
                "category_cache_hit_rate": getattr(self.category_cache, 'hits', 0) / max(getattr(self.category_cache, 'misses', 0) + getattr(self.category_cache, 'hits', 0), 1),
                "user_cache_size": len(self.user_cache),
                "stats_cache_size": len(self.stats_cache),
                "leaderboard_cache_size": len(self.leaderboard_cache),
                "permission_cache_size": len(self.permission_cache)
            },
            "fallback_operations": len(self.fallback_storage),
            "circuit_breaker_state": self.circuit_breaker.state,
            "batch_processor_queue": len(self.batch_processor.pending_operations) if self.batch_processor else 0
        }
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    @staticmethod
    def format_time(seconds: int) -> str:
        """Format seconds into human-readable time"""
        if seconds < 0:
            return "0s"
        
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        
        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if secs > 0 or not parts:
            parts.append(f"{secs}s")
        
        return " ".join(parts)
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()


# ============================================================================
# CLOCK MANAGER WITH PERMISSION INTEGRATION
# ============================================================================

class UltimateClockManager:
    """High-performance clock-in/out manager with permission integration"""
    
    def __init__(self, tracker: UltimateTimeTracker):
        self.tracker = tracker
        self._session_cache = TTLCache(maxsize=10000, ttl=300)  # 5-min session cache
    
    async def clock_in_with_permissions(self, server_id: int, user_id: int, category: str, user_role_ids: List[int]) -> Dict[str, Any]:
        """Clock in with permission checking"""
        # Check permissions first
        permission_check = await self.tracker.check_user_permissions(server_id, user_id, user_role_ids)
        if not permission_check.allowed:
            return {
                "success": False,
                "message": permission_check.reason,
                "permission_check": permission_check
            }
        
        # Proceed with clock in
        return await self.clock_in(server_id, user_id, category)
    
    async def clock_out_with_permissions(self, server_id: int, user_id: int, user_role_ids: List[int]) -> Dict[str, Any]:
        """Clock out with permission checking"""
        # Check permissions first
        permission_check = await self.tracker.check_user_permissions(server_id, user_id, user_role_ids)
        if not permission_check.allowed:
            return {
                "success": False,
                "message": permission_check.reason,
                "permission_check": permission_check
            }
        
        # Proceed with clock out
        return await self.clock_out(server_id, user_id)
    
    async def clock_in(self, server_id: int, user_id: int, category: str) -> Dict[str, Any]:
        """Clock in with validation and optimization"""
        # Validate inputs
        self.tracker._validate_ids(server_id, user_id)
        self.tracker._validate_category_name(category)
        
        # Check existing session
        session = await self._get_session(server_id, user_id)
        if session:
            return {
                "success": False,
                "message": f"Already clocked into '{session['category']}' since {session['start_time'].strftime('%H:%M:%S')}",
                "current_session": session
            }
        
        # Ensure category exists
        await self.tracker._ensure_server_metadata(server_id, {category})
        
        # Create session
        start_time = datetime.now()
        await self._save_session(server_id, user_id, category, start_time)
        
        logger.info(f"User {user_id} clocked into '{category}' in server {server_id}")
        
        return {
            "success": True,
            "message": f"Clocked into '{category}' at {start_time.strftime('%H:%M:%S')}",
            "category": category,
            "start_time": start_time
        }
    
    async def clock_out(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Clock out with time tracking"""
        self.tracker._validate_ids(server_id, user_id)
        
        session = await self._get_session(server_id, user_id)
        if not session:
            return {
                "success": False,
                "message": "Not currently clocked in"
            }
        
        end_time = datetime.now()
        duration_seconds = max(0, int(end_time.timestamp() - session["start_timestamp"]))
        
        # Add time to tracker
        new_total = await self.tracker.add_time(
            server_id, user_id, session["category"], duration_seconds
        )
        
        # Delete session
        await self._delete_session(server_id, user_id)
        
        logger.info(f"User {user_id} clocked out of '{session['category']}' after {duration_seconds}s")
        
        return {
            "success": True,
            "category": session["category"],
            "session_duration": duration_seconds,
            "session_duration_formatted": UltimateTimeTracker.format_time(duration_seconds),
            "category_total": new_total,
            "category_total_formatted": UltimateTimeTracker.format_time(new_total),
            "start_time": session["start_time"],
            "end_time": end_time
        }
    
    async def get_status(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Get comprehensive user status"""
        self.tracker._validate_ids(server_id, user_id)
        
        # Check active session
        session = await self._get_session(server_id, user_id)
        if session:
            current_duration = max(0, int(datetime.now().timestamp() - session["start_timestamp"]))
            return {
                "clocked_in": True,
                "category": session["category"],
                "start_time": session["start_time"],
                "current_duration": current_duration,
                "current_duration_formatted": UltimateTimeTracker.format_time(current_duration)
            }
        
        # Get stored data
        times = await self.tracker.get_user_times(server_id, user_id)
        total_time = sum(times.values())
        
        return {
            "clocked_in": False,
            "total_time": total_time,
            "total_time_formatted": UltimateTimeTracker.format_time(total_time),
            "categories": {cat: UltimateTimeTracker.format_time(time) for cat, time in times.items()}
        }
    
    async def _get_session(self, server_id: int, user_id: int) -> Optional[Dict]:
        """Get session with caching"""
        cache_key = f"session:{server_id}:{user_id}"
        if cache_key in self._session_cache:
            return self._session_cache[cache_key]
        
        session_key = self.tracker._get_session_key(server_id, user_id)
        session_data = await self.tracker.redis.hgetall(session_key)
        
        if session_data and 'category' in session_data:
            try:
                session = {
                    'category': session_data['category'],
                    'start_timestamp': float(session_data['start_timestamp']),
                    'start_time': datetime.fromisoformat(session_data['start_time'])
                }
                self._session_cache[cache_key] = session
                return session
            except (ValueError, KeyError):
                await self.tracker.redis.delete(session_key)
        
        return None
    
    async def _save_session(self, server_id: int, user_id: int, category: str, start_time: datetime):
        """Save session with caching"""
        session_key = self.tracker._get_session_key(server_id, user_id)
        session_data = {
            'category': category,
            'start_timestamp': str(start_time.timestamp()),
            'start_time': start_time.isoformat()
        }
        
        await self.tracker.redis.hset(session_key, mapping=session_data)
        await self.tracker.redis.expire(session_key, 86400)  # 24-hour expiry
        
        # Update cache
        cache_key = f"session:{server_id}:{user_id}"
        self._session_cache[cache_key] = {
            'category': category,
            'start_timestamp': start_time.timestamp(),
            'start_time': start_time
        }
    
    async def _delete_session(self, server_id: int, user_id: int):
        """Delete session and clear cache"""
        session_key = self.tracker._get_session_key(server_id, user_id)
        await self.tracker.redis.delete(session_key)
        
        cache_key = f"session:{server_id}:{user_id}"
        self._session_cache.pop(cache_key, None)
    
    async def force_clock_out_all(self, server_id: int) -> List[Dict]:
        """Force clock out all users in server"""
        self.tracker._ensure_connected()
        
        session_pattern = f"session:{server_id}:*"
        session_keys = await self.tracker.redis.keys(session_pattern)
        
        results = []
        for session_key in session_keys:
            try:
                user_id = int(session_key.split(':')[-1])
                result = await self.clock_out(server_id, user_id)
                results.append({"user_id": user_id, **result})
            except (ValueError, IndexError):
                await self.tracker.redis.delete(session_key)
                continue
        
        return results


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

async def create_ultimate_tracker(redis_url: Optional[str] = None, **kwargs) -> UltimateTimeTracker:
    """Create and connect an ultimate time tracker with permission system"""
    tracker = UltimateTimeTracker(redis_url, **kwargs)
    await tracker.connect()
    return tracker


async def create_ultimate_system(redis_url: Optional[str] = None, **kwargs) -> Tuple[UltimateTimeTracker, UltimateClockManager]:
    """Create complete time tracking system with integrated permissions"""
    tracker = await create_ultimate_tracker(redis_url, **kwargs)
    clock = UltimateClockManager(tracker)
    return tracker, clock


# Legacy compatibility functions
async def create_clock_manager(redis_url: Optional[str] = None, **kwargs) -> Tuple[UltimateTimeTracker, UltimateClockManager]:
    """Legacy function name for backwards compatibility"""
    return await create_ultimate_system(redis_url, **kwargs)


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

async def ultimate_example():
    """Demonstrate the ultimate time tracker with permissions"""
    print("🚀 Ultimate Discord Time Tracker with Permissions Demo\n")
    
    async with UltimateTimeTracker() as tracker:
        clock = UltimateClockManager(tracker)
        
        SERVER_ID = 123456789
        ALICE_ID = 111111
        BOB_ID = 222222
        ADMIN_ROLE = 333333
        
        # Health check
        health = await tracker.health_check()
        print(f"System Health: {health['status']}")
        print(f"Redis Latency: {health['components']['redis']['latency_ms']:.2f}ms\n")
        
        # Permission setup
        await tracker.add_required_role(SERVER_ID, ADMIN_ROLE)
        print(f"Added required role: {ADMIN_ROLE}")
        
        # Check permissions
        user_roles = [ADMIN_ROLE, 444444]  # Alice has admin role
        permission_check = await tracker.check_user_permissions(SERVER_ID, ALICE_ID, user_roles)
        print(f"Alice permission check: {permission_check.allowed} - {permission_check.reason}")
        
        user_roles_bob = [555555]  # Bob doesn't have admin role
        permission_check_bob = await tracker.check_user_permissions(SERVER_ID, BOB_ID, user_roles_bob)
        print(f"Bob permission check: {permission_check_bob.allowed} - {permission_check_bob.reason}")
        
        # Create categories
        await tracker.create_category(SERVER_ID, "development")
        await tracker.create_category(SERVER_ID, "meetings")
        
        # Clock in/out with permissions
        result = await clock.clock_in_with_permissions(SERVER_ID, ALICE_ID, "development", user_roles)
        print(f"✅ Alice: {result['message'] if result['success'] else f'❌ {result[\"message\"]}'}")
        
        result = await clock.clock_in_with_permissions(SERVER_ID, BOB_ID, "development", user_roles_bob)
        print(f"❌ Bob: {result['message']}")
        
        # Suspend user
        await tracker.suspend_user(SERVER_ID, ALICE_ID)
        print(f"Suspended Alice")
        
        # Try to clock out while suspended
        result = await clock.clock_out_with_permissions(SERVER_ID, ALICE_ID, user_roles)
        print(f"❌ Alice (suspended): {result['message']}")
        
        # Unsuspend and try again
        await tracker.unsuspend_user(SERVER_ID, ALICE_ID)
        print(f"Unsuspended Alice")
        
        # Check system status with permissions
        stats = await tracker.get_server_stats(SERVER_ID)
        print(f"\n📊 Server Stats:")
        print(f"  System Enabled: {stats.system_enabled}")
        print(f"  Suspended Users: {stats.suspended_users}")
        print(f"  Total Users: {stats.total_users}")
        
        # Permission stats
        perm_stats = await tracker.get_permission_stats(SERVER_ID)
        print(f"\n🔒 Permission Stats:")
        print(f"  Required Roles: {len(perm_stats['required_roles'])}")
        print(f"  Suspended Users: {perm_stats['suspended_users_count']}")
        print(f"  System Enabled: {perm_stats['system_enabled']}")
        
        print(f"\n✨ Ultimate Time Tracker with Permissions Demo Complete!")


if __name__ == "__main__":
    asyncio.run(ultimate_example())