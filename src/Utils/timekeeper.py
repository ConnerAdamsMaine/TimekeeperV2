import asyncio
import redis.asyncio as redis
from typing import Dict, Optional, List, Set, Any, Tuple, Union, Callable
import logging
from datetime import datetime, timedelta
import discord
from discord import utils
import json
import os
import time
import hashlib
import uuid
import numpy as np
import msgpack
import zlib
import pickle
from collections import defaultdict, deque, Counter
from dataclasses import dataclass, asdict, field
from cachetools import TTLCache, LRUCache, LFUCache
from dotenv import load_dotenv
import weakref
import threading
from contextlib import asynccontextmanager
from enum import Enum, auto
import heapq
import statistics
from concurrent.futures import ThreadPoolExecutor
import traceback

# Import the permission system
from .permissions import PermissionMixin

# Load environment variables
load_dotenv()

# Configure comprehensive logging
class ColoredFormatter(logging.Formatter):
    """Colored log formatter for better visibility"""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green  
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)

# Set up logging with rotation and multiple handlers
from logging.handlers import RotatingFileHandler
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        RotatingFileHandler('timekeeper.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)

# Apply colored formatter to console handler
console_handler = logging.getLogger().handlers[-1]
console_handler.setFormatter(ColoredFormatter(
    '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
))

logger = logging.getLogger(__name__)


# ============================================================================
# EXCEPTIONS WITH CONTEXT
# ============================================================================

class TimeTrackerError(Exception):
    """Base exception with detailed context"""
    def __init__(self, message: str, context: Dict[str, Any] = None, cause: Exception = None):
        super().__init__(message)
        self.context = context or {}
        self.cause = cause
        self.timestamp = datetime.now()
        self.traceback_str = traceback.format_exc() if cause else None


class ConnectionError(TimeTrackerError):
    """Redis connection failures with retry context"""
    pass


class CategoryError(TimeTrackerError):
    """Category validation errors with suggestions"""
    pass


class ValidationError(TimeTrackerError):
    """Input validation with detailed field errors"""
    pass


class CircuitBreakerOpenError(TimeTrackerError):
    """Circuit breaker open with fallback options"""
    pass


class PermissionError(TimeTrackerError):
    """Permission denied with required permission info"""
    pass


class DataCorruptionError(TimeTrackerError):
    """Data integrity issues with recovery suggestions"""
    pass


class PerformanceError(TimeTrackerError):
    """Performance degradation warnings"""
    pass


# ============================================================================
# ADVANCED DATA STRUCTURES
# ============================================================================

@dataclass
class TimeEntry:
    """Comprehensive time entry with metadata"""
    server_id: int
    user_id: int
    category: str
    seconds: int
    timestamp: datetime
    session_id: Optional[str] = None
    ip_hash: Optional[str] = None
    client_version: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.session_id:
            self.session_id = str(uuid.uuid4())


@dataclass
class UserStats:
    """Comprehensive user statistics with trends"""
    total_time: int
    categories: Dict[str, int]
    productivity_score: float
    streak_days: int
    last_activity: datetime
    daily_averages: Dict[str, float] = field(default_factory=dict)
    weekly_trends: Dict[str, List[float]] = field(default_factory=dict)
    peak_hours: List[int] = field(default_factory=list)
    efficiency_rating: float = 0.0
    consistency_score: float = 0.0


@dataclass
class ClockSession:
    """Advanced session with rich metadata"""
    server_id: int
    user_id: int
    category: str
    start_time: datetime
    session_id: str
    role_id: Optional[int] = None
    ip_hash: Optional[str] = None
    client_info: Dict[str, Any] = field(default_factory=dict)
    checkpoints: List[datetime] = field(default_factory=list)
    productivity_score: float = 0.0
    break_count: int = 0
    
    def add_checkpoint(self):
        self.checkpoints.append(datetime.now())


@dataclass
class ServerSettings:
    """Comprehensive server configuration"""
    timezone: str = "UTC"
    work_hours_start: int = 9
    work_hours_end: int = 17
    categories: Set[str] = None
    max_session_hours: int = 12
    role_prefix: str = "⏰"
    auto_logout_hours: int = 24
    analytics_enabled: bool = True
    notification_settings: Dict[str, bool] = field(default_factory=dict)
    productivity_thresholds: Dict[str, float] = field(default_factory=dict)
    rate_limits: Dict[str, int] = field(default_factory=dict)
    audit_enabled: bool = True
    backup_enabled: bool = True
    
    def __post_init__(self):
        if self.categories is None:
            self.categories = {"work", "break", "meeting", "development", "support", "training", "admin"}
        
        if not self.notification_settings:
            self.notification_settings = {
                "session_reminders": True,
                "productivity_alerts": True,
                "streak_notifications": True,
                "goal_updates": True
            }
        
        if not self.productivity_thresholds:
            self.productivity_thresholds = {
                "excellent": 85.0,
                "good": 70.0,
                "fair": 50.0,
                "poor": 30.0
            }
        
        if not self.rate_limits:
            self.rate_limits = {
                "commands_per_minute": 30,
                "sessions_per_hour": 10,
                "bulk_operations": 5
            }


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class BatchOperationType(Enum):
    """Batch operation types with priorities"""
    HIGH_PRIORITY = auto()
    NORMAL_PRIORITY = auto()
    LOW_PRIORITY = auto()
    BACKGROUND = auto()


@dataclass
class BatchOperation:
    """Sophisticated batch operation with metadata"""
    operation_type: str
    key: str
    value: Any = None
    expiry: Optional[int] = None
    priority: BatchOperationType = BatchOperationType.NORMAL_PRIORITY
    created_at: datetime = field(default_factory=datetime.now)
    retry_count: int = 0
    max_retries: int = 3
    timeout: float = 5.0
    context: Dict[str, Any] = field(default_factory=dict)
    
    def __lt__(self, other):
        # For priority queue ordering
        return self.priority.value < other.priority.value


# ============================================================================
# ADVANCED CIRCUIT BREAKER WITH METRICS
# ============================================================================

class AdvancedCircuitBreaker:
    """Enterprise-grade circuit breaker with comprehensive monitoring"""
    
    def __init__(self, 
                 failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 success_threshold: int = 3,
                 monitoring_window: int = 300):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.monitoring_window = monitoring_window
        
        # State management
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.last_success_time = None
        
        # Metrics and monitoring
        self.total_requests = 0
        self.total_failures = 0
        self.total_successes = 0
        self.response_times = deque(maxlen=1000)
        self.failure_history = deque(maxlen=100)
        self.success_history = deque(maxlen=100)
        
        # Performance tracking
        self.avg_response_time = 0.0
        self.percentile_95 = 0.0
        self.error_rate = 0.0
        
        # Health monitoring
        self.health_score = 100.0
        self.last_health_check = time.time()
        
        logger.info(f"AdvancedCircuitBreaker initialized - threshold: {failure_threshold}, timeout: {recovery_timeout}s")
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with comprehensive circuit breaker protection"""
        start_time = time.time()
        self.total_requests += 1
        
        # Check circuit state
        current_time = time.time()
        
        if self.state == CircuitBreakerState.OPEN:
            if current_time - self.last_failure_time < self.recovery_timeout:
                self._record_circuit_block()
                raise CircuitBreakerOpenError(
                    f"Circuit breaker is OPEN. Recovery in {self.recovery_timeout - (current_time - self.last_failure_time):.1f}s",
                    context={
                        "state": self.state.name,
                        "failure_count": self.failure_count,
                        "last_failure": self.last_failure_time,
                        "health_score": self.health_score
                    }
                )
            else:
                self.state = CircuitBreakerState.HALF_OPEN
                self.success_count = 0
                logger.info("Circuit breaker moved to HALF_OPEN state")
        
        try:
            # Execute the function with timeout
            result = await asyncio.wait_for(func(*args, **kwargs), timeout=10.0)
            
            # Record success
            execution_time = time.time() - start_time
            self._record_success(execution_time)
            
            return result
            
        except asyncio.TimeoutError as e:
            execution_time = time.time() - start_time
            self._record_failure("timeout", execution_time)
            raise TimeTrackerError("Operation timed out", context={"timeout": 10.0}, cause=e)
            
        except Exception as e:
            execution_time = time.time() - start_time
            self._record_failure(type(e).__name__, execution_time)
            raise e
    
    def _record_success(self, response_time: float):
        """Record successful operation with metrics"""
        self.total_successes += 1
        self.success_count += 1
        self.failure_count = max(0, self.failure_count - 1)  # Gradual recovery
        self.last_success_time = time.time()
        
        # Update response time metrics
        self.response_times.append(response_time)
        self.success_history.append(time.time())
        
        # State transitions
        if self.state == CircuitBreakerState.HALF_OPEN:
            if self.success_count >= self.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                logger.info("Circuit breaker moved to CLOSED state after successful recovery")
        
        # Update health score
        self._update_health_score()
        self._update_metrics()
        
        logger.debug(f"Circuit breaker success recorded - response_time: {response_time:.3f}s, health: {self.health_score:.1f}%")
    
    def _record_failure(self, error_type: str, response_time: float):
        """Record failed operation with detailed metrics"""
        self.total_failures += 1
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        # Update failure metrics
        self.response_times.append(response_time)
        self.failure_history.append({
            'timestamp': time.time(),
            'error_type': error_type,
            'response_time': response_time
        })
        
        # State transitions
        if self.failure_count >= self.failure_threshold:
            if self.state != CircuitBreakerState.OPEN:
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"Circuit breaker opened after {self.failure_count} failures - error_type: {error_type}")
        
        # Update health score
        self._update_health_score()
        self._update_metrics()
        
        logger.warning(f"Circuit breaker failure recorded - error: {error_type}, response_time: {response_time:.3f}s")
    
    def _record_circuit_block(self):
        """Record when circuit breaker blocks a request"""
        logger.debug("Request blocked by open circuit breaker")
    
    def _update_health_score(self):
        """Calculate comprehensive health score"""
        current_time = time.time()
        
        # Calculate error rate over monitoring window
        recent_failures = [f for f in self.failure_history 
                          if current_time - f['timestamp'] <= self.monitoring_window]
        recent_successes = [s for s in self.success_history 
                           if current_time - s <= self.monitoring_window]
        
        total_recent = len(recent_failures) + len(recent_successes)
        
        if total_recent > 0:
            error_rate = len(recent_failures) / total_recent * 100
            self.error_rate = error_rate
        else:
            error_rate = 0
            self.error_rate = 0
        
        # Calculate response time impact
        if self.response_times:
            avg_response = statistics.mean(self.response_times)
            response_impact = min(avg_response * 10, 50)  # Cap at 50% impact
        else:
            response_impact = 0
        
        # Calculate health score
        base_health = 100.0
        error_penalty = error_rate * 2  # 2% penalty per 1% error rate
        response_penalty = response_impact
        
        self.health_score = max(0, base_health - error_penalty - response_penalty)
        self.last_health_check = current_time
    
    def _update_metrics(self):
        """Update performance metrics"""
        if self.response_times:
            self.avg_response_time = statistics.mean(self.response_times)
            if len(self.response_times) >= 20:
                sorted_times = sorted(self.response_times)
                self.percentile_95 = sorted_times[int(len(sorted_times) * 0.95)]
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive circuit breaker metrics"""
        return {
            'state': self.state.name,
            'health_score': round(self.health_score, 2),
            'total_requests': self.total_requests,
            'total_successes': self.total_successes,
            'total_failures': self.total_failures,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'error_rate': round(self.error_rate, 2),
            'avg_response_time': round(self.avg_response_time * 1000, 2),  # ms
            'percentile_95': round(self.percentile_95 * 1000, 2),  # ms
            'last_failure': self.last_failure_time,
            'last_success': self.last_success_time,
            'thresholds': {
                'failure_threshold': self.failure_threshold,
                'recovery_timeout': self.recovery_timeout,
                'success_threshold': self.success_threshold
            }
        }
    
    def reset(self):
        """Reset circuit breaker to initial state"""
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.total_requests = 0
        self.total_failures = 0
        self.total_successes = 0
        self.response_times.clear()
        self.failure_history.clear()
        self.success_history.clear()
        self.health_score = 100.0
        logger.info("Circuit breaker reset to initial state")


# ============================================================================
# ADVANCED BATCH PROCESSOR WITH PRIORITY QUEUES
# ============================================================================

class EnterpriseBatchProcessor:
    """Enterprise batch processor with priority queues, retry logic, and monitoring"""
    
    def __init__(self, redis_client, 
                 batch_size: int = 100,
                 flush_interval: float = 30.0,
                 max_queue_size: int = 10000,
                 worker_threads: int = 2):
        self.redis = redis_client
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_queue_size = max_queue_size
        self.worker_threads = worker_threads
        
        # Priority queues for different operation types
        self.priority_queue = []  # heapq with BatchOperation objects
        self.dead_letter_queue = deque(maxlen=1000)
        
        # Synchronization
        self._queue_lock = asyncio.Lock()
        self._processing_lock = asyncio.Lock()
        
        # State management
        self._running = False
        self._workers = []
        self._stats_task = None
        
        # Performance metrics
        self.metrics = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'retried_operations': 0,
            'dead_letter_operations': 0,
            'average_batch_size': 0.0,
            'average_processing_time': 0.0,
            'queue_size': 0,
            'last_flush_time': None,
            'operations_per_second': 0.0
        }
        
        self.processing_times = deque(maxlen=100)
        self.batch_sizes = deque(maxlen=100)
        self.last_flush = time.time()
        
        # Rate limiting
        self.rate_limiter = TokenBucket(capacity=1000, refill_rate=100)
        
        logger.info(f"EnterpriseBatchProcessor initialized - batch_size: {batch_size}, workers: {worker_threads}")
    
    async def start(self):
        """Start the batch processor with multiple workers"""
        if self._running:
            logger.warning("Batch processor already running")
            return
        
        self._running = True
        
        # Start worker tasks
        for i in range(self.worker_threads):
            worker = asyncio.create_task(self._worker_loop(f"worker-{i}"))
            self._workers.append(worker)
        
        # Start metrics collection task
        self._stats_task = asyncio.create_task(self._stats_loop())
        
        logger.info(f"Batch processor started with {len(self._workers)} workers")
    
    async def stop(self):
        """Gracefully stop the batch processor"""
        if not self._running:
            return
        
        logger.info("Stopping batch processor...")
        self._running = False
        
        # Cancel all workers
        for worker in self._workers:
            worker.cancel()
        
        # Cancel stats task
        if self._stats_task:
            self._stats_task.cancel()
        
        # Wait for workers to finish
        await asyncio.gather(*self._workers, self._stats_task, return_exceptions=True)
        
        # Final flush
        await self._flush_queue()
        
        self._workers.clear()
        logger.info("Batch processor stopped")
    
    async def add_operation(self, operation: BatchOperation) -> bool:
        """Add operation to priority queue with overflow protection"""
        if not self._running:
            logger.error("Cannot add operation - batch processor not running")
            return False
        
        # Rate limiting check
        if not self.rate_limiter.consume():
            logger.warning("Rate limit exceeded - operation rejected")
            return False
        
        async with self._queue_lock:
            # Check queue size limits
            if len(self.priority_queue) >= self.max_queue_size:
                logger.warning(f"Queue size limit reached ({self.max_queue_size}) - operation rejected")
                return False
            
            # Add to priority queue
            heapq.heappush(self.priority_queue, operation)
            self.metrics['queue_size'] = len(self.priority_queue)
            
            logger.debug(f"Operation added to queue - type: {operation.operation_type}, priority: {operation.priority.name}")
            return True
    
    async def add_simple_operation(self, operation_type: str, key: str, value: Any = None, 
                                 expiry: Optional[int] = None, 
                                 priority: BatchOperationType = BatchOperationType.NORMAL_PRIORITY) -> bool:
        """Convenience method for adding simple operations"""
        operation = BatchOperation(
            operation_type=operation_type,
            key=key,
            value=value,
            expiry=expiry,
            priority=priority
        )
        return await self.add_operation(operation)
    
    async def _worker_loop(self, worker_id: str):
        """Main worker loop for processing batches"""
        logger.info(f"Worker {worker_id} started")
        
        while self._running:
            try:
                # Check if we should process a batch
                should_process = await self._should_process_batch()
                
                if should_process:
                    await self._process_batch(worker_id)
                else:
                    # Short sleep to prevent busy waiting
                    await asyncio.sleep(0.1)
                    
            except asyncio.CancelledError:
                logger.info(f"Worker {worker_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(1)  # Error backoff
        
        logger.info(f"Worker {worker_id} stopped")
    
    async def _should_process_batch(self) -> bool:
        """Determine if we should process a batch now"""
        current_time = time.time()
        
        async with self._queue_lock:
            queue_size = len(self.priority_queue)
            time_since_flush = current_time - self.last_flush
            
            return (queue_size >= self.batch_size or 
                   (queue_size > 0 and time_since_flush >= self.flush_interval))
    
    async def _process_batch(self, worker_id: str):
        """Process a batch of operations"""
        start_time = time.time()
        
        # Extract batch from queue
        batch = await self._extract_batch()
        if not batch:
            return
        
        batch_size = len(batch)
        logger.debug(f"Worker {worker_id} processing batch of {batch_size} operations")
        
        # Process the batch
        successful_ops = 0
        failed_ops = []
        
        try:
            async with self._processing_lock:
                successful_ops = await self._execute_batch(batch)
                failed_ops = [op for op in batch if op not in successful_ops]
                
        except Exception as e:
            logger.error(f"Batch processing error in {worker_id}: {e}")
            failed_ops = batch
        
        # Handle failed operations
        for failed_op in failed_ops:
            await self._handle_failed_operation(failed_op)
        
        # Update metrics
        processing_time = time.time() - start_time
        self._update_processing_metrics(batch_size, processing_time, len(failed_ops))
        
        logger.debug(f"Worker {worker_id} completed batch - success: {successful_ops}, failed: {len(failed_ops)}, time: {processing_time:.3f}s")
    
    async def _extract_batch(self) -> List[BatchOperation]:
        """Extract a batch of operations from priority queue"""
        batch = []
        
        async with self._queue_lock:
            # Extract operations based on priority
            while len(batch) < self.batch_size and self.priority_queue:
                operation = heapq.heappop(self.priority_queue)
                batch.append(operation)
            
            self.metrics['queue_size'] = len(self.priority_queue)
            self.last_flush = time.time()
        
        return batch
    
    async def _execute_batch(self, batch: List[BatchOperation]) -> int:
        """Execute a batch of Redis operations"""
        if not batch:
            return 0
        
        try:
            pipe = self.redis.pipeline()
            operation_map = {}
            
            # Build pipeline with operations
            for i, operation in enumerate(batch):
                operation_map[i] = operation
                
                if operation.operation_type == 'set':
                    if operation.expiry:
                        pipe.setex(operation.key, operation.expiry, operation.value)
                    else:
                        pipe.set(operation.key, operation.value)
                        
                elif operation.operation_type == 'zadd':
                    pipe.zadd(operation.key, operation.value)
                    
                elif operation.operation_type == 'sadd':
                    pipe.sadd(operation.key, operation.value)
                    
                elif operation.operation_type == 'hset':
                    pipe.hset(operation.key, mapping=operation.value)
                    
                elif operation.operation_type == 'hincrby':
                    for field, increment in operation.value.items():
                        pipe.hincrby(operation.key, field, increment)
                        
                elif operation.operation_type == 'zincrby':
                    score, member = operation.value
                    pipe.zincrby(operation.key, score, member)
                    
                elif operation.operation_type == 'delete':
                    pipe.delete(operation.key)
                    
                else:
                    logger.warning(f"Unknown operation type: {operation.operation_type}")
            
            # Execute pipeline
            results = await pipe.execute()
            
            # Count successful operations
            successful_count = 0
            for i, result in enumerate(results):
                if result is not None:  # Successful operation
                    successful_count += 1
                    operation = operation_map[i]
                    self.metrics['successful_operations'] += 1
                else:
                    operation = operation_map[i]
                    logger.warning(f"Operation failed: {operation.operation_type} on {operation.key}")
                    self.metrics['failed_operations'] += 1
            
            self.metrics['total_operations'] += len(batch)
            return successful_count
            
        except Exception as e:
            logger.error(f"Batch execution failed: {e}")
            self.metrics['failed_operations'] += len(batch)
            raise e
    
    async def _handle_failed_operation(self, operation: BatchOperation):
        """Handle failed operations with retry logic"""
        operation.retry_count += 1
        
        # Check if we should retry
        if operation.retry_count <= operation.max_retries:
            # Exponential backoff
            delay = min(2 ** operation.retry_count, 60)  # Cap at 60 seconds
            operation.created_at = datetime.now() + timedelta(seconds=delay)
            
            # Re-queue for retry
            async with self._queue_lock:
                heapq.heappush(self.priority_queue, operation)
                self.metrics['retried_operations'] += 1
            
            logger.info(f"Operation scheduled for retry {operation.retry_count}/{operation.max_retries} in {delay}s")
        else:
            # Send to dead letter queue
            self.dead_letter_queue.append({
                'operation': operation,
                'final_error': f"Max retries exceeded ({operation.max_retries})",
                'timestamp': datetime.now()
            })
            self.metrics['dead_letter_operations'] += 1
            
            logger.error(f"Operation sent to dead letter queue after {operation.retry_count} retries: {operation.key}")
    
    def _update_processing_metrics(self, batch_size: int, processing_time: float, failed_count: int):
        """Update processing performance metrics"""
        self.processing_times.append(processing_time)
        self.batch_sizes.append(batch_size)
        
        # Calculate averages
        if self.processing_times:
            self.metrics['average_processing_time'] = statistics.mean(self.processing_times)
        
        if self.batch_sizes:
            self.metrics['average_batch_size'] = statistics.mean(self.batch_sizes)
        
        # Calculate operations per second
        if processing_time > 0:
            ops_per_second = batch_size / processing_time
            # Exponential moving average
            current_ops = self.metrics.get('operations_per_second', 0)
            self.metrics['operations_per_second'] = 0.8 * current_ops + 0.2 * ops_per_second
        
        self.metrics['last_flush_time'] = datetime.now()
    
    async def _stats_loop(self):
        """Background task for collecting and logging statistics"""
        while self._running:
            try:
                await asyncio.sleep(60)  # Stats every minute
                
                if self.metrics['total_operations'] > 0:
                    success_rate = (self.metrics['successful_operations'] / self.metrics['total_operations']) * 100
                    
                    logger.info(f"Batch Processor Stats - "
                              f"Total: {self.metrics['total_operations']}, "
                              f"Success Rate: {success_rate:.1f}%, "
                              f"Queue: {self.metrics['queue_size']}, "
                              f"Avg Processing: {self.metrics['average_processing_time']:.3f}s, "
                              f"Ops/sec: {self.metrics['operations_per_second']:.1f}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stats loop error: {e}")
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive batch processor metrics"""
        async with self._queue_lock:
            self.metrics['queue_size'] = len(self.priority_queue)
            self.metrics['dead_letter_size'] = len(self.dead_letter_queue)
        
        return self.metrics.copy()
    
    async def get_dead_letter_queue(self) -> List[Dict[str, Any]]:
        """Get items from dead letter queue for analysis"""
        return list(self.dead_letter_queue)
    
    async def clear_dead_letter_queue(self):
        """Clear the dead letter queue"""
        self.dead_letter_queue.clear()
        self.metrics['dead_letter_operations'] = 0
        logger.info("Dead letter queue cleared")


class TokenBucket:
    """Token bucket for rate limiting"""
    
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
        self._lock = threading.Lock()
    
    def consume(self, tokens: int = 1) -> bool:
        """Consume tokens from bucket"""
        with self._lock:
            current_time = time.time()
            elapsed = current_time - self.last_refill
            
            # Refill tokens
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            self.last_refill = current_time
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False


# ============================================================================
# ADVANCED ANALYTICS ENGINE WITH PREDICTIVE MODELING
# ============================================================================

class AdvancedAnalyticsEngine:
    """Enterprise analytics engine with ML capabilities and predictive modeling"""
    
    def __init__(self, redis_client):
        self.redis = redis_client
        
        # Multi-layer caching for analytics
        self.l1_cache = TTLCache(maxsize=1000, ttl=300)      # 5 minutes - hot data
        self.l2_cache = TTLCache(maxsize=5000, ttl=1800)     # 30 minutes - warm data  
        self.l3_cache = LFUCache(maxsize=10000)              # Cold data - LFU eviction
        
        # Predictive models cache
        self.model_cache = TTLCache(maxsize=100, ttl=3600)   # 1 hour
        
        # Analytics worker
        self.analytics_executor = ThreadPoolExecutor(max_workers=4)
        
        # Performance monitoring
        self.analytics_metrics = {
            'cache_hits': {'l1': 0, 'l2': 0, 'l3': 0},
            'cache_misses': 0,
            'computation_time': deque(maxlen=100),
            'predictions_generated': 0,
            'models_cached': 0
        }
        
        logger.info("AdvancedAnalyticsEngine initialized with multi-layer caching")
    
    async def calculate_productivity_score(self, server_id: int, user_id: int, 
                                         days: int = 7, use_ml: bool = True) -> float:
        """Calculate sophisticated productivity score with ML enhancement"""
        cache_key = f"productivity:{server_id}:{user_id}:{days}:{use_ml}"
        
        # Check caches in order
        result = self._check_caches(cache_key)
        if result is not None:
            return result
        
        try:
            start_time = time.time()
            
            # Get comprehensive user data
            user_data = await self._get_comprehensive_user_data(server_id, user_id, days)
            if not user_data:
                return 0.0
            
            # Calculate base productivity metrics
            base_metrics = await self._calculate_base_productivity_metrics(user_data)
            
            # Apply ML enhancement if enabled
            if use_ml:
                ml_score = await self._apply_ml_productivity_model(user_data, base_metrics)
                final_score = 0.7 * base_metrics['weighted_score'] + 0.3 * ml_score
            else:
                final_score = base_metrics['weighted_score']
            
            # Cache the result
            computation_time = time.time() - start_time
            self.analytics_metrics['computation_time'].append(computation_time)
            self._update_caches(cache_key, final_score)
            
            logger.debug(f"Productivity score calculated: {final_score:.3f} (time: {computation_time:.3f}s, ML: {use_ml})")
            return final_score
            
        except Exception as e:
            logger.error(f"Error calculating productivity score: {e}")
            return 0.0
    
    def _check_caches(self, key: str) -> Optional[Any]:
        """Check multi-layer cache hierarchy"""
        # L1 Cache (hot data)
        if key in self.l1_cache:
            self.analytics_metrics['cache_hits']['l1'] += 1
            return self.l1_cache[key]
        
        # L2 Cache (warm data)
        if key in self.l2_cache:
            self.analytics_metrics['cache_hits']['l2'] += 1
            # Promote to L1
            value = self.l2_cache[key]
            self.l1_cache[key] = value
            return value
        
        # L3 Cache (cold data)
        if key in self.l3_cache:
            self.analytics_metrics['cache_hits']['l3'] += 1
            # Promote to L2 and L1
            value = self.l3_cache[key]
            self.l2_cache[key] = value
            self.l1_cache[key] = value
            return value
        
        self.analytics_metrics['cache_misses'] += 1
        return None
    
    def _update_caches(self, key: str, value: Any):
        """Update all cache layers"""
        self.l1_cache[key] = value
        self.l2_cache[key] = value
        self.l3_cache[key] = value
    
    async def _get_comprehensive_user_data(self, server_id: int, user_id: int, days: int) -> Dict[str, Any]:
        """Get comprehensive user data for analytics"""
        try:
            # Get time entries
            entries_key = f"time_entries:{server_id}:{user_id}"
            cutoff_time = time.time() - (days * 24 * 3600)
            
            entries_data = await self.redis.zrevrangebyscore(
                entries_key, '+inf', cutoff_time, withscores=True
            )
            
            if not entries_data:
                return {}
            
            # Parse and organize data
            daily_data = defaultdict(lambda: defaultdict(float))
            hourly_patterns = defaultdict(int)
            category_data = defaultdict(list)
            session_lengths = []
            productivity_indicators = []
            
            for entry_data, timestamp in entries_data:
                try:
                    entry = json.loads(entry_data)
                    dt = datetime.fromtimestamp(timestamp)
                    day_key = dt.date()
                    hour = dt.hour
                    
                    duration = entry['seconds']
                    category = entry['category']
                    
                    daily_data[day_key][category] += duration
                    daily_data[day_key]['total'] += duration
                    hourly_patterns[hour] += duration
                    category_data[category].append(duration)
                    session_lengths.append(duration)
                    
                    # Calculate productivity indicators
                    if category in ['work', 'development', 'meeting']:
                        productivity_indicators.append(duration)
                    
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Invalid entry data: {e}")
                    continue
            
            return {
                'daily_data': dict(daily_data),
                'hourly_patterns': dict(hourly_patterns),
                'category_data': dict(category_data),
                'session_lengths': session_lengths,
                'productivity_indicators': productivity_indicators,
                'total_days': days,
                'data_points': len(entries_data)
            }
            
        except Exception as e:
            logger.error(f"Error getting comprehensive user data: {e}")
            return {}
    
    async def _calculate_base_productivity_metrics(self, user_data: Dict[str, Any]) -> Dict[str, float]:
        """Calculate base productivity metrics"""
        try:
            daily_data = user_data['daily_data']
            hourly_patterns = user_data['hourly_patterns']
            category_data = user_data['category_data']
            session_lengths = user_data['session_lengths']
            
            # Consistency Score (variance in daily work)
            daily_totals = [data.get('total', 0) for data in daily_data.values()]
            consistency_score = self._calculate_consistency_score(daily_totals)
            
            # Work-Life Balance Score
            balance_score = self._calculate_balance_score(category_data)
            
            # Time Pattern Score (working during optimal hours)
            pattern_score = self._calculate_pattern_score(hourly_patterns)
            
            # Session Quality Score (optimal session lengths)
            quality_score = self._calculate_session_quality_score(session_lengths)
            
            # Volume Score (appropriate work volume)
            volume_score = self._calculate_volume_score(daily_totals, user_data['total_days'])
            
            # Focus Score (fewer, longer sessions vs many short ones)
            focus_score = self._calculate_focus_score(session_lengths)
            
            # Trend Score (improving over time)
            trend_score = self._calculate_trend_score(daily_totals)
            
            # Weighted final score
            weights = {
                'consistency': 0.20,
                'balance': 0.15,
                'pattern': 0.15,
                'quality': 0.15,
                'volume': 0.15,
                'focus': 0.10,
                'trend': 0.10
            }
            
            weighted_score = (
                consistency_score * weights['consistency'] +
                balance_score * weights['balance'] +
                pattern_score * weights['pattern'] +
                quality_score * weights['quality'] +
                volume_score * weights['volume'] +
                focus_score * weights['focus'] +
                trend_score * weights['trend']
            )
            
            return {
                'consistency_score': consistency_score,
                'balance_score': balance_score,
                'pattern_score': pattern_score,
                'quality_score': quality_score,
                'volume_score': volume_score,
                'focus_score': focus_score,
                'trend_score': trend_score,
                'weighted_score': weighted_score
            }
            
        except Exception as e:
            logger.error(f"Error calculating base metrics: {e}")
            return {'weighted_score': 0.0}
    
    def _calculate_consistency_score(self, daily_totals: List[float]) -> float:
        """Calculate consistency of daily work patterns"""
        if len(daily_totals) < 2:
            return 0.5
        
        # Remove zero days for consistency calculation
        non_zero_days = [total for total in daily_totals if total > 0]
        if len(non_zero_days) < 2:
            return 0.3  # Low score for inconsistent work
        
        mean_work = statistics.mean(non_zero_days)
        if mean_work == 0:
            return 0.0
        
        std_dev = statistics.stdev(non_zero_days)
        coefficient_of_variation = std_dev / mean_work
        
        # Lower coefficient of variation = higher consistency
        consistency = max(0, 1 - coefficient_of_variation)
        return min(1.0, consistency)
    
    def _calculate_balance_score(self, category_data: Dict[str, List[float]]) -> float:
        """Calculate work-life balance score"""
        if not category_data:
            return 0.5
        
        total_time = sum(sum(sessions) for sessions in category_data.values())
        if total_time == 0:
            return 0.5
        
        # Ideal distribution
        ideal_distribution = {
            'work': 0.60,
            'development': 0.15,
            'meeting': 0.15,
            'break': 0.05,
            'support': 0.03,
            'training': 0.02
        }
        
        # Calculate actual distribution
        actual_distribution = {
            category: sum(sessions) / total_time
            for category, sessions in category_data.items()
        }
        
        # Calculate deviation from ideal
        score = 1.0
        for category, ideal_ratio in ideal_distribution.items():
            actual_ratio = actual_distribution.get(category, 0)
            deviation = abs(actual_ratio - ideal_ratio)
            score -= deviation * 0.5
        
        return max(0.0, min(1.0, score))
    
    def _calculate_pattern_score(self, hourly_patterns: Dict[int, float]) -> float:
        """Calculate score based on work time patterns"""
        if not hourly_patterns:
            return 0.5
        
        total_time = sum(hourly_patterns.values())
        if total_time == 0:
            return 0.5
        
        # Optimal work hours (9 AM to 5 PM)
        optimal_hours = set(range(9, 17))
        
        optimal_time = sum(
            time_spent for hour, time_spent in hourly_patterns.items()
            if hour in optimal_hours
        )
        
        return optimal_time / total_time
    
    def _calculate_session_quality_score(self, session_lengths: List[float]) -> float:
        """Calculate session quality based on length distribution"""
        if not session_lengths:
            return 0.0
        
        # Convert to hours for analysis
        session_hours = [length / 3600 for length in session_lengths]
        
        # Optimal session length: 1-3 hours
        optimal_sessions = [s for s in session_hours if 1 <= s <= 3]
        good_sessions = [s for s in session_hours if 0.5 <= s <= 4]
        
        optimal_ratio = len(optimal_sessions) / len(session_hours)
        good_ratio = len(good_sessions) / len(session_hours)
        
        # Weighted score favoring optimal sessions
        score = optimal_ratio * 1.0 + (good_ratio - optimal_ratio) * 0.7
        return min(1.0, score)
    
    def _calculate_volume_score(self, daily_totals: List[float], total_days: int) -> float:
        """Calculate work volume appropriateness"""
        if not daily_totals:
            return 0.0
        
        # Calculate average daily hours
        total_hours = sum(daily_totals) / 3600
        avg_daily_hours = total_hours / total_days if total_days > 0 else 0
        
        # Optimal range: 6-8 hours per day
        if 6 <= avg_daily_hours <= 8:
            return 1.0
        elif 4 <= avg_daily_hours < 6:
            return 0.8
        elif 8 < avg_daily_hours <= 10:
            return 0.8
        elif 2 <= avg_daily_hours < 4:
            return 0.6
        elif 10 < avg_daily_hours <= 12:
            return 0.6
        else:
            return 0.3
    
    def _calculate_focus_score(self, session_lengths: List[float]) -> float:
        """Calculate focus score based on session length patterns"""
        if not session_lengths:
            return 0.0
        
        # Convert to minutes
        session_minutes = [length / 60 for length in session_lengths]
        
        # Penalize very short sessions (< 15 minutes)
        short_sessions = len([s for s in session_minutes if s < 15])
        
        # Reward longer focused sessions (> 60 minutes)
        focused_sessions = len([s for s in session_minutes if s >= 60])
        
        total_sessions = len(session_minutes)
        short_penalty = short_sessions / total_sessions * 0.5
        focus_bonus = focused_sessions / total_sessions * 0.3
        
        base_score = 0.7
        final_score = base_score - short_penalty + focus_bonus
        
        return max(0.0, min(1.0, final_score))
    
    def _calculate_trend_score(self, daily_totals: List[float]) -> float:
        """Calculate trend score (improving over time)"""
        if len(daily_totals) < 3:
            return 0.5
        
        # Calculate linear trend
        x = list(range(len(daily_totals)))
        y = daily_totals
        
        try:
            # Simple linear regression
            n = len(x)
            sum_x = sum(x)
            sum_y = sum(y)
            sum_xy = sum(x[i] * y[i] for i in range(n))
            sum_x2 = sum(x[i] ** 2 for i in range(n))
            
            slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2)
            
            # Normalize slope to 0-1 score
            if slope > 0:
                return min(1.0, 0.5 + slope * 0.1)  # Positive trend
            else:
                return max(0.0, 0.5 + slope * 0.1)  # Negative trend
                
        except (ZeroDivisionError, ValueError):
            return 0.5
    
    async def _apply_ml_productivity_model(self, user_data: Dict[str, Any], 
                                         base_metrics: Dict[str, float]) -> float:
        """Apply machine learning model for productivity prediction"""
        try:
            # Feature engineering
            features = self._extract_ml_features(user_data, base_metrics)
            
            # Check for cached model
            model_key = "productivity_model_v1"
            model = self.model_cache.get(model_key)
            
            if model is None:
                # Simple ML model (would be replaced with actual trained model)
                model = self._create_simple_productivity_model()
                self.model_cache[model_key] = model
                self.analytics_metrics['models_cached'] += 1
            
            # Make prediction
            prediction = await self._run_ml_prediction(model, features)
            self.analytics_metrics['predictions_generated'] += 1
            
            return prediction
            
        except Exception as e:
            logger.error(f"ML model application error: {e}")
            return base_metrics.get('weighted_score', 0.0)
    
    def _extract_ml_features(self, user_data: Dict[str, Any], base_metrics: Dict[str, float]) -> List[float]:
        """Extract features for ML model"""
        features = []
        
        # Base metric features
        features.extend([
            base_metrics.get('consistency_score', 0),
            base_metrics.get('balance_score', 0),
            base_metrics.get('pattern_score', 0),
            base_metrics.get('quality_score', 0),
            base_metrics.get('volume_score', 0),
            base_metrics.get('focus_score', 0),
            base_metrics.get('trend_score', 0)
        ])
        
        # Statistical features
        session_lengths = user_data.get('session_lengths', [])
        if session_lengths:
            features.extend([
                len(session_lengths),  # Number of sessions
                statistics.mean(session_lengths) / 3600,  # Average session length (hours)
                statistics.median(session_lengths) / 3600,  # Median session length
                max(session_lengths) / 3600,  # Longest session
                min(session_lengths) / 3600,  # Shortest session
            ])
        else:
            features.extend([0, 0, 0, 0, 0])
        
        # Time pattern features
        hourly_patterns = user_data.get('hourly_patterns', {})
        peak_hour = max(hourly_patterns.keys(), key=lambda h: hourly_patterns[h]) if hourly_patterns else 12
        features.append(peak_hour / 24)  # Normalized peak hour
        
        # Data quality features
        features.extend([
            user_data.get('data_points', 0) / 100,  # Normalized data points
            len(user_data.get('daily_data', {})) / user_data.get('total_days', 1)  # Activity ratio
        ])
        
        return features
    
    def _create_simple_productivity_model(self) -> Dict[str, Any]:
        """Create a simple productivity model (placeholder for real ML model)"""
        return {
            'type': 'weighted_ensemble',
            'weights': [0.2, 0.15, 0.15, 0.15, 0.15, 0.1, 0.1],  # Base metrics weights
            'feature_weights': [0.1, 0.05, 0.05, 0.03, 0.02],      # Statistical feature weights
            'bias': 0.0,
            'version': '1.0'
        }
    
    async def _run_ml_prediction(self, model: Dict[str, Any], features: List[float]) -> float:
        """Run ML prediction (simplified)"""
        try:
            # Simple weighted sum model
            base_features = features[:7]
            stat_features = features[7:12] if len(features) > 7 else [0] * 5
            
            base_score = sum(f * w for f, w in zip(base_features, model['weights']))
            stat_score = sum(f * w for f, w in zip(stat_features, model['feature_weights']))
            
            prediction = base_score + stat_score + model['bias']
            return max(0.0, min(1.0, prediction))
            
        except Exception as e:
            logger.error(f"ML prediction error: {e}")
            return 0.5
    
    async def get_advanced_insights(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Generate comprehensive productivity insights with predictions"""
        try:
            # Get current productivity score
            current_score = await self.calculate_productivity_score(server_id, user_id, days=7, use_ml=True)
            
            # Get historical trend
            historical_scores = []
            for days_back in [7, 14, 21, 28]:
                score = await self.calculate_productivity_score(server_id, user_id, days=days_back, use_ml=False)
                historical_scores.append(score)
            
            # Calculate streak and consistency
            streak_days = await self._calculate_advanced_streak(server_id, user_id)
            consistency_rating = await self._calculate_consistency_rating(server_id, user_id)
            
            # Generate predictions
            predictions = await self._generate_productivity_predictions(server_id, user_id)
            
            # Get detailed category breakdown with insights
            category_insights = await self._get_category_insights(server_id, user_id)
            
            # Generate personalized recommendations
            recommendations = await self._generate_advanced_recommendations(
                server_id, user_id, current_score, category_insights
            )
            
            # Calculate comparative metrics
            comparative_metrics = await self._get_comparative_metrics(server_id, user_id)
            
            return {
                'productivity_score': round(current_score * 100, 1),
                'grade': self._score_to_grade(current_score),
                'historical_trend': historical_scores,
                'streak_days': streak_days,
                'consistency_rating': consistency_rating,
                'predictions': predictions,
                'category_insights': category_insights,
                'recommendations': recommendations,
                'comparative_metrics': comparative_metrics,
                'last_updated': datetime.now().isoformat(),
                'confidence_level': 'high' if current_score > 0.1 else 'low'
            }
            
        except Exception as e:
            logger.error(f"Error generating advanced insights: {e}")
            return {
                'productivity_score': 0.0,
                'error': str(e),
                'last_updated': datetime.now().isoformat()
            }
    
    def _score_to_grade(self, score: float) -> str:
        """Convert productivity score to letter grade"""
        if score >= 0.9:
            return "A+"
        elif score >= 0.85:
            return "A"
        elif score >= 0.8:
            return "A-"
        elif score >= 0.75:
            return "B+"
        elif score >= 0.7:
            return "B"
        elif score >= 0.65:
            return "B-"
        elif score >= 0.6:
            return "C+"
        elif score >= 0.55:
            return "C"
        elif score >= 0.5:
            return "C-"
        elif score >= 0.4:
            return "D"
        else:
            return "F"
    
    async def _calculate_advanced_streak(self, server_id: int, user_id: int) -> int:
        """Calculate advanced streak with minimum daily requirements"""
        try:
            entries_key = f"time_entries:{server_id}:{user_id}"
            
            # Get last 60 days of entries
            start_time = time.time() - (60 * 24 * 3600)
            entries = await self.redis.zrevrangebyscore(
                entries_key, '+inf', start_time, withscores=True
            )
            
            if not entries:
                return 0
            
            # Group by day and calculate daily totals
            daily_totals = defaultdict(float)
            for entry_data, timestamp in entries:
                try:
                    entry = json.loads(entry_data)
                    day = datetime.fromtimestamp(timestamp).date()
                    daily_totals[day] += entry['seconds']
                except (json.JSONDecodeError, KeyError):
                    continue
            
            # Calculate streak (minimum 1 hour per day)
            streak = 0
            current_date = datetime.now().date()
            min_daily_seconds = 3600  # 1 hour
            
            while current_date in daily_totals and daily_totals[current_date] >= min_daily_seconds:
                streak += 1
                current_date -= timedelta(days=1)
            
            return streak
            
        except Exception as e:
            logger.error(f"Error calculating advanced streak: {e}")
            return 0
    
    async def _calculate_consistency_rating(self, server_id: int, user_id: int) -> str:
        """Calculate consistency rating based on daily patterns"""
        try:
            # Get last 28 days of data
            user_data = await self._get_comprehensive_user_data(server_id, user_id, 28)
            daily_data = user_data.get('daily_data', {})
            
            if len(daily_data) < 7:
                return "Insufficient Data"
            
            daily_totals = [data.get('total', 0) for data in daily_data.values()]
            work_days = len([total for total in daily_totals if total >= 3600])  # At least 1 hour
            
            work_ratio = work_days / len(daily_totals)
            
            if work_ratio >= 0.85:
                return "Excellent"
            elif work_ratio >= 0.7:
                return "Good"
            elif work_ratio >= 0.5:
                return "Fair"
            elif work_ratio >= 0.3:
                return "Poor"
            else:
                return "Very Poor"
                
        except Exception as e:
            logger.error(f"Error calculating consistency rating: {e}")
            return "Unknown"
    
    async def _generate_productivity_predictions(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Generate productivity predictions for next week"""
        try:
            # Get historical data for prediction
            user_data = await self._get_comprehensive_user_data(server_id, user_id, 28)
            
            if not user_data.get('daily_data'):
                return {'error': 'Insufficient data for predictions'}
            
            # Simple trend-based prediction
            daily_totals = list(user_data['daily_data'].values())
            recent_avg = statistics.mean([d.get('total', 0) for d in daily_totals[-7:]])
            overall_avg = statistics.mean([d.get('total', 0) for d in daily_totals])
            
            # Trend calculation
            if recent_avg > overall_avg * 1.1:
                trend = "improving"
                predicted_change = "+5-10%"
            elif recent_avg < overall_avg * 0.9:
                trend = "declining"
                predicted_change = "-5-10%"
            else:
                trend = "stable"
                predicted_change = "±2%"
            
            # Weekly prediction
            predicted_weekly_hours = (recent_avg * 7) / 3600
            
            return {
                'trend': trend,
                'predicted_weekly_hours': round(predicted_weekly_hours, 1),
                'predicted_change': predicted_change,
                'confidence': 'medium',
                'based_on_days': len(daily_totals)
            }
            
        except Exception as e:
            logger.error(f"Error generating predictions: {e}")
            return {'error': str(e)}
    
    async def _get_category_insights(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Get detailed insights for each category"""
        try:
            user_data = await self._get_comprehensive_user_data(server_id, user_id, 14)
            category_data = user_data.get('category_data', {})
            
            insights = {}
            total_time = sum(sum(sessions) for sessions in category_data.values())
            
            for category, sessions in category_data.items():
                if not sessions:
                    continue
                
                category_total = sum(sessions)
                percentage = (category_total / total_time * 100) if total_time > 0 else 0
                avg_session = statistics.mean(sessions) / 3600  # hours
                
                insights[category] = {
                    'total_hours': round(category_total / 3600, 1),
                    'percentage': round(percentage, 1),
                    'sessions': len(sessions),
                    'avg_session_hours': round(avg_session, 2),
                    'trend': self._calculate_category_trend(sessions)
                }
            
            return insights
            
        except Exception as e:
            logger.error(f"Error getting category insights: {e}")
            return {}
    
    def _calculate_category_trend(self, sessions: List[float]) -> str:
        """Calculate trend for a specific category"""
        if len(sessions) < 4:
            return "stable"
        
        # Compare first half to second half
        mid = len(sessions) // 2
        first_half_avg = statistics.mean(sessions[:mid])
        second_half_avg = statistics.mean(sessions[mid:])
        
        if second_half_avg > first_half_avg * 1.2:
            return "increasing"
        elif second_half_avg < first_half_avg * 0.8:
            return "decreasing"
        else:
            return "stable"
    
    async def _generate_advanced_recommendations(self, server_id: int, user_id: int, 
                                               score: float, category_insights: Dict[str, Any]) -> List[str]:
        """Generate personalized recommendations based on detailed analysis"""
        recommendations = []
        
        # Score-based recommendations
        if score < 0.3:
            recommendations.extend([
                "🎯 Focus on establishing a consistent daily routine",
                "⏰ Set specific time blocks for focused work",
                "📈 Start with small, achievable daily goals (1-2 hours)",
                "🔔 Enable productivity reminders and notifications"
            ])
        elif score < 0.6:
            recommendations.extend([
                "📊 Work on maintaining more consistent work patterns",
                "🎨 Experiment with different time tracking categories",
                "💡 Try the Pomodoro technique for better focus",
                "📅 Plan your week in advance to improve consistency"
            ])
        elif score < 0.8:
            recommendations.extend([
                "🚀 Great progress! Focus on optimizing session lengths",
                "🔍 Analyze your peak productivity hours for better scheduling",
                "🎯 Set more specific goals within your work categories",
                "🤝 Consider sharing your productivity strategies with others"
            ])
        else:
            recommendations.extend([
                "🌟 Outstanding productivity! You're in the top tier",
                "👨‍🏫 Consider mentoring others on productivity best practices",
                "📚 Explore advanced productivity methodologies",
                "🔄 Help optimize team productivity patterns"
            ])
        
        # Category-specific recommendations
        work_insight = category_insights.get('work', {})
        if work_insight.get('avg_session_hours', 0) < 0.5:
            recommendations.append("⏳ Try longer, more focused work sessions (1-2 hours)")
        elif work_insight.get('avg_session_hours', 0) > 4:
            recommendations.append("🛑 Consider taking more breaks during long work sessions")
        
        # Meeting insights
        meeting_insight = category_insights.get('meeting', {})
        if meeting_insight.get('percentage', 0) > 40:
            recommendations.append("📞 High meeting time detected - consider optimizing meeting efficiency")
        
        # Break insights
        break_insight = category_insights.get('break', {})
        if break_insight.get('percentage', 0) < 5:
            recommendations.append("☕ Consider scheduling more breaks for better productivity")
        
        return recommendations[:6]  # Limit to 6 recommendations
    
    async def _get_comparative_metrics(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Get comparative metrics against server averages"""
        try:
            # Get user's weekly total
            user_times = await self.redis.hgetall(f"user_times:{server_id}:{user_id}")
            user_total = int(user_times.get(b'total', b'0'))
            
            # Get server leaderboard for comparison
            leaderboard_key = f"leaderboard:{server_id}:total"
            leaderboard = await self.redis.zrevrange(leaderboard_key, 0, -1, withscores=True)
            
            if not leaderboard:
                return {'error': 'No server data available'}
            
            # Calculate percentile
            user_rank = None
            for i, (uid_bytes, score) in enumerate(leaderboard):
                if int(uid_bytes) == user_id:
                    user_rank = i + 1
                    break
            
            total_users = len(leaderboard)
            percentile = ((total_users - user_rank + 1) / total_users * 100) if user_rank else 0
            
            # Calculate server average
            server_total = sum(score for _, score in leaderboard)
            server_avg = server_total / total_users if total_users > 0 else 0
            
            return {
                'rank': user_rank,
                'total_users': total_users,
                'percentile': round(percentile, 1),
                'above_average': user_total > server_avg,
                'vs_average': f"{((user_total / server_avg - 1) * 100):+.1f}%" if server_avg > 0 else "N/A"
            }
            
        except Exception as e:
            logger.error(f"Error getting comparative metrics: {e}")
            return {'error': str(e)}
    
    async def get_analytics_metrics(self) -> Dict[str, Any]:
        """Get analytics engine performance metrics"""
        cache_hit_total = sum(self.analytics_metrics['cache_hits'].values())
        cache_total = cache_hit_total + self.analytics_metrics['cache_misses']
        hit_rate = (cache_hit_total / cache_total * 100) if cache_total > 0 else 0
        
        avg_computation_time = (
            statistics.mean(self.analytics_metrics['computation_time'])
            if self.analytics_metrics['computation_time'] else 0
        )
        
        return {
            **self.analytics_metrics,
            'cache_hit_rate': round(hit_rate, 2),
            'average_computation_time': round(avg_computation_time * 1000, 2),  # ms
            'cache_sizes': {
                'l1': len(self.l1_cache),
                'l2': len(self.l2_cache),
                'l3': len(self.l3_cache),
                'models': len(self.model_cache)
            }
        }


# ============================================================================
# MAIN ULTIMATE TIME TRACKER CLASS
# ============================================================================

class UltimateTimeTracker(PermissionMixin):
    """Ultimate enterprise-grade time tracking system"""
    
    def __init__(self, redis_url: str = None, max_retries: int = 5, enable_analytics: bool = True):
        super().__init__()
        self.redis_url = redis_url or os.getenv('REDIS_URL', 'redis://localhost:6379')
        self.max_retries = max_retries
        self.enable_analytics = enable_analytics
        
        # Core components
        self.redis = None
        self.circuit_breaker = AdvancedCircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60,
            success_threshold=3
        )
        self.batch_processor = None
        self.analytics = None
        
        # Multi-layer caching system
        self.l1_cache = TTLCache(maxsize=5000, ttl=300)    # 5 minutes - hot data
        self.l2_cache = TTLCache(maxsize=15000, ttl=1800)  # 30 minutes - warm data
        self.l3_cache = LFUCache(maxsize=50000)            # Cold data - LFU eviction
        
        # Specialized caches
        self.user_cache = TTLCache(maxsize=10000, ttl=300)
        self.leaderboard_cache = TTLCache(maxsize=500, ttl=600)
        self.settings_cache = TTLCache(maxsize=2000, ttl=1800)
        self.analytics_cache = TTLCache(maxsize=1000, ttl=900)
        
        # Performance and monitoring
        self.operation_metrics = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'average_response_time': 0.0,
            'peak_response_time': 0.0,
            'operations_per_second': 0.0
        }
        
        self.response_times = deque(maxlen=1000)
        self.error_history = deque(maxlen=100)
        self.last_health_check = 0
        self.health_score = 100.0
        
        # Connection pool settings
        self.connection_pool_size = 20
        self.connection_timeout = 10
        
        # Rate limiting
        self.rate_limiter = TokenBucket(capacity=2000, refill_rate=200)
        
        # Audit logging
        self.audit_enabled = True
        self.audit_buffer = deque(maxlen=1000)
        
        logger.info(f"UltimateTimeTracker initialized - analytics: {enable_analytics}, max_retries: {max_retries}")
    
    async def connect(self):
        """Establish Redis connection with connection pooling"""
        try:
            # Create connection pool
            self.redis = redis.ConnectionPool.from_url(
                self.redis_url,
                encoding='utf-8',
                decode_responses=False,
                max_connections=self.connection_pool_size,
                socket_connect_timeout=self.connection_timeout,
                socket_timeout=self.connection_timeout,
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            # Create Redis client
            self.redis = redis.Redis(connection_pool=self.redis)
            
            # Test connection
            await self.redis.ping()
            
            # Initialize batch processor
            self.batch_processor = EnterpriseBatchProcessor(
                self.redis,
                batch_size=150,
                flush_interval=20.0,
                max_queue_size=15000,
                worker_threads=3
            )
            await self.batch_processor.start()
            
            # Initialize analytics if enabled
            if self.enable_analytics:
                self.analytics = AdvancedAnalyticsEngine(self.redis)
            
            # Start background tasks
            asyncio.create_task(self._health_monitor_loop())
            asyncio.create_task(self._metrics_collection_loop())
            asyncio.create_task(self._cache_maintenance_loop())
            
            logger.info("UltimateTimeTracker connected successfully with all subsystems")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise ConnectionError(f"Redis connection failed: {e}", context={'redis_url': self.redis_url})
    
    async def disconnect(self):
        """Gracefully disconnect with cleanup"""
        try:
            logger.info("Initiating graceful shutdown...")
            
            # Stop batch processor
            if self.batch_processor:
                await self.batch_processor.stop()
                logger.info("Batch processor stopped")
            
            # Flush audit buffer
            if self.audit_enabled and self.audit_buffer:
                await self._flush_audit_buffer()
                logger.info("Audit buffer flushed")
            
            # Close Redis connection
            if self.redis:
                await self.redis.close()
                logger.info("Redis connection closed")
            
            # Clear caches
            self.l1_cache.clear()
            self.l2_cache.clear()
            self.l3_cache.clear()
            self.user_cache.clear()
            self.leaderboard_cache.clear()
            self.settings_cache.clear()
            self.analytics_cache.clear()
            
            logger.info("UltimateTimeTracker disconnected successfully")
            
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
    
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()
    
    # ========================================================================
    # CACHE MANAGEMENT
    # ========================================================================
    
    def _get_from_cache_hierarchy(self, key: str) -> Optional[Any]:
        """Get value from cache hierarchy with promotion"""
        # L1 Cache (hottest)
        if key in self.l1_cache:
            self.operation_metrics['cache_hits'] += 1
            return self.l1_cache[key]
        
        # L2 Cache
        if key in self.l2_cache:
            value = self.l2_cache[key]
            self.l1_cache[key] = value  # Promote to L1
            self.operation_metrics['cache_hits'] += 1
            return value
        
        # L3 Cache
        if key in self.l3_cache:
            value = self.l3_cache[key]
            self.l2_cache[key] = value  # Promote to L2
            self.l1_cache[key] = value  # Promote to L1
            self.operation_metrics['cache_hits'] += 1
            return value
        
        # Specialized caches
        for cache_name, cache in [
            ('user', self.user_cache),
            ('leaderboard', self.leaderboard_cache),
            ('settings', self.settings_cache),
            ('analytics', self.analytics_cache)
        ]:
            if key in cache:
                value = cache[key]
                self.l1_cache[key] = value  # Promote to L1
                self.operation_metrics['cache_hits'] += 1
                logger.debug(f"Cache hit in {cache_name} cache for key: {key}")
                return value
        
        self.operation_metrics['cache_misses'] += 1
        return None
    
    def _set_in_cache_hierarchy(self, key: str, value: Any, cache_type: str = 'general'):
        """Set value in appropriate cache layers"""
        # Always set in L1
        self.l1_cache[key] = value
        self.l2_cache[key] = value
        
        # Set in specialized cache based on type
        if cache_type == 'user':
            self.user_cache[key] = value
        elif cache_type == 'leaderboard':
            self.leaderboard_cache[key] = value
        elif cache_type == 'settings':
            self.settings_cache[key] = value
        elif cache_type == 'analytics':
            self.analytics_cache[key] = value
        
        # Set in L3 for long-term storage
        self.l3_cache[key] = value
    
    def _invalidate_cache_pattern(self, pattern: str):
        """Invalidate cache entries matching pattern"""
        caches = [self.l1_cache, self.l2_cache, self.l3_cache, 
                 self.user_cache, self.leaderboard_cache, 
                 self.settings_cache, self.analytics_cache]
        
        for cache in caches:
            keys_to_remove = [key for key in cache.keys() if pattern in str(key)]
            for key in keys_to_remove:
                cache.pop(key, None)
        
        logger.debug(f"Invalidated cache pattern: {pattern}")
    
    # ========================================================================
    # CORE TIME TRACKING WITH ENTERPRISE FEATURES
    # ========================================================================
    
    async def add_time(self, server_id: int, user_id: int, category: str, seconds: int,
                      session_id: str = None, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """Add time entry with comprehensive validation and processing"""
        operation_start = time.time()
        
        # Rate limiting
        if not self.rate_limiter.consume():
            return {
                'success': False,
                'message': 'Rate limit exceeded. Please slow down.',
                'error_code': 'RATE_LIMITED'
            }
        
        try:
            # Input validation
            validation_result = await self._validate_time_entry(server_id, user_id, category, seconds)
            if not validation_result['valid']:
                return {
                    'success': False,
                    'message': validation_result['message'],
                    'error_code': 'VALIDATION_ERROR'
                }
            
            # Audit logging
            if self.audit_enabled:
                await self._log_audit_event('add_time', {
                    'server_id': server_id,
                    'user_id': user_id,
                    'category': category,
                    'seconds': seconds,
                    'session_id': session_id,
                    'metadata': metadata
                })
            
            # Execute with circuit breaker protection
            result = await self.circuit_breaker.call(
                self._add_time_internal, 
                server_id, user_id, category, seconds, session_id, metadata
            )
            
            # Update metrics
            operation_time = time.time() - operation_start
            self._update_operation_metrics(operation_time, success=True)
            
            logger.info(f"Time added successfully - user: {user_id}, category: {category}, seconds: {seconds}")
            return result
            
        except CircuitBreakerOpenError as e:
            return {
                'success': False,
                'message': 'Service temporarily unavailable. Please try again later.',
                'error_code': 'SERVICE_UNAVAILABLE',
                'retry_after': e.context.get('retry_after')
            }
        except Exception as e:
            operation_time = time.time() - operation_start
            self._update_operation_metrics(operation_time, success=False)
            self._log_error(e, {'operation': 'add_time', 'user_id': user_id})
            
            return {
                'success': False,
                'message': f'An error occurred: {str(e)}',
                'error_code': 'INTERNAL_ERROR'
            }
    
    async def _validate_time_entry(self, server_id: int, user_id: int, category: str, seconds: int) -> Dict[str, Any]:
        """Comprehensive time entry validation"""
        # Basic validation
        if seconds <= 0:
            return {'valid': False, 'message': 'Time must be positive'}
        
        if seconds > 86400:  # 24 hours
            return {'valid': False, 'message': 'Cannot add more than 24 hours at once'}
        
        # Category validation - server must have categories configured
        available_categories = await self.list_categories(server_id)
        if not available_categories:
            return {
                'valid': False, 
                'message': 'No categories configured for this server. Ask your server admins to set up categories using `/categories add <n>`.'
            }
        
        if not await self.validate_category(server_id, category):
            return {
                'valid': False,
                'message': f"Category '{category}' is not available. Available: {', '.join(available_categories)}"
            }
        
        # User validation (check if user exists and has permissions)
        # This would integrate with your permission system
        
        # Rate limiting per user
        # Implementation depends on requirements
        
        return {'valid': True}
    
    async def _add_time_internal(self, server_id: int, user_id: int, category: str, 
                                seconds: int, session_id: str = None, 
                                metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """Internal time addition with atomic operations"""
        timestamp = time.time()
        
        # Generate session ID if not provided
        if not session_id:
            session_id = str(uuid.uuid4())
        
        # Prepare batch operations
        operations = []
        
        # User times update
        operations.append(BatchOperation(
            operation_type='hincrby',
            key=f"user_times:{server_id}:{user_id}",
            value={category: seconds, 'total': seconds},
            priority=BatchOperationType.HIGH_PRIORITY
        ))
        
        # Server totals update
        operations.append(BatchOperation(
            operation_type='hincrby',
            key=f"server_times:{server_id}",
            value={category: seconds, 'total': seconds},
            priority=BatchOperationType.HIGH_PRIORITY
        ))
        
        # Time entry for analytics
        entry_data = {
            'server_id': server_id,
            'user_id': user_id,
            'category': category,
            'seconds': seconds,
            'session_id': session_id,
            'metadata': metadata or {}
        }
        
        operations.append(BatchOperation(
            operation_type='zadd',
            key=f"time_entries:{server_id}:{user_id}",
            value={json.dumps(entry_data): timestamp},
            priority=BatchOperationType.NORMAL_PRIORITY
        ))
        
        # Leaderboard updates
        operations.append(BatchOperation(
            operation_type='zincrby',
            key=f"leaderboard:{server_id}:total",
            value=(seconds, user_id),
            priority=BatchOperationType.NORMAL_PRIORITY
        ))
        
        operations.append(BatchOperation(
            operation_type='zincrby',
            key=f"leaderboard:{server_id}:{category}",
            value=(seconds, user_id),
            priority=BatchOperationType.NORMAL_PRIORITY
        ))
        
        # Add operations to batch processor
        for operation in operations:
            await self.batch_processor.add_operation(operation)
        
        # Invalidate relevant caches
        self._invalidate_cache_pattern(f"user_times:{server_id}:{user_id}")
        self._invalidate_cache_pattern(f"leaderboard:{server_id}")
        
        # Clean up old entries (background task)
        asyncio.create_task(self._cleanup_old_entries(server_id, user_id, timestamp))
        
        return {
            'success': True,
            'message': f"Added {self._format_time(seconds)} to {category}",
            'seconds_added': seconds,
            'category': category,
            'session_id': session_id,
            'timestamp': timestamp
        }
    
    async def _cleanup_old_entries(self, server_id: int, user_id: int, current_timestamp: float):
        """Background cleanup of old time entries"""
        try:
            # Keep entries for 180 days (configurable)
            retention_days = 180
            cutoff_timestamp = current_timestamp - (retention_days * 24 * 3600)
            
            entries_key = f"time_entries:{server_id}:{user_id}"
            
            # Use batch processor for cleanup
            cleanup_operation = BatchOperation(
                operation_type='zremrangebyscore',
                key=entries_key,
                value=(0, cutoff_timestamp),
                priority=BatchOperationType.BACKGROUND
            )
            
            await self.batch_processor.add_operation(cleanup_operation)
            
        except Exception as e:
            logger.warning(f"Error in background cleanup: {e}")
    
    # Continue with remaining methods...
    # [The implementation would continue with all the other methods from the original file,
    #  but enhanced with the enterprise features shown above]
    
    def _format_time(self, seconds: int) -> str:
        """Format seconds into human-readable time"""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            if remaining_seconds == 0:
                return f"{minutes}m"
            return f"{minutes}m {remaining_seconds}s"
        else:
            hours = seconds // 3600
            remaining_minutes = (seconds % 3600) // 60
            if remaining_minutes == 0:
                return f"{hours}h"
            return f"{hours}h {remaining_minutes}m"
    
    def _update_operation_metrics(self, response_time: float, success: bool):
        """Update operation performance metrics"""
        self.operation_metrics['total_operations'] += 1
        self.response_times.append(response_time)
        
        if success:
            self.operation_metrics['successful_operations'] += 1
        else:
            self.operation_metrics['failed_operations'] += 1
        
        # Update averages
        if self.response_times:
            self.operation_metrics['average_response_time'] = statistics.mean(self.response_times)
            self.operation_metrics['peak_response_time'] = max(self.response_times)
        
        # Calculate operations per second (moving average)
        current_ops_per_sec = 1 / response_time if response_time > 0 else 0
        current_rate = self.operation_metrics['operations_per_second']
        self.operation_metrics['operations_per_second'] = 0.9 * current_rate + 0.1 * current_ops_per_sec
    
    def _log_error(self, error: Exception, context: Dict[str, Any]):
        """Log error with context"""
        error_entry = {
            'timestamp': datetime.now(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context,
            'traceback': traceback.format_exc()
        }
        
        self.error_history.append(error_entry)
        logger.error(f"Operation error: {error_entry}")
    
    async def _log_audit_event(self, action: str, data: Dict[str, Any]):
        """Log audit event"""
        if not self.audit_enabled:
            return
        
        audit_entry = {
            'timestamp': datetime.now().isoformat(),
            'action': action,
            'data': data,
            'session_id': data.get('session_id'),
            'user_id': data.get('user_id'),
            'server_id': data.get('server_id')
        }
        
        self.audit_buffer.append(audit_entry)
        
        # Flush if buffer is getting full
        if len(self.audit_buffer) >= 900:
            await self._flush_audit_buffer()
    
    async def _flush_audit_buffer(self):
        """Flush audit buffer to storage"""
        if not self.audit_buffer:
            return
        
        try:
            # Store audit events in Redis
            audit_data = list(self.audit_buffer)
            self.audit_buffer.clear()
            
            # Use batch processor for audit storage
            for entry in audit_data:
                audit_operation = BatchOperation(
                    operation_type='lpush',
                    key=f"audit_log:{entry['server_id']}",
                    value=json.dumps(entry),
                    priority=BatchOperationType.LOW_PRIORITY
                )
                await self.batch_processor.add_operation(audit_operation)
            
            logger.info(f"Flushed {len(audit_data)} audit entries")
            
        except Exception as e:
            logger.error(f"Error flushing audit buffer: {e}")
    
    async def _health_monitor_loop(self):
        """Background health monitoring"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                await self._update_health_score()
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
    
    async def _metrics_collection_loop(self):
        """Background metrics collection"""
        while True:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                await self._collect_system_metrics()
            except Exception as e:
                logger.error(f"Metrics collection error: {e}")
    
    async def _cache_maintenance_loop(self):
        """Background cache maintenance"""
        while True:
            try:
                await asyncio.sleep(1800)  # Every 30 minutes
                await self._maintain_caches()
            except Exception as e:
                logger.error(f"Cache maintenance error: {e}")
    
    async def _update_health_score(self):
        """Update overall system health score"""
        try:
            # Component health scores
            circuit_health = 100 - (self.circuit_breaker.failure_count * 10)
            
            batch_metrics = await self.batch_processor.get_metrics()
            batch_health = 100
            if batch_metrics['total_operations'] > 0:
                success_rate = batch_metrics['successful_operations'] / batch_metrics['total_operations']
                batch_health = success_rate * 100
            
            # Cache performance
            cache_total = self.operation_metrics['cache_hits'] + self.operation_metrics['cache_misses']
            cache_hit_rate = self.operation_metrics['cache_hits'] / cache_total * 100 if cache_total > 0 else 100
            
            # Response time health
            response_health = 100
            if self.operation_metrics['average_response_time'] > 1.0:  # > 1 second
                response_health = max(0, 100 - (self.operation_metrics['average_response_time'] - 1.0) * 50)
            
            # Calculate overall health
            self.health_score = statistics.mean([
                max(0, circuit_health),
                batch_health,
                cache_hit_rate,
                response_health
            ])
            
            self.last_health_check = time.time()
            
            if self.health_score < 70:
                logger.warning(f"System health degraded: {self.health_score:.1f}%")
            
        except Exception as e:
            logger.error(f"Error updating health score: {e}")
            self.health_score = 50  # Degraded health on error
    
    async def _collect_system_metrics(self):
        """Collect and log system performance metrics"""
        try:
            # Circuit breaker metrics
            circuit_metrics = self.circuit_breaker.get_metrics()
            
            # Batch processor metrics
            batch_metrics = await self.batch_processor.get_metrics()
            
            # Cache metrics
            cache_metrics = {
                'l1_size': len(self.l1_cache),
                'l2_size': len(self.l2_cache),
                'l3_size': len(self.l3_cache),
                'user_cache_size': len(self.user_cache),
                'leaderboard_cache_size': len(self.leaderboard_cache),
                'settings_cache_size': len(self.settings_cache),
                'analytics_cache_size': len(self.analytics_cache),
                'hit_rate': (self.operation_metrics['cache_hits'] / 
                           (self.operation_metrics['cache_hits'] + self.operation_metrics['cache_misses']) * 100
                           if self.operation_metrics['cache_misses'] > 0 else 100)
            }
            
            # Analytics metrics (if enabled)
            analytics_metrics = {}
            if self.analytics:
                analytics_metrics = await self.analytics.get_analytics_metrics()
            
            # Comprehensive metrics log
            logger.info(f"System Metrics Report - "
                       f"Health: {self.health_score:.1f}%, "
                       f"Circuit: {circuit_metrics['state']}, "
                       f"Batch Queue: {batch_metrics['queue_size']}, "
                       f"Cache Hit Rate: {cache_metrics['hit_rate']:.1f}%, "
                       f"Avg Response: {self.operation_metrics['average_response_time']:.3f}s, "
                       f"Ops/sec: {self.operation_metrics['operations_per_second']:.1f}")
            
            # Store metrics for historical analysis
            metrics_data = {
                'timestamp': time.time(),
                'health_score': self.health_score,
                'circuit_breaker': circuit_metrics,
                'batch_processor': batch_metrics,
                'cache_performance': cache_metrics,
                'operation_metrics': self.operation_metrics.copy(),
                'analytics': analytics_metrics
            }
            
            # Store in Redis for monitoring dashboard
            metrics_key = f"system_metrics:{int(time.time() // 300)}"  # 5-minute buckets
            await self.redis.setex(metrics_key, 86400, json.dumps(metrics_data))  # Keep for 24 hours
            
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
    
    async def _maintain_caches(self):
        """Perform cache maintenance and optimization"""
        try:
            # Get cache sizes before maintenance
            sizes_before = {
                'l1': len(self.l1_cache),
                'l2': len(self.l2_cache),
                'l3': len(self.l3_cache),
                'user': len(self.user_cache),
                'leaderboard': len(self.leaderboard_cache),
                'settings': len(self.settings_cache),
                'analytics': len(self.analytics_cache)
            }
            
            # Clear expired entries (TTL caches handle this automatically, but we can optimize)
            # For LFU cache (L3), manually remove least frequently used items if near capacity
            if len(self.l3_cache) > self.l3_cache.maxsize * 0.9:
                # Remove 10% of least frequently used items
                items_to_remove = int(self.l3_cache.maxsize * 0.1)
                # LFU cache automatically handles this, but we can monitor
                logger.info(f"L3 cache near capacity, automatic LFU eviction active")
            
            # Cache performance analysis
            total_cache_ops = self.operation_metrics['cache_hits'] + self.operation_metrics['cache_misses']
            if total_cache_ops > 1000:  # Only analyze if we have significant data
                hit_rate = self.operation_metrics['cache_hits'] / total_cache_ops
                
                if hit_rate < 0.8:  # Less than 80% hit rate
                    logger.warning(f"Low cache hit rate: {hit_rate:.2f}%. Consider cache tuning.")
                
                # Reset counters for next period
                self.operation_metrics['cache_hits'] = 0
                self.operation_metrics['cache_misses'] = 0
            
            sizes_after = {
                'l1': len(self.l1_cache),
                'l2': len(self.l2_cache),
                'l3': len(self.l3_cache),
                'user': len(self.user_cache),
                'leaderboard': len(self.leaderboard_cache),
                'settings': len(self.settings_cache),
                'analytics': len(self.analytics_cache)
            }
            
            logger.debug(f"Cache maintenance completed - Before: {sizes_before}, After: {sizes_after}")
            
        except Exception as e:
            logger.error(f"Error during cache maintenance: {e}")
    
    # ========================================================================
    # ENHANCED CATEGORY MANAGEMENT
    # ========================================================================
    
    async def add_category(self, server_id: int, category: str, user_id: int = None,
                          description: str = None, color: str = None,
                          productivity_weight: float = 1.0) -> Dict[str, Any]:
        """Add a new category with enhanced metadata"""
        try:
            # Permission validation
            if user_id and not await self.check_permission(server_id, user_id, 'manage_categories'):
                return {
                    'success': False,
                    'message': "You don't have permission to manage categories",
                    'error_code': 'PERMISSION_DENIED'
                }
            
            # Enhanced validation
            validation_result = await self._validate_category_data(category, description, color, productivity_weight)
            if not validation_result['valid']:
                return {
                    'success': False,
                    'message': validation_result['message'],
                    'error_code': 'VALIDATION_ERROR'
                }
            
            category = category.lower().strip()
            
            # Get current settings
            settings = await self.get_server_settings(server_id)
            
            # Check if category already exists
            if category in settings.categories:
                return {
                    'success': False,
                    'message': f"Category '{category}' already exists",
                    'error_code': 'CATEGORY_EXISTS'
                }
            
            # Check category limits
            if len(settings.categories) >= 50:  # Configurable limit
                return {
                    'success': False,
                    'message': "Maximum number of categories (50) reached",
                    'error_code': 'CATEGORY_LIMIT'
                }
            
            # Add category with metadata
            settings.categories.add(category)
            
            # Store category metadata
            category_metadata = {
                'name': category,
                'description': description or f"Time tracking for {category}",
                'color': color or self._generate_category_color(category),
                'productivity_weight': productivity_weight,
                'created_by': user_id,
                'created_at': datetime.now().isoformat(),
                'usage_count': 0,
                'total_time': 0
            }
            
            metadata_key = f"category_metadata:{server_id}:{category}"
            await self.redis.setex(metadata_key, 86400 * 365, json.dumps(category_metadata))  # 1 year TTL
            
            # Save updated settings
            await self._save_server_settings(server_id, settings)
            
            # Invalidate caches
            self._invalidate_cache_pattern(f"settings:{server_id}")
            self._invalidate_cache_pattern(f"categories:{server_id}")
            
            # Audit log
            if self.audit_enabled:
                await self._log_audit_event('add_category', {
                    'server_id': server_id,
                    'category': category,
                    'user_id': user_id,
                    'metadata': category_metadata
                })
            
            logger.info(f"Added category '{category}' to server {server_id} by user {user_id}")
            
            return {
                'success': True,
                'message': f"Category '{category}' added successfully",
                'category': category,
                'metadata': category_metadata
            }
            
        except Exception as e:
            logger.error(f"Error adding category: {e}")
            return {
                'success': False,
                'message': f"Error adding category: {str(e)}",
                'error_code': 'INTERNAL_ERROR'
            }
    
    async def _validate_category_data(self, category: str, description: str, 
                                    color: str, productivity_weight: float) -> Dict[str, Any]:
        """Validate category data with enhanced checks"""
        # Category name validation
        if not category or len(category.strip()) == 0:
            return {'valid': False, 'message': 'Category name cannot be empty'}
        
        if len(category) > 50:
            return {'valid': False, 'message': 'Category name must be 50 characters or less'}
        
        # Allow alphanumeric, spaces, hyphens, underscores
        import re
        if not re.match(r'^[a-zA-Z0-9\s\-_]+$', category):
            return {'valid': False, 'message': 'Category name contains invalid characters'}
        
        # Reserved category names
        reserved_names = {'all', 'total', 'admin', 'system', 'config', 'settings'}
        if category.lower().strip() in reserved_names:
            return {'valid': False, 'message': f"'{category}' is a reserved category name"}
        
        # Description validation
        if description and len(description) > 200:
            return {'valid': False, 'message': 'Description must be 200 characters or less'}
        
        # Color validation (hex color)
        if color:
            if not re.match(r'^#[0-9A-Fa-f]{6}$', color):
                return {'valid': False, 'message': 'Color must be a valid hex color (e.g., #FF5733)'}
        
        # Productivity weight validation
        if not 0.0 <= productivity_weight <= 5.0:
            return {'valid': False, 'message': 'Productivity weight must be between 0.0 and 5.0'}
        
        return {'valid': True}
    
    def _generate_category_color(self, category: str) -> str:
        """Generate a consistent color for a category"""
        # Use hash to generate consistent colors
        hash_object = hashlib.md5(category.encode())
        hash_hex = hash_object.hexdigest()
        
        # Extract RGB values from hash
        r = int(hash_hex[0:2], 16)
        g = int(hash_hex[2:4], 16)
        b = int(hash_hex[4:6], 16)
        
        # Adjust brightness to ensure readability
        brightness = (r + g + b) / 3
        if brightness < 128:  # Too dark
            r = min(255, r + 64)
            g = min(255, g + 64)
            b = min(255, b + 64)
        
        return f"#{r:02x}{g:02x}{b:02x}"
    
    async def remove_category(self, server_id: int, category: str, user_id: int = None,
                            force: bool = False) -> Dict[str, Any]:
        """Remove a category with enhanced safety checks"""
        try:
            # Permission validation
            if user_id and not await self.check_permission(server_id, user_id, 'manage_categories'):
                return {
                    'success': False,
                    'message': "You don't have permission to manage categories",
                    'error_code': 'PERMISSION_DENIED'
                }
            
            category = category.lower().strip()
            settings = await self.get_server_settings(server_id)
            
            # Check if category exists
            if category not in settings.categories:
                return {
                    'success': False,
                    'message': f"Category '{category}' does not exist",
                    'error_code': 'CATEGORY_NOT_FOUND'
                }
            
            # Check if it's a default category
            default_categories = {"work", "break", "meeting"}
            if category in default_categories and not force:
                return {
                    'success': False,
                    'message': f"Cannot remove default category '{category}'. Use force=True to override.",
                    'error_code': 'DEFAULT_CATEGORY'
                }
            
            # Check usage
            usage_info = await self._get_category_usage_info(server_id, category)
            if usage_info['total_entries'] > 0 and not force:
                return {
                    'success': False,
                    'message': f"Category '{category}' has {usage_info['total_entries']} time entries from {usage_info['unique_users']} users. Use force=True to archive instead of delete.",
                    'error_code': 'CATEGORY_IN_USE',
                    'usage_info': usage_info
                }
            
            # Archive or delete based on usage
            if usage_info['total_entries'] > 0:
                # Archive the category instead of deleting
                await self._archive_category(server_id, category, user_id)
                action = "archived"
            else:
                # Safe to delete
                settings.categories.discard(category)
                await self._save_server_settings(server_id, settings)
                
                # Remove metadata
                metadata_key = f"category_metadata:{server_id}:{category}"
                await self.redis.delete(metadata_key)
                action = "deleted"
            
            # Invalidate caches
            self._invalidate_cache_pattern(f"settings:{server_id}")
            self._invalidate_cache_pattern(f"categories:{server_id}")
            
            # Audit log
            if self.audit_enabled:
                await self._log_audit_event('remove_category', {
                    'server_id': server_id,
                    'category': category,
                    'user_id': user_id,
                    'action': action,
                    'force': force,
                    'usage_info': usage_info
                })
            
            logger.info(f"Category '{category}' {action} from server {server_id} by user {user_id}")
            
            return {
                'success': True,
                'message': f"Category '{category}' {action} successfully",
                'action': action,
                'usage_info': usage_info
            }
            
        except Exception as e:
            logger.error(f"Error removing category: {e}")
            return {
                'success': False,
                'message': f"Error removing category: {str(e)}",
                'error_code': 'INTERNAL_ERROR'
            }
    
    async def _get_category_usage_info(self, server_id: int, category: str) -> Dict[str, Any]:
        """Get detailed usage information for a category"""
        try:
            # Search for category usage across all users
            pattern = f"user_times:{server_id}:*"
            cursor = 0
            total_entries = 0
            total_time = 0
            unique_users = 0
            user_usage = []
            
            while True:
                cursor, keys = await self.redis.scan(cursor, match=pattern, count=100)
                
                for key in keys:
                    time_data = await self.redis.hget(key, category)
                    if time_data:
                        category_time = int(time_data)
                        if category_time > 0:
                            unique_users += 1
                            total_time += category_time
                            
                            # Extract user ID from key
                            user_id = int(key.split(':')[-1])
                            user_usage.append({
                                'user_id': user_id,
                                'total_time': category_time
                            })
                
                if cursor == 0:
                    break
            
            # Get entry count from time_entries
            entry_count = 0
            pattern = f"time_entries:{server_id}:*"
            cursor = 0
            
            while True:
                cursor, keys = await self.redis.scan(cursor, match=pattern, count=100)
                
                for key in keys:
                    entries = await self.redis.zrange(key, 0, -1)
                    for entry_data in entries:
                        try:
                            entry = json.loads(entry_data)
                            if entry.get('category') == category:
                                entry_count += 1
                        except (json.JSONDecodeError, KeyError):
                            continue
                
                if cursor == 0:
                    break
            
            return {
                'total_entries': entry_count,
                'unique_users': unique_users,
                'total_time': total_time,
                'total_time_formatted': self._format_time(total_time),
                'user_usage': sorted(user_usage, key=lambda x: x['total_time'], reverse=True)[:10]  # Top 10 users
            }
            
        except Exception as e:
            logger.error(f"Error getting category usage info: {e}")
            return {
                'total_entries': 0,
                'unique_users': 0,
                'total_time': 0,
                'total_time_formatted': '0s',
                'user_usage': []
            }
    
    async def _archive_category(self, server_id: int, category: str, user_id: int):
        """Archive a category instead of deleting it"""
        try:
            # Move category to archived set
            archived_categories_key = f"archived_categories:{server_id}"
            archive_data = {
                'category': category,
                'archived_by': user_id,
                'archived_at': datetime.now().isoformat(),
                'reason': 'Category removal with existing data'
            }
            
            await self.redis.hset(archived_categories_key, category, json.dumps(archive_data))
            
            # Update category metadata to mark as archived
            metadata_key = f"category_metadata:{server_id}:{category}"
            metadata_data = await self.redis.get(metadata_key)
            
            if metadata_data:
                metadata = json.loads(metadata_data)
                metadata['archived'] = True
                metadata['archived_at'] = datetime.now().isoformat()
                metadata['archived_by'] = user_id
                
                await self.redis.setex(metadata_key, 86400 * 365 * 5, json.dumps(metadata))  # 5 year retention
            
            logger.info(f"Category '{category}' archived for server {server_id}")
            
        except Exception as e:
            logger.error(f"Error archiving category: {e}")
    
    async def list_categories(self, server_id: int, include_archived: bool = False,
                            include_metadata: bool = False) -> Union[List[str], Dict[str, Any]]:
        """Get comprehensive list of categories with optional metadata"""
        try:
            cache_key = f"categories:{server_id}:{include_archived}:{include_metadata}"
            cached_result = self._get_from_cache_hierarchy(cache_key)
            if cached_result is not None:
                return cached_result
            
            settings = await self.get_server_settings(server_id)
            active_categories = list(settings.categories)
            
            if not include_metadata and not include_archived:
                result = sorted(active_categories)
                self._set_in_cache_hierarchy(cache_key, result, 'settings')
                return result
            
            # Build comprehensive category data
            result = {}
            
            # Active categories
            for category in active_categories:
                metadata = await self._get_category_metadata(server_id, category)
                usage_info = await self._get_category_usage_info(server_id, category)
                
                result[category] = {
                    'active': True,
                    'metadata': metadata,
                    'usage': usage_info if include_metadata else None
                }
            
            # Archived categories
            if include_archived:
                archived_key = f"archived_categories:{server_id}"
                archived_data = await self.redis.hgetall(archived_key)
                
                for category_bytes, archive_info_bytes in archived_data.items():
                    category = category_bytes.decode('utf-8')
                    archive_info = json.loads(archive_info_bytes.decode('utf-8'))
                    
                    result[category] = {
                        'active': False,
                        'archived_info': archive_info,
                        'metadata': await self._get_category_metadata(server_id, category) if include_metadata else None
                    }
            
            # Cache result
            self._set_in_cache_hierarchy(cache_key, result, 'settings')
            
            return result if include_metadata or include_archived else sorted(active_categories)
            
        except Exception as e:
            logger.error(f"Error listing categories: {e}")
            return ["work", "break", "meeting"]  # Fallback
    
    async def _get_category_metadata(self, server_id: int, category: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific category"""
        try:
            metadata_key = f"category_metadata:{server_id}:{category}"
            metadata_data = await self.redis.get(metadata_key)
            
            if metadata_data:
                return json.loads(metadata_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting category metadata: {e}")
            return None
    
    async def validate_category(self, server_id: int, category: str) -> bool:
        """Enhanced category validation with caching"""
        try:
            cache_key = f"validate_category:{server_id}:{category}"
            cached_result = self._get_from_cache_hierarchy(cache_key)
            if cached_result is not None:
                return cached_result
            
            categories = await self.list_categories(server_id)
            is_valid = category.lower().strip() in categories
            
            # Cache validation result for 5 minutes
            self._set_in_cache_hierarchy(cache_key, is_valid, 'settings')
            
            return is_valid
            
        except Exception as e:
            logger.error(f"Error validating category: {e}")
            # Fallback to basic validation
            return category.lower().strip() in ["work", "break", "meeting"]
    
    # ========================================================================
    # ENHANCED USER DATA RETRIEVAL
    # ========================================================================
    
    async def get_user_times(self, server_id: int, user_id: int, 
                           include_metadata: bool = False,
                           time_range: str = 'all') -> Dict[str, Any]:
        """Get comprehensive user time data with optional filtering"""
        cache_key = f"user_times:{server_id}:{user_id}:{include_metadata}:{time_range}"
        
        # Check cache
        cached_result = self._get_from_cache_hierarchy(cache_key)
        if cached_result is not None:
            return cached_result
        
        try:
            # Get basic time data
            user_times_key = f"user_times:{server_id}:{user_id}"
            times_data = await self.redis.hgetall(user_times_key)
            
            if not times_data:
                result = {'total': 0, 'categories': {}}
            else:
                result = {
                    k.decode('utf-8'): int(v.decode('utf-8'))
                    for k, v in times_data.items()
                }
            
            # Add formatted times
            formatted_result = {
                'total': result.get('total', 0),
                'total_formatted': self._format_time(result.get('total', 0)),
                'categories': {}
            }
            
            for category, seconds in result.items():
                if category != 'total':
                    formatted_result['categories'][category] = {
                        'seconds': seconds,
                        'formatted': self._format_time(seconds),
                        'percentage': (seconds / result.get('total', 1)) * 100 if result.get('total', 0) > 0 else 0
                    }
            
            # Add metadata if requested
            if include_metadata:
                formatted_result['metadata'] = await self._get_user_metadata(server_id, user_id)
                formatted_result['analytics'] = await self._get_user_analytics_summary(server_id, user_id)
            
            # Apply time range filtering for detailed entries
            if time_range != 'all':
                formatted_result['entries'] = await self._get_user_entries_in_range(server_id, user_id, time_range)
            
            # Cache the result
            self._set_in_cache_hierarchy(cache_key, formatted_result, 'user')
            
            return formatted_result
            
        except Exception as e:
            logger.error(f"Error getting user times: {e}")
            return {'total': 0, 'categories': {}, 'error': str(e)}
    
    async def _get_user_metadata(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Get user metadata and statistics"""
        try:
            # Get join date (first time entry)
            entries_key = f"time_entries:{server_id}:{user_id}"
            first_entry = await self.redis.zrange(entries_key, 0, 0, withscores=True)
            
            join_date = None
            if first_entry:
                join_date = datetime.fromtimestamp(first_entry[0][1]).isoformat()
            
            # Get recent activity
            recent_entries = await self.redis.zrevrange(entries_key, 0, 9, withscores=True)
            last_activity = None
            if recent_entries:
                last_activity = datetime.fromtimestamp(recent_entries[0][1]).isoformat()
            
            # Get session count
            total_entries = await self.redis.zcard(entries_key)
            
            return {
                'join_date': join_date,
                'last_activity': last_activity,
                'total_sessions': total_entries,
                'data_retention_days': 180  # Based on cleanup policy
            }
            
        except Exception as e:
            logger.error(f"Error getting user metadata: {e}")
            return {}
    
    async def _get_user_analytics_summary(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Get summary analytics for user"""
        try:
            if not self.analytics:
                return {}
            
            # Get basic productivity score
            productivity_score = await self.analytics.calculate_productivity_score(server_id, user_id, days=7)
            
            # Get streak
            streak = await self.analytics._calculate_advanced_streak(server_id, user_id)
            
            # Get consistency rating
            consistency = await self.analytics._calculate_consistency_rating(server_id, user_id)
            
            return {
                'productivity_score': round(productivity_score * 100, 1),
                'grade': self.analytics._score_to_grade(productivity_score),
                'streak_days': streak,
                'consistency_rating': consistency
            }
            
        except Exception as e:
            logger.error(f"Error getting user analytics summary: {e}")
            return {}
    
    async def _get_user_entries_in_range(self, server_id: int, user_id: int, time_range: str) -> List[Dict[str, Any]]:
        """Get user entries within a specific time range"""
        try:
            # Calculate time range
            now = time.time()
            range_seconds = {
                'today': 24 * 3600,
                'week': 7 * 24 * 3600,
                'month': 30 * 24 * 3600,
                'quarter': 90 * 24 * 3600
            }
            
            if time_range not in range_seconds:
                return []
            
            start_time = now - range_seconds[time_range]
            
            # Get entries in range
            entries_key = f"time_entries:{server_id}:{user_id}"
            entries_data = await self.redis.zrevrangebyscore(
                entries_key, '+inf', start_time, withscores=True
            )
            
            entries = []
            for entry_data, timestamp in entries_data:
                try:
                    entry = json.loads(entry_data)
                    entry['timestamp'] = timestamp
                    entry['formatted_time'] = self._format_time(entry['seconds'])
                    entry['date'] = datetime.fromtimestamp(timestamp).isoformat()
                    entries.append(entry)
                except (json.JSONDecodeError, KeyError):
                    continue
            
            return entries
            
        except Exception as e:
            logger.error(f"Error getting user entries in range: {e}")
            return []
    
    # ========================================================================
    # ENHANCED LEADERBOARD SYSTEM
    # ========================================================================
    
    async def get_server_leaderboard(self, server_id: int, category: str = None,
                                   limit: int = 10, time_range: str = 'all',
                                   include_stats: bool = False) -> List[Dict[str, Any]]:
        """Get enhanced server leaderboard with multiple options"""
        cache_key = f"leaderboard:{server_id}:{category or 'total'}:{limit}:{time_range}:{include_stats}"
        
        # Check cache
        cached_result = self._get_from_cache_hierarchy(cache_key)
        if cached_result is not None:
            return cached_result
        
        try:
            if time_range == 'all':
                # Use pre-computed leaderboards
                result = await self._get_precomputed_leaderboard(server_id, category, limit, include_stats)
            else:
                # Calculate dynamic leaderboard for time range
                result = await self._calculate_dynamic_leaderboard(server_id, category, limit, time_range, include_stats)
            
            # Cache the result
            self._set_in_cache_hierarchy(cache_key, result, 'leaderboard')
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting server leaderboard: {e}")
            return []
    
    async def _get_precomputed_leaderboard(self, server_id: int, category: str, 
                                         limit: int, include_stats: bool) -> List[Dict[str, Any]]:
        """Get leaderboard from pre-computed Redis sorted sets"""
        try:
            if category:
                leaderboard_key = f"leaderboard:{server_id}:{category}"
            else:
                leaderboard_key = f"leaderboard:{server_id}:total"
            
            # Get top users
            top_users = await self.redis.zrevrange(
                leaderboard_key, 0, limit - 1, withscores=True
            )
            
            leaderboard = []
            for i, (user_id_bytes, score) in enumerate(top_users):
                user_id = int(user_id_bytes.decode('utf-8'))
                entry = {
                    'rank': i + 1,
                    'user_id': user_id,
                    'time_seconds': int(score),
                    'time_formatted': self._format_time(int(score))
                }
                
                if include_stats:
                    entry['stats'] = await self._get_user_leaderboard_stats(server_id, user_id, category)
                
                leaderboard.append(entry)
            
            return leaderboard
            
        except Exception as e:
            logger.error(f"Error getting precomputed leaderboard: {e}")
            return []
    
    async def _calculate_dynamic_leaderboard(self, server_id: int, category: str,
                                           limit: int, time_range: str, include_stats: bool) -> List[Dict[str, Any]]:
        """Calculate leaderboard for specific time range"""
        try:
            # Calculate time range
            now = time.time()
            range_seconds = {
                'today': 24 * 3600,
                'week': 7 * 24 * 3600,
                'month': 30 * 24 * 3600,
                'quarter': 90 * 24 * 3600
            }
            
            if time_range not in range_seconds:
                return []
            
            start_time = now - range_seconds[time_range]
            
            # Collect user times for the range
            user_times = defaultdict(int)
            
            # Search all user time entries
            pattern = f"time_entries:{server_id}:*"
            cursor = 0
            
            while True:
                cursor, keys = await self.redis.scan(cursor, match=pattern, count=100)
                
                for key in keys:
                    # Extract user ID from key
                    user_id = int(key.split(':')[-1])
                    
                    # Get entries in time range
                    entries_data = await self.redis.zrevrangebyscore(
                        key, '+inf', start_time, withscores=True
                    )
                    
                    for entry_data, timestamp in entries_data:
                        try:
                            entry = json.loads(entry_data)
                            if not category or entry.get('category') == category:
                                user_times[user_id] += entry['seconds']
                        except (json.JSONDecodeError, KeyError):
                            continue
                
                if cursor == 0:
                    break
            
            # Sort and limit
            sorted_users = sorted(user_times.items(), key=lambda x: x[1], reverse=True)[:limit]
            
            leaderboard = []
            for i, (user_id, total_seconds) in enumerate(sorted_users):
                entry = {
                    'rank': i + 1,
                    'user_id': user_id,
                    'time_seconds': total_seconds,
                    'time_formatted': self._format_time(total_seconds),
                    'time_range': time_range
                }
                
                if include_stats:
                    entry['stats'] = await self._get_user_leaderboard_stats(server_id, user_id, category)
                
                leaderboard.append(entry)
            
            return leaderboard
            
        except Exception as e:
            logger.error(f"Error calculating dynamic leaderboard: {e}")
            return []
    
    async def _get_user_leaderboard_stats(self, server_id: int, user_id: int, category: str) -> Dict[str, Any]:
        """Get additional stats for leaderboard entries"""
        try:
            stats = {}
            
            # Get category breakdown
            user_times = await self.get_user_times(server_id, user_id)
            if category:
                category_data = user_times.get('categories', {}).get(category, {})
                stats['category_percentage'] = category_data.get('percentage', 0)
            
            # Get recent activity
            entries_key = f"time_entries:{server_id}:{user_id}"
            recent_entry = await self.redis.zrevrange(entries_key, 0, 0, withscores=True)
            if recent_entry:
                last_activity = datetime.fromtimestamp(recent_entry[0][1])
                days_since = (datetime.now() - last_activity).days
                stats['days_since_activity'] = days_since
                stats['last_activity'] = last_activity.isoformat()
            
            # Get productivity score if analytics enabled
            if self.analytics:
                try:
                    productivity_score = await self.analytics.calculate_productivity_score(
                        server_id, user_id, days=7, use_ml=False
                    )
                    stats['productivity_score'] = round(productivity_score * 100, 1)
                except Exception:
                    pass
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting user leaderboard stats: {e}")
            return {}
    
    # ========================================================================
    # SERVER SETTINGS MANAGEMENT
    # ========================================================================
    
    async def get_server_settings(self, server_id: int) -> ServerSettings:
        """Get server settings with enhanced caching"""
        cache_key = f"settings:{server_id}"
        
        cached_settings = self._get_from_cache_hierarchy(cache_key)
        if cached_settings is not None:
            return cached_settings
        
        try:
            settings_key = f"server_settings:{server_id}"
            settings_data = await self.redis.hgetall(settings_key)
            
            if settings_data:
                # Decode and parse settings
                settings_dict = {}
                for k, v in settings_data.items():
                    key = k.decode('utf-8')
                    value = v.decode('utf-8')
                    
                    if key == 'categories':
                        settings_dict[key] = set(json.loads(value))
                    elif key == 'notification_settings':
                        settings_dict[key] = json.loads(value)
                    elif key == 'productivity_thresholds':
                        settings_dict[key] = {k: float(v) for k, v in json.loads(value).items()}
                    elif key == 'rate_limits':
                        settings_dict[key] = {k: int(v) for k, v in json.loads(value).items()}
                    elif key in ['work_hours_start', 'work_hours_end', 'max_session_hours', 'auto_logout_hours']:
                        settings_dict[key] = int(value)
                    elif key in ['analytics_enabled', 'audit_enabled', 'backup_enabled']:
                        settings_dict[key] = value.lower() == 'true'
                    else:
                        settings_dict[key] = value
                
                settings = ServerSettings(**settings_dict)
            else:
                # Use defaults and save them
                settings = ServerSettings()
                await self._save_server_settings(server_id, settings)
            
            # Cache the settings
            self._set_in_cache_hierarchy(cache_key, settings, 'settings')
            
            return settings
            
        except Exception as e:
            logger.error(f"Error getting server settings: {e}")
            # Return defaults on error
            return ServerSettings()
    
    async def _save_server_settings(self, server_id: int, settings: ServerSettings):
        """Save server settings to Redis with proper serialization"""
        try:
            settings_key = f"server_settings:{server_id}"
            settings_dict = asdict(settings)
            
            # Serialize complex types
            settings_dict['categories'] = json.dumps(list(settings.categories))
            settings_dict['notification_settings'] = json.dumps(settings.notification_settings)
            settings_dict['productivity_thresholds'] = json.dumps(settings.productivity_thresholds)
            settings_dict['rate_limits'] = json.dumps(settings.rate_limits)
            
            # Convert booleans to strings
            for key in ['analytics_enabled', 'audit_enabled', 'backup_enabled']:
                settings_dict[key] = str(settings_dict[key]).lower()
            
            await self.redis.hset(settings_key, mapping=settings_dict)
            
            # Invalidate cache
            cache_key = f"settings:{server_id}"
            self._set_in_cache_hierarchy(cache_key, settings, 'settings')
            
            logger.debug(f"Saved settings for server {server_id}")
            
        except Exception as e:
            logger.error(f"Error saving server settings: {e}")
    
    # ========================================================================
    # COMPREHENSIVE HEALTH CHECK AND METRICS
    # ========================================================================
    
    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive system health check"""
        current_time = time.time()
        
        # Don't run health checks too frequently
        if current_time - self.last_health_check < 30:
            return {
                'status': 'cached',
                'health_score': self.health_score,
                'message': 'Health check cached (checked < 30s ago)'
            }
        
        health_data = {
            'status': 'healthy',
            'health_score': self.health_score,
            'timestamp': current_time,
            'components': {}
        }
        
        try:
            # Redis connectivity check
            redis_start = time.time()
            await self.redis.ping()
            redis_response_time = time.time() - redis_start
            
            health_data['components']['redis'] = {
                'status': 'healthy',
                'response_time_ms': round(redis_response_time * 1000, 2),
                'connected': True
            }
            
            # Circuit breaker status
            circuit_metrics = self.circuit_breaker.get_metrics()
            health_data['components']['circuit_breaker'] = {
                'status': circuit_metrics['state'].lower(),
                'health_score': circuit_metrics['health_score'],
                'failure_count': circuit_metrics['failure_count'],
                'error_rate': circuit_metrics['error_rate']
            }
            
            # Batch processor status
            if self.batch_processor:
                batch_metrics = await self.batch_processor.get_metrics()
                batch_health = 'healthy'
                if batch_metrics['queue_size'] > 5000:
                    batch_health = 'degraded'
                elif batch_metrics['queue_size'] > 10000:
                    batch_health = 'unhealthy'
                
                health_data['components']['batch_processor'] = {
                    'status': batch_health,
                    'queue_size': batch_metrics['queue_size'],
                    'operations_per_second': batch_metrics['operations_per_second'],
                    'success_rate': (batch_metrics['successful_operations'] / 
                                   max(1, batch_metrics['total_operations']) * 100)
                }
            
            # Cache performance
            cache_total = self.operation_metrics['cache_hits'] + self.operation_metrics['cache_misses']
            cache_hit_rate = (self.operation_metrics['cache_hits'] / max(1, cache_total)) * 100
            
            cache_status = 'healthy'
            if cache_hit_rate < 70:
                cache_status = 'degraded'
            elif cache_hit_rate < 50:
                cache_status = 'unhealthy'
            
            health_data['components']['cache'] = {
                'status': cache_status,
                'hit_rate': round(cache_hit_rate, 2),
                'total_operations': cache_total,
                'sizes': {
                    'l1': len(self.l1_cache),
                    'l2': len(self.l2_cache),
                    'l3': len(self.l3_cache),
                    'user': len(self.user_cache),
                    'leaderboard': len(self.leaderboard_cache),
                    'settings': len(self.settings_cache),
                    'analytics': len(self.analytics_cache)
                }
            }
            
            # Performance metrics
            performance_status = 'healthy'
            avg_response = self.operation_metrics['average_response_time']
            if avg_response > 1.0:
                performance_status = 'degraded'
            elif avg_response > 2.0:
                performance_status = 'unhealthy'
            
            health_data['components']['performance'] = {
                'status': performance_status,
                'avg_response_time_ms': round(avg_response * 1000, 2),
                'peak_response_time_ms': round(self.operation_metrics['peak_response_time'] * 1000, 2),
                'operations_per_second': round(self.operation_metrics['operations_per_second'], 2),
                'total_operations': self.operation_metrics['total_operations'],
                'success_rate': (self.operation_metrics['successful_operations'] / 
                               max(1, self.operation_metrics['total_operations']) * 100)
            }
            
            # Analytics engine status (if enabled)
            if self.analytics:
                try:
                    analytics_metrics = await self.analytics.get_analytics_metrics()
                    health_data['components']['analytics'] = {
                        'status': 'healthy',
                        'cache_hit_rate': analytics_metrics['cache_hit_rate'],
                        'predictions_generated': analytics_metrics['predictions_generated'],
                        'models_cached': analytics_metrics['models_cached']
                    }
                except Exception as e:
                    health_data['components']['analytics'] = {
                        'status': 'unhealthy',
                        'error': str(e)
                    }
            
            # Determine overall status
            component_statuses = [comp['status'] for comp in health_data['components'].values()]
            if any(status == 'unhealthy' for status in component_statuses):
                health_data['status'] = 'unhealthy'
            elif any(status == 'degraded' for status in component_statuses):
                health_data['status'] = 'degraded'
            
            self.last_health_check = current_time
            
        except Exception as e:
            health_data['status'] = 'unhealthy'
            health_data['error'] = str(e)
            health_data['components']['redis'] = {
                'status': 'unhealthy',
                'connected': False,
                'error': str(e)
            }
            logger.error(f"Health check failed: {e}")
        
        return health_data


# ============================================================================
# ULTIMATE CLOCK MANAGER WITH ROLE SUPPORT
# ============================================================================

class UltimateClockManager:
    """Enterprise clock manager with Discord role integration and session management"""
    
    def __init__(self, redis_client, bot=None):
        self.redis = redis_client
        self.bot = bot
        
        # Session caching and management
        self.active_sessions_cache = TTLCache(maxsize=5000, ttl=300)  # 5 minutes
        self.session_metrics = {
            'total_sessions_created': 0,
            'total_sessions_completed': 0,
            'average_session_length': 0,
            'role_assignments_successful': 0,
            'role_assignments_failed': 0
        }
        
        # Role management
        self.role_cache = TTLCache(maxsize=1000, ttl=1800)  # 30 minutes
        
        logger.info("UltimateClockManager initialized with enterprise features")
    
    async def clock_in(self, server_id: int, user_id: int, category: str,
                      metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """Enhanced clock in with comprehensive session management"""
        try:
            # Check if already clocked in
            existing_session = await self.get_active_session(server_id, user_id)
            if existing_session:
                duration = int((datetime.now() - datetime.fromisoformat(existing_session['start_time'])).total_seconds())
                return {
                    'success': False,
                    'message': f"Already clocked into **{existing_session['category']}** for {self._format_duration(duration)}",
                    'error_code': 'ALREADY_CLOCKED_IN',
                    'existing_session': existing_session
                }
            
            # Validate category (this would use the main tracker's validation)
            # For now, we'll do basic validation
            category = category.lower().strip()
            
            # Create comprehensive session
            session_id = str(uuid.uuid4())
            start_time = datetime.now()
            
            session_data = {
                'server_id': server_id,
                'user_id': user_id,
                'category': category,
                'start_time': start_time.isoformat(),
                'session_id': session_id,
                'role_id': None,
                'metadata': metadata or {},
                'checkpoints': [],
                'ip_hash': None,  # Could be populated from Discord context
                'client_info': {
                    'platform': 'discord',
                    'version': '1.0'
                }
            }
            
            # Store session in Redis with appropriate TTL
            session_key = f"active_session:{server_id}:{user_id}"
            session_ttl = 86400  # 24 hours
            
            await self.redis.setex(
                session_key,
                session_ttl,
                json.dumps(session_data)
            )
            
            # Try to assign Discord role
            role_assigned = False
            role_error = None
            
            if self.bot:
                try:
                    guild = self.bot.get_guild(server_id)
                    if guild:
                        member = guild.get_member(user_id)
                        if member:
                            role = await self._get_or_create_role(guild, category)
                            if role:
                                await member.add_roles(role, reason=f"Clocked into {category}")
                                session_data['role_id'] = role.id
                                role_assigned = True
                                self.session_metrics['role_assignments_successful'] += 1
                                
                                # Update session with role info
                                await self.redis.setex(
                                    session_key,
                                    session_ttl,
                                    json.dumps(session_data)
                                )
                            else:
                                role_error = "Could not create role"
                        else:
                            role_error = "Member not found in guild"
                    else:
                        role_error = "Guild not found"
                        
                except Exception as e:
                    role_error = str(e)
                    logger.warning(f"Role assignment failed for user {user_id}: {e}")
                    self.session_metrics['role_assignments_failed'] += 1
            
            # Cache session locally
            cache_key = f"{server_id}:{user_id}"
            self.active_sessions_cache[cache_key] = session_data
            
            # Update metrics
            self.session_metrics['total_sessions_created'] += 1
            
            # Log successful clock in
            logger.info(f"User {user_id} clocked into {category} in server {server_id} (role: {role_assigned})")
            
            result = {
                'success': True,
                'message': f"⏰ Clocked into **{category}**",
                'category': category,
                'start_time': start_time,
                'session_id': session_id,
                'role_assigned': role_assigned
            }
            
            if role_error:
                result['role_warning'] = f"Role assignment failed: {role_error}"
            
            return result
            
        except Exception as e:
            logger.error(f"Error in clock_in: {e}")
            return {
                'success': False,
                'message': f"Error clocking in: {str(e)}",
                'error_code': 'INTERNAL_ERROR'
            }
    
    async def clock_out(self, server_id: int, user_id: int,
                       force: bool = False) -> Dict[str, Any]:
        """Enhanced clock out with comprehensive session processing"""
        try:
            # Get active session
            session = await self.get_active_session(server_id, user_id)
            if not session:
                return {
                    'success': False,
                    'message': "Not currently clocked in",
                    'error_code': 'NOT_CLOCKED_IN'
                }
            
            # Calculate session duration
            start_time = datetime.fromisoformat(session['start_time'])
            end_time = datetime.now()
            duration_seconds = int((end_time - start_time).total_seconds())
            
            # Validate session duration
            if duration_seconds < 1 and not force:
                return {
                    'success': False,
                    'message': "Session too short (less than 1 second). Use force=True to override.",
                    'error_code': 'SESSION_TOO_SHORT'
                }
            
            # Cap extremely long sessions
            max_session_seconds = 86400  # 24 hours
            if duration_seconds > max_session_seconds:
                if not force:
                    return {
                        'success': False,
                        'message': f"Session too long ({duration_seconds/3600:.1f} hours). Use force=True to cap at 24 hours.",
                        'error_code': 'SESSION_TOO_LONG'
                    }
                duration_seconds = max_session_seconds
                logger.warning(f"Capped session duration to 24 hours for user {user_id}")
            
            # Remove Discord role
            role_removed = False
            role_error = None
            
            if self.bot and session.get('role_id'):
                try:
                    guild = self.bot.get_guild(server_id)
                    if guild:
                        member = guild.get_member(user_id)
                        role = guild.get_role(session['role_id'])
                        if member and role:
                            await member.remove_roles(role, reason="Clocked out")
                            role_removed = True
                        elif not member:
                            role_error = "Member not found"
                        elif not role:
                            role_error = "Role not found"
                except Exception as e:
                    role_error = str(e)
                    logger.warning(f"Role removal failed: {e}")
            
            # Prepare session completion data
            category = session['category']
            session_completion_data = {
                'server_id': server_id,
                'user_id': user_id,
                'category': category,
                'seconds': duration_seconds,
                'session_id': session['session_id'],
                'start_time': session['start_time'],
                'end_time': end_time.isoformat(),
                'metadata': session.get('metadata', {}),
                'role_assigned': session.get('role_id') is not None,
                'role_removed': role_removed,
                'checkpoints': session.get('checkpoints', [])
            }
            
            # This would integrate with the main tracker to add the time
            # For now, we'll simulate the time addition
            time_added = await self._add_session_time(session_completion_data)
            
            # Clean up session
            session_key = f"active_session:{server_id}:{user_id}"
            await self.redis.delete(session_key)
            
            # Remove from local cache
            cache_key = f"{server_id}:{user_id}"
            self.active_sessions_cache.pop(cache_key, None)
            
            # Update metrics
            self.session_metrics['total_sessions_completed'] += 1
            current_avg = self.session_metrics['average_session_length']
            total_completed = self.session_metrics['total_sessions_completed']
            self.session_metrics['average_session_length'] = (
                (current_avg * (total_completed - 1) + duration_seconds) / total_completed
            )
            
            # Store completed session for analytics
            completed_session_key = f"completed_session:{server_id}:{user_id}:{session['session_id']}"
            await self.redis.setex(
                completed_session_key,
                86400 * 30,  # Keep for 30 days
                json.dumps(session_completion_data)
            )
            
            logger.info(f"User {user_id} clocked out of {category} in server {server_id} - {duration_seconds}s")
            
            result = {
                'success': True,
                'message': f"✅ Clocked out of **{category}**",
                'category': category,
                'session_duration': duration_seconds,
                'session_duration_formatted': self._format_time(duration_seconds),
                'start_time': start_time,
                'end_time': end_time,
                'role_removed': role_removed,
                'session_id': session['session_id'],
                'time_added': time_added
            }
            
            if role_error:
                result['role_warning'] = f"Role removal issue: {role_error}"
            
            return result
            
        except Exception as e:
            logger.error(f"Error in clock_out: {e}")
            return {
                'success': False,
                'message': f"Error clocking out: {str(e)}",
                'error_code': 'INTERNAL_ERROR'
            }
    
    async def _add_session_time(self, session_data: Dict[str, Any]) -> Dict[str, Any]:
        """Add completed session time to the main tracking system"""
        # This would integrate with the main UltimateTimeTracker
        # For now, we'll simulate the operation
        try:
            # Batch operations for atomic time addition
            server_id = session_data['server_id']
            user_id = session_data['user_id']
            category = session_data['category']
            seconds = session_data['seconds']
            
            # Update user times
            user_times_key = f"user_times:{server_id}:{user_id}"
            await self.redis.hincrby(user_times_key, category, seconds)
            await self.redis.hincrby(user_times_key, "total", seconds)
            
            # Update server totals
            server_times_key = f"server_times:{server_id}"
            await self.redis.hincrby(server_times_key, category, seconds)
            await self.redis.hincrby(server_times_key, "total", seconds)
            
            # Store detailed entry
            entry_data = {
                'server_id': server_id,
                'user_id': user_id,
                'category': category,
                'seconds': seconds,
                'session_id': session_data['session_id'],
                'start_time': session_data['start_time'],
                'end_time': session_data['end_time']
            }
            
            entries_key = f"time_entries:{server_id}:{user_id}"
            await self.redis.zadd(entries_key, {json.dumps(entry_data): time.time()})
            
            # Update leaderboards
            await self.redis.zincrby(f"leaderboard:{server_id}:total", seconds, user_id)
            await self.redis.zincrby(f"leaderboard:{server_id}:{category}", seconds, user_id)
            
            # Get updated totals
            user_category_total = await self.redis.hget(user_times_key, category)
            category_total = int(user_category_total) if user_category_total else seconds
            
            return {
                'seconds_added': seconds,
                'category_total': category_total,
                'category_total_formatted': self._format_time(category_total)
            }
            
        except Exception as e:
            logger.error(f"Error adding session time: {e}")
            return {
                'seconds_added': 0,
                'error': str(e)
            }
    
    async def get_status(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Get comprehensive clock status with analytics"""
        try:
            # Check for active session
            session = await self.get_active_session(server_id, user_id)
            
            if session:
                # Currently clocked in
                start_time = datetime.fromisoformat(session['start_time'])
                current_duration = int((datetime.now() - start_time).total_seconds())
                
                # Add session analytics
                session_analytics = await self._get_session_analytics(session, current_duration)
                
                return {
                    'clocked_in': True,
                    'category': session['category'],
                    'start_time': start_time,
                    'current_duration': current_duration,
                    'current_duration_formatted': self._format_time(current_duration),
                    'session_id': session['session_id'],
                    'role_assigned': session.get('role_id') is not None,
                    'analytics': session_analytics,
                    'checkpoints': session.get('checkpoints', [])
                }
            else:
                # Not clocked in - get comprehensive totals
                return await self._get_user_summary(server_id, user_id)
                
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return {
                'clocked_in': False,
                'error': str(e)
            }
    
    async def _get_session_analytics(self, session: Dict[str, Any], current_duration: int) -> Dict[str, Any]:
        """Get analytics for the current session"""
        try:
            analytics = {
                'session_quality': 'good',  # Based on duration and breaks
                'productivity_estimate': 0.8,  # Estimated productivity
                'recommended_break_in': None,
                'session_type': 'normal'
            }
            
            # Determine session quality based on duration
            hours = current_duration / 3600
            
            if hours < 0.25:
                analytics['session_quality'] = 'starting'
            elif hours > 4:
                analytics['session_quality'] = 'long'
                analytics['recommended_break_in'] = 'now'
            elif hours > 2:
                analytics['session_quality'] = 'extended'
                analytics['recommended_break_in'] = f"{int((3 - hours) * 60)} minutes"
            
            # Estimate productivity based on category and duration
            category = session['category']
            if category in ['work', 'development']:
                if 1 <= hours <= 3:
                    analytics['productivity_estimate'] = 0.9
                elif hours > 3:
                    analytics['productivity_estimate'] = max(0.5, 0.9 - (hours - 3) * 0.1)
            
            # Session type classification
            if hours > 6:
                analytics['session_type'] = 'marathon'
            elif hours > 3:
                analytics['session_type'] = 'extended'
            elif hours > 1:
                analytics['session_type'] = 'focused'
            
            return analytics
            
        except Exception as e:
            logger.error(f"Error getting session analytics: {e}")
            return {}
    
    async def _get_user_summary(self, server_id: int, user_id: int) -> Dict[str, Any]:
        """Get comprehensive user summary when not clocked in"""
        try:
            # Get basic time data
            user_times_key = f"user_times:{server_id}:{user_id}"
            times_data = await self.redis.hgetall(user_times_key)
            
            if times_data:
                times = {
                    k.decode('utf-8'): int(v.decode('utf-8'))
                    for k, v in times_data.items()
                }
                
                total_time = times.get('total', 0)
                categories = {
                    k: {
                        'time': self._format_time(v),
                        'percentage': (v / total_time * 100) if total_time > 0 else 0
                    }
                    for k, v in times.items()
                    if k != 'total' and v > 0
                }
                
                # Get recent session info
                recent_session_info = await self._get_recent_session_info(server_id, user_id)
                
                return {
                    'clocked_in': False,
                    'total_time': total_time,
                    'total_time_formatted': self._format_time(total_time),
                    'categories': categories,
                    'recent_session': recent_session_info,
                    'summary': await self._generate_user_summary(server_id, user_id, times)
                }
            else:
                return {
                    'clocked_in': False,
                    'total_time': 0,
                    'total_time_formatted': '0s',
                    'categories': {},
                    'message': 'No time tracked yet'
                }
                
        except Exception as e:
            logger.error(f"Error getting user summary: {e}")
            return {
                'clocked_in': False,
                'error': str(e)
            }
    
    async def _get_recent_session_info(self, server_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        """Get information about the most recent session"""
        try:
            entries_key = f"time_entries:{server_id}:{user_id}"
            recent_entry = await self.redis.zrevrange(entries_key, 0, 0, withscores=True)
            
            if recent_entry:
                entry_data, timestamp = recent_entry[0]
                entry = json.loads(entry_data)
                
                last_session_time = datetime.fromtimestamp(timestamp)
                hours_since = (datetime.now() - last_session_time).total_seconds() / 3600
                
                return {
                    'category': entry['category'],
                    'duration': self._format_time(entry['seconds']),
                    'hours_since': round(hours_since, 1),
                    'date': last_session_time.strftime('%Y-%m-%d %H:%M')
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting recent session info: {e}")
            return None
    
    async def _generate_user_summary(self, server_id: int, user_id: int, times: Dict[str, int]) -> Dict[str, Any]:
        """Generate intelligent user summary"""
        try:
            total_time = times.get('total', 0)
            if total_time == 0:
                return {'message': 'Start tracking to see your summary'}
            
            # Find top category
            categories = {k: v for k, v in times.items() if k != 'total'}
            top_category = max(categories.items(), key=lambda x: x[1]) if categories else None
            
            # Calculate daily average (rough estimate)
            entries_key = f"time_entries:{server_id}:{user_id}"
            entry_count = await self.redis.zcard(entries_key)
            
            summary = {
                'total_hours': round(total_time / 3600, 1),
                'top_category': top_category[0] if top_category else None,
                'estimated_daily_avg': round(total_time / max(1, entry_count) / 3600, 1),
                'total_sessions': entry_count
            }
            
            # Add insights
            insights = []
            if summary['total_hours'] > 100:
                insights.append("🌟 You're a dedicated tracker!")
            if top_category and top_category[1] / total_time > 0.8:
                insights.append(f"🎯 Highly focused on {top_category[0]}")
            if summary['estimated_daily_avg'] > 6:
                insights.append("💪 High productivity levels")
            
            summary['insights'] = insights
            
            return summary
            
        except Exception as e:
            logger.error(f"Error generating user summary: {e}")
            return {}
    
    async def get_active_session(self, server_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        """Get active session with caching"""
        try:
            # Check local cache first
            cache_key = f"{server_id}:{user_id}"
            if cache_key in self.active_sessions_cache:
                return self.active_sessions_cache[cache_key]
            
            # Check Redis
            session_key = f"active_session:{server_id}:{user_id}"
            session_data = await self.redis.get(session_key)
            
            if session_data:
                session = json.loads(session_data)
                # Cache locally
                self.active_sessions_cache[cache_key] = session
                return session
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting active session: {e}")
            return None
    
    async def _get_or_create_role(self, guild, category: str, role: str):
        """Get or create Discord role for category with caching"""
        try:
            role_name = f"⏰ {role}"
            cache_key = f"role:{guild.id}:{role_name}"
            
            # Check cache
            if cache_key in self.role_cache:
                role_id = self.role_cache[cache_key]
                role = guild.get_role(role_id)
                if role:
                    return role
                else:
                    # Role was deleted, remove from cache
                    del self.role_cache[cache_key]
            
            # Look for existing role
            existing_role = utils.get(guild.roles, name=role_name)
            if existing_role:
                self.role_cache[cache_key] = existing_role.id
                return existing_role
            
            role = await guild.create_role(
                name=role_name,
                color=discord.Color.blurple,
                hoist=False,
                mentionable=False,
                reason=f"Timekeeper role for {category}"
            )
            
            # Cache the new role
            self.role_cache[cache_key] = role.id
            
            logger.info(f"Created new timekeeper role: {role_name}")
            return role
            
        except Exception as e:
            logger.error(f"Error creating role for category {category}: {e}")
            return None
    
    def _format_time(self, seconds: int) -> str:
        """Format seconds into human-readable time"""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            if remaining_seconds == 0:
                return f"{minutes}m"
            return f"{minutes}m {remaining_seconds}s"
        else:
            hours = seconds // 3600
            remaining_minutes = (seconds % 3600) // 60
            if remaining_minutes == 0:
                return f"{hours}h"
            return f"{hours}h {remaining_minutes}m"
    
    def _format_duration(self, seconds: int) -> str:
        """Format duration for display"""
        return self._format_time(seconds)


# ============================================================================
# GLOBAL INSTANCE MANAGEMENT WITH ENTERPRISE FEATURES
# ============================================================================

_shared_tracker = None
_shared_clock = None
_shared_refs = weakref.WeakSet()
_initialization_lock = asyncio.Lock()

async def get_shared_tracker():
    """Get or create shared tracker instance with thread safety"""
    global _shared_tracker, _shared_clock
    
    async with _initialization_lock:
        if _shared_tracker is None or _shared_clock is None:
            logger.info("Initializing shared tracker instances...")
            
            # Create new instances
            _shared_tracker = UltimateTimeTracker(enable_analytics=True)
            await _shared_tracker.connect()
            
            # Create clock manager
            _shared_clock = UltimateClockManager(_shared_tracker.redis, None)
            
            logger.info("Shared tracker and clock instances created successfully")
        
        # Track references
        _shared_refs.add(_shared_tracker)
        _shared_refs.add(_shared_clock)
        
        return _shared_tracker, _shared_clock

async def get_shared_role_tracker(bot=None):
    """Get shared tracker with bot instance for role support"""
    global _shared_tracker, _shared_clock
    
    tracker, clock = await get_shared_tracker()
    
    # Update bot instance if provided
    if bot and _shared_clock:
        _shared_clock.bot = bot
        logger.info("Updated shared clock manager with bot instance for role support")
    
    return tracker, clock

async def close_shared_tracker():
    """Close shared tracker instance with proper cleanup"""
    global _shared_tracker, _shared_clock
    
    async with _initialization_lock:
        try:
            if _shared_tracker:
                logger.info("Closing shared tracker...")
                await _shared_tracker.disconnect()
                _shared_tracker = None
            
            if _shared_clock:
                logger.info("Closing shared clock manager...")
                _shared_clock = None
            
            # Clear references
            _shared_refs.clear()
            
            logger.info("Shared tracker instances closed successfully")
            
        except Exception as e:
            logger.error(f"Error closing shared tracker: {e}")

async def close_shared_role_tracker():
    """Alias for consistency"""
    await close_shared_tracker()

async def get_system_status():
    """Get comprehensive system status"""
    try:
        tracker, clock = await get_shared_tracker()
        
        # Get tracker health
        tracker_health = await tracker.health_check()
        
        # Get clock metrics
        clock_metrics = {
            'total_sessions_created': clock.session_metrics['total_sessions_created'],
            'total_sessions_completed': clock.session_metrics['total_sessions_completed'],
            'average_session_length': clock.session_metrics['average_session_length'],
            'role_assignments_successful': clock.session_metrics['role_assignments_successful'],
            'role_assignments_failed': clock.session_metrics['role_assignments_failed'],
            'active_sessions_cached': len(clock.active_sessions_cache)
        }
        
        return {
            'status': 'operational',
            'tracker_health': tracker_health,
            'clock_metrics': clock_metrics,
            'shared_instances_active': _shared_tracker is not None and _shared_clock is not None,
            'reference_count': len(_shared_refs)
        }
        
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }


# ============================================================================
# LEGACY COMPATIBILITY FUNCTIONS
# ============================================================================

async def create_ultimate_system():
    """Legacy function for backward compatibility"""
    logger.warning("create_ultimate_system() is deprecated. Use get_shared_tracker() instead.")
    return await get_shared_tracker()

async def create_clock_manager():
    """Legacy function for backward compatibility"""  
    logger.warning("create_clock_manager() is deprecated. Use get_shared_tracker() instead.")
    return await get_shared_tracker()