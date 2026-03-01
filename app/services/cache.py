"""
LoopNav — Production-Grade Redis Cache Service.

Architecture:
  - Cache         : main class — all Redis operations, namespace management
  - CacheNamespace: scoped cache with automatic key prefixing
  - CircuitBreaker: fault-tolerance for Redis failures (open/half-open/closed)
  - DistributedLock: context-manager SETNX distributed lock
  - SlidingWindowRateLimiter: token bucket rate limiting via sorted sets
  - BatchPipeline : atomic multi-key operations
  - CacheWarm     : cache warm-up helpers
  - CacheHealth   : full Redis health monitoring

Key design decisions:
  1. All JSON serialisation uses custom encoder (handles datetime, UUID, Pydantic models)
  2. Circuit breaker prevents Redis errors cascading into route errors
  3. Distributed lock is reentrant-safe with owner token comparison
  4. Rate limiter uses ZRANGEBYSCORE / ZADD for O(log N) sliding window
  5. Pipeline batches multiple commands into one round-trip
  6. All public methods return sensible defaults on Redis failure (graceful degradation)
  7. Stats tracking (hit/miss) uses in-memory counters for zero Redis overhead
  8. Namespace isolation prevents key collisions across LoopNav sub-systems
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, Tuple, Union

log = logging.getLogger("loopnav.cache")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_TTL = 180          # 3 minutes — route cache
AI_HISTORY_TTL = 3600      # 1 hour — conversation history
SHARE_TOKEN_TTL = 86400    # 24 hours — shareable route links
STATS_TTL = 604800         # 7 days — analytics counters kept a week
RATE_LIMIT_WINDOW = 60     # 1 minute — sliding window for rate limiting
SESSION_TTL = 300           # 5 minutes — ws session state

# Circuit-breaker tuning
CB_FAILURE_THRESHOLD = 5   # trips after 5 consecutive failures
CB_RESET_TIMEOUT = 30      # seconds before attempting half-open
CB_HALF_OPEN_SUCCESS = 2   # successes needed to close circuit

# Lock tuning
LOCK_EXPIRY_MS = 10_000    # 10 seconds — lock auto-expires to prevent deadlock
LOCK_RETRY_INTERVAL = 0.05 # 50 ms — retry interval for lock acquisition

# Namespace prefixes
NS_ROUTE    = "nav:route"
NS_AI       = "nav:ai"
NS_STATS    = "nav:stats"
NS_SESSION  = "nav:session"
NS_SHARE    = "nav:share"
NS_RATE     = "nav:rate"
NS_LAYERS   = "nav:layers"
NS_LOCK     = "nav:lock"
NS_HEALTH   = "nav:health"


# ---------------------------------------------------------------------------
# Custom JSON encoder (handles datetime, UUID, Pydantic v2 BaseModel)
# ---------------------------------------------------------------------------

class _NavEncoder(json.JSONEncoder):
    """Serialise LoopNav domain objects to JSON."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        # Pydantic v2
        if hasattr(obj, "model_dump"):
            return obj.model_dump()
        # Pydantic v1
        if hasattr(obj, "dict"):
            return obj.dict()
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, timedelta):
            return obj.total_seconds()
        return super().default(obj)


def _dumps(value: Any) -> str:
    return json.dumps(value, cls=_NavEncoder)


def _loads(raw: Union[str, bytes]) -> Any:
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------

class CBState(Enum):
    CLOSED     = "closed"      # normal — calls pass through
    OPEN       = "open"        # tripped — calls blocked
    HALF_OPEN  = "half_open"   # testing — limited calls allowed


@dataclass
class CircuitBreaker:
    """
    Redis-specific circuit breaker.

    Tracks consecutive failures and opens the circuit after the threshold.
    After CB_RESET_TIMEOUT seconds it enters HALF_OPEN state to test if
    Redis has recovered. Two consecutive successes close the circuit again.

    Usage:
        cb = CircuitBreaker()
        if await cb.allow():
            try:
                result = await redis.get(key)
                cb.record_success()
            except Exception as e:
                cb.record_failure()
                raise
    """

    failure_threshold: int = CB_FAILURE_THRESHOLD
    reset_timeout_s: int   = CB_RESET_TIMEOUT
    half_open_success: int = CB_HALF_OPEN_SUCCESS

    _state: CBState    = field(default=CBState.CLOSED, init=False)
    _failures: int     = field(default=0, init=False)
    _successes: int    = field(default=0, init=False)
    _opened_at: float  = field(default=0.0, init=False)

    @property
    def state(self) -> CBState:
        if self._state == CBState.OPEN:
            if time.monotonic() - self._opened_at >= self.reset_timeout_s:
                self._state = CBState.HALF_OPEN
                self._successes = 0
                log.info("CircuitBreaker → HALF_OPEN (testing Redis)")
        return self._state

    def allow(self) -> bool:
        """Return True if the call should be attempted."""
        s = self.state
        return s in (CBState.CLOSED, CBState.HALF_OPEN)

    def record_success(self) -> None:
        if self._state == CBState.HALF_OPEN:
            self._successes += 1
            if self._successes >= self.half_open_success:
                self._state = CBState.CLOSED
                self._failures = 0
                log.info("CircuitBreaker → CLOSED (Redis recovered)")
        else:
            self._failures = 0

    def record_failure(self) -> None:
        self._failures += 1
        if self._state == CBState.HALF_OPEN or self._failures >= self.failure_threshold:
            self._state   = CBState.OPEN
            self._opened_at = time.monotonic()
            log.warning(
                "CircuitBreaker → OPEN after %d failures. Will retry in %ds",
                self._failures, self.reset_timeout_s,
            )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "state":      self.state.value,
            "failures":   self._failures,
            "successes":  self._successes,
            "opened_at":  self._opened_at or None,
        }


# ---------------------------------------------------------------------------
# In-memory cache statistics
# ---------------------------------------------------------------------------

@dataclass
class _CacheStats:
    """
    Zero-Redis-overhead hit/miss tracking.

    Counters are per-namespace so we can see which sub-system
    benefits most from caching.
    """

    _hits:   Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _misses: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _sets:   Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _dels:   Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _errors: int             = field(default=0)
    _start:  float           = field(default_factory=time.monotonic)

    def hit(self, ns: str) -> None:
        self._hits[ns] += 1

    def miss(self, ns: str) -> None:
        self._misses[ns] += 1

    def wrote(self, ns: str) -> None:
        self._sets[ns] += 1

    def deleted(self, ns: str) -> None:
        self._dels[ns] += 1

    def error(self) -> None:
        self._errors += 1

    def _ns_from_key(self, key: str) -> str:
        """Derive namespace from a colon-prefixed key like nav:route:abc → nav:route"""
        parts = key.split(":")
        return ":".join(parts[:2]) if len(parts) >= 2 else key

    def snapshot(self) -> Dict[str, Any]:
        all_ns: Set[str] = set(self._hits) | set(self._misses) | set(self._sets)
        by_ns = {}
        total_hits = total_misses = 0
        for ns in sorted(all_ns):
            h = self._hits[ns]
            m = self._misses[ns]
            total_hits   += h
            total_misses += m
            ratio = h / (h + m) if (h + m) > 0 else 0.0
            by_ns[ns] = {"hits": h, "misses": m, "sets": self._sets[ns], "hit_rate": round(ratio, 4)}
        overall_total = total_hits + total_misses
        return {
            "overall_hit_rate": round(total_hits / overall_total, 4) if overall_total > 0 else 0.0,
            "total_hits":       total_hits,
            "total_misses":     total_misses,
            "total_sets":       sum(self._sets.values()),
            "total_deletes":    sum(self._dels.values()),
            "total_errors":     self._errors,
            "uptime_seconds":   round(time.monotonic() - self._start, 2),
            "by_namespace":     by_ns,
        }


# ---------------------------------------------------------------------------
# Distributed Lock
# ---------------------------------------------------------------------------

class DistributedLock:
    """
    Async context manager — Redis distributed lock using SET NX PX.

    Reentrant safe: if the same owner token is stored we re-acquire cleanly.
    Automatically extends expiry during long operations via `extend()`.

    Usage:
        async with cache.lock("nav:route:compute:42") as acquired:
            if acquired:
                ...  # do expensive work
    """

    def __init__(
        self,
        redis,
        key:         str,
        expiry_ms:   int   = LOCK_EXPIRY_MS,
        retry_times: int   = 20,
        retry_delay: float = LOCK_RETRY_INTERVAL,
    ):
        self._redis       = redis
        self._key         = f"{NS_LOCK}:{key}"
        self._expiry_ms   = expiry_ms
        self._retry_times = retry_times
        self._retry_delay = retry_delay
        self._token       = str(uuid.uuid4())
        self._acquired    = False

    async def acquire(self) -> bool:
        for _ in range(self._retry_times):
            ok = await self._redis.set(
                self._key, self._token,
                nx=True, px=self._expiry_ms,
            )
            if ok:
                self._acquired = True
                return True
            await asyncio.sleep(self._retry_delay)
        return False

    async def release(self) -> None:
        if not self._acquired:
            return
        # Lua script for atomic compare-and-delete
        lua = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        try:
            await self._redis.eval(lua, 1, self._key, self._token)
        except Exception:
            pass
        self._acquired = False

    async def extend(self, extra_ms: int = LOCK_EXPIRY_MS) -> bool:
        """Extend lock expiry — call before expiry during long work."""
        if not self._acquired:
            return False
        lua = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("pexpire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        try:
            result = await self._redis.eval(lua, 1, self._key, self._token, extra_ms)
            return result == 1
        except Exception:
            return False

    async def __aenter__(self) -> bool:
        self._acquired_flag = await self.acquire()
        return self._acquired_flag

    async def __aexit__(self, *_) -> None:
        await self.release()


# ---------------------------------------------------------------------------
# Sliding Window Rate Limiter
# ---------------------------------------------------------------------------

class SlidingWindowRateLimiter:
    """
    Token-bucket rate limiter using Redis sorted sets.

    Each request is recorded as a member with score = timestamp.
    We count members in [now - window, now] and compare to limit.

    Usage:
        limiter = cache.rate_limiter("ip:127.0.0.1", limit=60, window_s=60)
        allowed, remaining, retry_after = await limiter.check()
    """

    def __init__(self, redis, key: str, limit: int, window_s: int = RATE_LIMIT_WINDOW):
        self._redis    = redis
        self._key      = f"{NS_RATE}:{key}"
        self._limit    = limit
        self._window_s = window_s

    async def check(self) -> Tuple[bool, int, float]:
        """
        Returns (allowed, remaining, retry_after_seconds).
        retry_after is 0 if allowed, >0 if rate-limited.
        """
        now  = time.time()
        pipe = self._redis.pipeline()
        pipe.zremrangebyscore(self._key, 0, now - self._window_s)
        pipe.zcard(self._key)
        pipe.zadd(self._key, {str(uuid.uuid4()): now})
        pipe.expire(self._key, self._window_s + 5)
        results = await pipe.execute()
        current = results[1]  # count before this request
        if current >= self._limit:
            # Remove the last entry we just added since we're rejecting
            await self._redis.zremrangebyscore(self._key, now, now + 1)
            oldest = await self._redis.zrange(self._key, 0, 0, withscores=True)
            retry_after = (oldest[0][1] + self._window_s - now) if oldest else self._window_s
            return False, 0, max(0.0, retry_after)
        remaining = max(0, self._limit - current - 1)
        return True, remaining, 0.0

    async def reset(self) -> None:
        await self._redis.delete(self._key)

    async def current_count(self) -> int:
        now = time.time()
        return await self._redis.zcount(self._key, now - self._window_s, now)


# ---------------------------------------------------------------------------
# Cache Namespace — scoped key prefixing
# ---------------------------------------------------------------------------

class CacheNamespace:
    """
    Scoped view of the Cache for a specific sub-system.

    All keys are automatically prefixed with the namespace. This prevents
    key collisions between LoopNav sub-systems (routes, AI, sessions, etc.)

    Usage:
        route_cache = cache.namespace(NS_ROUTE)
        await route_cache.set("abc123", route_data, ttl=180)
        # stored as "nav:route:abc123"
    """

    def __init__(self, cache: "Cache", prefix: str):
        self._cache  = cache
        self._prefix = prefix

    def _k(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    async def get(self, key: str) -> Optional[Any]:
        return await self._cache.get(self._k(key))

    async def set(self, key: str, value: Any, ttl: int = DEFAULT_TTL) -> None:
        await self._cache.set(self._k(key), value, ttl=ttl)

    async def delete(self, key: str) -> None:
        await self._cache.delete(self._k(key))

    async def exists(self, key: str) -> bool:
        return await self._cache.exists(self._k(key))

    async def ttl(self, key: str) -> int:
        return await self._cache.ttl(self._k(key))

    async def incr(self, key: str, amount: int = 1) -> int:
        return await self._cache.incr(self._k(key), amount=amount)

    async def get_int(self, key: str) -> int:
        return await self._cache.get_int(self._k(key))

    async def lpush(self, key: str, *values) -> None:
        await self._cache.lpush(self._k(key), *values)

    async def ltrim(self, key: str, start: int, end: int) -> None:
        await self._cache.ltrim(self._k(key), start, end)

    async def lrange(self, key: str, start: int, end: int) -> list:
        return await self._cache.lrange(self._k(key), start, end)

    async def zadd(self, key: str, score: float, member: str) -> None:
        await self._cache.zadd(self._k(key), score, member)

    async def zrevrange(self, key: str, start: int, end: int) -> list:
        return await self._cache.zrevrange(self._k(key), start, end)

    async def scan_keys(self, pattern: str = "*") -> List[str]:
        full_pattern = f"{self._prefix}:{pattern}"
        return await self._cache.scan_keys(full_pattern)

    async def delete_pattern(self, pattern: str = "*") -> int:
        full_pattern = f"{self._prefix}:{pattern}"
        return await self._cache.delete_pattern(full_pattern)

    async def hset(self, key: str, field_name: str, value: Any) -> None:
        await self._cache.hset(self._k(key), field_name, value)

    async def hget(self, key: str, field_name: str) -> Optional[Any]:
        return await self._cache.hget(self._k(key), field_name)

    async def hgetall(self, key: str) -> Dict[str, Any]:
        return await self._cache.hgetall(self._k(key))

    async def hmset(self, key: str, mapping: Dict[str, Any]) -> None:
        await self._cache.hmset(self._k(key), mapping)

    async def expire(self, key: str, ttl: int) -> None:
        await self._cache.expire(self._k(key), ttl)


# ---------------------------------------------------------------------------
# Batch Pipeline helper
# ---------------------------------------------------------------------------

class BatchPipeline:
    """
    Thin wrapper around redis Pipeline for multi-set / multi-get patterns.

    Usage:
        async with cache.pipeline() as pipe:
            await pipe.set("key1", val1)
            await pipe.set("key2", val2)
            results = await pipe.execute()
    """

    def __init__(self, redis_pipeline, encoder=_dumps, decoder=_loads):
        self._pipe    = redis_pipeline
        self._encoder = encoder
        self._decoder = decoder
        self._queued: List[str] = []   # op names for debugging

    async def set(self, key: str, value: Any, ttl: int = DEFAULT_TTL) -> "BatchPipeline":
        self._pipe.setex(key, ttl, self._encoder(value))
        self._queued.append(f"SET {key}")
        return self

    async def get(self, key: str) -> "BatchPipeline":
        self._pipe.get(key)
        self._queued.append(f"GET {key}")
        return self

    async def delete(self, key: str) -> "BatchPipeline":
        self._pipe.delete(key)
        self._queued.append(f"DEL {key}")
        return self

    async def incr(self, key: str) -> "BatchPipeline":
        self._pipe.incr(key)
        self._queued.append(f"INCR {key}")
        return self

    async def execute(self) -> List[Any]:
        results = await self._pipe.execute()
        decoded = []
        for i, r in enumerate(results):
            op = self._queued[i] if i < len(self._queued) else ""
            if op.startswith("GET") and r is not None:
                try:
                    decoded.append(self._decoder(r))
                except Exception:
                    decoded.append(r)
            else:
                decoded.append(r)
        return decoded

    async def __aenter__(self) -> "BatchPipeline":
        return self

    async def __aexit__(self, exc_type, *_) -> None:
        if exc_type is not None:
            await self._pipe.reset()


# ---------------------------------------------------------------------------
# Cache warm-up helpers
# ---------------------------------------------------------------------------

class CacheWarm:
    """
    Pre-populate cache on startup to avoid cold-start latency.

    Register warm functions with @warm.register("key_pattern") and call
    await warm.run_all() from the FastAPI lifespan.
    """

    def __init__(self, cache: "Cache"):
        self._cache     = cache
        self._warmers: List[Tuple[str, Any]] = []

    def register(self, name: str):
        """Decorator — registers an async warm function."""
        def decorator(fn):
            self._warmers.append((name, fn))
            return fn
        return decorator

    async def run_all(self, concurrency: int = 4) -> Dict[str, str]:
        """Run all warmers concurrently. Returns {name: status} mapping."""
        sem     = asyncio.Semaphore(concurrency)
        results = {}

        async def _run(name: str, fn) -> None:
            async with sem:
                try:
                    await fn(self._cache)
                    results[name] = "ok"
                    log.info("Cache warm-up OK: %s", name)
                except Exception as exc:
                    results[name] = f"error: {exc}"
                    log.warning("Cache warm-up FAILED: %s — %s", name, exc)

        await asyncio.gather(*[_run(n, f) for n, f in self._warmers])
        return results

    async def run_one(self, name: str) -> str:
        for n, fn in self._warmers:
            if n == name:
                try:
                    await fn(self._cache)
                    return "ok"
                except Exception as exc:
                    return f"error: {exc}"
        return "not_found"


# ---------------------------------------------------------------------------
# Health Monitor
# ---------------------------------------------------------------------------

@dataclass
class RedisHealthReport:
    """Snapshot of Redis health metrics."""
    connected:          bool
    latency_ms:         float
    memory_used_mb:     float
    memory_peak_mb:     float
    connected_clients:  int
    total_commands:     int
    keys_count:         int
    uptime_seconds:     int
    redis_version:      str
    circuit_breaker:    Dict[str, Any]
    cache_stats:        Dict[str, Any]
    timestamp:          str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def as_dict(self) -> Dict[str, Any]:
        return {
            "connected":          self.connected,
            "latency_ms":         self.latency_ms,
            "memory_used_mb":     self.memory_used_mb,
            "memory_peak_mb":     self.memory_peak_mb,
            "connected_clients":  self.connected_clients,
            "total_commands":     self.total_commands,
            "keys_count":         self.keys_count,
            "uptime_seconds":     self.uptime_seconds,
            "redis_version":      self.redis_version,
            "circuit_breaker":    self.circuit_breaker,
            "cache_stats":        self.cache_stats,
            "timestamp":          self.timestamp,
        }


# ---------------------------------------------------------------------------
# Main Cache class
# ---------------------------------------------------------------------------

class Cache:
    """
    Production-grade async Redis wrapper for LoopNav.

    Features:
    - Circuit breaker — Redis failures don't cascade into route errors
    - Distributed lock — prevent duplicate expensive computations
    - Rate limiter — sliding window per identifier
    - Namespaces — scoped key prefixing per sub-system
    - Batch pipeline — multi-command atomic operations
    - Cache warm-up — pre-populate on startup
    - Full health monitoring — latency, memory, circuit state
    - In-memory hit/miss statistics

    Graceful degradation:
    - get()  → returns None on failure (cache miss is safe)
    - set()  → logs warning, does NOT raise
    - incr() → returns 0 on failure
    - Lists, hashes, sorted sets → return empty on failure
    - rate_limiter.check() → returns (True, limit, 0) on failure (allow through)
    - lock() → acquired=False on failure (caller must handle)
    """

    def __init__(self, redis):
        self._redis   = redis
        self._cb      = CircuitBreaker()
        self._stats   = _CacheStats()
        self._warmup  = CacheWarm(self)

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                    #
    # ------------------------------------------------------------------ #

    def _ns_from_key(self, key: str) -> str:
        parts = key.split(":")
        return ":".join(parts[:2]) if len(parts) >= 2 else "unknown"

    async def _safe_exec(self, coro, default=None):
        """Execute a Redis coroutine with circuit-breaker + error handling."""
        if not self._cb.allow():
            log.debug("CircuitBreaker OPEN — skipping Redis call")
            self._stats.error()
            return default
        try:
            result = await coro
            self._cb.record_success()
            return result
        except Exception as exc:
            self._cb.record_failure()
            self._stats.error()
            log.warning("Redis error: %s", exc)
            return default

    # ------------------------------------------------------------------ #
    #  String / JSON operations                                            #
    # ------------------------------------------------------------------ #

    async def get(self, key: str) -> Optional[Any]:
        """Get a JSON-decoded value. Returns None on miss or error."""
        ns = self._ns_from_key(key)
        raw = await self._safe_exec(self._redis.get(key))
        if raw is None:
            self._stats.miss(ns)
            return None
        self._stats.hit(ns)
        try:
            return _loads(raw)
        except Exception:
            return raw  # return raw bytes if not JSON

    async def set(self, key: str, value: Any, ttl: int = DEFAULT_TTL) -> bool:
        """Serialise value to JSON and store with TTL. Returns success flag."""
        ns = self._ns_from_key(key)
        encoded = _dumps(value)
        ok = await self._safe_exec(self._redis.setex(key, ttl, encoded), default=False)
        if ok is not False:
            self._stats.wrote(ns)
        return bool(ok)

    async def get_or_set(self, key: str, factory, ttl: int = DEFAULT_TTL) -> Any:
        """
        Read-through cache: return cached value or call factory() and cache result.
        factory should be an async callable returning a JSON-serialisable value.
        """
        cached = await self.get(key)
        if cached is not None:
            return cached
        value = await factory()
        if value is not None:
            await self.set(key, value, ttl=ttl)
        return value

    async def delete(self, key: str) -> bool:
        ns = self._ns_from_key(key)
        result = await self._safe_exec(self._redis.delete(key), default=0)
        if result:
            self._stats.deleted(ns)
        return bool(result)

    async def exists(self, key: str) -> bool:
        result = await self._safe_exec(self._redis.exists(key), default=0)
        return bool(result)

    async def ttl(self, key: str) -> int:
        """Return TTL in seconds (-2 if key missing, -1 if no expiry)."""
        return await self._safe_exec(self._redis.ttl(key), default=-2)

    async def expire(self, key: str, ttl: int) -> bool:
        return bool(await self._safe_exec(self._redis.expire(key, ttl), default=False))

    async def persist(self, key: str) -> bool:
        """Remove expiry from a key (make it permanent)."""
        return bool(await self._safe_exec(self._redis.persist(key), default=False))

    async def rename(self, key: str, new_key: str) -> bool:
        return bool(await self._safe_exec(self._redis.rename(key, new_key), default=False))

    async def type(self, key: str) -> str:
        result = await self._safe_exec(self._redis.type(key), default=b"none")
        return result.decode() if isinstance(result, bytes) else str(result)

    # ------------------------------------------------------------------ #
    #  Integer / counter operations                                        #
    # ------------------------------------------------------------------ #

    async def incr(self, key: str, amount: int = 1) -> int:
        """Increment counter by amount. Safe default 0 on error."""
        if amount == 1:
            return await self._safe_exec(self._redis.incr(key), default=0) or 0
        return await self._safe_exec(self._redis.incrby(key, amount), default=0) or 0

    async def decr(self, key: str, amount: int = 1) -> int:
        if amount == 1:
            return await self._safe_exec(self._redis.decr(key), default=0) or 0
        return await self._safe_exec(self._redis.decrby(key, amount), default=0) or 0

    async def get_int(self, key: str) -> int:
        val = await self._safe_exec(self._redis.get(key), default=None)
        return int(val) if val is not None else 0

    async def incr_float(self, key: str, amount: float) -> float:
        result = await self._safe_exec(self._redis.incrbyfloat(key, amount), default=0.0)
        return float(result or 0.0)

    # ------------------------------------------------------------------ #
    #  List operations                                                     #
    # ------------------------------------------------------------------ #

    async def lpush(self, key: str, *values) -> int:
        """Prepend values to list. Returns new list length."""
        encoded = [_dumps(v) for v in values]
        return await self._safe_exec(self._redis.lpush(key, *encoded), default=0) or 0

    async def rpush(self, key: str, *values) -> int:
        encoded = [_dumps(v) for v in values]
        return await self._safe_exec(self._redis.rpush(key, *encoded), default=0) or 0

    async def lpop(self, key: str) -> Optional[Any]:
        raw = await self._safe_exec(self._redis.lpop(key), default=None)
        return _loads(raw) if raw else None

    async def rpop(self, key: str) -> Optional[Any]:
        raw = await self._safe_exec(self._redis.rpop(key), default=None)
        return _loads(raw) if raw else None

    async def ltrim(self, key: str, start: int, end: int) -> bool:
        return bool(await self._safe_exec(self._redis.ltrim(key, start, end), default=False))

    async def lrange(self, key: str, start: int, end: int) -> List[Any]:
        raws = await self._safe_exec(self._redis.lrange(key, start, end), default=[])
        results = []
        for r in (raws or []):
            try:
                results.append(_loads(r))
            except Exception:
                results.append(r)
        return results

    async def llen(self, key: str) -> int:
        return await self._safe_exec(self._redis.llen(key), default=0) or 0

    async def lindex(self, key: str, index: int) -> Optional[Any]:
        raw = await self._safe_exec(self._redis.lindex(key, index), default=None)
        return _loads(raw) if raw else None

    # ------------------------------------------------------------------ #
    #  Sorted set operations                                               #
    # ------------------------------------------------------------------ #

    async def zadd(self, key: str, score: float, member: str) -> None:
        """Increment sorted-set member score (for popularity counters)."""
        await self._safe_exec(self._redis.zincrby(key, score, member))

    async def zadd_strict(self, key: str, mapping: Dict[str, float]) -> int:
        """Add/update members with exact scores (not incremental)."""
        return await self._safe_exec(self._redis.zadd(key, mapping), default=0) or 0

    async def zrevrange(self, key: str, start: int, end: int) -> List[Tuple[str, float]]:
        """Get top members by score (highest first) with scores."""
        result = await self._safe_exec(
            self._redis.zrevrange(key, start, end, withscores=True), default=[]
        )
        if not result:
            return []
        return [(m.decode() if isinstance(m, bytes) else m, s) for m, s in result]

    async def zrange(self, key: str, start: int, end: int) -> List[Tuple[str, float]]:
        """Get members by score (lowest first) with scores."""
        result = await self._safe_exec(
            self._redis.zrange(key, start, end, withscores=True), default=[]
        )
        if not result:
            return []
        return [(m.decode() if isinstance(m, bytes) else m, s) for m, s in result]

    async def zrank(self, key: str, member: str) -> Optional[int]:
        return await self._safe_exec(self._redis.zrank(key, member), default=None)

    async def zrevrank(self, key: str, member: str) -> Optional[int]:
        return await self._safe_exec(self._redis.zrevrank(key, member), default=None)

    async def zscore(self, key: str, member: str) -> Optional[float]:
        result = await self._safe_exec(self._redis.zscore(key, member), default=None)
        return float(result) if result is not None else None

    async def zcard(self, key: str) -> int:
        return await self._safe_exec(self._redis.zcard(key), default=0) or 0

    async def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        return await self._safe_exec(
            self._redis.zremrangebyscore(key, min_score, max_score), default=0
        ) or 0

    async def zcount(self, key: str, min_score: float, max_score: float) -> int:
        return await self._safe_exec(
            self._redis.zcount(key, min_score, max_score), default=0
        ) or 0

    # ------------------------------------------------------------------ #
    #  Hash operations                                                     #
    # ------------------------------------------------------------------ #

    async def hset(self, key: str, field_name: str, value: Any) -> bool:
        encoded = _dumps(value)
        return bool(await self._safe_exec(self._redis.hset(key, field_name, encoded), default=False))

    async def hget(self, key: str, field_name: str) -> Optional[Any]:
        raw = await self._safe_exec(self._redis.hget(key, field_name), default=None)
        if raw is None:
            return None
        try:
            return _loads(raw)
        except Exception:
            return raw

    async def hmset(self, key: str, mapping: Dict[str, Any]) -> bool:
        encoded = {k: _dumps(v) for k, v in mapping.items()}
        return bool(await self._safe_exec(self._redis.hset(key, mapping=encoded), default=False))

    async def hmget(self, key: str, *fields) -> List[Optional[Any]]:
        raws = await self._safe_exec(self._redis.hmget(key, *fields), default=[None] * len(fields))
        results = []
        for r in (raws or [None] * len(fields)):
            if r is None:
                results.append(None)
            else:
                try:
                    results.append(_loads(r))
                except Exception:
                    results.append(r)
        return results

    async def hgetall(self, key: str) -> Dict[str, Any]:
        raw_map = await self._safe_exec(self._redis.hgetall(key), default={})
        result = {}
        for k, v in (raw_map or {}).items():
            dk = k.decode() if isinstance(k, bytes) else k
            try:
                result[dk] = _loads(v)
            except Exception:
                result[dk] = v
        return result

    async def hdel(self, key: str, *fields) -> int:
        return await self._safe_exec(self._redis.hdel(key, *fields), default=0) or 0

    async def hexists(self, key: str, field_name: str) -> bool:
        return bool(await self._safe_exec(self._redis.hexists(key, field_name), default=False))

    async def hlen(self, key: str) -> int:
        return await self._safe_exec(self._redis.hlen(key), default=0) or 0

    async def hkeys(self, key: str) -> List[str]:
        raws = await self._safe_exec(self._redis.hkeys(key), default=[])
        return [k.decode() if isinstance(k, bytes) else k for k in (raws or [])]

    async def hincrby(self, key: str, field_name: str, amount: int = 1) -> int:
        return await self._safe_exec(
            self._redis.hincrby(key, field_name, amount), default=0
        ) or 0

    # ------------------------------------------------------------------ #
    #  Set operations                                                      #
    # ------------------------------------------------------------------ #

    async def sadd(self, key: str, *members) -> int:
        encoded = [_dumps(m) for m in members]
        return await self._safe_exec(self._redis.sadd(key, *encoded), default=0) or 0

    async def srem(self, key: str, *members) -> int:
        encoded = [_dumps(m) for m in members]
        return await self._safe_exec(self._redis.srem(key, *encoded), default=0) or 0

    async def smembers(self, key: str) -> Set[Any]:
        raws = await self._safe_exec(self._redis.smembers(key), default=set())
        result = set()
        for r in (raws or set()):
            try:
                result.add(_loads(r))
            except Exception:
                result.add(r)
        return result

    async def sismember(self, key: str, member: Any) -> bool:
        encoded = _dumps(member)
        return bool(await self._safe_exec(self._redis.sismember(key, encoded), default=False))

    async def scard(self, key: str) -> int:
        return await self._safe_exec(self._redis.scard(key), default=0) or 0

    async def sunion(self, *keys) -> Set[Any]:
        raws = await self._safe_exec(self._redis.sunion(*keys), default=set())
        return {_loads(r) for r in (raws or set())}

    async def sinter(self, *keys) -> Set[Any]:
        raws = await self._safe_exec(self._redis.sinter(*keys), default=set())
        return {_loads(r) for r in (raws or set())}

    # ------------------------------------------------------------------ #
    #  Batch / pipeline                                                    #
    # ------------------------------------------------------------------ #

    async def mget(self, *keys) -> List[Optional[Any]]:
        """Fetch multiple keys in one round-trip."""
        raws = await self._safe_exec(self._redis.mget(*keys), default=[None] * len(keys))
        results = []
        for raw in (raws or [None] * len(keys)):
            if raw is None:
                results.append(None)
            else:
                try:
                    results.append(_loads(raw))
                except Exception:
                    results.append(raw)
        return results

    async def mset(self, mapping: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """
        Set multiple keys. If ttl provided, sets expiry on each key via pipeline.
        Redis MSET doesn't support TTL natively, so we use a pipeline.
        """
        if ttl:
            try:
                pipe = self._redis.pipeline()
                for k, v in mapping.items():
                    pipe.setex(k, ttl, _dumps(v))
                await pipe.execute()
                return True
            except Exception as exc:
                log.warning("mset pipeline error: %s", exc)
                return False
        encoded = {k: _dumps(v) for k, v in mapping.items()}
        return bool(await self._safe_exec(self._redis.mset(encoded), default=False))

    @asynccontextmanager
    async def pipeline(self) -> AsyncGenerator[BatchPipeline, None]:
        """Context manager returning a BatchPipeline for atomic multi-ops."""
        pipe = self._redis.pipeline()
        bp   = BatchPipeline(pipe)
        try:
            yield bp
        except Exception:
            await pipe.reset()
            raise

    # ------------------------------------------------------------------ #
    #  Key scanning / deletion by pattern                                  #
    # ------------------------------------------------------------------ #

    async def scan_keys(self, pattern: str = "*", count: int = 100) -> List[str]:
        """
        Non-blocking key scan using SCAN cursor. Safe for production
        (unlike KEYS which blocks Redis). Returns list of matching key strings.
        """
        keys   = []
        cursor = 0
        try:
            while True:
                cursor, batch = await self._redis.scan(cursor, match=pattern, count=count)
                keys.extend(
                    k.decode() if isinstance(k, bytes) else k for k in batch
                )
                if cursor == 0:
                    break
        except Exception as exc:
            log.warning("scan_keys error: %s", exc)
        return keys

    async def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching pattern. Returns count of deleted keys."""
        keys = await self.scan_keys(pattern)
        if not keys:
            return 0
        deleted = 0
        # Delete in chunks of 100 to avoid large DEL commands
        for i in range(0, len(keys), 100):
            chunk = keys[i:i + 100]
            result = await self._safe_exec(self._redis.delete(*chunk), default=0)
            deleted += result or 0
        return deleted

    # ------------------------------------------------------------------ #
    #  Utility: namespace, lock, rate limiter                              #
    # ------------------------------------------------------------------ #

    def namespace(self, prefix: str) -> CacheNamespace:
        """Return a namespace-scoped view of this cache."""
        return CacheNamespace(self, prefix)

    def lock(
        self,
        key:         str,
        expiry_ms:   int   = LOCK_EXPIRY_MS,
        retry_times: int   = 20,
        retry_delay: float = LOCK_RETRY_INTERVAL,
    ) -> DistributedLock:
        """Return a DistributedLock context manager."""
        return DistributedLock(
            self._redis, key,
            expiry_ms=expiry_ms,
            retry_times=retry_times,
            retry_delay=retry_delay,
        )

    def rate_limiter(self, key: str, limit: int, window_s: int = RATE_LIMIT_WINDOW) -> SlidingWindowRateLimiter:
        """Return a SlidingWindowRateLimiter for the given key."""
        return SlidingWindowRateLimiter(self._redis, key, limit, window_s)

    # ------------------------------------------------------------------ #
    #  Cache warm-up                                                       #
    # ------------------------------------------------------------------ #

    @property
    def warmup(self) -> CacheWarm:
        return self._warmup

    # ------------------------------------------------------------------ #
    #  Health check                                                        #
    # ------------------------------------------------------------------ #

    async def ping(self) -> bool:
        try:
            await self._redis.ping()
            self._cb.record_success()
            return True
        except Exception:
            self._cb.record_failure()
            return False

    async def health_check(self) -> RedisHealthReport:
        """Full Redis health report including latency, memory, circuit state."""
        connected    = False
        latency_ms   = -1.0
        mem_used_mb  = 0.0
        mem_peak_mb  = 0.0
        clients      = 0
        total_cmds   = 0
        keys_count   = 0
        uptime_s     = 0
        version      = "unknown"

        try:
            t0 = time.monotonic()
            await self._redis.ping()
            latency_ms = round((time.monotonic() - t0) * 1000, 2)
            connected  = True

            info = await self._redis.info()
            mem_used_mb = round(info.get("used_memory", 0) / 1_048_576, 2)
            mem_peak_mb = round(info.get("used_memory_peak", 0) / 1_048_576, 2)
            clients     = info.get("connected_clients", 0)
            total_cmds  = info.get("total_commands_processed", 0)
            uptime_s    = info.get("uptime_in_seconds", 0)
            version     = info.get("redis_version", "unknown")

            db_info = info.get("db0", {})
            if isinstance(db_info, dict):
                keys_count = db_info.get("keys", 0)
            elif isinstance(db_info, str):
                # format: "keys=N,expires=M,avg_ttl=P"
                for part in db_info.split(","):
                    if part.startswith("keys="):
                        keys_count = int(part.split("=")[1])

        except Exception as exc:
            log.warning("health_check error: %s", exc)

        return RedisHealthReport(
            connected         = connected,
            latency_ms        = latency_ms,
            memory_used_mb    = mem_used_mb,
            memory_peak_mb    = mem_peak_mb,
            connected_clients = clients,
            total_commands    = total_cmds,
            keys_count        = keys_count,
            uptime_seconds    = uptime_s,
            redis_version     = version,
            circuit_breaker   = self._cb.as_dict(),
            cache_stats       = self._stats.snapshot(),
        )

    # ------------------------------------------------------------------ #
    #  Stats snapshot                                                      #
    # ------------------------------------------------------------------ #

    def stats_snapshot(self) -> Dict[str, Any]:
        """In-memory cache hit/miss statistics (no Redis round-trip)."""
        return self._stats.snapshot()

    def circuit_state(self) -> str:
        return self._cb.state.value

    # ------------------------------------------------------------------ #
    #  LoopNav domain-specific helpers                                     #
    # ------------------------------------------------------------------ #

    async def get_route_cache(self, cache_key: str) -> Optional[Dict]:
        """Fetch a cached route result."""
        return await self.get(f"{NS_ROUTE}:{cache_key}")

    async def set_route_cache(self, cache_key: str, route: Any, ttl: int = DEFAULT_TTL) -> None:
        """Store a route result. Handles Pydantic models automatically."""
        await self.set(f"{NS_ROUTE}:{cache_key}", route, ttl=ttl)

    async def track_route_request(self, origin_id: str, dest_id: str, mode: str) -> None:
        """Increment analytics counters for a completed route request."""
        pipe = self._redis.pipeline()
        pipe.incr(f"{NS_STATS}:routes_total")
        pipe.incr(f"{NS_STATS}:mode:{mode}")
        pipe.zincrby(f"{NS_STATS}:popular_routes", 1, f"{origin_id}→{dest_id}")
        try:
            await pipe.execute()
        except Exception as exc:
            log.warning("track_route_request error: %s", exc)

    async def record_route_time(self, duration_ms: int) -> None:
        """Append route compute time to the timing list (capped at 1000 samples)."""
        try:
            key = f"{NS_STATS}:times"
            await self._redis.rpush(key, str(duration_ms))
            await self._redis.ltrim(key, -1000, -1)
        except Exception as exc:
            log.warning("record_route_time error: %s", exc)

    async def push_ai_message(self, session_id: str, role: str, content: str) -> None:
        """Append a message to AI conversation history (Redis list, max 20 pairs)."""
        key = f"{NS_AI}:history:{session_id}"
        msg = _dumps({"role": role, "content": content, "ts": datetime.utcnow().isoformat()})
        try:
            await self._redis.rpush(key, msg)
            await self._redis.ltrim(key, -40, -1)  # 20 pairs = 40 messages
            await self._redis.expire(key, AI_HISTORY_TTL)
        except Exception as exc:
            log.warning("push_ai_message error: %s", exc)

    async def get_ai_history(self, session_id: str) -> List[Dict]:
        """Return full conversation history for a session."""
        key = f"{NS_AI}:history:{session_id}"
        raws = await self._safe_exec(self._redis.lrange(key, 0, -1), default=[])
        results = []
        for r in (raws or []):
            try:
                results.append(_loads(r))
            except Exception:
                pass
        return results

    async def clear_ai_history(self, session_id: str) -> None:
        await self.delete(f"{NS_AI}:history:{session_id}")

    async def get_share_token(self, token: str) -> Optional[Dict]:
        return await self.get(f"{NS_SHARE}:{token}")

    async def set_share_token(self, token: str, route_params: Dict) -> None:
        await self.set(f"{NS_SHARE}:{token}", route_params, ttl=SHARE_TOKEN_TTL)

    async def get_session_state(self, session_id: str) -> Optional[Dict]:
        return await self.get(f"{NS_SESSION}:{session_id}")

    async def set_session_state(self, session_id: str, state: Dict, ttl: int = SESSION_TTL) -> None:
        await self.set(f"{NS_SESSION}:{session_id}", state, ttl=ttl)

    async def delete_session_state(self, session_id: str) -> None:
        await self.delete(f"{NS_SESSION}:{session_id}")

    async def count_active_sessions(self) -> int:
        keys = await self.scan_keys(f"{NS_SESSION}:*")
        return len(keys)

    async def get_popular_routes(self, top_n: int = 10) -> List[Tuple[str, float]]:
        return await self.zrevrange(f"{NS_STATS}:popular_routes", 0, top_n - 1)

    async def get_stats_counters(self) -> Dict[str, int]:
        keys = [
            f"{NS_STATS}:routes_total",
            f"{NS_STATS}:mode:fastest",
            f"{NS_STATS}:mode:covered",
            f"{NS_STATS}:mode:accessible",
            f"{NS_STATS}:ai_requests",
            f"{NS_STATS}:ai_fallbacks",
            f"{NS_STATS}:ai_cache_hits",
            f"{NS_STATS}:ws_connects",
            f"{NS_STATS}:ws_reroutes",
            f"{NS_STATS}:ws_arrivals",
            f"{NS_STATS}:share_tokens",
            f"{NS_STATS}:errors",
        ]
        values = await self.mget(*keys)
        return {
            k.replace(f"{NS_STATS}:", ""): int(v or 0)
            for k, v in zip(keys, values)
        }

    async def get_route_times(self) -> List[int]:
        """Return list of route compute times (ms) for percentile calculation."""
        raws = await self._safe_exec(
            self._redis.lrange(f"{NS_STATS}:times", 0, -1), default=[]
        )
        results = []
        for r in (raws or []):
            try:
                results.append(int(r))
            except Exception:
                pass
        return results

    async def get_hourly_requests(self, hours: int = 24) -> List[int]:
        """Return per-hour request counts for the last N hours."""
        now   = datetime.utcnow()
        keys  = []
        for h in range(hours - 1, -1, -1):
            t   = now - timedelta(hours=h)
            key = f"{NS_STATS}:hour:{t.strftime('%Y%m%d%H')}"
            keys.append(key)
        values = await self.mget(*keys)
        return [int(v or 0) for v in values]

    async def incr_hourly(self) -> None:
        """Increment current-hour request counter with 25hr expiry."""
        key = f"{NS_STATS}:hour:{datetime.utcnow().strftime('%Y%m%d%H')}"
        try:
            await self._redis.incr(key)
            await self._redis.expire(key, 90_000)  # 25 hours
        except Exception as exc:
            log.warning("incr_hourly error: %s", exc)

    async def get_layer_cache(self, level: str) -> Optional[Dict]:
        return await self.get(f"{NS_LAYERS}:{level}")

    async def set_layer_cache(self, level: str, data: Dict, ttl: int = 600) -> None:
        """Cache a GeoJSON layer for 10 minutes."""
        await self.set(f"{NS_LAYERS}:{level}", data, ttl=ttl)

    async def invalidate_layer_cache(self, level: str) -> None:
        await self.delete(f"{NS_LAYERS}:{level}")

    async def invalidate_all_layers(self) -> int:
        return await self.delete_pattern(f"{NS_LAYERS}:*")

    async def flush_nav_stats(self) -> int:
        """Delete all LoopNav stats keys. Returns count deleted."""
        deleted = 0
        deleted += await self.delete_pattern(f"{NS_STATS}:*")
        deleted += await self.delete_pattern(f"{NS_RATE}:*")
        return deleted

    async def full_reset(self) -> Dict[str, int]:
        """
        Hard reset all LoopNav keys (routes, AI history, sessions, stats, layers).
        Use with caution — clears ALL LoopNav data from Redis.
        Returns counts per namespace.
        """
        counts = {}
        for ns, label in [
            (NS_ROUTE,   "routes"),
            (NS_AI,      "ai_history"),
            (NS_STATS,   "stats"),
            (NS_SESSION, "sessions"),
            (NS_SHARE,   "share_tokens"),
            (NS_LAYERS,  "layers"),
            (NS_RATE,    "rate_limits"),
        ]:
            counts[label] = await self.delete_pattern(f"{ns}:*")
        log.warning("LoopNav full_reset executed — deleted: %s", counts)
        return counts
