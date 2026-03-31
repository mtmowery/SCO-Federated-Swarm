"""
Redis Cache Layer

Provides short-term caching for:
- MCP response caching with TTL
- Agent state storage
- Circuit breaker state tracking
- Query result caching for repeated questions
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import redis.asyncio as aioredis

from shared.config import get_settings

logger = logging.getLogger(__name__)


class RedisCache:
    """
    Async Redis cache for the federated swarm.

    Handles MCP response caching, circuit breaker state,
    and query result memoization.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._client: aioredis.Redis | None = None
        self._url = (
            f"redis://{settings.redis.host}:{settings.redis.port}"
            f"/{settings.redis.db}"
        )

    async def connect(self) -> None:
        """Initialize Redis connection."""
        if self._client is None:
            self._client = aioredis.from_url(
                self._url,
                decode_responses=True,
                max_connections=20,
            )
            # Verify connection
            await self._client.ping()
            logger.info(f"Redis connected: {self._url}")

    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("Redis connection closed")

    @property
    def client(self) -> aioredis.Redis:
        if self._client is None:
            raise RuntimeError("Redis not connected. Call connect() first.")
        return self._client

    # ── MCP Response Caching ────────────────────────────────────

    async def cache_mcp_response(
        self,
        agency: str,
        tool_name: str,
        arguments: dict,
        response: dict,
        ttl_seconds: int = 300,
    ) -> None:
        """
        Cache an MCP tool response.
        Key format: mcp:{agency}:{tool_name}:{args_hash}
        """
        key = self._mcp_cache_key(agency, tool_name, arguments)
        await self.client.setex(
            key,
            ttl_seconds,
            json.dumps(response, default=str),
        )
        logger.debug(f"Cached MCP response: {key} (TTL={ttl_seconds}s)")

    async def get_cached_mcp_response(
        self,
        agency: str,
        tool_name: str,
        arguments: dict,
    ) -> dict | None:
        """Retrieve cached MCP response if available."""
        key = self._mcp_cache_key(agency, tool_name, arguments)
        cached = await self.client.get(key)
        if cached:
            logger.debug(f"Cache hit: {key}")
            return json.loads(cached)
        return None

    def _mcp_cache_key(self, agency: str, tool_name: str, arguments: dict) -> str:
        import hashlib
        args_str = json.dumps(arguments, sort_keys=True, default=str)
        args_hash = hashlib.md5(args_str.encode()).hexdigest()[:12]
        return f"mcp:{agency}:{tool_name}:{args_hash}"

    # ── Query Result Caching ────────────────────────────────────

    async def cache_query_result(
        self,
        question: str,
        result: dict,
        ttl_seconds: int = 600,
    ) -> None:
        """Cache a full query result for semantic deduplication."""
        import hashlib
        key = f"query:{hashlib.md5(question.lower().encode()).hexdigest()}"
        await self.client.setex(key, ttl_seconds, json.dumps(result, default=str))

    async def get_cached_query_result(self, question: str) -> dict | None:
        """Check if we've answered this exact question recently."""
        import hashlib
        key = f"query:{hashlib.md5(question.lower().encode()).hexdigest()}"
        cached = await self.client.get(key)
        if cached:
            return json.loads(cached)
        return None

    # ── Circuit Breaker State ───────────────────────────────────

    async def record_agency_failure(self, agency: str) -> int:
        """
        Record a failure for an agency. Returns failure count.
        Used by the circuit breaker pattern in the MCP client.
        """
        key = f"circuit:{agency}:failures"
        count = await self.client.incr(key)
        await self.client.expire(key, 60)  # Reset after 60s of no failures
        return count

    async def record_agency_success(self, agency: str) -> None:
        """Reset failure count on success."""
        key = f"circuit:{agency}:failures"
        await self.client.delete(key)

    async def is_agency_available(self, agency: str, max_failures: int = 3) -> bool:
        """Check if agency circuit breaker is open."""
        key = f"circuit:{agency}:failures"
        count = await self.client.get(key)
        if count and int(count) >= max_failures:
            return False
        return True

    async def mark_agency_unavailable(self, agency: str, duration_seconds: int = 30) -> None:
        """Explicitly mark an agency as unavailable."""
        key = f"circuit:{agency}:unavailable"
        await self.client.setex(key, duration_seconds, "1")

    async def check_agency_circuit(self, agency: str) -> bool:
        """Combined check: failure count + explicit unavailable flag."""
        unavailable = await self.client.get(f"circuit:{agency}:unavailable")
        if unavailable:
            return False
        return await self.is_agency_available(agency)

    # ── Agent Execution State ───────────────────────────────────

    async def store_execution_state(
        self,
        request_id: str,
        state: dict,
        ttl_seconds: int = 3600,
    ) -> None:
        """Store LangGraph execution state for resume/audit."""
        key = f"execution:{request_id}"
        await self.client.setex(key, ttl_seconds, json.dumps(state, default=str))

    async def get_execution_state(self, request_id: str) -> dict | None:
        """Retrieve stored execution state."""
        key = f"execution:{request_id}"
        data = await self.client.get(key)
        if data:
            return json.loads(data)
        return None

    # ── Statistics ──────────────────────────────────────────────

    async def get_cache_stats(self) -> dict[str, Any]:
        """Get cache usage statistics."""
        info = await self.client.info("memory")
        keys = await self.client.dbsize()
        return {
            "total_keys": keys,
            "used_memory": info.get("used_memory_human", "unknown"),
            "connected_clients": (await self.client.info("clients")).get("connected_clients", 0),
        }


# Module-level singleton
_cache_instance: RedisCache | None = None


async def get_redis_cache() -> RedisCache:
    """Get or create the Redis cache singleton."""
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = RedisCache()
        await _cache_instance.connect()
    return _cache_instance
