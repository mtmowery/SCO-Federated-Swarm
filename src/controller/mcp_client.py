"""
Async MCP client for communicating with agency MCP servers.

Handles:
- HTTP communication with agency endpoints
- Retry logic with exponential backoff
- Circuit breaker pattern for fault tolerance
- Health checks and capability discovery
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Optional
from enum import Enum

import httpx
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Circuit broken, requests fail fast
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreakerState(BaseModel):
    """Circuit breaker state for an agency."""

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    success_count: int = 0


class MCPClient:
    """Async HTTP client for agency MCP servers with resilience patterns."""

    def __init__(
        self,
        endpoints: dict[str, str],
        timeout: int = 5,
        max_retries: int = 3,
        circuit_breaker_threshold: int = 3,
        circuit_breaker_reset_timeout: int = 30,
    ):
        """
        Initialize MCP client.

        Args:
            endpoints: Dict mapping agency names to base URLs
            timeout: Request timeout in seconds (default: 5)
            max_retries: Maximum number of retries (default: 3)
            circuit_breaker_threshold: Failures before opening circuit (default: 3)
            circuit_breaker_reset_timeout: Seconds before attempting recovery (default: 30)
        """
        self.endpoints = endpoints
        self.timeout = timeout
        self.max_retries = max_retries
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.circuit_breaker_reset_timeout = circuit_breaker_reset_timeout

        # Circuit breaker state per agency
        self.circuit_breakers: dict[str, CircuitBreakerState] = {
            agency: CircuitBreakerState() for agency in endpoints.keys()
        }

        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "MCPClient":
        """Async context manager entry."""
        self.client = httpx.AsyncClient(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        if self.client:
            await self.client.aclose()

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Ensure client is initialized."""
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=self.timeout)
        return self.client

    def _check_circuit_breaker(self, agency: str) -> bool:
        """
        Check if circuit breaker allows request.

        Args:
            agency: Agency name

        Returns:
            True if request should proceed, False if circuit is open

        Raises:
            ValueError: If agency is not recognized
        """
        if agency not in self.circuit_breakers:
            raise ValueError(f"Unknown agency: {agency}")

        breaker = self.circuit_breakers[agency]

        if breaker.state == CircuitState.CLOSED:
            return True

        if breaker.state == CircuitState.OPEN:
            # Check if recovery timeout has elapsed
            if breaker.last_failure_time:
                elapsed = (datetime.utcnow() - breaker.last_failure_time).total_seconds()
                if elapsed >= self.circuit_breaker_reset_timeout:
                    breaker.state = CircuitState.HALF_OPEN
                    breaker.success_count = 0
                    breaker.failure_count = 0
                    logger.info(
                        f"Circuit breaker for {agency} transitioning to HALF_OPEN"
                    )
                    return True
            return False

        if breaker.state == CircuitState.HALF_OPEN:
            return True

        return False

    def _record_success(self, agency: str) -> None:
        """Record successful request."""
        breaker = self.circuit_breakers[agency]

        if breaker.state == CircuitState.HALF_OPEN:
            breaker.success_count += 1
            if breaker.success_count >= 2:
                breaker.state = CircuitState.CLOSED
                breaker.failure_count = 0
                breaker.success_count = 0
                logger.info(f"Circuit breaker for {agency} closed (recovered)")

    def _record_failure(self, agency: str) -> None:
        """Record failed request."""
        breaker = self.circuit_breakers[agency]
        breaker.failure_count += 1
        breaker.last_failure_time = datetime.utcnow()

        if breaker.failure_count >= self.circuit_breaker_threshold:
            breaker.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker for {agency} opened after {breaker.failure_count} failures")

    async def _make_request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Make HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full request URL
            **kwargs: Additional arguments to pass to httpx

        Returns:
            Response object

        Raises:
            httpx.HTTPError: If all retries fail
        """
        client = await self._ensure_client()
        last_error: Optional[Exception] = None

        for attempt in range(self.max_retries):
            try:
                response = await client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as e:
                # Fail fast for client errors that won't resolve with a retry
                if e.response.status_code in (404, 400, 422):
                    raise e
                last_error = e
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.debug(f"Request to {url} failed with {e.response.status_code}, retrying in {wait_time}s")
                    await asyncio.sleep(wait_time)
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.debug(
                        f"Request to {url} failed (attempt {attempt + 1}/{self.max_retries}), "
                        f"retrying in {wait_time}s"
                    )
                    await asyncio.sleep(wait_time)

        if last_error:
            raise last_error

    async def get_capabilities(self, agency: str) -> dict[str, Any]:
        """
        Get agent capabilities for an agency.

        Args:
            agency: Agency name (idhw, idjc, idoc)

        Returns:
            Capabilities dictionary

        Raises:
            ValueError: If agency is not recognized
            httpx.HTTPError: If request fails after retries
        """
        if agency not in self.endpoints:
            raise ValueError(f"Unknown agency: {agency}")

        if not self._check_circuit_breaker(agency):
            raise RuntimeError(f"Circuit breaker open for {agency}")

        try:
            url = f"{self.endpoints[agency]}/capabilities"
            response = await self._make_request("GET", url)
            self._record_success(agency)
            return response.json()
        except Exception as e:
            self._record_failure(agency)
            logger.error(f"Failed to get capabilities for {agency}: {e}")
            raise

    async def execute_tool(
        self,
        agency: str,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Execute a tool on an agency MCP server.

        Args:
            agency: Agency name (idhw, idjc, idoc)
            tool_name: Name of the tool to execute
            arguments: Tool arguments

        Returns:
            Tool execution result

        Raises:
            ValueError: If agency is not recognized
            httpx.HTTPError: If request fails after retries
        """
        if agency not in self.endpoints:
            raise ValueError(f"Unknown agency: {agency}")

        if not self._check_circuit_breaker(agency):
            raise RuntimeError(f"Circuit breaker open for {agency}")

        try:
            url = f"{self.endpoints[agency]}/execute"
            payload = {
                "tool_name": tool_name,
                "parameters": arguments,
                "arguments": arguments,
                "params": arguments
            }
            response = await self._make_request(
                "POST",
                url,
                json=payload,
            )
            self._record_success(agency)
            data = response.json()
            is_success = data.get("success", False) or data.get("status") == "success"
            
            if not is_success:
                error_msg = data.get("error", data.get("detail", "Unknown execution error"))
                logger.error(f"Tool execution returned error: {error_msg}")
                raise ValueError(error_msg)
            return data.get("result", {})
        except Exception as e:
            self._record_failure(agency)
            logger.error(f"Failed to execute tool {tool_name} on {agency}: {e}")
            raise

    async def health_check(self, agency: str) -> bool:
        """
        Check if agency MCP server is healthy.

        Args:
            agency: Agency name (idhw, idjc, idoc)

        Returns:
            True if healthy, False otherwise
        """
        if agency not in self.endpoints:
            raise ValueError(f"Unknown agency: {agency}")

        try:
            url = f"{self.endpoints[agency]}/health"
            response = await self._make_request("GET", url)
            self._record_success(agency)
            return response.status_code == 200
        except Exception as e:
            self._record_failure(agency)
            logger.debug(f"Health check failed for {agency}: {e}")
            return False

    async def batch_health_check(self) -> dict[str, bool]:
        """
        Check health of all agencies.

        Returns:
            Dict mapping agency names to health status
        """
        results = {}
        tasks = [self.health_check(agency) for agency in self.endpoints.keys()]
        health_statuses = await asyncio.gather(*tasks, return_exceptions=True)

        for agency, status in zip(self.endpoints.keys(), health_statuses):
            if isinstance(status, Exception):
                results[agency] = False
            else:
                results[agency] = status

        return results

    def get_circuit_breaker_status(self, agency: str) -> dict[str, Any]:
        """
        Get circuit breaker status for an agency.

        Args:
            agency: Agency name

        Returns:
            Circuit breaker status dictionary

        Raises:
            ValueError: If agency is not recognized
        """
        if agency not in self.circuit_breakers:
            raise ValueError(f"Unknown agency: {agency}")

        breaker = self.circuit_breakers[agency]
        return {
            "agency": agency,
            "state": breaker.state.value,
            "failure_count": breaker.failure_count,
            "last_failure_time": breaker.last_failure_time.isoformat()
            if breaker.last_failure_time
            else None,
            "success_count": breaker.success_count,
        }
