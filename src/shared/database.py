"""
Database connection management for Idaho Federated AI Swarm.

Provides connection factories and session management for all data sources:
- PostgreSQL (agency-specific schemas)
- Neo4j (relationship graph)
- Qdrant (vector embeddings)
- Redis (caching and state)
"""

import logging
from typing import Optional, AsyncGenerator

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)
from sqlalchemy.pool import NullPool, QueuePool
from neo4j import AsyncDriver, AsyncGraphDatabase
from neo4j.exceptions import ServiceUnavailable
from qdrant_client import AsyncQdrantClient
from redis.asyncio import Redis, ConnectionPool

from .config import settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages all database connections and session factories."""

    _pg_engines: dict[str, AsyncEngine] = {}
    _pg_session_makers: dict[str, async_sessionmaker] = {}
    _neo4j_driver: Optional[AsyncDriver] = None
    _redis_client: Optional[Redis] = None
    _qdrant_client: Optional[AsyncQdrantClient] = None

    @classmethod
    async def get_pg_engine(cls, agency: str) -> AsyncEngine:
        """
        Get PostgreSQL async engine for an agency.

        Supports: idhw, idjc, idoc

        Args:
            agency: Agency name (lowercase)

        Returns:
            SQLAlchemy async engine with connection pooling

        Raises:
            ValueError: If agency is not recognized
        """
        agency = agency.lower()

        if agency not in ["idhw", "idjc", "idoc"]:
            raise ValueError(f"Unknown agency: {agency}")

        if agency in cls._pg_engines:
            return cls._pg_engines[agency]

        # Get database name for agency
        db_map = {
            "idhw": settings.postgres.idhw_database,
            "idjc": settings.postgres.idjc_database,
            "idoc": settings.postgres.idoc_database,
        }

        database = db_map[agency]

        # Build connection string
        url = (
            f"postgresql+asyncpg://{settings.postgres.user}:"
            f"{settings.postgres.password}@{settings.postgres.host}:"
            f"{settings.postgres.port}/{database}"
        )

        # Create engine with connection pooling
        engine = create_async_engine(
            url,
            echo=settings.debug,
            pool_size=settings.postgres.pool_size,
            max_overflow=settings.postgres.max_overflow,
            pool_timeout=settings.postgres.pool_timeout,
            pool_recycle=settings.postgres.pool_recycle,
            pool_pre_ping=True,
        )

        cls._pg_engines[agency] = engine
        logger.info(f"Created PostgreSQL engine for agency: {agency}")

        return engine

    @classmethod
    async def get_pg_session(
        cls, agency: str
    ) -> async_sessionmaker:
        """
        Get PostgreSQL session factory for an agency.

        Args:
            agency: Agency name (lowercase)

        Returns:
            Session factory for creating async sessions
        """
        agency = agency.lower()

        if agency in cls._pg_session_makers:
            return cls._pg_session_makers[agency]

        engine = await cls.get_pg_engine(agency)

        session_maker = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )

        cls._pg_session_makers[agency] = session_maker
        logger.info(f"Created PostgreSQL session factory for agency: {agency}")

        return session_maker

    @classmethod
    async def get_neo4j_driver(cls) -> AsyncDriver:
        """
        Get Neo4j async driver.

        Returns:
            Neo4j async driver with connection pooling
        """
        if cls._neo4j_driver is not None:
            return cls._neo4j_driver

        try:
            cls._neo4j_driver = AsyncGraphDatabase.driver(
                settings.neo4j.bolt_uri,
                auth=(settings.neo4j.user, settings.neo4j.password),
                max_pool_size=settings.neo4j.max_pool_size,
                trust="TRUST_SYSTEM_CA_SIGNED_CERTIFICATES",
                encrypted=False,
            )

            # Test connection
            async with cls._neo4j_driver.session() as session:
                await session.run("RETURN 1")

            logger.info(f"Connected to Neo4j at {settings.neo4j.bolt_uri}")

        except ServiceUnavailable as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise

        return cls._neo4j_driver

    @classmethod
    async def get_redis_client(cls) -> Redis:
        """
        Get Redis async client with connection pooling.

        Returns:
            Redis async client
        """
        if cls._redis_client is not None:
            return cls._redis_client

        connection_pool = ConnectionPool.from_url(
            settings.redis.url,
            max_connections=settings.redis.max_connections,
            socket_keepalive=settings.redis.socket_keepalive,
            socket_keepalive_intvl=60,
            decode_responses=False,
        )

        cls._redis_client = Redis(connection_pool=connection_pool)

        try:
            # Test connection
            await cls._redis_client.ping()
            logger.info(f"Connected to Redis at {settings.redis.host}:{settings.redis.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

        return cls._redis_client

    @classmethod
    async def get_qdrant_client(cls) -> AsyncQdrantClient:
        """
        Get Qdrant async client.

        Returns:
            Qdrant async client
        """
        if cls._qdrant_client is not None:
            return cls._qdrant_client

        cls._qdrant_client = AsyncQdrantClient(
            url=settings.qdrant.url,
            timeout=settings.qdrant.timeout,
            prefer_grpc=settings.qdrant.prefer_grpc,
        )

        try:
            # Test connection
            await cls._qdrant_client.get_collections()
            logger.info(
                f"Connected to Qdrant at {settings.qdrant.host}:{settings.qdrant.port}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Qdrant: {e}")
            raise

        return cls._qdrant_client

    @classmethod
    async def close_all(cls) -> None:
        """Close all database connections and cleanup resources."""
        # Close PostgreSQL engines
        for agency, engine in cls._pg_engines.items():
            await engine.dispose()
            logger.info(f"Closed PostgreSQL engine for agency: {agency}")

        cls._pg_engines.clear()
        cls._pg_session_makers.clear()

        # Close Neo4j driver
        if cls._neo4j_driver is not None:
            await cls._neo4j_driver.close()
            cls._neo4j_driver = None
            logger.info("Closed Neo4j driver")

        # Close Redis client
        if cls._redis_client is not None:
            await cls._redis_client.close()
            cls._redis_client = None
            logger.info("Closed Redis client")

        # Close Qdrant client (if it has a close method)
        if cls._qdrant_client is not None:
            # Qdrant client doesn't have explicit close, but we clear reference
            cls._qdrant_client = None
            logger.info("Cleared Qdrant client")


# Convenience functions for direct access
async def get_pg_engine(agency: str) -> AsyncEngine:
    """Get PostgreSQL engine for an agency."""
    return await DatabaseManager.get_pg_engine(agency)


async def get_pg_session(agency: str) -> async_sessionmaker:
    """Get PostgreSQL session factory for an agency."""
    return await DatabaseManager.get_pg_session(agency)


async def get_neo4j_driver() -> AsyncDriver:
    """Get Neo4j driver."""
    return await DatabaseManager.get_neo4j_driver()


async def get_redis_client() -> Redis:
    """Get Redis client."""
    return await DatabaseManager.get_redis_client()


async def get_qdrant_client() -> AsyncQdrantClient:
    """Get Qdrant client."""
    return await DatabaseManager.get_qdrant_client()


async def pg_session_context(agency: str) -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager for PostgreSQL sessions.

    Usage:
        async with pg_session_context("idhw") as session:
            # Use session
    """
    session_maker = await get_pg_session(agency)
    async with session_maker() as session:
        yield session


async def close_all_connections() -> None:
    """Close all database connections."""
    await DatabaseManager.close_all()
