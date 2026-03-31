"""
Configuration management for Idaho Federated AI Swarm.

Uses pydantic-settings with environment variable support and sensible defaults
for local Docker development.
"""

from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class PostgreSQLConfig(BaseSettings):
    """PostgreSQL database configuration."""

    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5434, description="PostgreSQL port (5434 to avoid conflicts)")
    user: str = Field(default="fed_swarm", description="PostgreSQL user")
    password: str = Field(default="fed_swarm_2026", description="PostgreSQL password")

    idhw_database: str = Field(default="fed_idhw", description="IDHW database name")
    idjc_database: str = Field(default="fed_idjc", description="IDJC database name")
    idoc_database: str = Field(default="fed_idoc", description="IDOC database name")

    pool_size: int = Field(default=20, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Max overflow connections")
    pool_timeout: int = Field(default=30, description="Pool timeout in seconds")
    pool_recycle: int = Field(default=3600, description="Connection recycle time")

    class Config:
        env_prefix = "PG_"
        case_sensitive = False


class Neo4jConfig(BaseSettings):
    """Neo4j graph database configuration."""

    bolt_uri: str = Field(default="bolt://localhost:7688", description="Neo4j Bolt URI (7688 to avoid conflicts)")
    user: str = Field(default="neo4j", description="Neo4j user")
    password: str = Field(default="fed_swarm_2026", description="Neo4j password")
    max_pool_size: int = Field(default=50, description="Max connection pool size")

    class Config:
        env_prefix = "NEO4J_"
        case_sensitive = False


class QdrantConfig(BaseSettings):
    """Qdrant vector database configuration."""

    host: str = Field(default="localhost", description="Qdrant host")
    port: int = Field(default=6335, description="Qdrant port (6335 to avoid conflicts)")
    timeout: Optional[int] = Field(default=30, description="Request timeout in seconds")
    prefer_grpc: bool = Field(default=False, description="Use gRPC transport")

    class Config:
        env_prefix = "QDRANT_"
        case_sensitive = False

    @property
    def url(self) -> str:
        """Construct Qdrant URL."""
        return f"http://{self.host}:{self.port}"


class RedisConfig(BaseSettings):
    """Redis configuration."""

    host: str = Field(default="localhost", description="Redis host")
    port: int = Field(default=6381, description="Redis port (6381 to avoid conflicts)")
    db: int = Field(default=0, description="Redis database number")
    password: Optional[str] = Field(default=None, description="Redis password")
    max_connections: int = Field(default=50, description="Max connection pool size")
    socket_keepalive: bool = Field(default=True, description="Enable socket keepalive")

    class Config:
        env_prefix = "REDIS_"
        case_sensitive = False

    @property
    def url(self) -> str:
        """Construct Redis URL."""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class OllamaConfig(BaseSettings):
    """Ollama local LLM configuration."""

    base_url: str = Field(
        default="http://localhost:11434", description="Ollama service base URL"
    )
    default_model: str = Field(
        default="llama3:8b", description="Default LLM model name"
    )
    embedding_model: str = Field(
        default="nomic-embed-text", description="Embedding model name"
    )
    embedding_dim: int = Field(default=768, description="Embedding dimension")
    temperature: float = Field(
        default=0.7, ge=0.0, le=2.0, description="Model temperature"
    )
    top_p: float = Field(
        default=0.9, ge=0.0, le=1.0, description="Nucleus sampling parameter"
    )
    timeout: int = Field(default=300, description="Request timeout in seconds")

    class Config:
        env_prefix = "OLLAMA_"
        case_sensitive = False


class MCPConfig(BaseSettings):
    """Agency MCP endpoint configuration."""

    idhw_host: str = Field(default="localhost", description="IDHW MCP host")
    idhw_port: int = Field(default=8001, description="IDHW MCP port")

    idjc_host: str = Field(default="localhost", description="IDJC MCP host")
    idjc_port: int = Field(default=8002, description="IDJC MCP port")

    idoc_host: str = Field(default="localhost", description="IDOC MCP host")
    idoc_port: int = Field(default=8003, description="IDOC MCP port")

    timeout: int = Field(default=30, description="MCP request timeout in seconds")

    class Config:
        env_prefix = "MCP_"
        case_sensitive = False

    @property
    def endpoints(self) -> dict[str, str]:
        """Get MCP endpoints as dict."""
        return {
            "idhw": f"http://{self.idhw_host}:{self.idhw_port}",
            "idjc": f"http://{self.idjc_host}:{self.idjc_port}",
            "idoc": f"http://{self.idoc_host}:{self.idoc_port}",
        }


class LoggingConfig(BaseSettings):
    """Logging configuration."""

    level: str = Field(default="INFO", description="Log level")
    format: str = Field(
        default="json", description="Log format: json or text"
    )
    file: Optional[str] = Field(default=None, description="Log file path")
    rotation_size: str = Field(default="100 MB", description="Log rotation size")
    retention: str = Field(default="7 days", description="Log retention period")
    use_correlation_ids: bool = Field(
        default=True, description="Enable request correlation IDs"
    )

    class Config:
        env_prefix = "LOGGING_"
        case_sensitive = False


class Settings(BaseSettings):
    """Root settings combining all configuration sections."""

    environment: str = Field(
        default="development", description="Environment: development, staging, production"
    )
    debug: bool = Field(default=False, description="Enable debug mode")
    api_version: str = Field(default="v1", description="API version")

    postgres: PostgreSQLConfig = Field(default_factory=PostgreSQLConfig)
    neo4j: Neo4jConfig = Field(default_factory=Neo4jConfig)
    qdrant: QdrantConfig = Field(default_factory=QdrantConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    ollama: OllamaConfig = Field(default_factory=OllamaConfig)
    mcp: MCPConfig = Field(default_factory=MCPConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        nested_delimiter = "__"


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Return the global settings singleton."""
    return settings
