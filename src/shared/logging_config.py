"""
Structured logging configuration for Idaho Federated AI Swarm.

Provides JSON-formatted logging with correlation IDs for request tracing.
Uses loguru for enhanced logging capabilities.
"""

import sys
import json
from typing import Optional
from contextvars import ContextVar
from datetime import datetime

from loguru import logger

from .config import settings

# Context variable for correlation ID tracking
_correlation_id: ContextVar[Optional[str]] = ContextVar(
    "correlation_id", default=None
)


def get_correlation_id() -> Optional[str]:
    """Get current correlation ID from context."""
    return _correlation_id.get()


def set_correlation_id(correlation_id: str) -> None:
    """Set correlation ID in context."""
    _correlation_id.set(correlation_id)


def clear_correlation_id() -> None:
    """Clear correlation ID from context."""
    _correlation_id.set(None)


class JSONFormatter:
    """Custom formatter for JSON structured logging."""

    def __call__(self, record: dict) -> str:
        """Format log record as JSON.

        Args:
            record: Loguru record dictionary

        Returns:
            JSON-formatted log line
        """
        log_data = {
            "timestamp": record["time"].isoformat(),
            "level": record["level"].name,
            "logger": record["name"],
            "message": record["message"],
            "module": record["module"],
            "function": record["function"],
            "line": record["line"],
            "process_id": record["process"].id,
            "thread_id": record["thread"].id,
        }

        # Add correlation ID if available
        if settings.logging.use_correlation_ids:
            corr_id = get_correlation_id()
            if corr_id:
                log_data["correlation_id"] = corr_id

        # Add exception info if present
        if record["exception"]:
            log_data["exception"] = {
                "type": record["exception"].type.__name__,
                "message": str(record["exception"].value),
                "traceback": record["extra"].get("_traceback"),
            }

        # Add extra fields
        if record["extra"]:
            log_data["extra"] = {
                k: v
                for k, v in record["extra"].items()
                if not k.startswith("_")
            }

        return json.dumps(log_data)


class TextFormatter:
    """Custom formatter for human-readable text logging."""

    def __call__(self, record: dict) -> str:
        """Format log record as text.

        Args:
            record: Loguru record dictionary

        Returns:
            Formatted log line
        """
        timestamp = record["time"].strftime("%Y-%m-%d %H:%M:%S")
        level = record["level"].name
        logger_name = record["name"]
        message = record["message"]

        # Build base log line
        parts = [f"{timestamp} | {level:8} | {logger_name} | {message}"]

        # Add correlation ID if available
        if settings.logging.use_correlation_ids:
            corr_id = get_correlation_id()
            if corr_id:
                parts.append(f" [corr_id: {corr_id}]")

        # Add location info
        parts.append(f" [{record['name']}:{record['line']}]")

        return "".join(parts)


def configure_logging() -> None:
    """Configure structured logging with loguru."""

    # Remove default handler
    logger.remove()

    # Get configuration
    log_level = settings.logging.level
    log_format = settings.logging.format

    # Create formatter
    if log_format == "json":
        formatter = JSONFormatter()
    else:
        formatter = TextFormatter()

    # Add stdout handler
    if log_format == "json":
        # For JSON, use a plain format string and let the sink callable handle it
        logger.add(
            lambda msg: sys.stdout.write(formatter(msg.record) + "\n"),
            format="{message}",
            level=log_level,
            colorize=False,
            backtrace=settings.debug,
            diagnose=settings.debug,
        )
    else:
        logger.add(
            sys.stdout,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level:8} | {name} | {message}",
            level=log_level,
            colorize=True,
            backtrace=settings.debug,
            diagnose=settings.debug,
        )

    # Add file handler if configured
    if settings.logging.file:
        if log_format == "json":
            logger.add(
                settings.logging.file,
                format="{message}",
                level=log_level,
                rotation=settings.logging.rotation_size,
                retention=settings.logging.retention,
                compression="zip",
                serialize=True,
                backtrace=settings.debug,
                diagnose=settings.debug,
            )
        else:
            logger.add(
                settings.logging.file,
                format="{time:YYYY-MM-DD HH:mm:ss} | {level:8} | {name} | {message}",
                level=log_level,
                rotation=settings.logging.rotation_size,
                retention=settings.logging.retention,
                compression="zip",
                backtrace=settings.debug,
                diagnose=settings.debug,
            )

    logger.info(
        f"Logging configured - level: {log_level}, format: {log_format}",
        extra={"environment": settings.environment},
    )


def bind_correlation_id(correlation_id: str) -> None:
    """
    Bind a correlation ID for request tracing.

    Args:
        correlation_id: Request correlation ID
    """
    set_correlation_id(correlation_id)
    logger.bind(correlation_id=correlation_id)


def get_logger(name: str) -> "logger":
    """
    Get a logger instance for a module.

    Args:
        name: Module name (typically __name__)

    Returns:
        Loguru logger instance
    """
    return logger.bind(name=name)


# Alias for main.py entry point
setup_logging = configure_logging

# Configure on import
configure_logging()
