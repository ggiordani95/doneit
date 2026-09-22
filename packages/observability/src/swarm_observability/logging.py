"""Structured logging.

Every log line carries the trace fields of the spec: workflow, run, task,
agent type, attempt, correlation and causation. They are bound once per task
via contextvars, so call sites never repeat them.

Secrets never reach the output: a redaction processor drops known-sensitive
keys regardless of where in the event dict they appear.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars
from structlog.types import EventDict, WrappedLogger

__all__ = ["bind_task_context", "clear_task_context", "configure_logging", "get_logger"]

_SENSITIVE_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "authorization",
        "access_token",
        "token",
        "password",
        "secret",
        "private_key",
        "webhook_secret",
        "jira_api_token",
        "github_token",
        "installation_token",
        "credentials",
    }
)

_REDACTED = "[redacted]"


def _redact(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Drop the value of any key that looks like a credential.

    Cheap insurance: a token that reaches a log aggregator has to be treated
    as compromised, and the cost of this check is a dict scan per line.
    """
    for key in list(event_dict):
        if key.lower() in _SENSITIVE_KEYS:
            event_dict[key] = _REDACTED
    return event_dict


def _drop_color_message(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """uvicorn duplicates its message under ``color_message``; drop it."""
    event_dict.pop("color_message", None)
    return event_dict


def configure_logging(
    *,
    level: str = "INFO",
    fmt: str = "console",
    service: str | None = None,
) -> None:
    """Configure structlog and route the standard library through it.

    ``fmt`` is ``console`` for local development and ``json`` everywhere else.
    """
    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        _drop_color_message,
        _redact,
    ]

    if service:
        shared.insert(0, _service_binder(service))

    renderer: Any = (
        structlog.dev.ConsoleRenderer(colors=True)
        if fmt == "console"
        else structlog.processors.JSONRenderer()
    )

    structlog.configure(
        processors=[
            *shared,
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Standard-library loggers (uvicorn, sqlalchemy, aiokafka) render the same way.
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(structlog.stdlib.ProcessorFormatter(processors=[_redact, renderer]))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())

    for noisy in ("aiokafka", "aiokafka.consumer.group_coordinator", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def _service_binder(service: str) -> Any:
    def bind(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
        event_dict.setdefault("service", service)
        return event_dict

    return bind


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)  # type: ignore[no-any-return]


def bind_task_context(
    *,
    workflow_id: str | None = None,
    workflow_run_id: str | None = None,
    task_id: str | None = None,
    agent_type: str | None = None,
    attempt: int | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    tenant_id: str | None = None,
    **extra: Any,
) -> None:
    """Bind the spec's trace fields for the current task.

    Call once when a task is received; every later log line in that context
    carries them. ``extra`` takes the integration fields when present:
    ``jira_issue_key``, ``repo``, ``pull_request_number``.
    """
    fields = {
        "workflow_id": workflow_id,
        "workflow_run_id": workflow_run_id,
        "task_id": task_id,
        "agent_type": agent_type,
        "attempt": attempt,
        "correlation_id": correlation_id,
        "causation_id": causation_id,
        "tenant_id": tenant_id,
        **extra,
    }
    bind_contextvars(**{k: v for k, v in fields.items() if v is not None})


def clear_task_context() -> None:
    """Drop the bound fields once a task finishes."""
    clear_contextvars()
