"""Tasks, results and the metadata every message carries."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import Field

from swarm_contracts.base import WireModel
from swarm_shared.time import utcnow

__all__ = [
    "AgentResult",
    "AgentTask",
    "ErrorInfo",
    "EventMetadata",
    "TaskMetadata",
    "TaskStatus",
]


class EventMetadata(WireModel):
    """Tracing context carried by every message.

    ``correlation_id`` identifies the whole run; ``causation_id`` identifies
    the message that caused this one. Together they reconstruct the causal
    chain of a run without reading the database.
    """

    correlation_id: str
    causation_id: str | None = None
    tenant_id: str | None = None


class TaskMetadata(EventMetadata):
    attempt: int = Field(default=1, ge=1)


class ErrorInfo(WireModel):
    code: str
    message: str
    retryable: bool = False
    retry_after_s: float | None = None


class AgentTask(WireModel):
    """One unit of agent work. A command, not an event."""

    id: str
    workflow_id: str
    workflow_run_id: str

    agent_type: str
    agent_version: str | None = None

    input: dict[str, Any] = Field(default_factory=dict)

    metadata: TaskMetadata
    created_at: datetime = Field(default_factory=utcnow)

    @property
    def partition_key(self) -> str:
        """Tasks of one run share a partition, so they stay ordered."""
        return self.workflow_run_id


class AgentResult(WireModel):
    """What an agent produces.

    Large artifacts are referenced, never inlined: state and messages stay
    small enough to checkpoint cheaply.
    """

    task_id: str
    output: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[str] = Field(default_factory=list)
    usage: dict[str, Any] | None = None


TaskStatus = Literal[
    "pending",
    "published",
    "received",
    "running",
    "completed",
    "failed",
    "cancelled",
]
