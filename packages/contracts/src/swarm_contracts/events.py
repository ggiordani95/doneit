"""Events.

A command tells a worker to do something; an event states that something
happened. The two never share a topic, and events are immutable and
past-tense.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import Field, TypeAdapter

from swarm_contracts.base import WireModel
from swarm_contracts.tasks import ErrorInfo, EventMetadata, TaskMetadata
from swarm_shared.time import utcnow

__all__ = [
    "AgentLifecycleEvent",
    "AgentResultEvent",
    "AgentTaskCompleted",
    "AgentTaskFailed",
    "AgentTaskRequested",
    "DeadLetter",
    "result_event_adapter",
]


class AgentTaskRequested(WireModel):
    """Command: execute this task. Published to ``agent.tasks``."""

    event_type: Literal["AgentTaskRequested"] = "AgentTaskRequested"
    event_version: int = 1

    task_id: str
    workflow_id: str
    workflow_run_id: str
    agent_type: str
    agent_version: str | None = None
    input: dict[str, Any] = Field(default_factory=dict)

    metadata: TaskMetadata
    created_at: datetime = Field(default_factory=utcnow)
    not_before: datetime | None = None
    """Set on a delayed retry; the worker waits until this instant."""


class AgentTaskCompleted(WireModel):
    """Event: the task succeeded. Published to ``agent.results``."""

    event_type: Literal["AgentTaskCompleted"] = "AgentTaskCompleted"
    event_version: int = 1

    task_id: str
    workflow_id: str
    workflow_run_id: str
    agent_type: str
    result: dict[str, Any] = Field(default_factory=dict)

    metadata: TaskMetadata
    completed_at: datetime = Field(default_factory=utcnow)


class AgentTaskFailed(WireModel):
    """Event: the task failed. Published to ``agent.results``."""

    event_type: Literal["AgentTaskFailed"] = "AgentTaskFailed"
    event_version: int = 1

    task_id: str
    workflow_id: str
    workflow_run_id: str
    agent_type: str
    error: ErrorInfo

    metadata: TaskMetadata
    failed_at: datetime = Field(default_factory=utcnow)


AgentResultEvent = Annotated[
    AgentTaskCompleted | AgentTaskFailed,
    Field(discriminator="event_type"),
]

result_event_adapter: TypeAdapter[AgentTaskCompleted | AgentTaskFailed] = TypeAdapter(
    AgentResultEvent
)


class AgentLifecycleEvent(WireModel):
    """Observability event. Published to ``agent.events``.

    Deliberately untyped in its payload: these are for humans and dashboards,
    never for control flow.
    """

    event_type: Literal[
        "AgentTaskReceived",
        "AgentTaskStarted",
        "AgentTaskCompleted",
        "AgentTaskFailed",
        "AgentTaskRetried",
        "AgentTaskDeadLettered",
        "AgentRegistered",
        "AgentHeartbeat",
    ]
    event_version: int = 1

    task_id: str | None = None
    workflow_id: str | None = None
    workflow_run_id: str | None = None
    agent_type: str
    attempt: int | None = None
    duration_ms: float | None = None
    detail: dict[str, Any] = Field(default_factory=dict)

    metadata: EventMetadata
    occurred_at: datetime = Field(default_factory=utcnow)


class DeadLetter(WireModel):
    """A task that could not be processed. Published to ``agent.dlq``.

    Carries the whole original command so the task can be replayed by hand
    after the underlying problem is fixed. Nothing is ever dropped silently.
    """

    event_type: Literal["AgentTaskDeadLettered"] = "AgentTaskDeadLettered"
    event_version: int = 1

    task: AgentTaskRequested
    error: ErrorInfo
    attempts: int
    agent_type: str
    workflow_id: str
    workflow_run_id: str

    metadata: TaskMetadata
    dead_lettered_at: datetime = Field(default_factory=utcnow)
