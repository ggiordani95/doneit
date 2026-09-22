"""Wire contracts for the AI Agent Swarm.

Anything crossing a process boundary is defined here and nowhere else.
"""

from swarm_contracts.agents import AgentDefinition, AgentRegistration, Permission
from swarm_contracts.base import WireModel
from swarm_contracts.events import (
    AgentLifecycleEvent,
    AgentResultEvent,
    AgentTaskCompleted,
    AgentTaskFailed,
    AgentTaskRequested,
    DeadLetter,
    result_event_adapter,
)
from swarm_contracts.tasks import (
    AgentResult,
    AgentTask,
    ErrorInfo,
    EventMetadata,
    TaskMetadata,
    TaskStatus,
)
from swarm_contracts.topics import Topic
from swarm_contracts.workflow import (
    ExternalReference,
    IntegrationEvent,
    WorkflowRun,
    WorkflowStatus,
    WorkflowTrigger,
)

__all__ = [
    "AgentDefinition",
    "AgentLifecycleEvent",
    "AgentRegistration",
    "AgentResult",
    "AgentResultEvent",
    "AgentTask",
    "AgentTaskCompleted",
    "AgentTaskFailed",
    "AgentTaskRequested",
    "DeadLetter",
    "ErrorInfo",
    "EventMetadata",
    "ExternalReference",
    "IntegrationEvent",
    "Permission",
    "TaskMetadata",
    "TaskStatus",
    "Topic",
    "WireModel",
    "WorkflowRun",
    "WorkflowStatus",
    "WorkflowTrigger",
    "result_event_adapter",
]
