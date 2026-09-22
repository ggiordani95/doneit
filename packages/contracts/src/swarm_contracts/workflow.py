"""Workflow runs, triggers and external references."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import Field

from swarm_contracts.base import WireModel
from swarm_shared.time import utcnow

__all__ = [
    "ExternalReference",
    "IntegrationEvent",
    "WorkflowRun",
    "WorkflowStatus",
    "WorkflowTrigger",
]


class WorkflowStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING = "waiting"
    """Blocked on agent results or on a human decision."""
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in {
            WorkflowStatus.COMPLETED,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
        }


class WorkflowTrigger(WireModel):
    """What started a run."""

    source: Literal["api", "github", "jira", "schedule"]
    external_id: str | None = None
    """"PROJ-42" or "acme/backend#17"."""
    delivery_id: str | None = None
    """Provider delivery ID, kept for auditing the webhook that caused this."""


class WorkflowRun(WireModel):
    id: str
    workflow_id: str
    status: WorkflowStatus = WorkflowStatus.PENDING
    state: dict[str, Any] = Field(default_factory=dict)
    trigger: WorkflowTrigger | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class IntegrationEvent(WireModel):
    """A GitHub or Jira webhook, normalized.

    Provider payloads are trimmed to what the trigger rules and workflows
    actually read. Everything in ``payload`` is third-party text and is
    treated as untrusted data, never as instructions.
    """

    event_type: str
    """Dotted and past-tense: "github.pull_request.opened"."""
    event_version: int = 1
    source: Literal["github", "jira"]
    delivery_id: str
    occurred_at: datetime = Field(default_factory=utcnow)
    tenant_id: str | None = None

    subject: dict[str, Any] = Field(default_factory=dict)
    """Identifiers: repo, PR number, issue key."""
    payload: dict[str, Any] = Field(default_factory=dict)

    metadata: dict[str, Any] = Field(default_factory=dict)


class ExternalReference(WireModel):
    """Links a run to an object in GitHub or Jira, in both directions."""

    run_id: str
    provider: Literal["github", "jira"]
    kind: Literal["jira_issue", "github_pr", "github_branch"]
    external_id: str
    url: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
