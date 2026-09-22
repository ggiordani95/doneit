"""The wire format is a compatibility surface: it must stay camelCase."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from swarm_contracts import (
    AgentTaskCompleted,
    AgentTaskFailed,
    AgentTaskRequested,
    ErrorInfo,
    TaskMetadata,
    Topic,
    WorkflowStatus,
    result_event_adapter,
)


def _metadata() -> TaskMetadata:
    return TaskMetadata(correlation_id="PROJ-42:corr-1", causation_id="evt-9", attempt=2)


def test_task_serializes_to_camel_case() -> None:
    ev = AgentTaskRequested(
        task_id="task-1",
        workflow_id="wf-1",
        workflow_run_id="run-1",
        agent_type="coder",
        input={"repo": "acme/backend"},
        metadata=_metadata(),
    )
    wire = json.loads(ev.model_dump_json(by_alias=True, exclude_none=True))

    assert wire["taskId"] == "task-1"
    assert wire["workflowRunId"] == "run-1"
    assert wire["agentType"] == "coder"
    assert wire["metadata"]["correlationId"] == "PROJ-42:corr-1"
    assert "task_id" not in wire


def test_task_accepts_both_spellings() -> None:
    payload = {
        "taskId": "task-1",
        "workflowId": "wf-1",
        "workflowRunId": "run-1",
        "agentType": "coder",
        "metadata": {"correlationId": "corr-1"},
    }
    ev = AgentTaskRequested.model_validate(payload)
    assert ev.task_id == "task-1"
    assert ev.metadata.attempt == 1


def test_unknown_field_is_rejected() -> None:
    """Typos must fail loudly rather than vanish into an ignored field."""
    with pytest.raises(ValidationError):
        AgentTaskRequested.model_validate(
            {
                "taskId": "task-1",
                "workflowId": "wf-1",
                "workflowRunId": "run-1",
                "agentType": "coder",
                "metadata": {"correlationId": "corr-1"},
                "agentTyp": "typo",
            }
        )


def test_result_event_discriminates_on_event_type() -> None:
    completed = AgentTaskCompleted(
        task_id="task-1",
        workflow_id="wf-1",
        workflow_run_id="run-1",
        agent_type="coder",
        result={"filesChanged": 7},
        metadata=_metadata(),
    )
    failed = AgentTaskFailed(
        task_id="task-2",
        workflow_id="wf-1",
        workflow_run_id="run-1",
        agent_type="coder",
        error=ErrorInfo(code="AGENT_EXECUTION_FAILED", message="boom", retryable=True),
        metadata=_metadata(),
    )

    for original in (completed, failed):
        raw = original.model_dump_json(by_alias=True, exclude_none=True)
        parsed = result_event_adapter.validate_json(raw)
        assert type(parsed) is type(original)
        assert parsed.task_id == original.task_id


def test_partition_key_is_the_run() -> None:
    """Tasks of one run must land on one partition to stay ordered."""
    from swarm_contracts import AgentTask

    task = AgentTask(
        id="task-1",
        workflow_id="wf-1",
        workflow_run_id="run-42",
        agent_type="coder",
        metadata=_metadata(),
    )
    assert task.partition_key == "run-42"


def test_topics_are_stable() -> None:
    assert Topic.TASKS.value == "agent.tasks"
    assert Topic.DLQ.value == "agent.dlq"
    assert len(set(Topic)) == 6


@pytest.mark.parametrize(
    ("status", "terminal"),
    [
        (WorkflowStatus.PENDING, False),
        (WorkflowStatus.RUNNING, False),
        (WorkflowStatus.WAITING, False),
        (WorkflowStatus.COMPLETED, True),
        (WorkflowStatus.FAILED, True),
        (WorkflowStatus.CANCELLED, True),
    ],
)
def test_terminal_statuses(status: WorkflowStatus, terminal: bool) -> None:
    assert status.is_terminal is terminal
