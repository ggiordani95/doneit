"""Topic names.

Kept in one place so that no string literal for a topic appears anywhere else
in the codebase.
"""

from __future__ import annotations

from enum import StrEnum

__all__ = ["Topic"]


class Topic(StrEnum):
    TASKS = "agent.tasks"
    RESULTS = "agent.results"
    EVENTS = "agent.events"
    DLQ = "agent.dlq"
    GITHUB_EVENTS = "integration.github.events"
    JIRA_EVENTS = "integration.jira.events"
