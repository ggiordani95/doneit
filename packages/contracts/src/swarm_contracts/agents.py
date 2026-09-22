"""Agent registry and permission model."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import Field

from swarm_contracts.base import WireModel

__all__ = ["AgentDefinition", "AgentRegistration", "Permission"]


class Permission(StrEnum):
    """Capabilities a tool requires and an agent may hold.

    The tool registry filters by these before exposing anything to the model,
    so an unpermitted tool is invisible rather than merely refused.
    """

    FILESYSTEM = "filesystem"
    SHELL = "shell"
    NETWORK = "network"
    GIT = "git"
    DATABASE = "database"
    GITHUB_READ = "github:read"
    GITHUB_WRITE = "github:write"
    GITHUB_MERGE = "github:merge"
    JIRA_READ = "jira:read"
    JIRA_WRITE = "jira:write"


class AgentDefinition(WireModel):
    id: str
    version: str
    capabilities: list[str] = Field(default_factory=list)


class AgentRegistration(WireModel):
    id: str
    type: str
    version: str
    capabilities: list[str] = Field(default_factory=list)
    permissions: list[Permission] = Field(default_factory=list)
    status: Literal["active", "disabled"] = "active"

    def allows(self, permission: Permission) -> bool:
        return permission in self.permissions
