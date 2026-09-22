"""The messaging abstraction.

Nothing outside this package may import a broker client. Agents, graphs and
services depend on ``MessageBus``, which is why swapping Redpanda for Apache
Kafka, or for an in-memory bus in tests, touches no business code.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

__all__ = ["MessageBus", "MessageHandler", "MessageMeta"]

T = TypeVar("T", bound=BaseModel)


@dataclass(frozen=True, slots=True)
class MessageMeta:
    """Delivery facts about one message.

    Handlers use this for logging and for the rare decision that depends on
    delivery rather than content, such as honouring a ``not_before`` header
    on a delayed retry.
    """

    topic: str
    partition: int
    offset: int
    key: str | None
    timestamp: datetime
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def coordinates(self) -> str:
        return f"{self.topic}[{self.partition}]@{self.offset}"


MessageHandler = Callable[[T, MessageMeta], Awaitable[None]]
"""Handle one message.

Returning normally means the message was processed and its offset may be
committed. Raising means it was not: the bus decides whether to retry or
dead-letter, and the offset is not advanced past it.
"""


@runtime_checkable
class MessageBus(Protocol):
    """Publish and subscribe, with at-least-once delivery.

    Implementations must guarantee that a handler which raises does not have
    its offset committed, and that offsets are committed only after the
    handler has returned.
    """

    async def publish(
        self,
        topic: str,
        message: BaseModel,
        *,
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Publish one message and wait for the broker to acknowledge it.

        ``key`` determines the partition; tasks of one run share a key so
        they stay ordered relative to each other.
        """
        ...

    async def subscribe(
        self,
        topic: str,
        group_id: str,
        model: type[T],
        handler: MessageHandler[T],
    ) -> None:
        """Register a handler. Delivery starts when ``start`` is called."""
        ...

    async def start(self) -> None:
        """Connect the producer and begin consuming for every subscription."""
        ...

    async def stop(self) -> None:
        """Drain in-flight work, commit what completed, close connections."""
        ...
