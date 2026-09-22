"""Redpanda adapter for :class:`~swarm_messaging.bus.MessageBus`.

Speaks the Kafka protocol through ``aiokafka``, so the same object works
against Redpanda, Apache Kafka, MSK or Confluent Cloud.

Three properties matter more than throughput here, because agent tasks are
long and expensive:

* **Offsets are committed only after a handler returns.** A worker that dies
  mid-task leaves the offset where it was, so the task is redelivered and the
  idempotency check decides whether to re-run it.
* **Order is preserved per partition.** Tasks of one run share a partition
  key, so they must not be reordered by concurrent processing. Messages
  within a partition run sequentially; different partitions run in parallel.
* **Rebalances move as little as possible.** ``StickyPartitionAssignor``
  keeps partitions with the consumer that already had them.

  Note that ``aiokafka`` 0.14 implements only the *eager* rebalance protocol:
  even the sticky assignor revokes every partition and reassigns, so a
  rebalance is still stop-the-world for the fetch loop. It is brief, and it
  does not interrupt work in progress as long as execution is decoupled from
  the poll loop, which is why the agent runtime hands tasks to a task group
  rather than awaiting them inside the handler.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypeVar

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.coordinator.assignors.sticky.sticky_assignor import StickyPartitionAssignor
from pydantic import BaseModel, ValidationError

from swarm_messaging.bus import MessageHandler, MessageMeta
from swarm_observability import get_logger
from swarm_shared import BrokerSettings

__all__ = ["RedpandaMessageBus"]

log = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)

_POLL_TIMEOUT_MS = 1_000


@dataclass(slots=True)
class _Subscription:
    topic: str
    group_id: str
    model: type[BaseModel]
    handler: MessageHandler[Any]
    concurrency: int
    max_poll_interval_ms: int
    session_timeout_ms: int


class _RebalanceLogger(ConsumerRebalanceListener):
    """Log partition movement so rebalance storms are visible in the logs."""

    def __init__(self, group_id: str) -> None:
        self._group_id = group_id

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        if revoked:
            log.info(
                "broker.partitions_revoked",
                group_id=self._group_id,
                partitions=sorted(f"{tp.topic}[{tp.partition}]" for tp in revoked),
            )

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        if assigned:
            log.info(
                "broker.partitions_assigned",
                group_id=self._group_id,
                partitions=sorted(f"{tp.topic}[{tp.partition}]" for tp in assigned),
            )


class RedpandaMessageBus:
    """A :class:`MessageBus` backed by Redpanda.

    Usage::

        bus = RedpandaMessageBus(settings.broker)
        await bus.subscribe(Topic.TASKS, "agent-coder", AgentTaskRequested, handle)
        await bus.start()
        ...
        await bus.stop()
    """

    def __init__(
        self,
        settings: BrokerSettings,
        *,
        client_id: str | None = None,
        drop_undeserializable: bool = True,
        redelivery_delay_s: float = 1.0,
    ) -> None:
        self._settings = settings
        self._client_id = client_id or settings.client_id
        self._redelivery_delay_s = redelivery_delay_s
        self._drop_undeserializable = drop_undeserializable
        """A payload that fails validation can never succeed on retry, so it
        is logged and skipped rather than blocking its partition forever."""

        self._producer: AIOKafkaProducer | None = None
        self._subscriptions: list[_Subscription] = []
        self._consumers: list[AIOKafkaConsumer] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping = asyncio.Event()

    # ------------------------------------------------------------------ setup

    def _connection_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "bootstrap_servers": self._settings.servers,
            "security_protocol": self._settings.security_protocol,
        }
        if self._settings.sasl_mechanism:
            kwargs["sasl_mechanism"] = self._settings.sasl_mechanism
            kwargs["sasl_plain_username"] = self._settings.sasl_username
            kwargs["sasl_plain_password"] = (
                self._settings.sasl_password.get_secret_value()
                if self._settings.sasl_password
                else None
            )
        return kwargs

    async def subscribe(
        self,
        topic: str,
        group_id: str,
        model: type[T],
        handler: MessageHandler[T],
        *,
        concurrency: int = 4,
        max_poll_interval_ms: int = 660_000,
        session_timeout_ms: int = 45_000,
    ) -> None:
        """Register a handler. Consumption begins at :meth:`start`.

        ``max_poll_interval_ms`` must outlast the slowest task the handler can
        run, even though execution is decoupled from the poll loop upstream.
        """
        self._subscriptions.append(
            _Subscription(
                topic=topic,
                group_id=group_id,
                model=model,
                handler=handler,
                concurrency=concurrency,
                max_poll_interval_ms=max_poll_interval_ms,
                session_timeout_ms=session_timeout_ms,
            )
        )

    async def start(self) -> None:
        self._stopping.clear()

        self._producer = AIOKafkaProducer(
            client_id=f"{self._client_id}-producer",
            enable_idempotence=True,
            acks="all",
            request_timeout_ms=self._settings.request_timeout_ms,
            **self._connection_kwargs(),
        )
        await self._producer.start()
        log.info("broker.producer_started", servers=self._settings.servers)

        for sub in self._subscriptions:
            consumer = AIOKafkaConsumer(
                client_id=f"{self._client_id}-{sub.group_id}",
                group_id=sub.group_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                partition_assignment_strategy=(StickyPartitionAssignor,),
                max_poll_records=sub.concurrency,
                max_poll_interval_ms=sub.max_poll_interval_ms,
                session_timeout_ms=sub.session_timeout_ms,
                heartbeat_interval_ms=3_000,
                **self._connection_kwargs(),
            )
            await consumer.start()
            consumer.subscribe([sub.topic], listener=_RebalanceLogger(sub.group_id))
            self._consumers.append(consumer)
            self._tasks.append(
                asyncio.create_task(
                    self._consume_loop(consumer, sub),
                    name=f"consume:{sub.topic}:{sub.group_id}",
                )
            )
            log.info("broker.consumer_started", topic=sub.topic, group_id=sub.group_id)

    async def stop(self) -> None:
        """Stop consuming, let in-flight handlers finish, then disconnect."""
        self._stopping.set()

        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=30)
        self._tasks.clear()

        for consumer in self._consumers:
            with contextlib.suppress(Exception):
                await consumer.stop()
        self._consumers.clear()

        if self._producer is not None:
            with contextlib.suppress(Exception):
                await self._producer.stop()
            self._producer = None

        log.info("broker.stopped")

    # -------------------------------------------------------------- publishing

    async def publish(
        self,
        topic: str,
        message: BaseModel,
        *,
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        if self._producer is None:
            raise RuntimeError("MessageBus.publish called before start()")

        payload = message.model_dump_json(by_alias=True, exclude_none=True).encode()
        encoded_headers = [(k, v.encode()) for k, v in (headers or {}).items()]

        await self._producer.send_and_wait(
            topic,
            value=payload,
            key=key.encode() if key else None,
            headers=encoded_headers,
        )
        log.debug("broker.published", topic=topic, key=key, bytes=len(payload))

    # --------------------------------------------------------------- consuming

    async def _consume_loop(self, consumer: AIOKafkaConsumer, sub: _Subscription) -> None:
        """Fetch, process and commit, one batch at a time.

        The batch size is bounded by ``max_poll_records``, which is the
        backpressure mechanism: no more messages are fetched than the handler
        can run concurrently.
        """
        semaphore = asyncio.Semaphore(sub.concurrency)

        while not self._stopping.is_set():
            try:
                batches = await consumer.getmany(
                    timeout_ms=_POLL_TIMEOUT_MS, max_records=sub.concurrency
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("broker.fetch_failed", topic=sub.topic, group_id=sub.group_id)
                await asyncio.sleep(1)
                continue

            if not batches:
                continue

            results = await asyncio.gather(
                *(
                    self._process_partition(tp, records, sub, semaphore)
                    for tp, records in batches.items()
                ),
                return_exceptions=False,
            )

            offsets = {tp: offset for tp, offset, _ in results if offset is not None}
            if offsets:
                try:
                    await consumer.commit(offsets)
                except Exception:
                    # The batch will be redelivered; idempotency absorbs it.
                    log.exception("broker.commit_failed", group_id=sub.group_id)

            # A handler that raised leaves its offset uncommitted, but the
            # consumer's in-memory position has already moved past it. Without
            # seeking back, that message would only be redelivered after a
            # restart or a rebalance, so the task would sit unprocessed.
            failures = [(tp, offset) for tp, _, offset in results if offset is not None]
            for tp, failed_offset in failures:
                consumer.seek(tp, failed_offset)
                log.warning(
                    "broker.rewound",
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=failed_offset,
                    group_id=sub.group_id,
                )
            if failures:
                # Keep a failing handler from spinning the CPU. Real retry
                # policy (backoff, attempt counting, DLQ) belongs to the agent
                # runtime, which normally handles its own exceptions.
                await asyncio.sleep(self._redelivery_delay_s)

    async def _process_partition(
        self,
        tp: TopicPartition,
        records: list[ConsumerRecord[bytes, bytes]],
        sub: _Subscription,
        semaphore: asyncio.Semaphore,
    ) -> tuple[TopicPartition, int | None, int | None]:
        """Run one partition's records in order.

        Returns ``(partition, committable, failed)``:

        * ``committable`` is one past the last record handled successfully;
        * ``failed`` is the offset of the record whose handler raised, which
          the caller seeks back to so it is redelivered.

        A failure stops the partition there, so nothing after an unprocessed
        message is ever marked as done.
        """
        committable: int | None = None
        failed: int | None = None

        for record in records:
            if self._stopping.is_set():
                break

            message = self._deserialize(record, sub)
            if message is _SKIP:
                committable = record.offset + 1
                continue
            if message is None:
                failed = record.offset
                break

            meta = _meta_from(record)
            async with semaphore:
                try:
                    await sub.handler(message, meta)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception(
                        "broker.handler_failed",
                        topic=record.topic,
                        partition=record.partition,
                        offset=record.offset,
                        group_id=sub.group_id,
                    )
                    failed = record.offset
                    break
            committable = record.offset + 1

        return tp, committable, failed

    def _deserialize(self, record: ConsumerRecord[bytes, bytes], sub: _Subscription) -> Any:
        """Validate a record into its model.

        Returns ``_SKIP`` for a payload that can never be valid, so the
        partition is not blocked by a poison message.
        """
        if record.value is None:
            return _SKIP
        try:
            return sub.model.model_validate_json(record.value)
        except ValidationError as exc:
            log.error(
                "broker.deserialize_failed",
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                errors=exc.error_count(),
            )
            return _SKIP if self._drop_undeserializable else None


class _Skip:
    __slots__ = ()


_SKIP = _Skip()


def _meta_from(record: ConsumerRecord[bytes, bytes]) -> MessageMeta:
    return MessageMeta(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        key=record.key.decode() if record.key else None,
        timestamp=datetime.fromtimestamp(record.timestamp / 1000, tz=UTC),
        headers={k: v.decode() for k, v in (record.headers or ())},
    )
