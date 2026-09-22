"""Integration tests for the Redpanda message bus.

These run against the broker from ``docker/compose.yaml``. They are marked
``integration`` so the unit suite stays runnable with no containers:

    pytest -m "not integration"     # unit only
    pytest -m integration           # needs docker compose up
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import pytest

from swarm_contracts import AgentTaskRequested, TaskMetadata, Topic
from swarm_messaging import MessageBus, MessageMeta, RedpandaMessageBus
from swarm_shared import BrokerSettings

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
_RECEIVE_TIMEOUT = 30


def _settings() -> BrokerSettings:
    return BrokerSettings(bootstrap_servers=BOOTSTRAP)  # type: ignore[call-arg]


def _task(run_id: str, *, agent_type: str = "researcher", attempt: int = 1) -> AgentTaskRequested:
    return AgentTaskRequested(
        task_id=f"task-{uuid.uuid4()}",
        workflow_id="wf-test",
        workflow_run_id=run_id,
        agent_type=agent_type,
        input={"question": "why is the build red?"},
        metadata=TaskMetadata(correlation_id=f"corr-{run_id}", attempt=attempt),
    )


@pytest.fixture
async def bus() -> AsyncIterator[RedpandaMessageBus]:
    b = RedpandaMessageBus(_settings(), client_id=f"test-{uuid.uuid4().hex[:8]}")
    yield b
    await b.stop()


async def test_publish_and_consume_roundtrip(bus: RedpandaMessageBus) -> None:
    """A published task comes back intact, through a real broker."""
    group = f"test-roundtrip-{uuid.uuid4().hex[:8]}"
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    received: asyncio.Queue[tuple[AgentTaskRequested, MessageMeta]] = asyncio.Queue()

    # A fresh consumer group starts at the earliest offset, so the topic still
    # holds tasks from previous runs of this suite. Only ours are of interest.
    async def handler(msg: AgentTaskRequested, meta: MessageMeta) -> None:
        if msg.workflow_run_id == run_id:
            await received.put((msg, meta))

    await bus.subscribe(Topic.TASKS, group, AgentTaskRequested, handler)
    await bus.start()

    sent = _task(run_id)
    await bus.publish(Topic.TASKS, sent, key=sent.workflow_run_id)

    msg, meta = await asyncio.wait_for(received.get(), timeout=_RECEIVE_TIMEOUT)

    assert msg.task_id == sent.task_id
    assert msg.agent_type == "researcher"
    assert msg.input == {"question": "why is the build red?"}
    assert msg.metadata.correlation_id == sent.metadata.correlation_id
    assert meta.topic == Topic.TASKS
    assert meta.key == run_id


async def test_run_key_pins_tasks_to_one_partition(bus: RedpandaMessageBus) -> None:
    """Tasks of one run must stay ordered, which requires one partition."""
    group = f"test-partition-{uuid.uuid4().hex[:8]}"
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    seen: list[MessageMeta] = []
    done = asyncio.Event()

    async def handler(msg: AgentTaskRequested, meta: MessageMeta) -> None:
        if msg.workflow_run_id != run_id:
            return
        seen.append(meta)
        if len(seen) == 5:
            done.set()

    await bus.subscribe(Topic.TASKS, group, AgentTaskRequested, handler)
    await bus.start()

    for i in range(5):
        await bus.publish(Topic.TASKS, _task(run_id, attempt=i + 1), key=run_id)

    await asyncio.wait_for(done.wait(), timeout=_RECEIVE_TIMEOUT)

    assert len({m.partition for m in seen}) == 1, "one run must map to one partition"
    assert [m.offset for m in seen] == sorted(m.offset for m in seen), "order preserved"


async def test_failed_handler_does_not_commit(bus: RedpandaMessageBus) -> None:
    """A handler that raises must leave the offset, so the task is redelivered.

    This is what makes a worker killed mid-task recoverable: nothing is marked
    done until it actually is.
    """
    group = f"test-nocommit-{uuid.uuid4().hex[:8]}"
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    attempts = 0
    succeeded = asyncio.Event()

    async def flaky(msg: AgentTaskRequested, __: MessageMeta) -> None:
        nonlocal attempts
        if msg.workflow_run_id != run_id:
            return
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient failure on first delivery")
        succeeded.set()

    await bus.subscribe(Topic.TASKS, group, AgentTaskRequested, flaky)
    await bus.start()

    await bus.publish(Topic.TASKS, _task(run_id), key=run_id)

    await asyncio.wait_for(succeeded.wait(), timeout=_RECEIVE_TIMEOUT)
    assert attempts >= 2, "the task must be redelivered after the handler raised"


async def test_poison_message_does_not_block_the_partition(
    bus: RedpandaMessageBus,
) -> None:
    """An undeserializable payload is skipped, not retried forever.

    A malformed message can never become valid, so blocking its partition
    would stall every run that hashes to it.
    """
    group = f"test-poison-{uuid.uuid4().hex[:8]}"
    good_received = asyncio.Event()

    run_id = f"run-{uuid.uuid4().hex[:8]}"

    async def handler(msg: AgentTaskRequested, _: MessageMeta) -> None:
        if msg.workflow_run_id == run_id and msg.agent_type == "reviewer":
            good_received.set()

    await bus.subscribe(Topic.TASKS, group, AgentTaskRequested, handler)
    await bus.start()

    # Publish garbage that cannot validate, on the same key as the good task.
    assert bus._producer is not None
    await bus._producer.send_and_wait(Topic.TASKS, value=b'{"nope": true}', key=run_id.encode())
    await bus.publish(Topic.TASKS, _task(run_id, agent_type="reviewer"), key=run_id)

    await asyncio.wait_for(good_received.wait(), timeout=_RECEIVE_TIMEOUT)


async def test_bus_satisfies_the_protocol() -> None:
    """The adapter must remain substitutable for the abstraction."""
    assert isinstance(RedpandaMessageBus(_settings()), MessageBus)


async def test_publish_before_start_is_an_error() -> None:
    b = RedpandaMessageBus(_settings())
    with pytest.raises(RuntimeError, match="before start"):
        await b.publish(Topic.TASKS, _task("run-x"))
