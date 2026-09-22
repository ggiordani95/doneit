"""IDs must be unique and ordered; error classification drives retry."""

from __future__ import annotations

import time

import pytest

from swarm_shared import PermanentError, TransientError, classify
from swarm_shared.ids import correlation_id, new_id, uuid7


def test_uuid7_is_version_7_and_unique() -> None:
    ids = {uuid7() for _ in range(1000)}
    assert len(ids) == 1000
    assert all(u.version == 7 for u in ids)


def test_uuid7_is_time_ordered() -> None:
    first = uuid7()
    time.sleep(0.005)
    second = uuid7()
    assert str(first) < str(second)


def test_prefixed_ids() -> None:
    assert new_id("task").startswith("task-")
    bare = new_id()
    assert bare
    assert "-" in bare


def test_correlation_id_embeds_the_ticket() -> None:
    """Traces must be searchable by Jira key."""
    assert correlation_id("PROJ-42").startswith("PROJ-42:")
    assert not correlation_id().startswith(":")


@pytest.mark.parametrize(
    ("exc", "retryable"),
    [
        (TransientError("timeout"), True),
        (PermanentError("bad input"), False),
        (TimeoutError(), True),
        (ConnectionError(), True),
        (ValueError("malformed"), False),
        (KeyError("missing"), False),
        (RuntimeError("unknown"), True),
    ],
)
def test_classification(exc: BaseException, retryable: bool) -> None:
    assert classify(exc) is retryable


def test_transient_error_carries_provider_delay() -> None:
    """A 429 tells us when to come back; that overrides our backoff."""
    err = TransientError("rate limited", code="RATE_LIMITED", retry_after_s=30)
    assert err.retry_after_s == 30
    assert err.code == "RATE_LIMITED"
    assert err.retryable is True
