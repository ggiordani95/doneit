"""Error taxonomy.

The retry machinery only needs one bit of information: may this be retried?
Everything else is detail carried for logging and the DLQ record.
"""

from __future__ import annotations

__all__ = ["PermanentError", "SwarmError", "TransientError", "classify"]


class SwarmError(Exception):
    """Base class for errors raised inside the swarm."""

    code: str = "SWARM_ERROR"
    retryable: bool = False

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code


class TransientError(SwarmError):
    """Failure that may succeed on a later attempt.

    Provider timeouts, 429s, 5xx responses, broker hiccups, non-fast-forward
    pushes.
    """

    code = "TRANSIENT_ERROR"
    retryable = True

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        retry_after_s: float | None = None,
    ) -> None:
        super().__init__(message, code=code)
        self.retry_after_s = retry_after_s
        """Provider-supplied delay that overrides the computed backoff."""


class PermanentError(SwarmError):
    """Failure that will fail the same way forever.

    Malformed input, unknown agent type, 401/403/404, invalid workflow state.
    """

    code = "PERMANENT_ERROR"
    retryable = False


def classify(exc: BaseException) -> bool:
    """Return True when ``exc`` should be retried.

    Unknown exceptions are treated as transient: a task that is genuinely
    broken will exhaust its attempts and land in the DLQ, which is safer than
    discarding work because of an unrecognised error type.
    """
    if isinstance(exc, SwarmError):
        return exc.retryable
    if isinstance(exc, TimeoutError | ConnectionError | OSError):
        return True
    return not isinstance(exc, ValueError | TypeError | KeyError | NotImplementedError)
