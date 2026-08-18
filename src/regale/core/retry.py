import random
import time
from collections.abc import Callable
from dataclasses import dataclass

from regale.core.errors import ErrorClass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How many times to retry, and how long to wait between attempts,
    depending on how a failure was classified.

    Permanent failures never retry — trying again can't fix bad SQL or a
    constraint violation, and retrying only delays surfacing the real
    problem. Ambiguous failures get fewer attempts than transient ones,
    since we can't be sure retrying helps.
    """

    max_attempts_transient: int = 4
    max_attempts_ambiguous: int = 2
    base_delay_seconds: float = 1.0
    backoff_factor: float = 4.0
    jitter_seconds: float = 0.5

    def max_attempts(self, error_class: ErrorClass) -> int:
        if error_class is ErrorClass.TRANSIENT:
            return self.max_attempts_transient
        if error_class is ErrorClass.AMBIGUOUS:
            return self.max_attempts_ambiguous
        return 1  # PERMANENT

    def delay_seconds(self, attempt: int) -> float:
        """The delay before the next try, after the given (1-indexed) attempt."""
        backoff = self.base_delay_seconds * (self.backoff_factor ** (attempt - 1))
        return backoff + random.uniform(0, self.jitter_seconds)


def run_with_retry(
    func: Callable[[], None],
    *,
    classify: Callable[[Exception], ErrorClass],
    policy: RetryPolicy = RetryPolicy(),
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Run func(), retrying in-process on transient/ambiguous failures with
    exponential backoff and jitter.

    This is the worker-local retry layer: it never touches a queue. A
    crashed worker (as opposed to one that merely raised) is instead
    handled by a distributed broker's own at-least-once delivery — a later
    step — and an exhausted retry here still propagates to the caller,
    which decides whether that means dead-lettering the task.
    """
    attempt = 1
    while True:
        try:
            func()
            return
        except Exception as exc:
            error_class = classify(exc)
            if attempt >= policy.max_attempts(error_class):
                raise
            sleep(policy.delay_seconds(attempt))
            attempt += 1
