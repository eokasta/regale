import pytest

from regale.core.errors import ErrorClass
from regale.core.retry import RetryPolicy, run_with_retry


def test_run_with_retry_succeeds_on_first_try():
    calls = {"n": 0}

    def func():
        calls["n"] += 1

    sleeps = []
    run_with_retry(func, classify=lambda exc: ErrorClass.TRANSIENT, sleep=sleeps.append)

    assert calls["n"] == 1
    assert sleeps == []


def test_run_with_retry_retries_transient_failures_then_succeeds():
    calls = {"n": 0}

    def func():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")

    sleeps = []
    run_with_retry(func, classify=lambda exc: ErrorClass.TRANSIENT, sleep=sleeps.append)

    assert calls["n"] == 3
    assert len(sleeps) == 2


def test_run_with_retry_raises_after_exhausting_transient_attempts():
    policy = RetryPolicy(max_attempts_transient=2)
    calls = {"n": 0}

    def func():
        calls["n"] += 1
        raise RuntimeError("always fails")

    with pytest.raises(RuntimeError, match="always fails"):
        run_with_retry(
            func, classify=lambda exc: ErrorClass.TRANSIENT, policy=policy, sleep=lambda s: None
        )

    assert calls["n"] == 2


def test_run_with_retry_never_retries_permanent_failures():
    calls = {"n": 0}

    def func():
        calls["n"] += 1
        raise ValueError("bad sql")

    with pytest.raises(ValueError, match="bad sql"):
        run_with_retry(func, classify=lambda exc: ErrorClass.PERMANENT, sleep=lambda s: None)

    assert calls["n"] == 1


def test_run_with_retry_uses_fewer_attempts_for_ambiguous():
    policy = RetryPolicy(max_attempts_ambiguous=2, max_attempts_transient=5)
    calls = {"n": 0}

    def func():
        calls["n"] += 1
        raise RuntimeError("maybe transient")

    with pytest.raises(RuntimeError):
        run_with_retry(
            func, classify=lambda exc: ErrorClass.AMBIGUOUS, policy=policy, sleep=lambda s: None
        )

    assert calls["n"] == 2


def test_max_attempts_dispatches_by_error_class():
    policy = RetryPolicy(max_attempts_transient=4, max_attempts_ambiguous=2)
    assert policy.max_attempts(ErrorClass.TRANSIENT) == 4
    assert policy.max_attempts(ErrorClass.AMBIGUOUS) == 2
    assert policy.max_attempts(ErrorClass.PERMANENT) == 1


def test_delay_seconds_grows_exponentially_and_respects_jitter_bounds():
    policy = RetryPolicy(base_delay_seconds=1.0, backoff_factor=4.0, jitter_seconds=0.5)

    for attempt in (1, 2, 3):
        backoff = 1.0 * (4.0 ** (attempt - 1))
        delay = policy.delay_seconds(attempt)
        assert backoff <= delay <= backoff + 0.5
