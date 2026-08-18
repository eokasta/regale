from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Protocol

from regale.api.discovery import discover
from regale.core.errors import RegaleError
from regale.core.retry import RetryPolicy
from regale.core.runner import run_partition_with_retry


class Executor(Protocol):
    def run(self, pipeline_id: str, partitions: Sequence[dict[str, Any]]) -> None: ...


@dataclass
class LocalExecutor:
    """Runs a pipeline's partitions in this process (workers=1, the
    default) or across worker processes on this machine (workers>1) — the
    vertical-scaling half of Regale's execution model.

    workers>1 uses ProcessPoolExecutor. Python's multiprocessing spawns
    fresh processes rather than forking (always on Windows, and by default
    on macOS), so each worker process starts with an EMPTY pipeline
    registry — it does not inherit the parent's in-memory @regale
    decorators. discover_packages tells every worker process which
    packages to import via discover() before it processes its first
    partition — the same requirement a distributed worker has, for the
    same reason.
    """

    workers: int = 1
    policy: RetryPolicy = field(default_factory=RetryPolicy)
    discover_packages: tuple[str, ...] = ()

    def run(self, pipeline_id: str, partitions: Sequence[dict[str, Any]]) -> None:
        if self.workers <= 1:
            for params in partitions:
                run_partition_with_retry(pipeline_id, params, policy=self.policy)
            return

        if not self.discover_packages:
            raise RegaleError(
                "LocalExecutor(workers>1) spawns separate processes, which start with "
                "an empty pipeline registry — pass discover_packages=(...) so each "
                "worker process can rebuild it via regale.discover() before running "
                "a partition"
            )

        with ProcessPoolExecutor(
            max_workers=self.workers,
            initializer=_init_worker,
            initargs=(self.discover_packages,),
        ) as pool:
            futures = [
                pool.submit(run_partition_with_retry, pipeline_id, params, policy=self.policy)
                for params in partitions
            ]
            for future in futures:
                future.result()  # re-raises the first partition failure, if any


def _init_worker(packages: tuple[str, ...]) -> None:
    discover(*packages)
