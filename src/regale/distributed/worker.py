import logging
from dataclasses import dataclass, field
from typing import Any

from regale.api.config import configure
from regale.api.discovery import discover
from regale.core.errors import ErrorClass
from regale.core.pipeline import Pipeline
from regale.core.registry import registry
from regale.core.retry import RetryPolicy
from regale.core.runner import run_partition_with_retry
from regale.distributed.broker import Broker
from regale.distributed.deadletter import DeadLetterQueue
from regale.drivers.registry import resolve_driver

logger = logging.getLogger("regale.worker")


@dataclass
class Worker:
    """Consumes partition tasks from a Broker and executes them via
    run_partition_with_retry — the in-process retry layer. This layer
    handles a worker that dies mid-task: Redis's at-least-once delivery
    (claim_stale) redelivers it to another consumer, and delivery_count is
    what decides when to stop redelivering and dead-letter it instead.

    discover() and connection validation run once at start(), so a
    typo'd pipeline or connection name fails immediately rather than
    partway into the first task.
    """

    broker: Broker
    dlq: DeadLetterQueue
    consumer_name: str
    discover_packages: tuple[str, ...] = ()
    policy: RetryPolicy = field(default_factory=RetryPolicy)
    max_deliveries: int = 3
    claim_min_idle_ms: int = 5 * 60 * 1000  # 5 minutes

    def start(self) -> None:
        discover(*self.discover_packages)
        configure.validate_pipeline_connections()

    def run_forever(self, *, block_ms: int = 5000, max_iterations: int | None = None) -> None:
        self.start()
        iterations = 0
        while max_iterations is None or iterations < max_iterations:
            self.drain_once(block_ms=block_ms)
            iterations += 1

    def drain_once(self, *, block_ms: int = 5000) -> None:
        """Reclaim any stale tasks left by a dead consumer, then consume
        new ones. Exposed separately from run_forever so tests (and a
        manually-driven worker loop) can step it deterministically.
        """
        for message_id, task in self.broker.claim_stale(
            self.consumer_name, min_idle_ms=self.claim_min_idle_ms
        ):
            self._handle(message_id, task)
        for message_id, task in self.broker.consume(self.consumer_name, block_ms=block_ms):
            self._handle(message_id, task)

    def _handle(self, message_id: str, task: dict[str, Any]) -> None:
        pipeline_id = task["pipeline_id"]
        params = task["params"]
        try:
            run_partition_with_retry(pipeline_id, params, policy=self.policy)
        except Exception as exc:
            self._handle_failure(message_id, task, exc)
            return
        self.broker.ack(message_id)

    def _handle_failure(self, message_id: str, task: dict[str, Any], exc: Exception) -> None:
        error_class = self._classify(task["pipeline_id"], exc)
        delivered = self.broker.delivery_count(message_id)
        if error_class is ErrorClass.PERMANENT or delivered >= self.max_deliveries:
            logger.error("dead-lettering task %s after %d deliveries: %s", task, delivered, exc)
            self.dlq.add(task, error=str(exc), attempts=delivered)
            self.broker.ack(message_id)
        else:
            logger.warning(
                "leaving task %s pending for redelivery (%d/%d attempts): %s",
                task,
                delivered,
                self.max_deliveries,
                exc,
            )

    def _classify(self, pipeline_id: str, exc: Exception) -> ErrorClass:
        entry = registry.get(pipeline_id)
        pipeline = Pipeline.from_registration(entry)
        driver = resolve_driver(configure.source(pipeline.query.source).url)
        return driver.classify(exc)
