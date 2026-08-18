from typing import Any, Protocol


class Broker(Protocol):
    """A queue of small task descriptors (pipeline_id + partition params —
    a few hundred bytes), never the data itself. A homogeneous worker runs
    query→transform→load for one partition entirely in its own process, so
    nothing bigger than a task descriptor ever needs to cross this queue.
    """

    def publish(self, payload: dict[str, Any]) -> str:
        """Enqueue a task, returning its message id."""
        ...

    def consume(
        self, consumer: str, *, count: int, block_ms: int
    ) -> list[tuple[str, dict[str, Any]]]:
        """Read up to `count` new tasks, blocking up to block_ms if none
        are available yet.
        """
        ...

    def claim_stale(
        self, consumer: str, *, min_idle_ms: int, count: int
    ) -> list[tuple[str, dict[str, Any]]]:
        """Reclaim tasks left pending by a consumer that died before
        acking them, once they've been idle at least min_idle_ms.
        """
        ...

    def ack(self, message_id: str) -> None:
        """Mark a task as successfully handled."""
        ...

    def delivery_count(self, message_id: str) -> int:
        """How many times this task has been delivered (first delivery
        counts as 1), used to decide when to give up and dead-letter it.
        """
        ...
