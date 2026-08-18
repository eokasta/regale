import json
import time
from typing import Any

import redis

from regale.core.errors import RegaleError
from regale.distributed.redis_broker import RedisBroker


class DeadLetterQueue:
    """Tasks that exhausted their delivery budget or hit a permanent
    failure. Kept as its own stream, separate from the live task stream,
    so listing/retrying/purging dead tasks never has to filter live ones
    out of the same stream.
    """

    def __init__(self, client: redis.Redis, *, stream: str) -> None:
        self._client = client
        self.stream = stream

    @classmethod
    def from_url(cls, url: str, *, stream: str) -> "DeadLetterQueue":
        client = redis.Redis.from_url(url, decode_responses=True)
        return cls(client, stream=stream)

    def add(self, task: dict[str, Any], *, error: str, attempts: int) -> str:
        return self._client.xadd(
            self.stream,
            {"task": json.dumps(task), "error": error, "attempts": str(attempts)},
        )

    def list(self, count: int = 100) -> list[dict[str, Any]]:
        entries = self._client.xrange(self.stream, count=count)
        return [_decode(message_id, fields) for message_id, fields in entries]

    def retry(self, message_id: str, broker: RedisBroker) -> None:
        entries = self._client.xrange(self.stream, min=message_id, max=message_id)
        if not entries:
            raise RegaleError(f"no dead-lettered task with id {message_id!r}")
        _id, fields = entries[0]
        broker.publish(json.loads(fields["task"]))
        self._client.xdel(self.stream, message_id)

    def purge(self, older_than_seconds: float) -> int:
        before = self._client.xlen(self.stream)
        cutoff_ms = int(time.time() * 1000) - int(older_than_seconds * 1000)
        self._client.xtrim(self.stream, minid=f"{cutoff_ms}-0")
        after = self._client.xlen(self.stream)
        return before - after


def _decode(message_id: str, fields: dict[str, str]) -> dict[str, Any]:
    return {
        "id": message_id,
        "task": json.loads(fields["task"]),
        "error": fields["error"],
        "attempts": int(fields["attempts"]),
    }
