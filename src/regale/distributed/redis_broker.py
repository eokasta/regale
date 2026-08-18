import json
from typing import Any

import redis


class RedisBroker:
    """Redis Streams-backed Broker. One stream, one consumer group; workers
    are consumers within that group so each task goes to exactly one
    worker at a time, with Redis tracking delivery via its PEL (Pending
    Entries List) for at-least-once delivery and crash recovery.
    """

    def __init__(self, client: redis.Redis, *, stream: str, group: str) -> None:
        self._client = client
        self.stream = stream
        self.group = group
        self._ensure_group()

    @classmethod
    def from_url(cls, url: str, *, stream: str, group: str = "regale") -> "RedisBroker":
        client = redis.Redis.from_url(url, decode_responses=True)
        return cls(client, stream=stream, group=group)

    def _ensure_group(self) -> None:
        try:
            self._client.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def publish(self, payload: dict[str, Any]) -> str:
        return self._client.xadd(self.stream, {"data": json.dumps(payload)})

    def consume(
        self, consumer: str, *, count: int = 1, block_ms: int = 5000
    ) -> list[tuple[str, dict[str, Any]]]:
        result = self._client.xreadgroup(
            self.group, consumer, {self.stream: ">"}, count=count, block=block_ms
        )
        if not result:
            return []
        entries = []
        for _stream_name, messages in result:
            entries.extend(_decode(message_id, fields) for message_id, fields in messages)
        return entries

    def claim_stale(
        self, consumer: str, *, min_idle_ms: int, count: int = 10
    ) -> list[tuple[str, dict[str, Any]]]:
        _cursor, messages, _deleted = self._client.xautoclaim(
            self.stream, self.group, consumer, min_idle_ms, start_id="0-0", count=count
        )
        return [_decode(message_id, fields) for message_id, fields in messages]

    def ack(self, message_id: str) -> None:
        self._client.xack(self.stream, self.group, message_id)

    def delivery_count(self, message_id: str) -> int:
        entries = self._client.xpending_range(
            self.stream, self.group, min=message_id, max=message_id, count=1
        )
        if not entries:
            return 0
        return entries[0]["times_delivered"]


def _decode(message_id: str, fields: dict[str, str]) -> tuple[str, dict[str, Any]]:
    return message_id, json.loads(fields["data"])
