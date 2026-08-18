import uuid

from regale.core.context import Context
from regale.core.errors import RegaleError
from regale.core.pipeline import Pipeline
from regale.core.registry import registry


def submit(
    pipeline_id: str,
    *,
    broker: str,
    stream: str | None = None,
    group: str = "regale",
    run_id: str | None = None,
) -> int:
    """Enumerate a pipeline's partitions and publish one task per
    partition to a Redis stream, for one or more `regale worker` processes
    (possibly on other machines) to consume. Returns the number of
    partitions published.

    This is the horizontal-scaling counterpart to run(): the same
    pipeline module works with either, unchanged.
    """
    try:
        from regale.distributed.redis_broker import RedisBroker
    except ImportError as exc:
        raise RegaleError(
            "submit() requires the 'redis' extra: pip install 'regale[redis]'"
        ) from exc

    entry = registry.get(pipeline_id)
    pipeline = Pipeline.from_registration(entry)
    ctx = Context(run_id=run_id if run_id is not None else uuid.uuid4().hex)
    partitions = pipeline.partition_params(ctx)

    redis_broker = RedisBroker.from_url(
        broker, stream=stream or f"regale:tasks:{pipeline_id}", group=group
    )
    for params in partitions:
        redis_broker.publish({"pipeline_id": pipeline_id, "params": params})
    return len(partitions)
