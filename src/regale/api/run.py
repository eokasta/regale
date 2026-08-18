import uuid

from regale.core.context import Context
from regale.core.executor import LocalExecutor
from regale.core.pipeline import Pipeline
from regale.core.registry import registry
from regale.core.retry import RetryPolicy


def run(
    pipeline_id: str,
    *,
    workers: int = 1,
    discover_packages: tuple[str, ...] = (),
    policy: RetryPolicy = RetryPolicy(),
    run_id: str | None = None,
) -> None:
    """Run every partition of a registered pipeline.

    workers=1 (the default) runs sequentially in this process — nothing
    to configure for a simple, single-machine ETL. workers>1 distributes
    partitions across worker processes on this machine (vertical scaling);
    see discover_packages on LocalExecutor for what that requires. For
    distribution across machines, see submit() instead.
    """
    entry = registry.get(pipeline_id)
    pipeline = Pipeline.from_registration(entry)
    ctx = Context(run_id=run_id if run_id is not None else uuid.uuid4().hex)
    partitions = pipeline.partition_params(ctx)
    executor = LocalExecutor(workers=workers, policy=policy, discover_packages=discover_packages)
    executor.run(pipeline_id, partitions)
