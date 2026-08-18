from collections.abc import Callable, Iterable
from typing import Any, TypeVar

from regale.core.errors import RegistrationError
from regale.core.registry import registry
from regale.core.steps import LoadMode, LoadStep, PartitionStep, QueryStep, TransformStep

F = TypeVar("F", bound=Callable[..., Any])


def partitions(pipeline_id: str) -> Callable[[F], F]:
    """Register a generator of partition parameter dicts for a pipeline."""

    def decorator(func: F) -> F:
        registry.add_partitions(PartitionStep(pipeline_id=pipeline_id, func=func))
        return func

    return decorator


def query(pipeline_id: str, *, source: str, chunksize: int | None = None) -> Callable[[F], F]:
    """Register the extraction step of a pipeline. Exactly one per pipeline."""

    def decorator(func: F) -> F:
        registry.add_query(
            QueryStep(pipeline_id=pipeline_id, func=func, source=source, chunksize=chunksize)
        )
        return func

    return decorator


def transform(
    pipeline_id: str,
    *,
    priority: int = 0,
    chunked: bool = True,
    frame: str = "pandas",
) -> Callable[[F], F]:
    """Register a transform step. Transforms run in ascending order of priority."""
    if frame not in ("pandas", "arrow"):
        raise RegistrationError(f"transform frame must be 'pandas' or 'arrow', got {frame!r}")

    def decorator(func: F) -> F:
        registry.add_transform(
            TransformStep(
                pipeline_id=pipeline_id,
                func=func,
                priority=priority,
                chunked=chunked,
                frame=frame,
            )
        )
        return func

    return decorator


def load(
    pipeline_id: str,
    *,
    target: str,
    table: str,
    mode: str,
    keys: Iterable[str] = (),
    partition_keys: Iterable[str] = (),
    commit_every: int | None = None,
) -> Callable[[F], F]:
    """Register the load step of a pipeline.

    mode="upsert" requires keys, since retry safety depends on knowing which
    columns identify a row for conflict resolution. mode="replace_partition"
    requires partition_keys: the destination columns, matched by name
    against the partition's own params, that identify which rows to delete
    before inserting. Only equality predicates are supported in v1 — a
    range partition (e.g. inicio/fim date bounds) needs a destination column
    that holds the exact partition value (such as "ano"), not the bounds
    themselves. mode="append" combined with commit_every is rejected: a
    worker crash mid-partition would leave already-committed chunks in
    place, and a retry would duplicate them.
    """
    try:
        load_mode = LoadMode(mode)
    except ValueError as exc:
        valid = ", ".join(m.value for m in LoadMode)
        raise RegistrationError(f"load mode must be one of {valid}, got {mode!r}") from exc

    resolved_keys = tuple(keys)
    resolved_partition_keys = tuple(partition_keys)
    if load_mode is LoadMode.UPSERT and not resolved_keys:
        raise RegistrationError(
            f"pipeline {pipeline_id!r}: mode='upsert' requires keys=[...] to know "
            "which columns identify a row for conflict resolution"
        )
    if load_mode is LoadMode.REPLACE_PARTITION and not resolved_partition_keys:
        raise RegistrationError(
            f"pipeline {pipeline_id!r}: mode='replace_partition' requires "
            "partition_keys=[...] — the destination columns matched against "
            "the partition's own params that identify which rows to delete "
            "before inserting"
        )
    if load_mode is LoadMode.APPEND and commit_every is not None:
        raise RegistrationError(
            f"pipeline {pipeline_id!r}: mode='append' with commit_every is not allowed — "
            "a worker crash mid-partition would duplicate already-committed chunks on retry. "
            "Use mode='upsert' or mode='replace_partition' if you need commit_every."
        )

    def decorator(func: F) -> F:
        registry.add_load(
            LoadStep(
                pipeline_id=pipeline_id,
                func=func,
                target=target,
                table=table,
                mode=load_mode,
                keys=resolved_keys,
                partition_keys=resolved_partition_keys,
                commit_every=commit_every,
            )
        )
        return func

    return decorator
