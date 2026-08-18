from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import Any

import pandas as pd
from sqlalchemy.engine import Connection

from regale.api.config import configure
from regale.core.pipeline import Pipeline
from regale.core.registry import registry
from regale.core.steps import LoadStep
from regale.drivers.base import Driver
from regale.drivers.registry import resolve_driver


def run_partition(pipeline_id: str, params: dict[str, Any]) -> None:
    """Execute one partition of a pipeline end to end — extract, transform,
    load — entirely within this process. Each load step commits as a single
    transaction spanning every chunk of the partition, so a crash mid-run
    rolls back cleanly and a retry can always reprocess the partition from
    scratch without duplicating data (mode permitting).

    Atomicity is per load step's target engine, not across load steps: two
    loads to two different destinations cannot share one commit without a
    distributed transaction coordinator, which is out of scope for v1.
    """
    entry = registry.get(pipeline_id)
    pipeline = Pipeline.from_registration(entry)
    _run(pipeline, params)


def _run(pipeline: Pipeline, params: dict[str, Any]) -> None:
    source = configure.source(pipeline.query.source)
    driver = resolve_driver(source.url)
    sql = pipeline.query.func(params)
    chunks = driver.read_batches(source.engine(), sql, params, pipeline.query.chunksize)

    if pipeline.requires_full_materialization:
        _run_materialized(pipeline, params, chunks)
    else:
        _run_streaming(pipeline, params, chunks)


def _apply_transforms(pipeline: Pipeline, df: pd.DataFrame) -> pd.DataFrame:
    for step in pipeline.transforms:
        df = step.func(df)
    return df


def _run_streaming(
    pipeline: Pipeline, params: dict[str, Any], chunks: Iterator[pd.DataFrame]
) -> None:
    with _open_load_transactions(pipeline) as txns:
        for chunk in chunks:
            transformed = _apply_transforms(pipeline, chunk)
            for load_step, txn in zip(pipeline.loads, txns, strict=True):
                to_write = load_step.func(transformed, params)
                schema = configure.target(load_step.target).schema
                txn.write(load_step.table, to_write, partition=params, schema=schema)


def _run_materialized(
    pipeline: Pipeline, params: dict[str, Any], chunks: Iterator[pd.DataFrame]
) -> None:
    # The generic driver always yields at least one DataFrame per read, even
    # when zero rows match, so this guard is defensive rather than the
    # common case — but a native driver could conceivably yield nothing.
    frames = list(chunks)
    if not frames:
        return
    full = pd.concat(frames, ignore_index=True)
    transformed = _apply_transforms(pipeline, full)
    with _open_load_transactions(pipeline) as txns:
        for load_step, txn in zip(pipeline.loads, txns, strict=True):
            to_write = load_step.func(transformed, params)
            schema = configure.target(load_step.target).schema
            txn.write(load_step.table, to_write, partition=params, schema=schema)


class _LoadTransaction:
    """One load step's connection/transaction for the life of a partition.

    Chunks accumulate in the current transaction. If commit_every is set,
    a fresh transaction starts automatically once that many chunks have
    been written, trading whole-partition atomicity for a bound on how
    much undo a single transaction has to hold. (mode="append" combined
    with commit_every is rejected at registration time precisely because
    this makes a crash mid-partition partially durable.)
    """

    def __init__(self, load_step: LoadStep, driver: Driver, connection: Connection) -> None:
        self.load_step = load_step
        self.driver = driver
        self.connection = connection
        self._transaction = connection.begin()
        self._chunks_since_commit = 0

    def write(
        self,
        table: str,
        df: pd.DataFrame,
        *,
        partition: dict[str, Any],
        schema: str | None,
    ) -> None:
        self.driver.write_chunk(
            self.connection,
            table,
            df,
            mode=self.load_step.mode,
            keys=self.load_step.keys,
            partition_keys=self.load_step.partition_keys,
            partition=partition,
            schema=schema,
        )
        self._chunks_since_commit += 1
        if (
            self.load_step.commit_every is not None
            and self._chunks_since_commit >= self.load_step.commit_every
        ):
            self._transaction.commit()
            self._transaction = self.connection.begin()
            self._chunks_since_commit = 0

    def finish(self) -> None:
        self._transaction.commit()

    def abort(self) -> None:
        self._transaction.rollback()


@contextmanager
def _open_load_transactions(pipeline: Pipeline) -> Iterator[list[_LoadTransaction]]:
    with ExitStack() as stack:
        txns = [
            _LoadTransaction(
                load_step,
                resolve_driver(configure.target(load_step.target).url),
                stack.enter_context(configure.target(load_step.target).engine().connect()),
            )
            for load_step in pipeline.loads
        ]
        try:
            yield txns
        except BaseException:
            for txn in txns:
                txn.abort()
            raise
        else:
            for txn in txns:
                txn.finish()
