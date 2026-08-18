import os
import socket

import typer

from regale.api.discovery import discover
from regale.api.run import run as _run_pipeline
from regale.api.submit import submit as _submit_pipeline
from regale.core.errors import RegaleError

app = typer.Typer(name="regale", help="Regale ETL framework CLI.", no_args_is_help=True)
dlq_app = typer.Typer(
    name="dlq", help="Inspect and manage dead-lettered tasks.", no_args_is_help=True
)
app.add_typer(dlq_app, name="dlq")

PIPELINES_OPTION = typer.Option(
    None,
    "--pipelines",
    envvar="REGALE_PIPELINES",
    help="Comma-separated packages to import via discover().",
)
BROKER_OPTION = typer.Option(..., "--broker", envvar="REGALE_BROKER", help="Redis URL.")


def _fail(message: str) -> None:
    typer.echo(message, err=True)
    raise typer.Exit(1)


def _packages(pipelines: str | None) -> tuple[str, ...]:
    if not pipelines:
        return ()
    return tuple(p.strip() for p in pipelines.split(",") if p.strip())


def _dead_letter_stream(stream: str) -> str:
    return f"{stream}:dead"


def _require_redis():
    try:
        from regale.distributed.deadletter import DeadLetterQueue
        from regale.distributed.redis_broker import RedisBroker
        from regale.distributed.worker import Worker
    except ImportError:
        _fail("This command requires the 'redis' extra: pip install 'regale[redis]'")
    return RedisBroker, DeadLetterQueue, Worker


@app.command()
def run(
    pipeline_id: str,
    workers: int = typer.Option(1, help="Number of local worker processes."),
    pipelines: str | None = PIPELINES_OPTION,
) -> None:
    """Run a pipeline's partitions locally — sequentially, or across local worker processes."""
    packages = _packages(pipelines)
    try:
        if packages:
            discover(*packages)
        _run_pipeline(pipeline_id, workers=workers, discover_packages=packages)
    except RegaleError as exc:
        _fail(str(exc))


@app.command()
def submit(
    pipeline_id: str,
    broker: str = BROKER_OPTION,
    stream: str | None = typer.Option(
        None, help="Stream name; defaults to regale:tasks:<pipeline_id>."
    ),
    group: str = typer.Option("regale", help="Consumer group name."),
    pipelines: str | None = PIPELINES_OPTION,
) -> None:
    """Publish a pipeline's partitions to a Redis stream for distributed workers to consume."""
    packages = _packages(pipelines)
    try:
        if packages:
            discover(*packages)
        count = _submit_pipeline(pipeline_id, broker=broker, stream=stream, group=group)
    except RegaleError as exc:
        _fail(str(exc))
    resolved_stream = stream or f"regale:tasks:{pipeline_id}"
    typer.echo(f"published {count} partition(s) to stream {resolved_stream!r}")


@app.command()
def worker(
    stream: str = typer.Option(..., help="Stream name to consume from."),
    broker: str = BROKER_OPTION,
    group: str = typer.Option("regale", help="Consumer group name."),
    pipelines: str | None = PIPELINES_OPTION,
    consumer_name: str | None = typer.Option(
        None, help="Defaults to '<hostname>-<pid>' if not given."
    ),
    max_deliveries: int = typer.Option(3, help="Deliveries before a task is dead-lettered."),
    block_ms: int = typer.Option(5000, help="How long to block waiting for a new task."),
    max_iterations: int | None = typer.Option(
        None, help="Stop after this many poll iterations; omit to run forever."
    ),
) -> None:
    """Run a long-lived worker consuming partition tasks from a Redis stream."""
    RedisBroker, DeadLetterQueue, Worker = _require_redis()
    packages = _packages(pipelines)
    consumer = consumer_name or f"{socket.gethostname()}-{os.getpid()}"
    redis_broker = RedisBroker.from_url(broker, stream=stream, group=group)
    dlq = DeadLetterQueue.from_url(broker, stream=_dead_letter_stream(stream))
    instance = Worker(
        broker=redis_broker,
        dlq=dlq,
        consumer_name=consumer,
        discover_packages=packages,
        max_deliveries=max_deliveries,
    )
    try:
        instance.run_forever(block_ms=block_ms, max_iterations=max_iterations)
    except RegaleError as exc:
        _fail(str(exc))


@dlq_app.command("list")
def dlq_list(
    stream: str = typer.Option(..., help="The live task stream's name (not the :dead stream)."),
    broker: str = BROKER_OPTION,
    count: int = typer.Option(100, help="Maximum entries to show."),
) -> None:
    """List dead-lettered tasks, with their error and delivery count."""
    _RedisBroker, DeadLetterQueue, _Worker = _require_redis()
    dlq = DeadLetterQueue.from_url(broker, stream=_dead_letter_stream(stream))
    for entry in dlq.list(count=count):
        typer.echo(
            f"{entry['id']}  attempts={entry['attempts']}  "
            f"error={entry['error']!r}  task={entry['task']}"
        )


@dlq_app.command("retry")
def dlq_retry(
    message_id: str,
    stream: str = typer.Option(..., help="The live task stream's name (not the :dead stream)."),
    broker: str = BROKER_OPTION,
    group: str = typer.Option("regale", help="Consumer group name."),
) -> None:
    """Requeue a dead-lettered task onto the live stream."""
    RedisBroker, DeadLetterQueue, _Worker = _require_redis()
    redis_broker = RedisBroker.from_url(broker, stream=stream, group=group)
    dlq = DeadLetterQueue.from_url(broker, stream=_dead_letter_stream(stream))
    try:
        dlq.retry(message_id, redis_broker)
    except RegaleError as exc:
        _fail(str(exc))
    typer.echo(f"requeued {message_id}")


@dlq_app.command("purge")
def dlq_purge(
    older_than: float = typer.Option(..., help="Seconds; entries older than this are removed."),
    stream: str = typer.Option(..., help="The live task stream's name (not the :dead stream)."),
    broker: str = BROKER_OPTION,
) -> None:
    """Delete dead-lettered entries older than a cutoff."""
    _RedisBroker, DeadLetterQueue, _Worker = _require_redis()
    dlq = DeadLetterQueue.from_url(broker, stream=_dead_letter_stream(stream))
    removed = dlq.purge(older_than_seconds=older_than)
    typer.echo(f"purged {removed} entr{'y' if removed == 1 else 'ies'}")


if __name__ == "__main__":
    app()
