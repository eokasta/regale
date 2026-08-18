from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class LoadMode(StrEnum):
    APPEND = "append"
    REPLACE_PARTITION = "replace_partition"
    UPSERT = "upsert"


@dataclass(frozen=True, slots=True)
class PartitionStep:
    pipeline_id: str
    func: Callable[[Any], Iterable[dict[str, Any]]]


@dataclass(frozen=True, slots=True)
class QueryStep:
    pipeline_id: str
    func: Callable[..., Any]
    source: str
    chunksize: int | None = None


@dataclass(frozen=True, slots=True)
class TransformStep:
    pipeline_id: str
    func: Callable[..., Any]
    priority: int
    chunked: bool = True
    frame: str = "pandas"


@dataclass(frozen=True, slots=True)
class LoadStep:
    pipeline_id: str
    func: Callable[..., Any]
    target: str
    table: str
    mode: LoadMode
    keys: tuple[str, ...] = ()
    commit_every: int | None = None
