from dataclasses import dataclass
from typing import Any

from regale.core.context import Context
from regale.core.registry import PipelineRegistration
from regale.core.steps import LoadStep, PartitionStep, QueryStep, TransformStep


@dataclass(frozen=True, slots=True)
class Pipeline:
    """The assembled, ready-to-run form of a registered pipeline: transforms
    sorted by priority, loads in registration order.
    """

    pipeline_id: str
    partitions: PartitionStep | None
    query: QueryStep
    transforms: tuple[TransformStep, ...]
    loads: tuple[LoadStep, ...]

    @classmethod
    def from_registration(cls, entry: PipelineRegistration) -> "Pipeline":
        if entry.query is None:
            raise ValueError(f"pipeline {entry.pipeline_id!r} has no query step registered")
        if not entry.loads:
            raise ValueError(f"pipeline {entry.pipeline_id!r} has no load step registered")
        ordered = tuple(sorted(entry.transforms, key=lambda t: t.priority))
        return cls(
            pipeline_id=entry.pipeline_id,
            partitions=entry.partitions,
            query=entry.query,
            transforms=ordered,
            loads=tuple(entry.loads),
        )

    @property
    def requires_full_materialization(self) -> bool:
        """True if any transform needs the whole partition at once rather
        than one chunk at a time. Forces the entire partition to be read
        and held in memory before any transform runs.
        """
        return any(not t.chunked for t in self.transforms)

    def partition_params(self, ctx: Context) -> list[dict[str, Any]]:
        if self.partitions is None:
            return [{}]
        return list(self.partitions.func(ctx))
