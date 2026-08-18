from dataclasses import dataclass, field

from regale.core.errors import RegistrationError
from regale.core.steps import LoadStep, PartitionStep, QueryStep, TransformStep


@dataclass
class PipelineRegistration:
    pipeline_id: str
    partitions: PartitionStep | None = None
    query: QueryStep | None = None
    transforms: list[TransformStep] = field(default_factory=list)
    loads: list[LoadStep] = field(default_factory=list)


class Registry:
    """Process-wide registry of pipeline steps, populated as @regale decorators run on import."""

    def __init__(self) -> None:
        self._pipelines: dict[str, PipelineRegistration] = {}

    def _get_or_create(self, pipeline_id: str) -> PipelineRegistration:
        return self._pipelines.setdefault(pipeline_id, PipelineRegistration(pipeline_id))

    def add_partitions(self, step: PartitionStep) -> None:
        entry = self._get_or_create(step.pipeline_id)
        if entry.partitions is not None:
            raise RegistrationError(
                f"pipeline {step.pipeline_id!r} already has a @partitions function "
                f"({entry.partitions.func.__qualname__}); only one is allowed"
            )
        entry.partitions = step

    def add_query(self, step: QueryStep) -> None:
        entry = self._get_or_create(step.pipeline_id)
        if entry.query is not None:
            raise RegistrationError(
                f"pipeline {step.pipeline_id!r} already has a @query function "
                f"({entry.query.func.__qualname__}); only one query per pipeline is allowed"
            )
        entry.query = step

    def add_transform(self, step: TransformStep) -> None:
        entry = self._get_or_create(step.pipeline_id)
        if any(t.priority == step.priority for t in entry.transforms):
            raise RegistrationError(
                f"pipeline {step.pipeline_id!r} already has a @transform with "
                f"priority={step.priority}; priorities must be unique within a pipeline"
            )
        entry.transforms.append(step)

    def add_load(self, step: LoadStep) -> None:
        entry = self._get_or_create(step.pipeline_id)
        entry.loads.append(step)

    def get(self, pipeline_id: str) -> PipelineRegistration:
        try:
            return self._pipelines[pipeline_id]
        except KeyError:
            raise RegistrationError(f"no pipeline registered under id {pipeline_id!r}") from None

    def pipeline_ids(self) -> list[str]:
        return list(self._pipelines)

    def validate(self, pipeline_id: str) -> None:
        entry = self.get(pipeline_id)
        if entry.query is None:
            raise RegistrationError(f"pipeline {pipeline_id!r} has no @query function registered")
        if not entry.loads:
            raise RegistrationError(f"pipeline {pipeline_id!r} has no @load function registered")

    def validate_all(self) -> None:
        for pipeline_id in self.pipeline_ids():
            self.validate(pipeline_id)

    def clear(self) -> None:
        self._pipelines.clear()


registry = Registry()
