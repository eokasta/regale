from dataclasses import dataclass

from regale.api.config import configure
from regale.sources.base import SQLSource
from regale.targets.base import SQLTarget


@dataclass(frozen=True, slots=True)
class Context:
    """Passed to a @regale.partitions generator, giving it access to
    configured sources/targets by name — e.g. to SELECT DISTINCT a list of
    store ids to partition by.
    """

    run_id: str

    def source(self, name: str) -> SQLSource:
        return configure.source(name)

    def target(self, name: str) -> SQLTarget:
        return configure.target(name)
