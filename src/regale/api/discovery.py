import importlib
import pkgutil
from importlib.metadata import entry_points

from regale.core.registry import registry

ENTRY_POINT_GROUP = "regale.pipelines"


def discover(*packages: str) -> None:
    """Import every module under the given packages, plus any module
    referenced by a 'regale.pipelines' entry point, so their @regale
    decorators run — then validate that every registered pipeline has a
    query and a load step.

    Failing here, at startup, is the point: a worker that discovers a
    broken pipeline should refuse to start, not fail partway through a run.
    """
    for package_name in packages:
        _import_package_tree(package_name)

    for entry_point in entry_points(group=ENTRY_POINT_GROUP):
        entry_point.load()

    registry.validate_all()


def _import_package_tree(package_name: str) -> None:
    package = importlib.import_module(package_name)
    if not hasattr(package, "__path__"):
        return  # a plain module, not a package — nothing more to walk

    prefix = package.__name__ + "."
    for _finder, name, _is_pkg in pkgutil.walk_packages(package.__path__, prefix):
        importlib.import_module(name)
