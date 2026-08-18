from urllib.parse import urlsplit

from regale.drivers.base import Driver
from regale.drivers.generic import GenericDriver

_NATIVE_DRIVERS: dict[str, type[Driver]] = {}


def register_driver(scheme: str, driver_cls: type[Driver]) -> None:
    """Register a native driver for a URL scheme, e.g.
    register_driver("postgresql", PostgresDriver).
    """
    _NATIVE_DRIVERS[scheme] = driver_cls


def resolve_driver(url: str) -> Driver:
    """Pick the fastest Driver available for a connection url's dialect,
    falling back to GenericDriver when no native driver is registered.
    """
    scheme = urlsplit(url).scheme.split("+")[0]  # "postgresql+psycopg" -> "postgresql"
    driver_cls = _NATIVE_DRIVERS.get(scheme)
    if driver_cls is not None:
        return driver_cls()
    return GenericDriver()
