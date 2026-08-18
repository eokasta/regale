from enum import StrEnum


class RegaleError(Exception):
    """Base class for all errors raised by Regale."""


class RegistrationError(RegaleError):
    """Raised when a pipeline is registered in an invalid or ambiguous way."""


class ErrorClass(StrEnum):
    """How a Driver classifies a runtime exception, so the retry layer knows
    whether trying again could possibly help.
    """

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    AMBIGUOUS = "ambiguous"
