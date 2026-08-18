class RegaleError(Exception):
    """Base class for all errors raised by Regale."""


class RegistrationError(RegaleError):
    """Raised when a pipeline is registered in an invalid or ambiguous way."""
