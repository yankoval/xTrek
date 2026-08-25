class ReportsError(Exception):
    """Base exception for the reports package."""


class OptionalDependencyError(ReportsError):
    """Raised when a requested renderer needs an optional dependency."""
