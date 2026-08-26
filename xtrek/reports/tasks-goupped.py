"""Compatibility entry point for the requested ``tasks-goupped`` report id."""

from .tasks_goupped.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
