"""Backward-compatible module entry point for the misspelled package name."""

from ..tasks_grouped.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
