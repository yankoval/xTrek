"""Compatibility entry point for existing Celery deployments."""

import sys

from xtrek import tasks as _tasks

sys.modules[__name__] = _tasks
