"""Daily xTrek tasks report grouped by article.

The package name intentionally keeps the requested report identifier
``tasks-goupped`` (with ``_`` instead of ``-`` for Python module syntax).
"""

from ..tasks.source import S3TaskSource, TaskObjectRef, TaskSource
from .data import (
    ArticleTasksData,
    TaskFileData,
    TasksGouppedReportData,
    collect,
    collect_range,
)
from .document import REPORT_TITLE, build

__all__ = [
    "ArticleTasksData",
    "REPORT_TITLE",
    "S3TaskSource",
    "TaskObjectRef",
    "TaskFileData",
    "TaskSource",
    "TasksGouppedReportData",
    "build",
    "collect",
    "collect_range",
]
