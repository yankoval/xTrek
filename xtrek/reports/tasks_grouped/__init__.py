"""Daily xTrek tasks report grouped by article."""

from ..tasks.source import S3TaskSource, TaskObjectRef, TaskSource
from .data import (
    ArticleTasksData,
    TaskFileData,
    TasksGroupedReportData,
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
    "TasksGroupedReportData",
    "build",
    "collect",
    "collect_range",
]
