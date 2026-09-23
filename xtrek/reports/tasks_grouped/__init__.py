"""xTrek tasks report with zero to two ordered article/operator groupings."""

from ..tasks.source import S3TaskSource, TaskObjectRef, TaskSource
from .data import (
    ArticleTasksData,
    OperatorTasksData,
    TaskFileData,
    TasksGroupedReportData,
    collect,
    collect_range,
)
from .document import REPORT_TITLE, build

__all__ = [
    "ArticleTasksData",
    "OperatorTasksData",
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
