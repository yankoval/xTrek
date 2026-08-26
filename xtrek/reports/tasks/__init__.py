"""Report about xTrek tasks received during one calendar day."""

from .data import TaskPeriod, TasksDataError, TasksReportData, collect, collect_range
from .document import REPORT_TITLE, build
from .source import S3TaskSource, TaskObjectRef, TaskSource

__all__ = [
    "REPORT_TITLE",
    "S3TaskSource",
    "TaskObjectRef",
    "TaskPeriod",
    "TaskSource",
    "TasksDataError",
    "TasksReportData",
    "build",
    "collect",
    "collect_range",
]
