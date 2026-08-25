"""Report about xTrek tasks received during one calendar day."""

from .data import TasksDataError, TasksReportData, collect
from .document import REPORT_TITLE, build
from .source import S3TaskSource, TaskObjectRef, TaskSource

__all__ = [
    "REPORT_TITLE",
    "S3TaskSource",
    "TaskObjectRef",
    "TaskSource",
    "TasksDataError",
    "TasksReportData",
    "build",
    "collect",
]
