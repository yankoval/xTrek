"""Backward-compatible data imports for :mod:`xtrek.reports.tasks_grouped`."""

from ..tasks_grouped.data import (
    MISSING_ARTICLE,
    ArticleTasksData,
    TaskFileData,
    TasksGroupedReportData,
    collect,
    collect_range,
)

TasksGouppedReportData = TasksGroupedReportData

__all__ = [
    "MISSING_ARTICLE",
    "ArticleTasksData",
    "TaskFileData",
    "TasksGroupedReportData",
    "TasksGouppedReportData",
    "collect",
    "collect_range",
]
