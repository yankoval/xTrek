"""Finished xTrek equipment reports grouped by operator and article."""

from .data import (
    EquipmentArticleData,
    EquipmentOperatorData,
    EquipmentReportData,
    EquipmentReportDetail,
    collect,
    collect_range,
)
from .document import REPORT_TITLE, build
from .source import (
    DEFAULT_BUCKET,
    DEFAULT_ENDPOINT_URL,
    DEFAULT_PRODUCTION_ORDERS_PREFIX,
    DEFAULT_REPORTS_PREFIX,
    EquipmentReportObjectRef,
    S3EquipmentReportSource,
)

__all__ = [
    "DEFAULT_BUCKET",
    "DEFAULT_ENDPOINT_URL",
    "DEFAULT_PRODUCTION_ORDERS_PREFIX",
    "DEFAULT_REPORTS_PREFIX",
    "EquipmentArticleData",
    "EquipmentOperatorData",
    "EquipmentReportData",
    "EquipmentReportDetail",
    "EquipmentReportObjectRef",
    "REPORT_TITLE",
    "S3EquipmentReportSource",
    "build",
    "collect",
    "collect_range",
]
