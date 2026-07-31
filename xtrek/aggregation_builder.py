"""Canonical equipment-report adapter and True API aggregation builder."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Sequence

from .suz_api_models import AggregationReport, AggregationUnit


class AggregationBuildError(ValueError):
    """The equipment report cannot be converted into an aggregation document."""


@dataclass(frozen=True)
class CanonicalBox:
    sscc: str
    product_codes: tuple[str, ...]
    full_product_codes: tuple[str, ...]


@dataclass(frozen=True)
class CanonicalPallet:
    sscc: Optional[str]
    boxes: tuple[CanonicalBox, ...]


@dataclass(frozen=True)
class CanonicalEquipmentReport:
    source_schema_version: int
    pallets: tuple[CanonicalPallet, ...]


def get_equipment_report_version(report: Mapping[str, Any]) -> int:
    """Return the declared equipment-report version and reject ambiguity."""
    if not isinstance(report, Mapping):
        raise AggregationBuildError("Equipment report must be an object")

    schema_version = report.get("schemaVersion")
    if schema_version == 2 or "readyPallet" in report:
        if schema_version != 2:
            raise AggregationBuildError(
                "Reports with readyPallet must declare schemaVersion=2"
            )
        if "readyPallet" not in report:
            raise AggregationBuildError(
                "Reports with schemaVersion=2 must contain readyPallet"
            )
        return 2

    if schema_version not in (None, 1):
        raise AggregationBuildError(
            f"Unsupported equipment report schemaVersion: {schema_version!r}"
        )
    return 1


def iter_equipment_report_pallets(
    report: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...]:
    """Return raw v2 pallets; v1 reports have no explicit pallet objects."""
    if get_equipment_report_version(report) == 1:
        return ()

    raw_pallets = report.get("readyPallet")
    if not isinstance(raw_pallets, Sequence) or isinstance(
        raw_pallets, (str, bytes)
    ):
        raise AggregationBuildError("readyPallet must be an array")

    pallets = []
    for pallet_index, raw_pallet in enumerate(raw_pallets):
        if not isinstance(raw_pallet, Mapping):
            raise AggregationBuildError(
                f"readyPallet[{pallet_index}] must be an object"
            )
        pallets.append(raw_pallet)
    return tuple(pallets)


def iter_equipment_report_boxes(
    report: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...]:
    """Return physical boxes from either root readyBox (v1) or pallets (v2)."""
    if get_equipment_report_version(report) == 1:
        raw_boxes = report.get("readyBox", [])
        if not isinstance(raw_boxes, Sequence) or isinstance(
            raw_boxes, (str, bytes)
        ):
            raise AggregationBuildError("readyBox must be an array")
        locations_and_boxes = [
            (f"readyBox[{box_index}]", raw_box)
            for box_index, raw_box in enumerate(raw_boxes)
        ]
    else:
        locations_and_boxes = []
        for pallet_index, raw_pallet in enumerate(
            iter_equipment_report_pallets(report)
        ):
            raw_boxes = raw_pallet.get("readyBox")
            if not isinstance(raw_boxes, Sequence) or isinstance(
                raw_boxes, (str, bytes)
            ):
                raise AggregationBuildError(
                    f"readyPallet[{pallet_index}].readyBox must be an array"
                )
            locations_and_boxes.extend(
                (
                    f"readyPallet[{pallet_index}].readyBox[{box_index}]",
                    raw_box,
                )
                for box_index, raw_box in enumerate(raw_boxes)
            )

    boxes = []
    for location, raw_box in locations_and_boxes:
        if not isinstance(raw_box, Mapping):
            raise AggregationBuildError(f"{location} must be an object")
        boxes.append(raw_box)
    return tuple(boxes)


def extract_full_product_codes(report: Mapping[str, Any]) -> list[str]:
    """Validate a v1/v2 report and flatten its full marking codes."""
    canonical = normalize_equipment_report(report)
    return [
        code
        for pallet in canonical.pallets
        for box in pallet.boxes
        for code in box.full_product_codes
    ]


def cut_crypto_tail(code: str) -> str:
    """Return the KI part before the first GS separator."""
    if not isinstance(code, str) or not code:
        raise AggregationBuildError("Marking code must be a non-empty string")
    return code.split("\u001d", 1)[0]


def normalize_sscc(value: Any, field_name: str = "SSCC") -> str:
    """Normalize an 18-digit SSCC to the 20-digit AI 00 representation."""
    raw = str(value).strip()
    if len(raw) == 18 and raw.isdigit():
        return f"00{raw}"
    if len(raw) == 20 and raw.startswith("00") and raw.isdigit():
        return raw
    raise AggregationBuildError(
        f"{field_name} must contain 18 digits or 20 digits starting with 00: {raw!r}"
    )


def _parse_box(
    raw_box: Any,
    location: str,
    *,
    require_aggregate_flag: bool = False,
) -> CanonicalBox:
    if not isinstance(raw_box, Mapping):
        raise AggregationBuildError(f"{location} must be an object")

    if require_aggregate_flag and raw_box.get("boxAgregate") is not True:
        raise AggregationBuildError(f"{location}.boxAgregate must be true")

    if not raw_box.get("boxNumber"):
        raise AggregationBuildError(f"{location}.boxNumber is required")
    sscc = normalize_sscc(raw_box["boxNumber"], f"{location}.boxNumber")

    raw_codes = raw_box.get("productNumbersFull")
    if not isinstance(raw_codes, Sequence) or isinstance(raw_codes, (str, bytes)):
        raise AggregationBuildError(f"{location}.productNumbersFull must be an array")
    if not raw_codes:
        raise AggregationBuildError(f"{location}.productNumbersFull must not be empty")

    full_product_codes = tuple(raw_codes)
    product_codes = tuple(cut_crypto_tail(code) for code in full_product_codes)

    short_codes = raw_box.get("productNumbers")
    if short_codes is not None:
        if not isinstance(short_codes, Sequence) or isinstance(
            short_codes, (str, bytes)
        ):
            raise AggregationBuildError(f"{location}.productNumbers must be an array")
        if tuple(short_codes) != product_codes:
            raise AggregationBuildError(
                f"{location}.productNumbers does not match productNumbersFull"
            )
    return CanonicalBox(
        sscc=sscc,
        product_codes=product_codes,
        full_product_codes=full_product_codes,
    )


def normalize_equipment_report(
    report: Mapping[str, Any],
    *,
    legacy_pallet_sscc: Optional[str] = None,
) -> CanonicalEquipmentReport:
    """
    Normalize equipment report v1/v2.

    A v1 report remains boxes-only unless an idempotently allocated
    ``legacy_pallet_sscc`` is supplied by the caller. This preserves the
    existing flow while allowing the v1 -> single pallet migration.
    """
    if not isinstance(report, Mapping):
        raise AggregationBuildError("Equipment report must be an object")

    schema_version = get_equipment_report_version(report)
    if schema_version == 2:
        raw_pallets = report.get("readyPallet")
        if not isinstance(raw_pallets, Sequence) or isinstance(
            raw_pallets, (str, bytes)
        ):
            raise AggregationBuildError("readyPallet must be an array")
        if not raw_pallets:
            raise AggregationBuildError("readyPallet must not be empty")

        pallets = []
        for pallet_index, raw_pallet in enumerate(raw_pallets):
            location = f"readyPallet[{pallet_index}]"
            if not isinstance(raw_pallet, Mapping):
                raise AggregationBuildError(f"{location} must be an object")
            if not raw_pallet.get("palletNumber"):
                raise AggregationBuildError(f"{location}.palletNumber is required")
            if raw_pallet.get("palletAggregate") is not True:
                raise AggregationBuildError(f"{location}.palletAggregate must be true")
            pallet_sscc = normalize_sscc(
                raw_pallet["palletNumber"], f"{location}.palletNumber"
            )

            raw_boxes = raw_pallet.get("readyBox")
            if not isinstance(raw_boxes, Sequence) or isinstance(
                raw_boxes, (str, bytes)
            ):
                raise AggregationBuildError(f"{location}.readyBox must be an array")
            if not raw_boxes:
                raise AggregationBuildError(f"{location}.readyBox must not be empty")

            boxes = tuple(
                _parse_box(
                    box,
                    f"{location}.readyBox[{box_index}]",
                    require_aggregate_flag=True,
                )
                for box_index, box in enumerate(raw_boxes)
            )
            pallets.append(CanonicalPallet(sscc=pallet_sscc, boxes=boxes))
        canonical = CanonicalEquipmentReport(
            source_schema_version=2,
            pallets=tuple(pallets),
        )
    else:
        raw_boxes = report.get("readyBox")
        if not isinstance(raw_boxes, Sequence) or isinstance(
            raw_boxes, (str, bytes)
        ):
            raise AggregationBuildError("readyBox must be an array")
        if not raw_boxes:
            raise AggregationBuildError("readyBox must not be empty")

        boxes = tuple(
            _parse_box(box, f"readyBox[{box_index}]")
            for box_index, box in enumerate(raw_boxes)
        )
        pallet_sscc = (
            normalize_sscc(legacy_pallet_sscc, "legacy_pallet_sscc")
            if legacy_pallet_sscc
            else None
        )
        canonical = CanonicalEquipmentReport(
            source_schema_version=1,
            pallets=(CanonicalPallet(sscc=pallet_sscc, boxes=boxes),),
        )

    _validate_uniqueness(canonical)
    return canonical


def _validate_uniqueness(report: CanonicalEquipmentReport) -> None:
    aggregate_codes: set[str] = set()
    product_codes: set[str] = set()

    for pallet in report.pallets:
        if pallet.sscc:
            if pallet.sscc in aggregate_codes:
                raise AggregationBuildError(
                    f"Duplicate aggregate SSCC: {pallet.sscc}"
                )
            aggregate_codes.add(pallet.sscc)

        for box in pallet.boxes:
            if box.sscc in aggregate_codes:
                raise AggregationBuildError(f"Duplicate aggregate SSCC: {box.sscc}")
            aggregate_codes.add(box.sscc)

            for product_code in box.product_codes:
                if product_code in product_codes:
                    raise AggregationBuildError(
                        f"Duplicate product code: {product_code}"
                    )
                product_codes.add(product_code)


def build_aggregation_report(
    equipment_report: Mapping[str, Any],
    participant_id: str,
    *,
    legacy_pallet_sscc: Optional[str] = None,
) -> AggregationReport:
    """Build boxes first and pallets second for one True API document."""
    canonical = normalize_equipment_report(
        equipment_report,
        legacy_pallet_sscc=legacy_pallet_sscc,
    )

    units = []
    for pallet in canonical.pallets:
        for box in pallet.boxes:
            units.append(
                AggregationUnit(
                    unitSerialNumber=box.sscc,
                    aggregationType="AGGREGATION",
                    sntins=list(box.product_codes),
                )
            )

    for pallet in canonical.pallets:
        if pallet.sscc:
            units.append(
                AggregationUnit(
                    unitSerialNumber=pallet.sscc,
                    aggregationType="AGGREGATION",
                    sntins=[box.sscc for box in pallet.boxes],
                )
            )

    return AggregationReport(
        participantId=str(participant_id),
        aggregationUnits=units,
    )
