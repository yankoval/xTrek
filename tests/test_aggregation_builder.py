import json
from pathlib import Path

import pytest

from xtrek.aggregation_builder import (
    AggregationBuildError,
    build_aggregation_report,
    normalize_equipment_report,
)


PRODUCTS = [
    "0104630040775895215AAAAA\u001d93CRYPTO1",
    "0104630040775895215BBBBB\u001d93CRYPTO2",
    "0104630040775895215CCCCC\u001d93CRYPTO3",
    "0104630040775895215DDDDD\u001d93CRYPTO4",
]


def box(sscc, products):
    return {
        "boxNumber": sscc,
        "boxAgregate": True,
        "productNumbersFull": products,
    }


def test_v2_builds_boxes_before_pallet_in_one_document():
    report = {
        "schemaVersion": 2,
        "id": "report-v2",
        "readyPallet": [
            {
                "palletNumber": "046070517921585754",
                "palletAggregate": True,
                "readyBox": [
                    box("046070517921585730", PRODUCTS[:2]),
                    box("046070517921585747", PRODUCTS[2:]),
                ],
            }
        ],
    }

    result = build_aggregation_report(report, "7733154124").to_dict()

    assert result == {
        "participantId": "7733154124",
        "aggregationUnits": [
            {
                "sntins": [
                    "0104630040775895215AAAAA",
                    "0104630040775895215BBBBB",
                ],
                "aggregationType": "AGGREGATION",
                "unitSerialNumber": "00046070517921585730",
            },
            {
                "sntins": [
                    "0104630040775895215CCCCC",
                    "0104630040775895215DDDDD",
                ],
                "aggregationType": "AGGREGATION",
                "unitSerialNumber": "00046070517921585747",
            },
            {
                "sntins": [
                    "00046070517921585730",
                    "00046070517921585747",
                ],
                "aggregationType": "AGGREGATION",
                "unitSerialNumber": "00046070517921585754",
            },
        ],
    }


def test_v1_preserves_boxes_only_without_allocated_pallet():
    report = {
        "id": "report-v1",
        "readyBox": [box("046070517921585730", PRODUCTS[:2])],
    }

    result = build_aggregation_report(report, "7733154124").to_dict()

    assert len(result["aggregationUnits"]) == 1
    assert result["aggregationUnits"][0]["unitSerialNumber"] == (
        "00046070517921585730"
    )


def test_v1_wraps_all_boxes_in_one_allocated_pallet():
    report = {
        "id": "report-v1",
        "readyBox": [
            box("046070517921585730", PRODUCTS[:2]),
            box("046070517921585747", PRODUCTS[2:]),
        ],
    }

    result = build_aggregation_report(
        report,
        "7733154124",
        legacy_pallet_sscc="046070517921585754",
    ).to_dict()

    assert [unit["unitSerialNumber"] for unit in result["aggregationUnits"]] == [
        "00046070517921585730",
        "00046070517921585747",
        "00046070517921585754",
    ]
    assert result["aggregationUnits"][-1]["sntins"] == [
        "00046070517921585730",
        "00046070517921585747",
    ]


def test_rejects_duplicate_product_across_boxes():
    report = {
        "schemaVersion": 2,
        "readyPallet": [
            {
                "palletNumber": "046070517921585754",
                "palletAggregate": True,
                "readyBox": [
                    box("046070517921585730", [PRODUCTS[0]]),
                    box("046070517921585747", [PRODUCTS[0]]),
                ],
            }
        ],
    }

    with pytest.raises(AggregationBuildError, match="Duplicate product code"):
        normalize_equipment_report(report)


def test_rejects_box_used_in_two_pallets():
    report = {
        "schemaVersion": 2,
        "readyPallet": [
            {
                "palletNumber": "046070517921585754",
                "palletAggregate": True,
                "readyBox": [box("046070517921585730", [PRODUCTS[0]])],
            },
            {
                "palletNumber": "046070517921585761",
                "palletAggregate": True,
                "readyBox": [box("046070517921585730", [PRODUCTS[1]])],
            },
        ],
    }

    with pytest.raises(AggregationBuildError, match="Duplicate aggregate SSCC"):
        normalize_equipment_report(report)


@pytest.mark.parametrize(
    "bad_sscc",
    [
        "123",
        "04607051792158575X",
        "11046070517921585730",
    ],
)
def test_rejects_invalid_sscc(bad_sscc):
    report = {
        "readyBox": [box(bad_sscc, [PRODUCTS[0]])],
    }

    with pytest.raises(AggregationBuildError, match="must contain 18 digits"):
        normalize_equipment_report(report)


def test_rejects_open_v2_pallet_or_box():
    report = {
        "schemaVersion": 2,
        "readyPallet": [
            {
                "palletNumber": "046070517921585754",
                "palletAggregate": False,
                "readyBox": [box("046070517921585730", [PRODUCTS[0]])],
            }
        ],
    }

    with pytest.raises(AggregationBuildError, match="palletAggregate must be true"):
        normalize_equipment_report(report)

    report["readyPallet"][0]["palletAggregate"] = True
    report["readyPallet"][0]["readyBox"][0]["boxAgregate"] = False
    with pytest.raises(AggregationBuildError, match="boxAgregate must be true"):
        normalize_equipment_report(report)


def test_rejects_short_codes_that_do_not_match_full_codes():
    raw_box = box("046070517921585730", [PRODUCTS[0]])
    raw_box["productNumbers"] = ["different"]
    report = {"readyBox": [raw_box]}

    with pytest.raises(AggregationBuildError, match="does not match"):
        normalize_equipment_report(report)


@pytest.mark.parametrize(
    "example_name",
    [
        "equipment-report-v2-unit.example.json",
        "equipment-report-v2-set.example.json",
    ],
)
def test_documented_v2_examples_match_builder_contract(example_name):
    example_path = (
        Path(__file__).parents[1] / "docs" / "examples" / example_name
    )
    report = json.loads(example_path.read_text(encoding="utf-8"))

    normalized = normalize_equipment_report(report)

    assert normalized.source_schema_version == 2
    assert normalized.pallets
