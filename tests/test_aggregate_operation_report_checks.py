import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from xtrek import utils
from xtrek.trueapi import HonestSignAPI


AGGREGATE = "00000123456789012345"
CHILD = "010460000000000021ABC"


class FakeTrueAPI:
    def __init__(self, info_by_code, composition):
        self.info_by_code = info_by_code
        self.composition = composition

    def get_list_cis_info(self, codes):
        return [
            {
                "requestedCis": code,
                "cisInfo": self.info_by_code.get(code, {}),
            }
            for code in codes
        ]

    def get_aggregated_cis_list(self, codes):
        return {
            code: self.composition[code]
            for code in codes
            if code in self.composition
        }


class FailingTrueAPI(FakeTrueAPI):
    def get_aggregated_cis_list(self, codes):
        return {"error": "503 Service Unavailable"}


def _write_report(path: Path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _disaggregation_report():
    return {
        "participant_inn": "7701234567",
        "products_list": [{"uitu": "000123456789012345"}],
    }


def _removing_report():
    return {
        "participant_inn": "7701234567",
        "reaggregation_type": "REMOVING",
        "uitu": "000123456789012345",
        "uit_uitu_list": [{"uit_uitu": CHILD}],
    }


def _tags(path):
    return json.loads(Path(f"{path}.tags").read_text(encoding="utf-8"))


def test_disaggregation_initial_check_sets_empty_check_tag(tmp_path):
    path = _write_report(tmp_path / "disaggregation.json", _disaggregation_report())
    api = FakeTrueAPI(
        {
            AGGREGATE: {
                "cis": AGGREGATE,
                "status": "INTRODUCED",
                "packageType": "BOX",
            },
        },
        {AGGREGATE: {CHILD: []}},
    )

    result = utils.check_disaggregation_report(path, api=api, config={})

    assert result is None
    assert _tags(path) == {"check": ""}


def test_disaggregation_accepts_true_api_level1_box(tmp_path):
    path = _write_report(tmp_path / "disaggregation.json", _disaggregation_report())
    api = FakeTrueAPI(
        {
            AGGREGATE: {
                "cis": AGGREGATE,
                "status": "INTRODUCED",
                "packageType": "LEVEL1",
                "generalPackageType": "BOX",
            },
        },
        {AGGREGATE: {CHILD: []}},
    )

    result = utils.check_disaggregation_report(path, api=api, config={})

    assert result is None
    assert _tags(path) == {"check": ""}


def test_disaggregation_final_check_sets_finished(tmp_path):
    path = _write_report(tmp_path / "disaggregation.json", _disaggregation_report())
    api = FakeTrueAPI(
        {
            AGGREGATE: {
                "cis": AGGREGATE,
                "status": "DISAGGREGATION",
                "packageType": "BOX",
            },
        },
        {},
    )

    result = utils.check_disaggregation_report(path, api=api, config={})

    assert result == {"finished": ["All aggregates are disaggregated"]}
    assert _tags(path) == {"check": "finished"}


def test_disaggregation_missing_aggregate_is_finished_only_after_checked_ok(tmp_path):
    path = _write_report(tmp_path / "disaggregation.json", _disaggregation_report())
    api = FakeTrueAPI({}, {})

    initial_result = utils.check_disaggregation_report(path, api=api, config={})

    assert initial_result == {"aggregatenotfound": [AGGREGATE]}
    assert _tags(path) == {"check": "aggregatenotfound"}

    final_result = utils.check_disaggregation_report(
        path,
        api=api,
        config={},
        final=True,
    )

    assert final_result == {"finished": ["All aggregates are disaggregated"]}
    assert _tags(path) == {"check": "finished"}


def test_true_api_error_does_not_create_check_tag(tmp_path):
    path = _write_report(tmp_path / "disaggregation.json", _disaggregation_report())
    api = FailingTrueAPI(
        {
            AGGREGATE: {
                "cis": AGGREGATE,
                "status": "INTRODUCED",
                "packageType": "BOX",
            },
        },
        {},
    )

    result = utils.check_disaggregation_report(path, api=api, config={})

    assert result == {"api_error": ["503 Service Unavailable"]}
    assert not Path(f"{path}.tags").exists()


def test_reaggregation_removing_initial_check_sets_empty_tag(tmp_path):
    path = _write_report(tmp_path / "removing.json", _removing_report())
    api = FakeTrueAPI(
        {
            AGGREGATE: {
                "cis": AGGREGATE,
                "status": "INTRODUCED",
                "packageType": "BOX",
                "ownerInn": "7701234567",
            },
            CHILD: {
                "cis": CHILD,
                "status": "INTRODUCED",
                "packageType": "UNIT",
                "ownerInn": "7701234567",
                "parent": AGGREGATE,
            },
        },
        {AGGREGATE: {CHILD: []}},
    )

    result = utils.check_reaggregation_removing_report(path, api=api, config={})

    assert result is None
    assert _tags(path) == {"check": ""}


def test_reaggregation_removing_accepts_true_api_level1_box(tmp_path):
    path = _write_report(tmp_path / "removing.json", _removing_report())
    api = FakeTrueAPI(
        {
            AGGREGATE: {
                "cis": AGGREGATE,
                "status": "INTRODUCED",
                "packageType": "LEVEL1",
                "generalPackageType": "BOX",
                "ownerInn": "7701234567",
            },
            CHILD: {
                "cis": CHILD,
                "status": "INTRODUCED",
                "packageType": "UNIT",
                "ownerInn": "7701234567",
                "parent": AGGREGATE,
            },
        },
        {AGGREGATE: {CHILD: []}},
    )

    result = utils.check_reaggregation_removing_report(path, api=api, config={})

    assert result is None
    assert _tags(path) == {"check": ""}


def test_reaggregation_removing_final_check_sets_finished(tmp_path):
    path = _write_report(tmp_path / "removing.json", _removing_report())
    api = FakeTrueAPI(
        {
            AGGREGATE: {
                "cis": AGGREGATE,
                "status": "INTRODUCED",
                "packageType": "BOX",
                "ownerInn": "7701234567",
            },
            CHILD: {
                "cis": CHILD,
                "status": "INTRODUCED",
                "packageType": "UNIT",
                "ownerInn": "7701234567",
            },
        },
        {AGGREGATE: {}},
    )

    result = utils.check_reaggregation_removing_report(path, api=api, config={})

    assert result == {"finished": ["All requested codes are removed"]}
    assert _tags(path) == {"check": "finished"}


def test_reaggregation_removing_missing_aggregate_is_not_finished(tmp_path):
    path = _write_report(tmp_path / "removing.json", _removing_report())
    not_found = MagicMock(status_code=404)

    with patch(
        "xtrek.trueapi.requests.post",
        side_effect=[not_found, not_found],
    ):
        result = utils.check_reaggregation_removing_report(
            path,
            api=HonestSignAPI(token="test-token"),
            config={},
        )

    assert result == {"aggregatenotfound": [AGGREGATE]}
    assert _tags(path) == {"check": "aggregatenotfound"}


def test_reaggregation_retryable_api_error_preserves_check_tag(tmp_path):
    path = _write_report(tmp_path / "removing.json", _removing_report())
    Path(f"{path}.tags").write_text(
        json.dumps({"check": ""}),
        encoding="utf-8",
    )
    api = FailingTrueAPI(
        {
            AGGREGATE: {
                "cis": AGGREGATE,
                "status": "INTRODUCED",
                "packageType": "BOX",
                "ownerInn": "7701234567",
            },
            CHILD: {
                "cis": CHILD,
                "status": "INTRODUCED",
                "packageType": "UNIT",
                "ownerInn": "7701234567",
                "parent": AGGREGATE,
            },
        },
        {},
    )

    result = utils.check_reaggregation_removing_report(path, api=api, config={})

    assert result == {"api_error": ["503 Service Unavailable"]}
    assert _tags(path) == {"check": ""}


def test_reaggregation_owner_error_uses_existing_error_tag_style(tmp_path):
    path = _write_report(tmp_path / "removing.json", _removing_report())
    api = FakeTrueAPI(
        {
            AGGREGATE: {
                "cis": AGGREGATE,
                "status": "INTRODUCED",
                "packageType": "BOX",
                "ownerInn": "7700000000",
            },
            CHILD: {
                "cis": CHILD,
                "status": "INTRODUCED",
                "packageType": "UNIT",
                "ownerInn": "7700000000",
                "parent": AGGREGATE,
            },
        },
        {AGGREGATE: {CHILD: []}},
    )

    result = utils.check_reaggregation_removing_report(path, api=api, config={})

    assert set(result) == {"wrongowner"}
    assert _tags(path) == {"check": "wrongowner"}


def test_invalid_report_sets_validation_error_tag(tmp_path):
    path = _write_report(tmp_path / "removing.json", {"participant_inn": "bad"})

    result = utils.check_reaggregation_removing_report(
        path,
        api=FakeTrueAPI({}, {}),
        config={},
    )

    assert set(result) == {"invalidreport"}
    assert _tags(path) == {"check": "invalidreport"}
