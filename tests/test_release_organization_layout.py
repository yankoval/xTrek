import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ORGS_DIR = PROJECT_ROOT / "xtrek" / "my_orgs"
TEST_ORG_FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "my_orgs" / "org123.json"
TEST_INN = "1234567890"


def test_test_organization_is_only_a_fixture():
    fixture = json.loads(TEST_ORG_FIXTURE.read_text(encoding="utf-8"))
    assert fixture["inn"] == TEST_INN

    runtime_inns = {
        org.get("inn")
        for path in RUNTIME_ORGS_DIR.glob("*.json")
        for org in _organizations(json.loads(path.read_text(encoding="utf-8")))
    }
    assert TEST_INN not in runtime_inns


def _organizations(payload):
    if "org_id" in payload:
        return [payload]
    return payload.values()
