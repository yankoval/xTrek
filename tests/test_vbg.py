import json

import pytest

from xtrek import vbg


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return FakeResponse(payload=self.payload)


def test_get_gtin_info_fetches_and_caches(tmp_path):
    cache_path = tmp_path / "vbg-cache.json"
    session = FakeSession({
        "data": [{
            "gtin": "4670167220663",
            "success": True,
            "brandName": [{"value": "Brand"}],
            "license": {
                "licenseeName": 'ООО "СМАРТ КОСМЕТИК"',
                "licenseeINN": "9723161905",
                "licenseeGLN": "4670167220007",
            },
        }]
    })

    result = vbg.get_gtin_info(
        ["4670167220663"],
        token="token",
        cache_path=str(cache_path),
        session=session,
    )

    assert list(result) == ["04670167220663"]
    assert result["04670167220663"]["license"]["licenseeINN"] == "9723161905"
    assert session.calls[0]["json"] == ["04670167220663"]
    assert session.calls[0]["headers"]["Token"] == "token"

    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    assert cache["04670167220663"]["data"]["license"]["licenseeName"] == 'ООО "СМАРТ КОСМЕТИК"'


def test_get_gtin_info_uses_fresh_cache_without_token(tmp_path):
    cache_path = tmp_path / "vbg-cache.json"
    cache_path.write_text(
        json.dumps({
            "04670167220663": {
                "checked_at": 9999999999,
                "data": {"gtin": "4670167220663", "success": True},
            }
        }),
        encoding="utf-8",
    )

    result = vbg.get_gtin_info(["04670167220663"], cache_path=str(cache_path))

    assert result["04670167220663"]["success"] is True


def test_summarize_gtin_extracts_owner_inn_and_name():
    summary = vbg.summarize_gtin({
        "gtin": "5449000000996",
        "success": True,
        "productDescription": [{"value": "Drink"}],
        "license": {
            "licenseeName": "COCA-COLA SERVICES SA/NV",
            "licenseeINN": None,
            "licenseeGLN": "5449000000006",
        },
    })

    assert summary["gtin"] == "05449000000996"
    assert summary["licenseeName"] == "COCA-COLA SERVICES SA/NV"
    assert summary["licenseeINN"] is None
    assert summary["productDescription"] == "Drink"


@pytest.mark.parametrize("bad_gtin", ["", "abc", "123456789012345"])
def test_normalize_gtin_rejects_invalid_values(bad_gtin):
    with pytest.raises(ValueError):
        vbg.normalize_gtin(bad_gtin)
