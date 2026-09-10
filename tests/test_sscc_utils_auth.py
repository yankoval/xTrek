from unittest.mock import Mock, call, patch

import pytest
import requests

from xtrek import SSCC_Utils
from xtrek.create_emission_task_sample import _create_pallet_assignment


FUNCTION_URL = "https://functions.yandexcloud.net/function-id"


def setup_function():
    SSCC_Utils._clear_iam_token_cache()


def _response(*, status_code=200, payload=None):
    response = Mock(status_code=status_code)
    response.json.return_value = payload or {"ssccs": ["000000000000000001"]}
    if status_code >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(
            f"HTTP {status_code}",
            response=response,
        )
    return response


@patch("xtrek.SSCC_Utils.requests.get")
@patch("xtrek.SSCC_Utils.requests.post")
def test_unauthenticated_mode_does_not_read_vm_metadata(post, get):
    post.return_value = _response()

    result = SSCC_Utils.get_sscc_from_service(
        "https://sscc.example.test",
        "460705179",
        1,
    )

    assert result == ["000000000000000001"]
    get.assert_not_called()
    assert post.call_args.kwargs["headers"] is None


@patch("xtrek.SSCC_Utils.requests.get")
@patch("xtrek.SSCC_Utils.requests.post")
def test_yandex_iam_mode_uses_and_caches_metadata_token(post, get):
    get.return_value = _response(
        payload={
            "access_token": "short-lived-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }
    )
    post.return_value = _response()

    for _ in range(2):
        result = SSCC_Utils.get_sscc_from_service(
            FUNCTION_URL,
            "460705179",
            1,
            auth_mode="yandex_iam",
        )
        assert result == ["000000000000000001"]

    get.assert_called_once_with(
        SSCC_Utils.YC_METADATA_TOKEN_URL,
        headers={"Metadata-Flavor": "Google"},
        timeout=(1, 3),
    )
    assert post.call_count == 2
    assert all(
        item.kwargs["headers"]
        == {"Authorization": "Bearer short-lived-token"}
        for item in post.call_args_list
    )


@patch("xtrek.SSCC_Utils.requests.get")
@patch("xtrek.SSCC_Utils.requests.post")
def test_yandex_iam_mode_refreshes_token_once_after_401(post, get):
    get.side_effect = [
        _response(payload={"access_token": "expired", "expires_in": 3600}),
        _response(payload={"access_token": "fresh", "expires_in": 3600}),
    ]
    post.side_effect = [_response(status_code=401), _response()]

    result = SSCC_Utils.get_sscc_from_service(
        FUNCTION_URL,
        "460705179",
        1,
        auth_mode="yandex_iam",
    )

    assert result == ["000000000000000001"]
    assert post.call_args_list == [
        call(
            FUNCTION_URL,
            json={"prefix": "460705179", "count": 1},
            headers={"Authorization": "Bearer expired"},
            timeout=15,
        ),
        call(
            FUNCTION_URL,
            json={"prefix": "460705179", "count": 1},
            headers={"Authorization": "Bearer fresh"},
            timeout=15,
        ),
    ]


@patch("xtrek.SSCC_Utils.requests.get")
@patch("xtrek.SSCC_Utils.requests.post")
def test_yandex_iam_token_is_not_sent_to_another_host(post, get):
    with pytest.raises(ValueError, match="Refusing to send"):
        SSCC_Utils.get_sscc_from_service(
            "https://example.test/sscc",
            "460705179",
            1,
            auth_mode="yandex_iam",
        )

    get.assert_not_called()
    post.assert_not_called()


@patch("xtrek.create_emission_task_sample.get_sscc_from_service")
def test_pallet_assignment_passes_explicit_auth_mode(get_sscc):
    get_sscc.return_value = ["046070517921585754"]

    assignment = _create_pallet_assignment(
        {"Quantity": "17"},
        {
            "sscc_service_url": FUNCTION_URL,
            "sscc_prefix": "460705179",
            "sscc_extension": "0",
            "sscc_auth_mode": "yandex_iam",
        },
    )

    assert assignment["palletNumbers"] == ["046070517921585754"]
    get_sscc.assert_called_once_with(
        FUNCTION_URL,
        "460705179",
        1,
        "0",
        auth_mode="yandex_iam",
    )


@patch('xtrek.SSCC_Utils.time.monotonic')
@patch('xtrek.SSCC_Utils.requests.get')
def test_iam_cache_refreshes_before_expiry_without_restart(get, monotonic):
    monotonic.side_effect = [0, 3539, 3540]
    get.side_effect = [
        _response(payload={'access_token': 'first', 'expires_in': 3600}),
        _response(payload={'access_token': 'second', 'expires_in': 3600}),
    ]
    assert SSCC_Utils._get_yandex_iam_token() == 'first'
    assert SSCC_Utils._get_yandex_iam_token() == 'first'
    assert SSCC_Utils._get_yandex_iam_token() == 'second'
    assert get.call_count == 2


@patch('xtrek.SSCC_Utils.requests.get')
@patch('xtrek.SSCC_Utils.requests.post')
def test_metadata_failure_does_not_send_unauthenticated_sscc_request(post, get):
    get.side_effect = requests.Timeout('metadata timeout')
    assert SSCC_Utils.get_sscc_from_service(FUNCTION_URL, '460705179', 1, auth_mode='yandex_iam') == []
    post.assert_not_called()
