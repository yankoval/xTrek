"""Opt-in tests against Linux CSP/pycades and an installed test certificate.

Set XTREK_SIGN_TEST_THUMBPRINT and optionally XTREK_SIGN_TEST_PIN_FILE.
These tests sign synthetic data only; no requests to CRPT or S3 are sent.
"""
import base64
import os
from pathlib import Path
import subprocess
import sys

import pytest

from xtrek import sign

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or not os.environ.get("XTREK_SIGN_TEST_THUMBPRINT"),
    reason="Requires opt-in Linux CSP certificate via XTREK_SIGN_TEST_THUMBPRINT",
)
DOCUMENT = b'\xef\xbb\xbf{\r\n  "test_only":true,"text":"\\u0410","quantity":1\r\n}\r\n'
CHALLENGE = "Synthetic auth challenge: Тест YWJjZA==\r\n+/="


@pytest.fixture
def credentials():
    return dict(thumbprint=os.environ["XTREK_SIGN_TEST_THUMBPRINT"],
                pin_file=os.environ.get("XTREK_SIGN_TEST_PIN_FILE"))


def verify_python(signature, data, detached):
    import pycades
    verifier = pycades.SignedData()
    verifier.ContentEncoding = pycades.CADESCOM_BASE64_TO_BINARY
    if detached:
        verifier.Content = base64.b64encode(data).decode("ascii")
    verifier.VerifyCades(signature, pycades.CADESCOM_CADES_BES, detached)
    if not detached:
        assert base64.b64decode(verifier.Content) == data


def verify_cryptcp(signature, data, detached, tmp_path):
    """Independent native CLI check; capture its output, don't log certificates."""
    executable = Path(os.environ.get("XTREK_SIGN_TEST_CRYPTCP", "/opt/cprocsp/bin/amd64/cryptcp"))
    assert executable.is_file(), "Set XTREK_SIGN_TEST_CRYPTCP for this Linux architecture"
    source = tmp_path / "payload.bin"
    sig = tmp_path / "payload.sig"
    recovered = tmp_path / "recovered.bin"
    source.write_bytes(data)
    sig.write_text(signature, encoding="ascii")
    cmd = [str(executable), "-verify", "-detached" if detached else "-attached", "-verall", "-cadesbes"]
    cmd += [str(source), str(sig)] if detached else [str(sig), str(recovered)]
    result = subprocess.run(cmd, capture_output=True, timeout=45)
    assert result.returncode == 0, "cryptcp verification failed"
    if not detached:
        assert recovered.read_bytes() == data


def test_document_signature_and_tampering(credentials, tmp_path):
    import pycades
    signature = sign.sign_document(DOCUMENT, **credentials)
    assert signature and not any(c.isspace() for c in signature)
    verify_python(signature, DOCUMENT, True)
    verify_cryptcp(signature, DOCUMENT, True, tmp_path)
    verifier = pycades.SignedData()
    verifier.ContentEncoding = pycades.CADESCOM_BASE64_TO_BINARY
    verifier.Content = base64.b64encode(DOCUMENT + b"!").decode("ascii")
    with pytest.raises(Exception) as error:
        verifier.VerifyCades(signature, pycades.CADESCOM_CADES_BES, True)
    assert "0x80090006" in str(error.value).lower() or "-2146893818" in str(error.value)


@pytest.mark.parametrize("format_name", ["true_api_uuid", "true_api_jwt", "suz"])
def test_token_challenge_attached(credentials, tmp_path, format_name):
    # All three use attached CAdES of auth/key.data, not the auth response JSON.
    data = CHALLENGE + " " + format_name
    signature = sign.sign_token_data(data, **credentials)
    verify_python(signature, data.encode("utf-8"), False)
    verify_cryptcp(signature, data.encode("utf-8"), False, tmp_path)


def test_suz_ping_detached(credentials, tmp_path):
    path = "/api/v3/ping?omsId=11111111-1111-4111-8111-111111111111"
    signature = sign.sign_token_data(path, detached=True, **credentials)
    verify_python(signature, path.encode(), True)
    verify_cryptcp(signature, path.encode(), True, tmp_path)


@pytest.mark.parametrize("suffix,detached,data", [(".json", True, DOCUMENT), (".txt", False, CHALLENGE.encode())])
def test_storage_file_preserves_source(credentials, tmp_path, suffix, detached, data):
    source = tmp_path / ("synthetic" + suffix)
    source.write_bytes(data)
    target = sign.sign_file(source, **credentials)
    assert source.read_bytes() == data
    signature = Path(target).read_text(encoding="ascii")
    verify_python(signature, data, detached)
    verify_cryptcp(signature, data, detached, tmp_path)


def test_cli_stdin_challenge(credentials):
    command = [sys.executable, "-m", "xtrek.sign", "token", "-", "--thumbprint", credentials["thumbprint"]]
    if credentials["pin_file"]:
        command += ["--pin-file", credentials["pin_file"]]
    result = subprocess.run(command, input=CHALLENGE.encode(), capture_output=True, timeout=45)
    assert result.returncode == 0, "CLI signing failed"
    verify_python(result.stdout.decode().strip(), CHALLENGE.encode(), False)


def test_configured_signer_self_test_and_local_files(credentials):
    signer = sign.DocumentSigner({'signing': {'local_by_inn': {'7701234567': credentials}}})
    assert signer.self_test() == {'7701234567': True}
    with signer.prepare('7701234567', DOCUMENT, 'document.json', None, 0) as signed:
        assert signed.body_path.read_bytes() == DOCUMENT
        verify_python(signed.signature, DOCUMENT, True)
