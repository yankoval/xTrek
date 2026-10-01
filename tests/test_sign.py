"""SignJS contract and storage/CLI tests: no CSP, credentials or network required."""
import base64
import io
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from xtrek import sign
from xtrek.storage import LocalStorage, S3Storage

THUMB = "A1" * 20
PAYLOAD = b'\xef\xbb\xbf{\r\n  "data":"\\u0410", "quantity":1\r\n}\x00'


@pytest.fixture
def native(monkeypatch):
    """Record bytes/profiles and emulate only the native API boundary."""
    cert = MagicMock()
    cert.HasPrivateKey.return_value = True
    matches = MagicMock(Count=1)
    matches.Item.return_value = cert
    store = MagicMock()
    store.Certificates.Find.return_value = matches
    module = SimpleNamespace(
        CADESCOM_CURRENT_USER_STORE=2, CADESCOM_LOCAL_MACHINE_STORE=1,
        CAPICOM_MY_STORE="My", CAPICOM_CERTIFICATE_FIND_SHA1_HASH=0,
        CADESCOM_BASE64_TO_BINARY=1, CADESCOM_CADES_BES=1,
        Store=MagicMock(return_value=store), Signer=MagicMock(),
    )
    calls = []
    # This is a protocol double, not a cryptographic verifier. Real crypto has
    # a separate opt-in integration suite on Linux.
    class Data:
        def SignCades(self, signer, profile, detached):
            calls.append((self.Content, self.ContentEncoding, profile, detached, signer))
            return " c2ln\r\nbmF0dXJl \n"

        def VerifyCades(self, signature, profile, detached):
            assert signature == "c2lnbmF0dXJl"
            assert profile == 1
            if detached:
                assert self.Content == calls[-1][0]
            else:
                self.Content = calls[-1][0]

    module.SignedData = Data
    monkeypatch.setattr(sign, "_load_pycades", lambda: module)
    return SimpleNamespace(module=module, calls=calls, store=store, matches=matches, cert=cert)


def test_document_preserves_bytes_and_uses_detached_bes(native):
    signature = sign.sign_document(PAYLOAD, THUMB.lower())
    assert signature == "c2lnbmF0dXJl"
    content, encoding, profile, detached, signer = native.calls[0]
    assert base64.b64decode(content) == PAYLOAD
    assert (encoding, profile, detached) == (1, 1, True)
    assert signer.Certificate is native.cert
    assert signer.CheckCertificate is True
    assert signer.KeyPin == ""
    native.store.Open.assert_called_once_with(2, "My", 0)
    native.store.Certificates.Find.assert_called_once_with(0, THUMB)
    native.store.Close.assert_called_once()


@pytest.mark.parametrize("data", ["Тест\r\n+/=", "YWJjZA==", b'\x00\xff\r\n'])
def test_token_challenge_is_literal_attached_data(native, data):
    sign.sign_token_data(data, THUMB)
    assert base64.b64decode(native.calls[0][0]) == (data.encode() if isinstance(data, str) else data)
    assert native.calls[0][3] is False


def test_suz_ping_signs_exact_path_detached(native):
    value = "/api/v3/ping?omsId=example&value=%2F+"
    sign.sign_token_data(value, THUMB, detached=True)
    assert base64.b64decode(native.calls[0][0]) == value.encode()
    assert native.calls[0][3] is True


def test_explicit_machine_store_no_fallback(native):
    sign.sign_document(b"", THUMB, store_location="local_machine")
    native.store.Open.assert_called_once_with(1, "My", 0)
    native.store.Open.side_effect = RuntimeError("private native error")
    with pytest.raises(sign.SigningError, match="open store"):
        sign.sign_document(b"x", THUMB)
    assert native.store.Open.call_count == 2


@pytest.mark.parametrize("count", [0, 2])
def test_certificate_must_match_exactly_once(native, count):
    native.matches.Count = count
    with pytest.raises(sign.SigningError, match="exactly one"):
        sign.sign_document(b"x", THUMB)
    assert not native.calls
    native.store.Close.assert_called_once()


def test_no_private_key(native):
    native.cert.HasPrivateKey.return_value = False
    with pytest.raises(sign.SigningError, match="no private key"):
        sign.sign_document(b"x", THUMB)
    native.store.Close.assert_called_once()


def test_pin_file_preserves_spaces_and_is_not_in_errors(native, tmp_path):
    path = tmp_path / "pin"
    path.write_text(" secret pin \n")
    path.chmod(0o600)
    sign.sign_document(b"x", THUMB, pin_file=path)
    assert native.calls[0][4].KeyPin == " secret pin "
    native.module.SignedData = MagicMock(side_effect=RuntimeError("secret pin 0x80090016"))
    with pytest.raises(sign.SigningError) as error:
        sign.sign_document(b"x", THUMB, pin_file=path)
    assert "secret" not in str(error.value)
    assert "0x80090016" in str(error.value)


def test_pin_file_must_be_private(native, tmp_path):
    path = tmp_path / "pin"
    path.write_text("not logged")
    path.chmod(0o644)
    with pytest.raises(sign.SigningError, match="only to its owner"):
        sign.sign_document(b"x", THUMB, pin_file=path)
    assert not native.calls


@pytest.mark.parametrize("options,error", [
    ({"pin": "secret", "pin_file": "missing"}, ValueError),
    ({"pin_file": "missing"}, sign.SigningError),
    ({"pin": "two\nlines"}, ValueError),
    ({"detached": "false"}, TypeError),
    ({"store_location": "unknown"}, ValueError),
])
def test_invalid_options_fail_before_signing(native, options, error):
    with pytest.raises(error):
        sign.sign_bytes(b"data", THUMB, **options)
    assert not native.calls


@pytest.mark.parametrize("thumb", ["", "A" * 39, "Z" * 40, None])
def test_invalid_thumbprint(native, thumb):
    with pytest.raises(ValueError):
        sign.sign_document(b"x", thumb)
    assert not native.calls


def test_document_requires_bytes(native):
    with pytest.raises(TypeError):
        sign.sign_document('{"x":1}', THUMB)


@pytest.mark.parametrize("suffix,detached", [(".json", True), (".txt", False), (".bin", True)])
def test_local_files_and_default_sig(native, tmp_path, suffix, detached):
    path = tmp_path / ("7733154124_payload" + suffix)
    path.write_bytes(PAYLOAD)
    out = sign.sign_file(path, THUMB)
    assert out == str(path) + ".sig"
    assert Path(out).read_bytes() == b"c2lnbmF0dXJl"
    assert path.read_bytes() == PAYLOAD
    assert base64.b64decode(native.calls[0][0]) == PAYLOAD
    assert native.calls[0][3] is detached


def test_file_uri_and_explicit_mode(native, tmp_path):
    source = tmp_path / "with space.txt"
    source.write_bytes(PAYLOAD)
    target = tmp_path / "other" / "result.sig"
    assert sign.sign_file(source.as_uri(), THUMB, output=target.as_uri(), detached=True) == str(target)
    assert native.calls[0][3] is True


def test_source_cannot_be_output(native, tmp_path):
    source = tmp_path / "data.json"
    source.write_bytes(PAYLOAD)
    with pytest.raises(ValueError, match="differ"):
        sign.sign_file(source, THUMB, output=source.as_uri())
    assert source.read_bytes() == PAYLOAD
    assert not native.calls


def test_missing_source_does_not_create_signature(native, tmp_path):
    with pytest.raises(sign.SigningError, match="read source"):
        sign.sign_file(tmp_path / "missing", THUMB)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("uri", ["https://example/data", "s3://bucket/", "file://remote/data", "s3://bucket/x?secret=y"])
def test_unsupported_locations(native, uri):
    with pytest.raises(ValueError):
        sign.sign_file(uri, THUMB)
    assert not native.calls


def test_s3_storage_exact_download_upload(native, monkeypatch):
    client = MagicMock()
    downloads, uploads = [], []
    def download(bucket, key, filename):
        downloads.append((bucket, key))
        Path(filename).write_bytes(PAYLOAD)
    def upload(filename, bucket, key):
        uploads.append((bucket, key, Path(filename).read_bytes()))
    client.download_file.side_effect = download
    client.upload_file.side_effect = upload
    monkeypatch.setattr("xtrek.storage.boto3.client", lambda *a, **kw: client)
    out = sign.sign_file("s3://source/sign/data.json", THUMB, output="s3://dest/result.sig")
    assert out == "s3://dest/result.sig"
    assert downloads == [("source", "sign/data.json")]
    assert uploads == [("dest", "result.sig", b"c2lnbmF0dXJl")]
    assert base64.b64decode(native.calls[0][0]) == PAYLOAD
    client.delete_object.assert_not_called()
    client.put_object_tagging.assert_not_called()


def test_local_to_s3_uses_independent_storage(native, monkeypatch, tmp_path):
    source = tmp_path / "data.txt"
    source.write_bytes(PAYLOAD)
    remote = MagicMock(spec=S3Storage)
    saved = []
    remote.upload.side_effect = lambda local, uri: saved.append((uri, Path(local).read_bytes()))
    monkeypatch.setattr(sign, "get_storage", lambda uri, cfg: remote if uri.startswith("s3://") else LocalStorage())
    sign.sign_file(source, THUMB, output="s3://test/result.sig")
    assert saved == [("s3://test/result.sig", b"c2lnbmF0dXJl")]


@pytest.mark.parametrize("phase", ["sign", "verify"])
def test_crypto_failure_never_publishes_signature(native, tmp_path, phase):
    source = tmp_path / "data.json"
    source.write_bytes(PAYLOAD)
    cls = native.module.SignedData
    method = "SignCades" if phase == "sign" else "VerifyCades"
    setattr(cls, method, MagicMock(side_effect=RuntimeError("SECRET data 0x80090006")))
    with pytest.raises(sign.SigningError) as error:
        sign.sign_file(source, THUMB)
    assert "SECRET" not in str(error.value)
    assert "0x80090006" in str(error.value)
    assert not Path(str(source) + ".sig").exists()
    native.store.Close.assert_called_once()


def test_attached_content_mismatch_is_rejected(native):
    def wrong(self, *args):
        self.Content = base64.b64encode(b"different").decode()
    native.module.SignedData.VerifyCades = wrong
    with pytest.raises(sign.SigningError, match="differs"):
        sign.sign_token_data("original", THUMB)


def test_storage_errors_are_redacted(native, monkeypatch, tmp_path):
    storage = MagicMock()
    storage.download.side_effect = RuntimeError("credential URL and payload")
    monkeypatch.setattr(sign, "get_storage", lambda *a: storage)
    with pytest.raises(sign.SigningError, match="^Cannot read source through storage$"):
        sign.sign_file("s3://test/x", THUMB)


def test_cli_token_stdin(native, monkeypatch, capsys):
    monkeypatch.setattr(sign.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(b"YQ==\r\n")))
    assert sign.main(["token", "-", "--thumbprint", THUMB]) == 0
    assert capsys.readouterr().out == "c2lnbmF0dXJl\n"
    assert base64.b64decode(native.calls[0][0]) == b"YQ==\r\n"
    assert native.calls[0][3] is False


def test_cli_document_file(native, tmp_path, capsys):
    source = tmp_path / "document.txt"
    source.write_bytes(PAYLOAD)
    assert sign.main(["document", str(source), "--thumbprint", THUMB]) == 0
    assert native.calls[0][3] is True
    assert capsys.readouterr().out.strip() == str(source) + ".sig"


def test_cli_auto_stdin_requires_mode(native, monkeypatch, capsys):
    monkeypatch.setattr(sign.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(b"data")))
    assert sign.main(["file", "-", "--thumbprint", THUMB]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "requires --mode" in captured.err
    assert not native.calls


def test_platform_and_optional_dependency(monkeypatch):
    monkeypatch.setattr(sign.sys, "platform", "win32")
    with pytest.raises(sign.SigningError, match="only on Linux"):
        sign.sign_document(b"data", THUMB)
    monkeypatch.setattr(sign.sys, "platform", "linux")
    monkeypatch.setattr(sign.importlib, "import_module", MagicMock(side_effect=ImportError("secret path")))
    with pytest.raises(sign.SigningError, match="official pycades"):
        sign.sign_document(b"data", THUMB)


def test_source_hardlink_cannot_be_output(native, tmp_path):
    import os
    source = tmp_path / "source.json"
    target = tmp_path / "source.sig"
    source.write_bytes(PAYLOAD)
    os.link(source, target)
    with pytest.raises(ValueError, match="differ"):
        sign.sign_file(source, THUMB, output=target)
    assert source.read_bytes() == PAYLOAD


def test_bad_native_base64_is_rejected(native):
    native.module.SignedData.SignCades = MagicMock(return_value="not base64!")
    with pytest.raises(sign.SigningError, match="sign failed"):
        sign.sign_document(b"x", THUMB)
    native.store.Close.assert_called_once()


def test_close_failure_preserves_primary_crypto_error(native):
    native.module.SignedData.SignCades = MagicMock(side_effect=RuntimeError("0x80090006 secret"))
    native.store.Close.side_effect = RuntimeError("close secret")
    with pytest.raises(sign.SigningError, match="sign failed.*0x80090006"):
        sign.sign_document(b"x", THUMB)


def test_upload_failure_is_reported(native, monkeypatch, tmp_path):
    source = tmp_path / "data.json"
    source.write_bytes(PAYLOAD)
    storage = LocalStorage()
    storage.upload = MagicMock(side_effect=RuntimeError("access-key secret"))
    monkeypatch.setattr(sign, "get_storage", lambda *a: storage)
    with pytest.raises(sign.SigningError, match="^Cannot write signature through storage$"):
        sign.sign_file(source, THUMB)
    assert source.read_bytes() == PAYLOAD


def test_cli_failure_does_not_output_signature(native, monkeypatch, capsys):
    monkeypatch.setattr(sign.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(b"data")))
    native.module.SignedData.VerifyCades = MagicMock(side_effect=RuntimeError("secret 0x80090006"))
    assert sign.main(["document", "-", "--thumbprint", THUMB]) == 1
    result = capsys.readouterr()
    assert not result.out
    assert "secret" not in result.err
    assert "0x80090006" in result.err


LOCAL_INN = '7701234567'
OTHER_INN = '7707654321'


def local_config():
    return {'sign': 's3://unused/sign', 'signing': {
        'local_by_inn': {LOCAL_INN: {'thumbprint': THUMB}}}}


def test_document_signer_local_never_uses_storage_and_preserves_bytes(native, monkeypatch):
    storage = MagicMock(side_effect=AssertionError('must not access signing storage'))
    monkeypatch.setattr(sign, 'get_storage', storage)
    with sign.DocumentSigner(local_config()).prepare(
            LOCAL_INN, PAYLOAD, 'document.json', None, 0) as signed:
        assert signed.body_path.read_bytes() == PAYLOAD
        assert signed.signature_path.read_text() == signed.signature == 'c2lnbmF0dXJl'
        directory = signed.body_path.parent
    assert not directory.exists()
    assert base64.b64decode(native.calls[0][0]) == PAYLOAD
    storage.assert_not_called()


def test_local_failure_does_not_fall_back_or_yield(native, monkeypatch):
    native.matches.Count = 0
    storage = MagicMock()
    monkeypatch.setattr(sign, 'get_storage', storage)
    with pytest.raises(sign.SigningError):
        with sign.DocumentSigner(local_config()).prepare(
                LOCAL_INN, PAYLOAD, 'document.json', '/unused', 0):
            pytest.fail('must not submit unsigned document')
    storage.assert_not_called()


def test_self_test_verifies_synthetic_data_only_and_uses_no_storage(native, monkeypatch, caplog):
    storage = MagicMock()
    monkeypatch.setattr(sign, 'get_storage', storage)
    with caplog.at_level('INFO'):
        result = sign.DocumentSigner(local_config()).self_test()
    assert result == {LOCAL_INN: True}
    assert base64.b64decode(native.calls[0][0]).startswith(b'xTrek local signing self-test\x00')
    assert 'passed' in caplog.text
    storage.assert_not_called()


def test_self_test_failure_is_advisory_and_rechecked_on_document(native, monkeypatch, caplog):
    signer = sign.DocumentSigner(local_config())
    native.matches.Count = 0
    assert signer.self_test() == {LOCAL_INN: False}
    assert 'failed' in caplog.text
    native.matches.Count = 1
    with signer.prepare(LOCAL_INN, b'real document', 'document.json', None, 0):
        pass
    assert len(native.calls) == 1


def test_self_test_continues_for_other_inns_without_logging_secrets(monkeypatch, caplog):
    config = local_config()
    config['signing']['local_by_inn'][OTHER_INN] = {'thumbprint': 'B2' * 20}
    calls = []
    def probe(data, **options):
        calls.append(options['thumbprint'])
        if options['thumbprint'] == THUMB:
            raise RuntimeError('secret PIN and private data')
        return 'c2ln'
    monkeypatch.setattr(sign, 'sign_document', probe)
    assert sign.DocumentSigner(config).self_test() == {LOCAL_INN: False, OTHER_INN: True}
    assert len(calls) == 2
    assert 'secret' not in caplog.text


@pytest.mark.parametrize('section', [None, [], {'local_by_inn': []},
    {'local_by_in': {}}, {'local_by_inn': {'bad-inn': {'thumbprint': THUMB}}}])
def test_invalid_routing_is_logged_and_cannot_silently_select_storage(section, monkeypatch):
    storage = MagicMock()
    monkeypatch.setattr(sign, 'get_storage', storage)
    signer = sign.DocumentSigner({'signing': section})
    assert signer.self_test() == {'configuration': False}
    with pytest.raises(sign.SigningError):
        with signer.prepare(LOCAL_INN, b'x', 'document.json', '/unused', 0):
            pytest.fail('invalid configuration must not sign')
    storage.assert_not_called()


@pytest.mark.parametrize('entry', [None, {}, {'thumbprint': 'invalid'},
    {'thumbprint': THUMB, 'store_location': 'auto'}, {'thumbprint': THUMB, 'pin': 'secret'}])
def test_bad_local_entry_is_not_treated_as_absent(entry, monkeypatch):
    storage = MagicMock()
    monkeypatch.setattr(sign, 'get_storage', storage)
    config = local_config()
    config['signing']['local_by_inn'][LOCAL_INN] = entry
    signer = sign.DocumentSigner(config)
    assert signer.self_test() == {LOCAL_INN: False}
    with pytest.raises(sign.SigningError):
        with signer.prepare(LOCAL_INN, b'x', 'document.json', '/unused', 0):
            pytest.fail('bad local entry must not sign')
    storage.assert_not_called()


def test_signing_snapshot_is_independent_of_config_changes_and_other_instances(native):
    config = local_config()
    first = sign.DocumentSigner(config)
    config['signing']['local_by_inn'][LOCAL_INN]['thumbprint'] = 'B2' * 20
    second = sign.DocumentSigner(config)
    with first.prepare(LOCAL_INN, b'a', 'same.json', None, 0) as a:
        with second.prepare(LOCAL_INN, b'b', 'same.json', None, 0) as b:
            assert a.body_path != b.body_path
            assert a.body_path.read_bytes() == b'a'
            assert b.body_path.read_bytes() == b'b'
    assert [c.args[1] for c in native.store.Certificates.Find.call_args_list] == [THUMB, 'B2' * 20]


@pytest.mark.parametrize('config', [{}, {'signing': {'local_by_inn': {}}}, local_config()])
def test_unlisted_inn_uses_storage_without_native_dependencies(config, tmp_path, monkeypatch):
    folder = tmp_path / 'exchange'
    folder.mkdir()
    config = dict(config, sign=str(folder))
    (folder / 'document.json.sig').write_text('c2ln\n')
    native = MagicMock(side_effect=AssertionError('native signer must not load'))
    monkeypatch.setattr(sign, '_load_pycades', native)
    monkeypatch.setattr(sign.time, 'sleep', lambda _: None)
    with sign.DocumentSigner(config).prepare(OTHER_INN, PAYLOAD, 'document.json', '/ignored', 1) as result:
        assert (folder / 'document.json').read_bytes() == PAYLOAD
        assert result.body_path.read_bytes() == PAYLOAD
        assert result.signature == 'c2ln'
    assert list(folder.iterdir()) == []
    native.assert_not_called()


def test_storage_timeout_cleans_source_without_yielding(tmp_path):
    signer = sign.DocumentSigner({'sign': str(tmp_path)})
    with pytest.raises(sign.SigningError, match='timed out'):
        with signer.prepare(OTHER_INN, b'x', 'document.json', None, 0):
            pytest.fail('must not submit without signature')
    assert not (tmp_path / 'document.json').exists()


def test_cleanup_failure_does_not_turn_successful_submission_into_retry(native, monkeypatch):
    real_cleanup = sign.shutil.rmtree
    monkeypatch.setattr(sign.shutil, 'rmtree', MagicMock(side_effect=OSError('cleanup failure')))
    with sign.DocumentSigner(local_config()).prepare(LOCAL_INN, b'x', 'document.json', None, 0) as signed:
        directory = signed.body_path.parent
    real_cleanup(directory)
