"""Linux CAdES-BES signing compatible with SignJS (Python 3.9+).

Importing this module does not load pycades or access a key. Signatures are
single-line Base64. Documents use detached CAdES, authentication challenges
use attached CAdES. No token issuance, document submission or queue polling.
"""
import argparse
import base64
import binascii
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
import importlib
import logging
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import tempfile
import threading
import time
from urllib.parse import unquote, urlparse

from .storage import get_storage

__all__ = ["SigningError", "DocumentSigner", "sign_bytes", "sign_document",
           "sign_token_data", "sign_file", "main"]
_NATIVE_LOCK = threading.RLock()
logger = logging.getLogger(__name__)


class SigningError(RuntimeError):
    """Signing failed; messages exclude key PINs, payloads and native error text."""


def _native_error(stage, exc):
    codes = sorted(set(re.findall(r"0x[0-9a-fA-F]{8}", str(exc))))
    suffix = " (" + ", ".join(codes) + ")" if codes else ""
    return SigningError("CAdES " + stage + " failed" + suffix)


def _thumbprint(value):
    if not isinstance(value, str):
        raise ValueError("thumbprint must be a SHA-1 certificate fingerprint")
    value = "".join(value.split()).upper()
    if not re.fullmatch(r"[0-9A-F]{40}", value):
        raise ValueError("thumbprint must contain exactly 40 hexadecimal characters")
    return value


def _pin_value(pin, pin_file):
    if pin is not None and pin_file is not None:
        raise ValueError("Use either pin or pin_file")
    if pin_file is not None:
        try:
            with open(pin_file, "r", encoding="utf-8", newline="") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                    raise SigningError("PIN file must be a regular file accessible only to its owner")
                pin = stream.read(4097)
        except OSError:
            raise SigningError("Cannot read PIN file") from None
        except UnicodeError:
            raise SigningError("PIN file must contain UTF-8 text") from None
        # Permit a text file's terminal newline; preserve spaces in the PIN.
        pin = pin.removesuffix("\n").removesuffix("\r")
    if pin is None:
        pin = ""
    if not isinstance(pin, str) or len(pin) > 4096 or any(c in pin for c in "\r\n\0"):
        raise ValueError("PIN must be a single line of text")
    return pin


def _load_pycades():
    if sys.platform != "linux":
        raise SigningError("Signing is supported only on Linux")
    try:
        return importlib.import_module("pycades")
    except (ImportError, OSError):
        raise SigningError("Install CryptoPro CSP, CAdES and official pycades for this Python") from None


def sign_bytes(data, thumbprint, *, detached=True, pin=None, pin_file=None,
               store_location="current_user"):
    """Sign and verify exact bytes; return CAdES-BES as whitespace-free Base64.

    Select exactly one certificate by SHA-1 fingerprint in the My store.
    ``store_location`` is current_user (default) or local_machine; no fallback.
    Certificate validation is enabled. Supply a PIN or a private UTF-8 PIN file
    for a protected key; omitted PIN means an empty PIN, never a CLI prompt.
    Native objects are created per call and calls are serialized within a process.
    """
    if not isinstance(data, bytes):
        raise TypeError("data must be bytes; encode text explicitly")
    if not isinstance(detached, bool):
        raise TypeError("detached must be a bool")
    thumbprint = _thumbprint(thumbprint)
    if store_location not in ("current_user", "local_machine"):
        raise ValueError("store_location must be current_user or local_machine")
    module = _load_pycades()
    key_pin = _pin_value(pin, pin_file)
    content = base64.b64encode(data).decode("ascii")
    # pycades wraps native state; never share stores/signers across requests.
    with _NATIVE_LOCK:
        store = None
        stage = "open store"
        succeeded = False
        try:
            store = module.Store()
            location = (module.CADESCOM_CURRENT_USER_STORE if store_location == "current_user"
                        else module.CADESCOM_LOCAL_MACHINE_STORE)
            store.Open(location, module.CAPICOM_MY_STORE, 0)
            stage = "select certificate"
            matches = store.Certificates.Find(module.CAPICOM_CERTIFICATE_FIND_SHA1_HASH, thumbprint)
            if matches.Count != 1:
                raise SigningError("Expected exactly one certificate matching thumbprint")
            certificate = matches.Item(1)
            if not certificate.HasPrivateKey():
                raise SigningError("Selected certificate has no private key")
            signer = module.Signer()
            signer.Certificate = certificate
            signer.CheckCertificate = True
            signer.KeyPin = key_pin
            signed = module.SignedData()
            signed.ContentEncoding = module.CADESCOM_BASE64_TO_BINARY
            signed.Content = content
            stage = "sign"
            signature = "".join(signed.SignCades(signer, module.CADESCOM_CADES_BES, detached).split())
            if not signature or not base64.b64decode(signature, validate=True):
                raise SigningError("CAdES returned an empty signature")
            stage = "verify"
            verified = module.SignedData()
            verified.ContentEncoding = module.CADESCOM_BASE64_TO_BINARY
            if detached:
                verified.Content = content
            verified.VerifyCades(signature, module.CADESCOM_CADES_BES, detached)
            if not detached and base64.b64decode(verified.Content, validate=True) != data:
                raise SigningError("Verified attached content differs from source bytes")
            succeeded = True
            return signature
        except SigningError:
            raise
        except Exception as exc:
            raise _native_error(stage, exc) from None
        finally:
            if store is not None:
                try:
                    store.Close()
                except Exception as exc:
                    if succeeded:
                        raise _native_error("close store", exc) from None


def sign_document(data, thumbprint, **options):
    """Sign document bytes with detached CAdES-BES; do not serialize JSON again."""
    return sign_bytes(data, thumbprint, detached=True, **options)


@dataclass(frozen=True)
class SignedDocument:
    body_path: Path
    signature_path: Path
    signature: str


class DocumentSigner:
    """Per-worker snapshot of document-signing settings; no native state.

    Startup self_test is advisory. Invalid settings remain invalid for real
    requests, and local signing failures never select storage as a fallback.
    Other configuration (including tokens) is not cached here.
    """

    def __init__(self, config):
        self._config = deepcopy({key: config[key] for key in
                                 ("signing", "sign", "SIGNING_TIMEOUT", "s3_config")
                                 if key in config})

    def _local_entries(self):
        section = self._config.get("signing", {})
        if not isinstance(section, dict) or set(section) - {"local_by_inn"}:
            raise SigningError("signing must be an object containing local_by_inn")
        entries = section.get("local_by_inn", {})
        if not isinstance(entries, dict):
            raise SigningError("signing.local_by_inn must be an object")
        if any(not isinstance(inn, str) or not re.fullmatch(r"[0-9]{10}|[0-9]{12}", inn)
               for inn in entries):
            raise SigningError("Local signing INNs must be strings of 10 or 12 digits")
        return entries

    def _local_options(self, inn):
        entries = self._local_entries()
        if inn not in entries:
            return None
        options = entries[inn]
        if not isinstance(options, dict) or set(options) - {"thumbprint", "store_location", "pin_file"}:
            raise SigningError("Local signing accepts thumbprint, store_location and pin_file only")
        try:
            thumbprint = _thumbprint(options.get("thumbprint"))
        except ValueError as exc:
            raise SigningError(str(exc)) from None
        location = options.get("store_location", "current_user")
        if location not in ("current_user", "local_machine"):
            raise SigningError("store_location must be current_user or local_machine")
        pin_file = options.get("pin_file")
        if pin_file is not None and (not isinstance(pin_file, str) or not pin_file):
            raise SigningError("pin_file must be a nonempty path string")
        return dict(thumbprint=thumbprint, store_location=location, pin_file=pin_file)

    def self_test(self):
        """Sign and verify synthetic bytes for each INN; log failures, never raise.

        No business documents, storage calls or API requests. Successful probes
        do not bypass certificate/signature checks on subsequent documents.
        """
        try:
            entries = self._local_entries()
        except SigningError as exc:
            logger.error("Local signing self-test: invalid configuration: %s", exc)
            return {"configuration": False}
        results = {}
        for inn in entries:
            try:
                options = self._local_options(inn)
                sign_document(b"xTrek local signing self-test\x00" + os.urandom(32), **options)
            except Exception as exc:
                # Native errors are sanitized by sign_bytes. Never log arbitrary
                # exception text: it may contain paths, PINs or provider details.
                reason = str(exc) if isinstance(exc, SigningError) else type(exc).__name__
                logger.error("Local signing self-test failed: inn=%s; %s", inn, reason)
                results[inn] = False
            else:
                logger.info("Local signing self-test passed: inn=%s", inn)
                results[inn] = True
        if not entries:
            logger.info("Local signing self-test: no local INNs configured")
        return results

    @contextmanager
    def prepare(self, inn, data, filename, signing_dir, timeout):
        """Keep exact body/.sig files alive through submission and receipt save."""
        if not isinstance(data, bytes):
            raise TypeError("Document signing requires bytes")
        inn = str(inn)
        if not re.fullmatch(r"[0-9]{10}|[0-9]{12}", inn):
            raise SigningError("Document signer INN must contain 10 or 12 digits")
        if (not isinstance(filename, str) or not filename.endswith(".json")
                or Path(filename).name != filename or "\\" in filename):
            raise SigningError("A document .json filename without directories is required")
        options = self._local_options(inn)
        temporary = Path(tempfile.mkdtemp(prefix="xtrek-document-"))
        storage = None
        remote_paths = ()
        started = time.monotonic()
        mode = "local" if options is not None else "storage"
        try:
            body_path = temporary / filename
            signature_path = temporary / (filename + ".sig")
            body_path.write_bytes(data)
            logger.info("Document signing: inn=%s mode=%s document=%s", inn, mode, filename)
            if options is not None:
                signature = sign_document(data, **options)
                signature_path.write_text(signature, encoding="ascii")
            else:
                signing_dir = self._config.get("sign") or signing_dir
                timeout = self._config.get("SIGNING_TIMEOUT", timeout)
                storage = get_storage(signing_dir, self._config.get("s3_config"))
                remote_body = f"{signing_dir.rstrip('/')}/{filename}"
                remote_signature = remote_body + ".sig"
                remote_paths = (remote_body, remote_signature)
                storage.upload(str(body_path), remote_body)
                waiting_since = time.monotonic()
                while not storage.exists(remote_signature):
                    if time.monotonic() - waiting_since >= timeout:
                        raise SigningError("Storage signature wait timed out")
                    time.sleep(2)
                # Preserve the existing writer-settle delay for filesystem signers.
                time.sleep(0.5)
                storage.download(remote_signature, str(signature_path))
                signature = signature_path.read_text(encoding="utf-8").strip()
                if not signature:
                    raise SigningError("Storage returned an empty signature")
            logger.info("Document signed: inn=%s mode=%s duration=%.3fs", inn, mode,
                        time.monotonic() - started)
            yield SignedDocument(body_path, signature_path, signature)
        finally:
            for remote in remote_paths:
                try:
                    if storage.exists(remote):
                        storage.delete(remote)
                except Exception:
                    logger.warning("Signing storage cleanup failed: inn=%s document=%s", inn, filename)
            try:
                shutil.rmtree(temporary)
            except OSError:
                # Cleanup must not turn an accepted document into a retry.
                logger.warning("Local signing cleanup failed: inn=%s document=%s", inn, filename)


def sign_token_data(data, thumbprint, *, detached=False, **options):
    """Sign auth/key's data as literal UTF-8 text (or exact bytes).

    The default attached signature is shared by True API JWT/UUID and SUZ
    authentication. Do not Base64-decode the challenge. SUZ signed-ping uses
    detached=True for its exact request path, including the query string.
    This function neither requests a challenge nor issues a token.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return sign_bytes(data, thumbprint, detached=detached, **options)


def _location(value):
    value = os.fspath(value)
    parsed = urlparse(value)
    if parsed.scheme == "s3":
        if not parsed.netloc or not parsed.path.lstrip("/") or parsed.path.endswith("/"):
            raise ValueError("An S3 object URI is required, not a bucket or prefix")
        if parsed.query or parsed.fragment:
            raise ValueError("Use s3://bucket/key, without query or fragment")
        return value
    if parsed.scheme == "file":
        if parsed.netloc not in ("", "localhost") or parsed.query or parsed.fragment:
            raise ValueError("Only local file:// URIs are supported")
        value = unquote(parsed.path)
    elif parsed.scheme:
        raise ValueError("Use a local path, file:// URI or s3://bucket/key")
    return str(Path(value).expanduser().resolve())


def _mode(source, detached):
    if detached is not None:
        if not isinstance(detached, bool):
            raise TypeError("detached must be a bool or None")
        return detached
    # SignJS cloud: .txt attached, .json and unknown extensions detached.
    return not source.endswith(".txt")


def _write_signature(signature, target, s3_config):
    try:
        with tempfile.TemporaryDirectory(prefix="xtrek-sign-") as temporary:
            local = Path(temporary) / "signature.sig"
            local.write_bytes(signature.encode("ascii"))
            get_storage(target, s3_config).upload(str(local), target)
    except Exception:
        raise SigningError("Cannot write signature through storage") from None


def sign_file(source, thumbprint, *, output=None, detached=None, s3_config=None,
              pin=None, pin_file=None, store_location="current_user"):
    """Sign a local/S3 object, write Base64 to <source>.sig, return its location.

    ``output`` may select another local/S3 object. Explicit output is replaced
    on success, as in SignJS. No existing signature is trusted or reused.
    Reading uses storage.download, preserving BOM, CRLF and all binary bytes.
    The source is never rewritten/deleted or marked processed. This is a single
    object operation; queue ownership and retries belong to the calling task.
    """
    source = _location(source)
    target = _location(output) if output is not None else source + ".sig"
    same_local_file = (not source.startswith("s3://") and not target.startswith("s3://")
                       and Path(source).exists() and Path(target).exists()
                       and os.path.samefile(source, target))
    if source == target or same_local_file:
        raise ValueError("Signature output must differ from source")
    detached = _mode(source, detached)
    try:
        with tempfile.TemporaryDirectory(prefix="xtrek-sign-") as temporary:
            local = Path(temporary) / "payload.bin"
            get_storage(source, s3_config).download(source, str(local))
            content = local.read_bytes()
    except Exception:
        raise SigningError("Cannot read source through storage") from None
    signature = sign_bytes(content, thumbprint, detached=detached, pin=pin,
                           pin_file=pin_file, store_location=store_location)
    _write_signature(signature, target, s3_config)
    return target


def main(argv=None):
    """CLI entry point; - reads stdin bytes and prints Base64 unless -o is given."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name, description in (("file", "Infer mode: .txt attached, otherwise detached"),
                              ("document", "Detached document signature"),
                              ("token", "Attached authentication-data signature")):
        sub = commands.add_parser(name, help=description)
        sub.add_argument("source", help="Local path, file:// or s3:// URI; - for stdin")
        sub.add_argument("--thumbprint", required=True)
        sub.add_argument("-o", "--output", help="Signature path/URI (default: SOURCE.sig)")
        sub.add_argument("--pin-file", help="Owner-only UTF-8 PIN file; PIN is never a CLI argument")
        sub.add_argument("--store-location", choices=("current_user", "local_machine"), default="current_user")
        sub.add_argument("--s3-config", help="JSON file with storage.S3Storage settings")
        if name == "file":
            sub.add_argument("--mode", choices=("auto", "attached", "detached"), default="auto")
    args = parser.parse_args(argv)
    try:
        config = None
        if args.s3_config:
            import json
            try:
                config = json.loads(Path(args.s3_config).read_text(encoding="utf-8"))
                if not isinstance(config, dict):
                    raise ValueError()
            except (OSError, ValueError, UnicodeError):
                raise SigningError("Cannot read S3 configuration object") from None
        detached = {"document": True, "token": False}.get(args.command)
        if args.command == "file":
            detached = {"auto": None, "attached": False, "detached": True}[args.mode]
        options = dict(pin_file=args.pin_file, store_location=args.store_location)
        if args.source == "-":
            if detached is None:
                raise ValueError("stdin requires --mode attached/detached or document/token command")
            signature = sign_bytes(sys.stdin.buffer.read(), args.thumbprint, detached=detached, **options)
            if args.output:
                target = _location(args.output)
                _write_signature(signature, target, config)
                print(target)
            else:
                print(signature)
        else:
            print(sign_file(args.source, args.thumbprint, output=args.output,
                            detached=detached, s3_config=config, **options))
        return 0
    except (SigningError, ValueError, TypeError, binascii.Error) as exc:
        print("xtrek-sign: " + str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
