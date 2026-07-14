import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests


DEFAULT_BASE_URL = "https://be-rich.gs1ru.org/vbg-api/v3.2.4"
DEFAULT_TOKEN_PATH = "~/.gs1rus-7733154124"
DEFAULT_CACHE_PATH = "~/.cache/xtrek/vbg_gtin_cache.json"
DEFAULT_TTL_SECONDS = 30 * 24 * 60 * 60
DEFAULT_BATCH_SIZE = 50


class VbgAPIError(RuntimeError):
    pass


def normalize_gtin(gtin: Any) -> str:
    value = str(gtin).strip()
    if value.endswith(".0"):
        value = value[:-2]
    if not value.isdigit() or len(value) > 14:
        raise ValueError(f"Invalid GTIN: {gtin}")
    return value.zfill(14)


def read_token(token: Optional[str] = None, token_path: Optional[str] = None) -> str:
    config = _load_xtrek_config()
    return read_token_from_config(config, token=token, token_path=token_path)


def _load_xtrek_config() -> Dict[str, Any]:
    try:
        from .config_loader import load_config
        return load_config("suz_worker_config")
    except Exception:
        return {}


def read_token_from_config(
    config: Optional[Dict[str, Any]] = None,
    *,
    token: Optional[str] = None,
    token_path: Optional[str] = None,
) -> str:
    if token:
        return token.strip()

    env_token = os.getenv("GS1_VBG_TOKEN")
    if env_token:
        return env_token.strip()

    config = config or {}
    path = (
        token_path
        or os.getenv("GS1_VBG_TOKEN_FILE")
        or config.get("vbg_api_key_path")
        or config.get("vbg_token_path")
        or config.get("gs1_vbg_token_path")
        or DEFAULT_TOKEN_PATH
    )

    if str(path).startswith("s3://"):
        try:
            from .storage import get_storage
            return get_storage(path, config.get("s3_config")).read_text(path).strip()
        except Exception as exc:
            raise VbgAPIError(f"VbG token could not be read from S3 path: {path}") from exc

    token_file = Path(str(path)).expanduser()
    try:
        return token_file.read_text(encoding="utf-8").strip()
    except FileNotFoundError as exc:
        raise VbgAPIError(f"VbG token file not found: {token_file}") from exc


def upload_token_to_s3(
    remote_path: str,
    *,
    local_path: str = DEFAULT_TOKEN_PATH,
    config: Optional[Dict[str, Any]] = None,
) -> str:
    if not remote_path.startswith("s3://"):
        raise ValueError("remote_path must be an s3:// URL")
    config = config or _load_xtrek_config()
    local_file = Path(local_path).expanduser()
    if not local_file.exists():
        raise VbgAPIError(f"Local VbG token file not found: {local_file}")
    from .storage import get_storage
    get_storage(remote_path, config.get("s3_config")).upload(str(local_file), remote_path)
    return remote_path


def default_cache_path() -> Path:
    return Path(os.getenv("GS1_VBG_CACHE_PATH", DEFAULT_CACHE_PATH)).expanduser()


def load_cache(cache_path: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    path = Path(cache_path).expanduser() if cache_path else default_cache_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def save_cache(cache: Dict[str, Dict[str, Any]], cache_path: Optional[str] = None) -> None:
    path = Path(cache_path).expanduser() if cache_path else default_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def _chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]


def _is_fresh(entry: Dict[str, Any], ttl_seconds: int) -> bool:
    checked_at = entry.get("checked_at")
    return isinstance(checked_at, (int, float)) and time.time() - checked_at < ttl_seconds


def _fetch_gtins(
    gtins: List[str],
    token: str,
    base_url: str,
    timeout: int,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    http = session or requests.Session()
    response = http.post(
        f"{base_url.rstrip('/')}/gtin",
        json=gtins,
        headers={"Content-Type": "application/json", "Token": token},
        timeout=timeout,
    )
    if response.status_code >= 400:
        raise VbgAPIError(f"VbG /gtin failed: HTTP {response.status_code} {response.text[:500]}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise VbgAPIError(f"VbG /gtin returned non-JSON response: {response.text[:500]}") from exc

    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        raise VbgAPIError(f"VbG /gtin returned unexpected payload: {payload}")
    return data


def get_gtin_info(
    gtins: Iterable[Any],
    *,
    token: Optional[str] = None,
    token_path: Optional[str] = None,
    cache_path: Optional[str] = None,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    force_refresh: bool = False,
    timeout: int = 30,
    base_url: str = DEFAULT_BASE_URL,
    batch_size: int = DEFAULT_BATCH_SIZE,
    session: Optional[requests.Session] = None,
) -> Dict[str, Dict[str, Any]]:
    normalized = []
    for gtin in gtins:
        item = normalize_gtin(gtin)
        if item not in normalized:
            normalized.append(item)

    cache = load_cache(cache_path)
    result: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []

    for gtin in normalized:
        entry = cache.get(gtin)
        if not force_refresh and isinstance(entry, dict) and _is_fresh(entry, ttl_seconds):
            result[gtin] = entry.get("data", {})
        else:
            missing.append(gtin)

    if missing:
        api_token = read_token(token=token, token_path=token_path)
        now = time.time()
        for batch in _chunks(missing, batch_size):
            for item in _fetch_gtins(batch, api_token, base_url, timeout, session=session):
                key = normalize_gtin(item.get("gtin") or item.get("gtinCode") or batch[0])
                cache[key] = {"checked_at": now, "data": item}
                result[key] = item
        save_cache(cache, cache_path)

    return {gtin: result.get(gtin, {}) for gtin in normalized}


def _first_localized_value(value: Any) -> Any:
    if isinstance(value, dict):
        if "value" in value:
            return value.get("value")
        if value:
            return next(iter(value.values()))
    if isinstance(value, list) and value:
        return _first_localized_value(value[0])
    return value


def summarize_gtin(item: Dict[str, Any]) -> Dict[str, Any]:
    license_info = item.get("license") or {}
    return {
        "gtin": normalize_gtin(item.get("gtin", "")) if item.get("gtin") else None,
        "success": item.get("success"),
        "gtinRecordStatus": item.get("gtinRecordStatus"),
        "error": item.get("error"),
        "errorNote": item.get("errorNote"),
        "brandName": _first_localized_value(item.get("brandName")),
        "productDescription": _first_localized_value(item.get("productDescription")),
        "nkStatus": item.get("nkStatus"),
        "isComplete": item.get("isComplete"),
        "licenseeName": license_info.get("licenseeName"),
        "licenseeINN": license_info.get("licenseeINN"),
        "licenseeGLN": license_info.get("licenseeGLN"),
        "licenceKey": license_info.get("licenceKey"),
        "licenceStatus": license_info.get("licenceStatus"),
        "importersCount": len(item.get("importers") or []),
    }


def format_summary(summary: Dict[str, Any]) -> str:
    parts = [
        f"GTIN: {summary.get('gtin')}",
        f"success: {summary.get('success')}",
        f"owner: {summary.get('licenseeName') or '-'}",
        f"INN: {summary.get('licenseeINN') or '-'}",
        f"GLN: {summary.get('licenseeGLN') or '-'}",
        f"brand: {summary.get('brandName') or '-'}",
        f"name: {summary.get('productDescription') or '-'}",
    ]
    if summary.get("error") or summary.get("errorNote"):
        parts.append(f"error: {summary.get('error') or '-'} / {summary.get('errorNote') or '-'}")
    return "\n".join(parts)


def diagnose_gtins(gtins: Iterable[Any], **kwargs: Any) -> List[Dict[str, Any]]:
    return [summarize_gtin(item) for item in get_gtin_info(gtins, **kwargs).values()]


def main() -> None:
    parser = argparse.ArgumentParser(description="GS1 Russia VbG GTIN diagnostics")
    parser.add_argument("gtins", nargs="+", help="GTIN values")
    parser.add_argument("--token", help="VbG API token. By default GS1_VBG_TOKEN or token file is used.")
    parser.add_argument("--token-file", default=None, help=f"Token file path, default {DEFAULT_TOKEN_PATH}")
    parser.add_argument("--cache-path", default=None, help=f"Cache path, default {DEFAULT_CACHE_PATH}")
    parser.add_argument("--force-refresh", action="store_true", help="Ignore cached values")
    parser.add_argument("--json", action="store_true", help="Print JSON summaries")
    parser.add_argument("--upload-token-to-s3", help="Upload local VbG token file to this s3:// path")
    parser.add_argument("--local-token-file", default=DEFAULT_TOKEN_PATH, help="Local token file for --upload-token-to-s3")
    args = parser.parse_args()

    if args.upload_token_to_s3:
        upload_token_to_s3(args.upload_token_to_s3, local_path=args.local_token_file)
        print(f"Uploaded VbG token to {args.upload_token_to_s3}")
        return

    summaries = diagnose_gtins(
        args.gtins,
        token=args.token,
        token_path=args.token_file,
        cache_path=args.cache_path,
        force_refresh=args.force_refresh,
    )

    if args.json:
        print(json.dumps(summaries, ensure_ascii=False, indent=2))
        return

    for index, summary in enumerate(summaries):
        if index:
            print()
        print(format_summary(summary))


if __name__ == "__main__":
    main()
