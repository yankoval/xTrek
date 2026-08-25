"""Command-line interface for the received-tasks report."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

from botocore.exceptions import BotoCoreError, ClientError

from .. import render
from ..errors import ReportsError
from .data import TasksDataError, collect
from .document import build
from .source import DEFAULT_BUCKET, DEFAULT_ENDPOINT_URL, DEFAULT_PREFIX, S3TaskSource


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Отчёт о полученных заданиях")
    result.add_argument("--date", required=True, help="Дата отчёта в формате YYYY-MM-DD")
    result.add_argument("--format", choices=("html", "md", "pdf"), default="html")
    result.add_argument(
        "--profile",
        choices=("browser", "printer", "messenger"),
        default=None,
    )
    result.add_argument("--theme", default="default")
    result.add_argument("--output", help="Файл результата; HTML/MD без него идут в stdout")
    result.add_argument("--bucket", default=DEFAULT_BUCKET)
    result.add_argument("--prefix", default=DEFAULT_PREFIX)
    result.add_argument("--endpoint-url", default=DEFAULT_ENDPOINT_URL)
    result.add_argument("--region", default="ru-central1")
    result.add_argument("--timezone", default="Europe/Moscow")
    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = parser().parse_args(argv)
    if arguments.format == "pdf" and not arguments.output:
        parser().error("--output обязателен для PDF")

    profile = arguments.profile or (
        "printer" if arguments.format == "pdf" else "browser"
    )
    try:
        source = S3TaskSource(
            bucket=arguments.bucket,
            prefix=arguments.prefix,
            endpoint_url=arguments.endpoint_url,
            region_name=arguments.region,
        )
        data = collect(source, day=arguments.date, timezone_name=arguments.timezone)
        document = build(data)
        content = render(
            document,
            output_format=arguments.format,
            profile=profile,
            theme=arguments.theme,
        )
    except (
        TasksDataError,
        ReportsError,
        BotoCoreError,
        ClientError,
        OSError,
        ValueError,
    ) as exc:
        print(f"Ошибка формирования отчёта: {exc}", file=sys.stderr)
        return 1

    if arguments.output:
        output = Path(arguments.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            output.write_bytes(content)
        else:
            output.write_text(content, encoding="utf-8")
        print(output)
    elif isinstance(content, str):
        print(content, end="")
    else:  # guarded above, kept for alternative binary renderers
        print("Для бинарного формата необходимо указать --output", file=sys.stderr)
        return 2
    return 0
