"""CLI for the ``tasks-grouped`` report."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

from botocore.exceptions import BotoCoreError, ClientError

from .. import render
from ..errors import ReportsError
from ..tasks.data import TasksDataError
from ..tasks.source import (
    DEFAULT_BUCKET,
    DEFAULT_ENDPOINT_URL,
    DEFAULT_PREFIX,
    S3TaskSource,
)
from .data import collect, collect_range
from .document import build


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Отчет о заданиях с группировкой по артикулам"
    )
    period = result.add_mutually_exclusive_group(required=True)
    period.add_argument("--date", help="Дата отчёта в формате YYYY-MM-DD")
    period.add_argument(
        "--from",
        dest="date_from",
        help="Начало диапазона в формате YYYY-MM-DDTHH:MM",
    )
    result.add_argument(
        "--to",
        dest="date_to",
        help="Конец диапазона включительно, формат YYYY-MM-DDTHH:MM",
    )
    result.add_argument("--format", choices=("html", "md", "pdf"), default="html")
    result.add_argument(
        "--profile",
        choices=("browser", "printer", "messenger"),
        default=None,
    )
    result.add_argument("--theme", default="default")
    result.add_argument(
        "--details",
        choices=("auto", "full", "none"),
        default="auto",
        help="Расшифровка по файлам: автоматически, полностью или без неё",
    )
    result.add_argument("--output", help="Файл результата; HTML/MD без него идут в stdout")
    result.add_argument("--bucket", default=DEFAULT_BUCKET)
    result.add_argument("--prefix", default=DEFAULT_PREFIX)
    result.add_argument("--endpoint-url", default=DEFAULT_ENDPOINT_URL)
    result.add_argument("--region", default="ru-central1")
    result.add_argument("--timezone", default="Europe/Moscow")
    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    arguments = parser().parse_args(argv)
    if arguments.date_from and not arguments.date_to:
        parser().error("--to обязателен вместе с --from")
    if arguments.date and arguments.date_to:
        parser().error("--to можно использовать только вместе с --from")
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
        if arguments.date:
            data = collect(
                source,
                day=arguments.date,
                timezone_name=arguments.timezone,
            )
        else:
            data = collect_range(
                source,
                date_from=arguments.date_from,
                date_to=arguments.date_to,
                timezone_name=arguments.timezone,
            )
        include_details = arguments.details == "full" or (
            arguments.details == "auto" and profile != "messenger"
        )
        document = build(data, include_details=include_details)
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
    else:
        print("Для бинарного формата необходимо указать --output", file=sys.stderr)
        return 2
    return 0
