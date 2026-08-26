from datetime import datetime, timezone

from xtrek.reports.tasks import TaskObjectRef
from xtrek.reports.tasks import cli


class FakeSource:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def list_objects(self):
        return [
            TaskObjectRef(
                "Задания/task.json",
                datetime(2026, 8, 25, 10, tzinfo=timezone.utc),
            )
        ]

    def read_object(self, ref):
        return {"Quantity": 4, "PasportData": {"Product_PackQty": 6}}


def test_cli_prints_messenger_html_to_stdout(monkeypatch, capsys):
    monkeypatch.setattr(cli, "S3TaskSource", FakeSource)

    code = cli.main(
        [
            "--date",
            "2026-08-25",
            "--format",
            "html",
            "--profile",
            "messenger",
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert captured.out.startswith("<b>Отчёт о полученных заданиях</b>")
    assert "<b>Кодов:</b> 24\n" in captured.out
    assert "<br" not in captured.out
    assert captured.err == ""


def test_cli_writes_markdown_file(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(cli, "S3TaskSource", FakeSource)
    output = tmp_path / "tasks.md"

    code = cli.main(
        [
            "--date",
            "2026-08-25",
            "--format",
            "md",
            "--output",
            str(output),
        ]
    )

    assert code == 0
    assert output.read_text(encoding="utf-8").startswith("# Отчёт о полученных заданиях")
    assert capsys.readouterr().out.strip() == str(output)


def test_cli_supports_inclusive_minute_range(monkeypatch, capsys):
    monkeypatch.setattr(cli, "S3TaskSource", FakeSource)

    code = cli.main(
        [
            "--from",
            "2026-08-25T12:59",
            "--to",
            "2026-08-25T13:00",
            "--format",
            "html",
            "--profile",
            "messenger",
        ]
    )

    content = capsys.readouterr().out
    assert code == 0
    assert "<b>Период:</b> 25.08.2026 12:59 — 25.08.2026 13:00\n" in content
    assert "<b>Заданий:</b> 1\n" in content
