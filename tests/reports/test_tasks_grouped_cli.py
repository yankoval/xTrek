from datetime import datetime, timezone

from xtrek.reports.tasks import TaskObjectRef
from xtrek.reports.tasks_grouped import cli


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
        return {
            "Article": "A-100",
            "Quantity": 4,
            "PasportData": {"Product_PackQty": 6},
        }


def test_cli_writes_grouped_markdown(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(cli, "S3TaskSource", FakeSource)
    output = tmp_path / "tasks-grouped.md"

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

    content = output.read_text(encoding="utf-8")
    assert code == 0
    assert content.startswith("# Отчет о заданиях с группировкой по артикулам")
    assert "| A\\-100 | 1 | 4 | 24 |" in content
    assert "| Итого | 1 | 4 | 24 |" in content
    assert "## Расшифровка по файлам" in content
    assert "| 13:00 | 4 | 24 | task\\.json |" in content
    assert capsys.readouterr().out.strip() == str(output)


def test_cli_omits_file_details_from_messenger_by_default(monkeypatch, capsys):
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

    content = capsys.readouterr().out
    assert code == 0
    assert "A-100" in content
    assert "Расшифровка по файлам" not in content
    assert "task.json" not in content


def test_cli_supports_grouped_minute_range(monkeypatch, capsys):
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
    assert "<b>Дата и время от:</b> 25.08.2026 12:59\n" in content
    assert "<b>Дата и время до:</b> 25.08.2026 13:00\n" in content
    assert "A-100" in content
