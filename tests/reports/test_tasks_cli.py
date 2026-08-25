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
    assert "<b>Кодов:</b> 24<br>" in captured.out
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
