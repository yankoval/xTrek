import pytest

from xtrek.reports import render
from xtrek.reports.tasks import TasksDataError, collect as collect_totals, collect_range as collect_totals_range
from xtrek.reports.tasks import build as build_totals
from xtrek.reports.tasks_grouped import build, collect, collect_range, cli
from xtrek.reports.tasks_goupped import collect as legacy_collect
from test_tasks_grouped import FakeSource
from test_tasks_by_operator import task
from test_tasks_grouped_cli import FakeSource as CLISource


@pytest.fixture
def source():
    return FakeSource([
        task("ivan-a", "Иван", 2, "A"),
        task("ivan-b", "Иван", 3, "B"),
        task("anna-a", "Анна", 4, "A"),
        task("anna-b", "Анна", 5, "B"),
        task("zero", "Пётр", 0, "C"),
        task("outside", "Пётр", 100, "C", "2026-08-24T10:00:00+03:00"),
    ])


@pytest.mark.parametrize("grouping", [(), ("operator",), ("article",),
                                     ("operator", "article"), ("article", "operator")])
@pytest.mark.parametrize("period", ["day", "range"])
def test_all_groupings_preserve_totals_and_leaf_membership(source, grouping, period):
    if period == "day":
        collector, args = collect, {"day": "2026-08-25"}
    else:
        collector, args = collect_range, {"date_from": "2026-08-25T10:00",
                                          "date_to": "2026-08-25T10:00"}
    data = collector(source, **args, group_by=grouping)
    assert data.group_by == grouping
    assert (data.tasks, data.labels, data.codes) == (4, 14, 42)

    def leaf_files(groups, depth):
        found = []
        for group in groups:
            if depth < len(grouping) - 1:
                assert group.tasks == sum(child.tasks for child in group.groups)
                assert group.labels == sum(child.labels for child in group.groups)
                assert group.codes == sum(child.codes for child in group.groups)
                found.extend(leaf_files(group.groups, depth + 1))
            else:
                assert not group.groups
                found.extend(item.file for item in group.files)
        return found

    files = leaf_files(data.groups, 0) if grouping else [f.file for f in data.files]
    assert sorted(files) == ["anna-a.json", "anna-b.json", "ivan-a.json", "ivan-b.json"]
    if grouping and grouping[0] == "operator":
        assert [g.value for g in data.groups] == ["Анна", "Иван"]
        assert [g.labels for g in data.groups] == [9, 5]
    elif grouping:
        assert [g.value for g in data.groups] == ["A", "B"]
        assert [g.labels for g in data.groups] == [6, 8]


@pytest.mark.parametrize("grouping,prefix,rows", [
    (("operator", "article"), "Итого по оператору", (("Анна", 2, 9, 27), ("Иван", 2, 5, 15))),
    (("article", "operator"), "Итого по артикулу", (("A", 2, 6, 18), ("B", 2, 8, 24))),
])
@pytest.mark.parametrize("details", [False, True])
@pytest.mark.parametrize("fmt", ["html", "md"])
def test_two_level_order_and_mandatory_subtotals(source, grouping, prefix, rows, details, fmt):
    data = collect(source, day="2026-08-25", group_by=grouping)
    doc = build(data, include_details=details)
    summary = doc.pages[0].blocks
    for index, (value, tasks, labels, codes) in enumerate(rows):
        assert summary[index * 2 + 1].rows[-1] == (f"{prefix}: {value}", tasks, labels, codes)
        assert [r[0] for r in summary[index * 2 + 1].rows[:-1]] == (
            ["A", "B"] if grouping[1] == "article" else ["Анна", "Иван"]
        )
    assert summary[-1].rows == (("Итого", 4, 14, 42),)
    text = render(doc, output_format=fmt, profile="messenger")
    assert text.count(prefix) == 2
    assert ("Расшифровка по файлам" in text) == details
    if details:
        for filename in ("anna-a", "anna-b", "ivan-a", "ivan-b"):
            escaped = filename.replace("-", "\\-") + "\\.json" if fmt == "md" else filename + ".json"
            assert text.count(escaped) == 1


@pytest.mark.parametrize("period", ["day", "range"])
def test_no_grouping_reuses_the_existing_summary_document(source, period):
    args = {"day": "2026-08-25"} if period == "day" else {
        "date_from": "2026-08-25T10:00", "date_to": "2026-08-25T10:00",
    }
    grouped_collector = collect if period == "day" else collect_range
    totals_collector = collect_totals if period == "day" else collect_totals_range
    expected = build_totals(totals_collector(source, **args))
    actual = build(grouped_collector(source, **args), include_details=True)
    assert actual.pages == expected.pages
    assert actual.title == expected.title
    assert actual.header == expected.header


@pytest.mark.parametrize("grouping", [None, [], (), "", "operator", "article",
                                     ["operator", "article"], ["article", "operator"]])
def test_empty_dataset(grouping):
    data = collect(FakeSource([]), day="2026-08-25", group_by=grouping)
    assert (data.tasks, data.labels, data.codes) == (0, 0, 0)
    assert data.groups == ()
    assert build(data, include_details=False).pages[0].blocks[0].rows[0][1:] == (0, 0, 0)


@pytest.mark.parametrize("fields", ["unknown", "aricle", ["operator", "operator"],
    ["article", "article"], ["operator", "article", "operator"], 123, {"operator": 1}])
def test_invalid_grouping_rejected_before_reading_source(fields):
    with pytest.raises(TasksDataError):
        collect(None, day="2026-08-25", group_by=fields)


def test_legacy_alias_and_python_string_list(source):
    explicit = collect(source, day="2026-08-25", group_by=["article", "operator"])
    alias = legacy_collect(source, day="2026-08-25", group_by="article operator")
    assert alias.groups == explicit.groups
    assert legacy_collect(source, day="2026-08-25").groups == ()


@pytest.mark.parametrize("grouping_args", [[], ["--group-by"], ["--group-by", "operator"],
    ["--group-by", "article"], ["--group-by", "operator", "article"],
    ["--group-by", "article", "operator"]])
@pytest.mark.parametrize("period", [["--date", "2026-08-25"],
    ["--from", "2026-08-25T12:59", "--to", "2026-08-25T13:00"]])
@pytest.mark.parametrize("fmt", ["html", "md", "pdf"])
def test_cli_all_groupings_and_formats(monkeypatch, tmp_path, grouping_args, period, fmt):
    monkeypatch.setattr(cli, "S3TaskSource", CLISource)
    expected_fields = grouping_args[1:]

    def renderer(doc, **options):
        assert doc.metadata["group_by"] == " ".join(expected_fields)
        assert options["output_format"] == fmt
        if not expected_fields:
            assert len(doc.pages) == 1
            assert doc.pages[0].blocks[0].rows[0][1:] == (1, 4, 24)
        else:
            assert len(doc.pages) == 2
            column = doc.pages[0].blocks[1 if len(expected_fields) == 2 else 0].columns[0]
            assert column.title == ("Оператор" if expected_fields[-1] == "operator" else "Артикул")
        return b"%PDF-test" if fmt == "pdf" else "report"

    monkeypatch.setattr(cli, "render", renderer)
    output = tmp_path / f"report.{fmt}"
    assert cli.main(period + grouping_args + ["--details", "full", "--format", fmt,
                                              "--output", str(output)]) == 0
    assert output.exists()


@pytest.mark.parametrize("fields", [["unknown"], ["aricle"], ["operator", "operator"],
                                   ["article", "operator", "article"]])
def test_cli_rejects_invalid_grouping_before_s3(monkeypatch, fields):
    def no_source(**kwargs):
        pytest.fail("Invalid grouping must not access S3")
    monkeypatch.setattr(cli, "S3TaskSource", no_source)
    with pytest.raises(SystemExit) as error:
        cli.main(["--date", "2026-08-25", "--group-by", *fields])
    assert error.value.code == 2


def test_empty_group_by_at_end_of_command(monkeypatch, capsys):
    monkeypatch.setattr(cli, "S3TaskSource", CLISource)
    assert cli.main(["--date", "2026-08-25", "--profile", "messenger", "--group-by"]) == 0
    output = capsys.readouterr().out
    assert "<b>Заданий:</b> 1" in output
    assert "Иван" not in output
    assert "A-100" not in output


@pytest.mark.parametrize("grouping", [None, "article"])
def test_unused_operator_does_not_affect_totals(grouping):
    data = collect(FakeSource([task("bad-operator", {"unexpected": "object"}, 2)]),
                   day="2026-08-25", group_by=grouping)
    assert (data.tasks, data.labels, data.codes) == (1, 2, 6)


@pytest.mark.parametrize("grouping", [("operator", "article"), ("article", "operator")])
def test_missing_values_form_named_groups_in_both_orders(grouping):
    item = task("missing", None)
    del item[1]["Article"]
    data = collect(FakeSource([item]), day="2026-08-25", group_by=grouping)
    expected = {"operator": "Без оператора", "article": "Без артикула"}
    assert data.groups[0].value == expected[grouping[0]]
    assert data.groups[0].groups[0].value == expected[grouping[1]]
    assert data.groups[0].labels == data.groups[0].groups[0].labels == 2
