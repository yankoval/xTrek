"""Mobile-first HTML presentation; never changes report data or file profiles."""

from __future__ import annotations

from html import escape
import textwrap

from ..model import Link, Report
from .values import format_value

LINE_WIDTH = 20
REPORT_TITLES = {
    "tasks": "ЗАДАНИЯ",
    "tasks-grouped": "ЗАДАНИЯ",
    "equipment-grouped": "ОТЧЁТЫ ОБОРУДОВАНИЯ",
}
LABELS = {
    "Паспортов (ярлыков)": "Ярлыков",
    "Дата и время создания отчёта": "Сформирован",
    "Дата и время от": "С",
    "Дата и время до": "По",
}


def _wrap(text: str) -> list[str]:
    # Long identifiers remain intact/copyable, even when wider than the target.
    return [line for part in text.splitlines() for line in textwrap.wrap(
        part, width=LINE_WIDTH, break_long_words=False, break_on_hyphens=False,
        replace_whitespace=False,
    )] or [""]


def render_compact_message(report: Report) -> str:
    lines: list[str] = []

    def add(text: str, *, bold: bool = False, link: Link | None = None):
        for part in _wrap(text):
            encoded = escape(part)
            if link is not None:
                encoded = f'<a href="{escape(link.url, quote=True)}">{encoded}</a>'
            lines.append(f"<b>{encoded}</b>" if bold else encoded)

    def pair(label, item, column=None):
        label = LABELS.get(label, label)
        value = format_value(item, column)
        if label in {"С", "По"}:
            add(f"{label} {value}")
            return
        if label == "Период" and " — " in value:
            start, end = value.split(" — ", 1)
            add("Период:")
            add(f"С {start}")
            add(f"По {end}")
            return
        if len(f"{label}: {value}") <= LINE_WIDTH and not isinstance(item, Link):
            # Keep the label bold but count only visible characters.
            lines.append(f"<b>{escape(label)}:</b> {escape(value)}")
        else:
            add(f"{label}:", bold=True)
            add(value, link=item if isinstance(item, Link) else None)

    def blocks(items):
        for block in items:
            if block.kind == "heading":
                lines.append("")
                if block.text.startswith(("Оператор: ", "Артикул: ")):
                    label, value = block.text.split(": ", 1)
                    pair(label, value)
                else:
                    add(block.text, bold=True)
            elif block.kind == "paragraph":
                add(format_value(block.text),
                    link=block.text if isinstance(block.text, Link) else None)
            elif block.kind == "key_value":
                pair(block.label, block.value)
            elif block.kind == "table":
                if block.caption:
                    add(block.caption, bold=True)
                for row_index, row in enumerate(block.rows):
                    lines.append("")
                    is_last = row_index == len(block.rows) - 1
                    for index, (column, cell) in enumerate(zip(block.columns, row)):
                        value = format_value(cell, column)
                        if is_last and index == 0 and value in {"Итого", "Общий итог"}:
                            add("ИТОГО", bold=True)
                        elif is_last and index == 0 and value.startswith("Итого по "):
                            # The original full group name is retained, not abbreviated.
                            add(value, bold=True)
                        else:
                            pair(column.title, cell, column)

    add(REPORT_TITLES[report.metadata["report"]], bold=True)
    groups = report.metadata.get("group_by", "").split()
    if groups:
        names = {"operator": "оператор", "article": "артикул"}
        grouping = ("по операторам" if groups == ["operator"] else
                    "по артикулам" if groups == ["article"] else
                    " → ".join(names[field] for field in groups))
        add(grouping)
    blocks(report.header)
    for page in report.pages:
        blocks(page.header)
        blocks(page.blocks)
        blocks(page.footer)
    blocks(report.footer)
    return "\n".join(lines).strip() + "\n"
