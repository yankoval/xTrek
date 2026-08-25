"""Jinja2 Markdown renderer."""

from __future__ import annotations

import re

from jinja2 import Environment, PackageLoader

from ..model import Report
from ..options import RenderOptions
from .values import format_value, markdown_alignment, markdown_url


def _escape_markdown(value: object) -> str:
    return re.sub(r"([\\`*_{}\[\]()#+.!|>-])", r"\\\1", str(value))


def render_markdown(report: Report, options: RenderOptions) -> str:
    environment = Environment(
        loader=PackageLoader("xtrek.reports", "templates/markdown"),
        autoescape=False,
        keep_trailing_newline=True,
    )
    environment.filters["md"] = _escape_markdown
    environment.filters["display"] = format_value
    environment.filters["md_align"] = markdown_alignment
    environment.filters["md_url"] = markdown_url
    template = environment.get_template("report.md.jinja")
    rendered = template.render(report=report, profile=options.profile.value)
    lines = [line.rstrip() for line in rendered.splitlines()]
    if options.profile.value == "messenger":
        return "\n".join(line for line in lines if line).strip() + "\n"
    compact = []
    for line in lines:
        if line or not compact or compact[-1]:
            compact.append(line)
    return "\n".join(compact).strip() + "\n"
