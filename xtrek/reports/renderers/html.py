"""Jinja2 HTML renderer."""

from __future__ import annotations

from jinja2 import Environment, PackageLoader

from ..model import Report
from ..options import RenderOptions
from .resources import load_styles
from .values import effective_alignment, format_value


def _border_css(border) -> str:
    if border.width == 0 or border.style == "none":
        return "none"
    return f"{border.width:g}pt {border.style} {border.color}"


def _table_css(table) -> str:
    style = table.style
    return "; ".join(
        (
            f"--outer-border: {_border_css(style.outer_border)}",
            f"--row-border: {_border_css(style.row_border)}",
            f"--column-border: {_border_css(style.column_border)}",
            f"--header-border: {_border_css(style.header_border)}",
            f"--cell-padding: {style.cell_padding:g}px",
        )
    )


def _column_css(column, value=None, *, header=False) -> str:
    if header:
        alignment = column.header_align or (
            column.align if column.align in {"center", "right"} else
            "right" if column.align == "decimal" else "left"
        )
    else:
        alignment = effective_alignment(value, column)
    declarations = [
        f"text-align: {alignment}",
        f"vertical-align: {column.vertical_align}",
    ]
    if column.width:
        declarations.append(f"width: {column.width}")
    return "; ".join(declarations)


def render_html(report: Report, options: RenderOptions) -> str:
    environment = Environment(
        loader=PackageLoader("xtrek.reports", "templates/html"),
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    environment.filters["display"] = format_value
    environment.filters["table_css"] = _table_css
    environment.filters["cell_css"] = lambda value, column: _column_css(
        column, value
    )
    environment.globals["header_css"] = lambda column: _column_css(
        column, header=True
    )
    template = environment.get_template("report.html.jinja")
    css = ""
    if options.profile.value != "messenger":
        css = load_styles(options.profile.value, options.theme)
    rendered = template.render(
        report=report,
        profile=options.profile.value,
        css=css,
    )
    return rendered.strip() + "\n"
