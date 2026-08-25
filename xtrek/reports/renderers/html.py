"""Jinja2 HTML renderer."""

from __future__ import annotations

from jinja2 import Environment, PackageLoader

from ..model import Report
from ..options import RenderOptions
from .resources import load_styles


def render_html(report: Report, options: RenderOptions) -> str:
    environment = Environment(
        loader=PackageLoader("xtrek.reports", "templates/html"),
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
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
