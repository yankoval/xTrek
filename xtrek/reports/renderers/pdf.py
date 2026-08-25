"""Optional WeasyPrint PDF renderer."""

from __future__ import annotations

from ..errors import OptionalDependencyError
from ..model import Report
from ..options import RenderOptions, RenderProfile
from .html import render_html


def render_pdf(report: Report, options: RenderOptions) -> bytes:
    try:
        from weasyprint import HTML
    except (ImportError, OSError) as exc:
        raise OptionalDependencyError(
            "PDF rendering requires WeasyPrint and its system libraries. "
            "Install them, then run: pip install 'xtrek[reports-pdf]'"
        ) from exc

    print_options = RenderOptions(
        output_format=options.output_format,
        profile=(
            options.profile
            if options.profile is not RenderProfile.BROWSER
            else RenderProfile.PRINTER
        ),
        theme=options.theme,
    )
    html = render_html(report, print_options)
    return HTML(string=html).write_pdf()
