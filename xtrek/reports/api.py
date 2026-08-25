"""Public rendering API."""

from __future__ import annotations

from pathlib import Path
from typing import Union

from .errors import OptionalDependencyError
from .model import Report
from .options import OutputFormat, RenderOptions, RenderProfile


RenderedContent = Union[str, bytes]


def render(
    report: Report,
    *,
    output_format: str = "html",
    profile: str = "browser",
    theme: str = "default",
) -> RenderedContent:
    """Render a neutral report to HTML, Markdown, or PDF."""

    options = RenderOptions(
        output_format=OutputFormat.parse(output_format),
        profile=RenderProfile.parse(profile),
        theme=theme,
    )

    if options.output_format is OutputFormat.HTML:
        try:
            from .renderers.html import render_html
        except ImportError as exc:
            raise OptionalDependencyError(
                "HTML rendering requires Jinja2: pip install 'xtrek[reports]'"
            ) from exc

        return render_html(report, options)
    if options.output_format is OutputFormat.MARKDOWN:
        try:
            from .renderers.markdown import render_markdown
        except ImportError as exc:
            raise OptionalDependencyError(
                "Markdown rendering requires Jinja2: pip install 'xtrek[reports]'"
            ) from exc

        return render_markdown(report, options)

    from .renderers.pdf import render_pdf

    return render_pdf(report, options)


def render_to_file(
    report: Report,
    output: Union[str, Path],
    *,
    output_format: str = "html",
    profile: str = "browser",
    theme: str = "default",
) -> Path:
    """Render a report and write it to a file."""

    path = Path(output)
    content = render(
        report,
        output_format=output_format,
        profile=profile,
        theme=theme,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")
    return path
