"""Load bundled templates and styles without relying on the current directory."""

from __future__ import annotations

import re
from importlib import resources


_STYLE_NAME = re.compile(r"^[a-z0-9_-]+$")


def load_styles(profile: str, theme: str) -> str:
    if not _STYLE_NAME.fullmatch(profile) or not _STYLE_NAME.fullmatch(theme):
        raise ValueError("Style names may contain only lowercase letters, digits, _ and -")

    root = resources.files("xtrek.reports")
    paths = (
        root.joinpath("styles", "base.css"),
        root.joinpath("styles", "themes", f"{theme}.css"),
        root.joinpath("styles", "profiles", f"{profile}.css"),
    )
    try:
        return "\n".join(path.read_text(encoding="utf-8") for path in paths)
    except FileNotFoundError as exc:
        raise ValueError(f"Unknown report profile or theme: {profile}/{theme}") from exc
