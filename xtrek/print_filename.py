"""Operator abbreviations for VDF print jobs."""
import re

_TRANSLIT = str.maketrans({
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e",
    "ё": "yo", "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k",
    "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
    "с": "s", "т": "t", "у": "u", "ф": "f", "х": "kh", "ц": "ts",
    "ч": "ch", "ш": "sh", "щ": "shch", "ъ": "", "ы": "y", "ь": "",
    "э": "e", "ю": "yu", "я": "ya",
})
_WORD = r"[А-Яа-яЁёA-Za-z]+(?:-[А-Яа-яЁёA-Za-z]+)*"
_FIO = re.compile(rf"{_WORD}\s+{_WORD}\s+{_WORD}")
_TOKENS = re.compile(rf"{_WORD}|[0-9]+")


def short_operator(value):
    text = value.strip() if isinstance(value, str) else ""
    is_fio = _FIO.fullmatch(text) and text.lower().endswith(("ич", "овна", "евна", "ична"))
    parts = []
    for index, token in enumerate(_TOKENS.findall(text)):
        token = token.replace("-", "")
        if token.isdigit():
            parts.append(token)
        elif not is_fio and token.isupper() and len(token) <= 4:
            parts.append(token.lower().translate(_TRANSLIT).upper())
        else:
            width = 5 if index == 0 else (1 if is_fio else 2)
            parts.append(token[:width].lower().translate(_TRANSLIT).capitalize())
    return "".join(parts) or "NA"


def operator_prefix(inn, passport):
    """Leave room for a 12-digit INN, two separators, UUID and .vdf in 255 characters."""
    operator = short_operator(passport.get("operator"))
    return f"{inn}_{operator[:201]}"
