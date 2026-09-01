"""Validation and normalization of equipment reports for aggregate operations."""

from typing import Any, Dict, List, Tuple


AGGREGATE_OPERATION_MAX_CODES = 30000


def validate_participant_inn(participant_inn: Any) -> str:
    participant_inn = str(participant_inn or "").strip()
    if not participant_inn.isdigit() or len(participant_inn) not in (10, 12):
        raise ValueError("ИНН участника должен содержать 10 или 12 цифр")
    return participant_inn


def normalize_kitu(code: Any, field_name: str = "КИТУ") -> str:
    """Return the GS1 AI 00 representation required by True API documents."""
    clean_code = str(code or "").strip()
    if len(clean_code) == 18 and clean_code.isdigit():
        return f"00{clean_code}"
    if len(clean_code) == 20 and clean_code.startswith("00") and clean_code.isdigit():
        return clean_code
    raise ValueError(
        f"{field_name} должен содержать 18 цифр SSCC или 20 цифр с префиксом AI 00"
    )


def validate_codes(
    codes: Any,
    field_name: str = "codes",
    allow_group_separator: bool = False,
) -> List[str]:
    if isinstance(codes, (str, bytes)) or not isinstance(codes, (list, tuple)):
        raise ValueError(f"{field_name} должен быть списком кодов")
    if not codes:
        raise ValueError(f"{field_name} не содержит кодов")
    if len(codes) > AGGREGATE_OPERATION_MAX_CODES:
        raise ValueError(
            f"Количество кодов {len(codes)} превышает лимит "
            f"{AGGREGATE_OPERATION_MAX_CODES}"
        )

    clean_codes = []
    seen = set()
    for index, code in enumerate(codes, start=1):
        if not isinstance(code, str) or not code.strip():
            raise ValueError(f"Пустой или некорректный код в позиции {index}")
        clean_code = code.strip()
        if any(
            character.isspace()
            and not (allow_group_separator and character == "\u001d")
            for character in clean_code
        ):
            raise ValueError(f"Код в позиции {index} содержит пробельные символы")
        if clean_code in seen:
            raise ValueError(f"Коды должны быть уникальны: {clean_code}")
        seen.add(clean_code)
        clean_codes.append(clean_code)
    return clean_codes


def normalize_disaggregation_report(data: Any) -> Dict[str, Any]:
    """Convert a disaggregation equipment report to a True API task body."""
    if not isinstance(data, dict):
        raise ValueError("Отчет о расформировании должен быть JSON-объектом")
    if set(data) != {"participant_inn", "products_list"}:
        raise ValueError(
            "Отчет о расформировании должен содержать только participant_inn "
            "и products_list"
        )

    participant_inn = validate_participant_inn(data.get("participant_inn"))
    products = data.get("products_list")
    if not isinstance(products, list) or not products:
        raise ValueError("В отчете отсутствует массив products_list")

    codes = []
    for index, product in enumerate(products, start=1):
        if not isinstance(product, dict) or set(product) != {"uitu"}:
            raise ValueError(f"products_list[{index}] должен содержать только поле uitu")
        codes.append(product.get("uitu"))
    codes = validate_codes(codes, "products_list")

    return {
        "participant_inn": participant_inn,
        "products_list": [
            {"uitu": normalize_kitu(code, f"products_list[{index}].uitu")}
            for index, code in enumerate(codes, start=1)
        ],
    }


def normalize_reaggregation_removing_report(
    data: Any,
) -> Tuple[Dict[str, Any], str]:
    """Convert a REMOVING equipment report to a True API task body."""
    if not isinstance(data, dict):
        raise ValueError("Отчет об изъятии должен быть JSON-объектом")
    expected_keys = {
        "participant_inn",
        "reaggregation_type",
        "uitu",
        "uit_uitu_list",
    }
    if set(data) != expected_keys:
        raise ValueError(
            "Отчет об изъятии должен содержать только participant_inn, "
            "reaggregation_type, uitu и uit_uitu_list"
        )

    participant_inn = validate_participant_inn(data.get("participant_inn"))
    if data.get("reaggregation_type") != "REMOVING":
        raise ValueError("Поддерживается только reaggregation_type=REMOVING")
    target = normalize_kitu(data.get("uitu"), "uitu")

    items = data.get("uit_uitu_list")
    if not isinstance(items, list) or not items:
        raise ValueError("В отчете отсутствует массив uit_uitu_list")

    fields = []
    codes = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"uit_uitu_list[{index}] должен быть объектом")
        present = [field for field in ("uit_uitu", "kitu") if item.get(field)]
        if len(present) != 1 or set(item) != {present[0]}:
            raise ValueError(
                f"uit_uitu_list[{index}] должен содержать ровно одно поле: "
                "uit_uitu или kitu"
            )
        fields.append(present[0])
        codes.append(item[present[0]])

    if len(set(fields)) != 1:
        raise ValueError("Нельзя смешивать uit_uitu и kitu в одном документе")
    code_field = fields[0]
    codes = validate_codes(codes, "uit_uitu_list", allow_group_separator=True)
    if code_field == "uit_uitu":
        codes = [code.split("\u001d")[0] for code in codes]
        codes = validate_codes(codes, "uit_uitu_list")
    else:
        codes = [
            normalize_kitu(code, f"uit_uitu_list[{index}].kitu")
            for index, code in enumerate(codes, start=1)
        ]

    if target in codes:
        raise ValueError("Код трансформируемого агрегата нельзя изъять из самого себя")

    return (
        {
            "participant_inn": participant_inn,
            "reaggregation_type": "REMOVING",
            "uitu": target,
            "uit_uitu_list": [{code_field: code} for code in codes],
        },
        code_field,
    )


def validate_disaggregation_payload(payload: Any) -> Dict[str, Any]:
    normalized = normalize_disaggregation_report(payload)
    if normalized != payload:
        raise ValueError("Коды агрегатов должны быть записаны с префиксом AI 00")
    return payload


def validate_reaggregation_removing_payload(payload: Any) -> str:
    normalized, code_field = normalize_reaggregation_removing_report(payload)
    if normalized != payload:
        raise ValueError("Коды агрегатов должны быть записаны с префиксом AI 00")
    return code_field
