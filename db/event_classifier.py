from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


TARGET_EVENTS = [
    "Тренд репорты",
    "Демо Ред",
    "Демо Блю",
    "ПБХ",
    "Blue Team Stepik",
    "Опен дэй",
    "Встреча с экспертом",
    "Другое",
]


@dataclass(frozen=True)
class ClassificationResult:
    event: str
    source_field: str
    matched_pattern: str
    confidence: str


def normalize_text(v: object) -> str:
    s = "" if v is None else str(v)
    s = s.strip().lower().replace("ё", "е")
    s = re.sub(r"[_\-]+", " ", s)
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


FIELD_PRIORITY = [
    "Название сделки",
    "Код_курса_сайт",
    "Код курса",
    "UTM Campaign",
    "Источник (подробно)",
    "Источник обращения",
]


# Ordered high-precision rules.
# 'Attacking January' is first so it takes priority over generic event matching.
EVENT_RULES = [
    ("Attacking January", [r"атакующ\w*\s+январ\w*", r"attacking[_ ]?january"]),
    ("Тренд репорты", [r"\bтренд\b", r"\btrend report"]),
    ("Демо Ред", [r"\bдемо ред\b", r"\bdemo red\b", r"\bprof pentest demo\b"]),
    ("Демо Блю", [r"\bдемо блю\b", r"\bdemo blue\b", r"\bprof soc demo\b"]),
    ("ПБХ", [r"\bпбх\b"]),
    ("Blue Team Stepik", [r"\bblue team stepik\b", r"\bstepik\b"]),
    ("Опен дэй", [r"\bопен деи\b", r"\bопен дэй\b", r"\bopen day\b"]),
    ("Встреча с экспертом", [r"\bвстреча с экспертом\b", r"\bexpert meeting\b"]),
]


def _confidence_for_field(field: str) -> str:
    if field == "Название сделки":
        return "high"
    if field in {"Код_курса_сайт", "Код курса"}:
        return "high"
    if field == "UTM Campaign":
        return "medium"
    return "low"


def classify_event_from_row(row: dict, fields: Iterable[str] = FIELD_PRIORITY) -> ClassificationResult:
    for field in fields:
        raw = row.get(field, "")
        txt = normalize_text(raw)
        if not txt:
            continue
        for event, patterns in EVENT_RULES:
            for pattern in patterns:
                if re.search(pattern, txt, flags=re.IGNORECASE):
                    return ClassificationResult(
                        event=event,
                        source_field=field,
                        matched_pattern=pattern,
                        confidence=_confidence_for_field(field),
                    )
    return ClassificationResult(
        event="Другое",
        source_field="",
        matched_pattern="",
        confidence="low",
    )


def is_attacking_january(row: dict, fields: Iterable[str] = FIELD_PRIORITY) -> bool:
    """Compat shim — use classify_event_from_row(...).event == 'Attacking January' instead."""
    return classify_event_from_row(row, fields).event == "Attacking January"


def normalize_course_code(raw: object) -> str:
    s0 = "" if raw is None else str(raw)
    s = s0.strip().upper().replace("Ё", "Е")
    if not s:
        return ""

    # Remove common textual prefixes around codes.
    s = re.sub(
        r"(КОД\s*КУРСА|COURSE\s*CODE|CODE\s*DU\s*COURS|CODIGO\s*DO\s*CURSO|KURSUN\s*KODU|MA\s*KHOA\s*HOC|КОД\s*МЕРОПРИЯТИЯ|EVENT\s*CODE|ACTIVITY\s*CODE)\s*[:\-]?\s*",
        "",
        s,
        flags=re.IGNORECASE,
    )

    s = s.replace("_", "-")
    s = re.sub(r"\s+", "", s)

    # Preserve common named buckets.
    literal_map = {
        "ATTACKINGJANUARY": "ATTACKINGJANUARY",
        "ZIMOWN": "ZIM_OWN",
        "MAINPAGE": "MAIN_PAGE",
        "FORMINACTIVE": "FORM_INACTIVE",
        "PHD21": "PHD21",
    }
    if s in literal_map:
        return literal_map[s]

    # Normalize course/event code patterns, e.g. R301-M0A -> R-301-M0A
    m = re.search(r"\b([A-ZА-Я])[-_ ]?(\d{3})(?:[-_ ]?([A-ZА-Я0-9]{1,4}))?\b", s)
    if m:
        base = f"{m.group(1)}-{m.group(2)}"
        suffix = m.group(3)
        return f"{base}-{suffix}" if suffix else base

    # W-type event shorthand.
    mw = re.search(r"\bW[-_ ]?(\d{1,3})\b", s)
    if mw:
        return f"W-{int(mw.group(1))}"

    cleaned = re.sub(r"[^A-ZА-Я0-9]+", "_", s).strip("_")
    return cleaned
