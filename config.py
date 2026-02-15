import os
import re

BOT_TOKEN = os.getenv("SERVICEBOT_TOKEN", "")

# Дефолтный регион для автодополнения номеров
DEFAULT_REGION = "797"

# Соответствие английских букв русским
ENG_TO_RUS = {
    "A": "А", "B": "В", "C": "С", "E": "Е", "H": "Н",
    "K": "К", "M": "М", "O": "О", "P": "Р", "T": "Т",
    "X": "Х", "Y": "У",
}

RUS_LETTERS = "АВЕКМНОРСТУХ"


def normalize_car_number(text: str) -> str:
    if not text:
        return ""

    normalized = text.strip().upper().replace(" ", "").replace("-", "").replace("_", "")
    normalized = "".join(ENG_TO_RUS.get(ch, ch) for ch in normalized)
    allowed = set(RUS_LETTERS + "0123456789")
    normalized = "".join(ch for ch in normalized if ch in allowed)

    # Формат "ВКК044" -> "В044КК797"
    compact_three_letters = rf"^[{RUS_LETTERS}]{{3}}\d{{3}}$"
    if re.match(compact_three_letters, normalized):
        return f"{normalized[0]}{normalized[3:6]}{normalized[1:3]}{DEFAULT_REGION}"

    letters = sum(1 for c in normalized if c in RUS_LETTERS)
    digits = sum(1 for c in normalized if c.isdigit())

    if letters >= 3 and digits >= 3 and digits < 6:
        normalized += DEFAULT_REGION[: 6 - digits]
    elif len(normalized) <= 6:
        normalized += DEFAULT_REGION

    return normalized


def validate_car_number(text: str) -> tuple[bool, str, str]:
    normalized = normalize_car_number(text)
    pattern = rf"^[{RUS_LETTERS}]\d{{3}}[{RUS_LETTERS}]{{2}}\d{{2,3}}$"
    if not normalized:
        return False, "", "Номер пустой"
    if not re.match(pattern, normalized):
        return False, normalized, "Некорректный формат номера"
    return True, normalized, ""


# Минимальный базовый набор услуг (можно расширить в runtime/БД).
SERVICES = {
    1: {"name": "🚗 Мойка кузова", "day_price": 500, "night_price": 600},
    2: {"name": "🧽 Комплекс", "day_price": 1200, "night_price": 1400},
    3: {"name": "✨ Пылесос", "day_price": 300, "night_price": 350},
}
