from __future__ import annotations

from typing import Any


def format_money_rub(amount: int | float) -> str:
    return f"{int(amount):,} ₽".replace(",", " ")


def ellipsize(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    if max_chars <= 1:
        return "…"
    return text[: max_chars - 1].rstrip() + "…"


def ellipsize_px(text: str, max_px: int, draw: Any, font: Any) -> str:
    if draw.textbbox((0, 0), text, font=font)[2] <= max_px:
        return text
    cut = text
    while len(cut) > 1 and draw.textbbox((0, 0), cut + "…", font=font)[2] > max_px:
        cut = cut[:-1]
    return (cut + "…") if cut else "…"
