from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any


@dataclass
class Screen:
    name: str
    kind: str  # inline | reply
    payload: dict[str, Any] | None = None


def _stack(context) -> list[dict]:
    return context.user_data.setdefault("nav_stack", [])


def push_screen(context, screen: Screen) -> None:
    _stack(context).append(asdict(screen))


def pop_screen(context) -> Screen | None:
    st = _stack(context)
    if not st:
        return None
    raw = st.pop()
    return Screen(**raw)


def get_current_screen(context) -> Screen | None:
    st = _stack(context)
    if not st:
        return None
    return Screen(**st[-1])


def pop_to_prev_reply(context) -> Screen | None:
    st = _stack(context)
    while st:
        raw = st.pop()
        if raw.get("kind") == "reply":
            return Screen(**raw)
    return None
