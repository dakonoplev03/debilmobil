from telegram import InlineKeyboardMarkup, InlineKeyboardButton


def onboarding_start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Быстрый тур", callback_data="onb:start")],
        [InlineKeyboardButton("Пропустить", callback_data="onb:skip")],
    ])


def onboarding_exit_keyboard(step_next: str | None = None) -> InlineKeyboardMarkup:
    rows = []
    if step_next:
        rows.append([InlineKeyboardButton("➡️ Дальше", callback_data=step_next)])
    rows.append([InlineKeyboardButton("✖️ Выйти из тура", callback_data="onb:exit")])
    rows.append([InlineKeyboardButton("🔙 Назад", callback_data="nav:back")])
    return InlineKeyboardMarkup(rows)
