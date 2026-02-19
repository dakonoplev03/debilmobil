"""
🤖 БОТ ДЛЯ УЧЁТА УСЛУГ 
"""

import logging
import asyncio
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import json
import os
import calendar
import re
import importlib.util
from io import BytesIO
from typing import List

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InputMediaPhoto,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    CallbackContext,
    filters,
)

from config import BOT_TOKEN, SERVICES, validate_car_number
from database import DatabaseManager, init_database, DB_PATH
from exports import create_decade_pdf, create_decade_xlsx, create_month_xlsx
from leaderboard.avatars import get_avatar_image as get_avatar_image_async
from services.status import send_status, edit_status, done_status
from ui.texts import STATUS_LEADERBOARD
from ui.keyboards import onboarding_start_keyboard, onboarding_exit_keyboard
from ui.nav import push_screen, pop_screen, get_current_screen, Screen

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
APP_VERSION = "2026.02.19-hotfix-23"
APP_UPDATED_AT = "19.02.2026 13:40 (МСК)"
APP_TIMEZONE = "Europe/Moscow"
LOCAL_TZ = ZoneInfo(APP_TIMEZONE)
ADMIN_TELEGRAM_IDS = {8379101989}
TRIAL_DAYS = 7
SUBSCRIPTION_PRICE_TEXT = "200 ₽/месяц"
SUBSCRIPTION_CONTACT = "@dakonoplev2"

MONTH_NAMES = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

MONTH_NAMES_NOMINATIVE = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

# Инициализация базы данных
init_database()

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def get_current_price(service_id: int, mode: str = "day") -> int:
    """Получение цены по выбранному прайсу"""
    service = SERVICES.get(service_id)
    if not service:
        return 0
    if mode == "night":
        return service.get("night_price", 0)
    return service.get("day_price", 0)


def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)

def format_money(amount: int) -> str:
    """Форматирование денежной суммы"""
    return f"{amount:,}₽".replace(",", " ")


def plain_service_name(name: str) -> str:
    """Убираем декоративные emoji/символы в начале названия услуги."""
    return re.sub(r"^[^0-9A-Za-zА-Яа-я]+\s*", "", name).strip()


FAST_SERVICE_ALIASES = {
    1: ["проверка", "пров", "провер", "чек"],
    2: ["заправка", "запр", "топливо", "бенз"],
    3: ["омыв", "омывка", "омывайка", "зали", "зо", "заливка"],
    14: ["перепарковка", "перепарк", "парковка", "некорректная", "некк", "нек", "некорр"],
}


def parse_fast_car_with_services(text: str) -> tuple[str | None, list[int]]:
    parts = [p.strip(" ,.;:!?").lower() for p in text.split() if p.strip()]
    if not parts:
        return None, []

    is_valid, normalized, _ = validate_car_number(parts[0])
    if not is_valid:
        return None, []

    service_ids: list[int] = []
    for token in parts[1:]:
        for service_id, aliases in FAST_SERVICE_ALIASES.items():
            if token in aliases:
                service_ids.append(service_id)
                break
    return normalized, service_ids


def get_mode_by_time(current_dt: datetime | None = None) -> str:
    current = current_dt or now_local()
    hour = current.hour
    return "night" if hour >= 21 or hour < 9 else "day"


def get_next_price_boundary(current_dt: datetime | None = None) -> datetime:
    current = current_dt or now_local()
    today_9 = current.replace(hour=9, minute=0, second=0, microsecond=0)
    today_21 = current.replace(hour=21, minute=0, second=0, microsecond=0)

    if current < today_9:
        return today_9
    if current < today_21:
        return today_21
    return (current + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)


def sync_price_mode_by_schedule(context: CallbackContext, user_id: int) -> str:
    now_dt = now_local()
    current_mode = DatabaseManager.get_price_mode(user_id)
    lock_until_raw = DatabaseManager.get_price_mode_lock_until(user_id)
    lock_until = None

    if lock_until_raw:
        try:
            lock_until = datetime.fromisoformat(lock_until_raw)
            if lock_until.tzinfo is None:
                lock_until = lock_until.replace(tzinfo=LOCAL_TZ)
        except ValueError:
            lock_until = None

    if lock_until and now_dt < lock_until:
        context.user_data["price_mode"] = current_mode
        return current_mode

    target_mode = get_mode_by_time(now_dt)
    if current_mode != target_mode or lock_until_raw:
        DatabaseManager.set_price_mode(user_id, target_mode, "")
        current_mode = target_mode

    context.user_data["price_mode"] = current_mode
    return current_mode


def set_manual_price_mode(context: CallbackContext, user_id: int, mode: str) -> str:
    normalized_mode = "night" if mode == "night" else "day"
    next_boundary = get_next_price_boundary(now_local())
    DatabaseManager.set_price_mode(user_id, normalized_mode, next_boundary.isoformat())
    context.user_data["price_mode"] = normalized_mode
    return normalized_mode


def get_price_mode(context: CallbackContext, user_id: int | None = None) -> str:
    if user_id:
        return sync_price_mode_by_schedule(context, user_id)

    mode = context.user_data.get("price_mode")
    if mode in {"day", "night"}:
        return mode
    return "day"


def format_decade_range(start: date, end: date) -> str:
    return f"{start.day:02d}.{start.month:02d}–{end.day:02d}.{end.month:02d}"


def get_decade_period(target: date | None = None):
    current = target or now_local().date()
    if current.day <= 10:
        start_day, end_day, idx = 1, 10, 1
    elif current.day <= 20:
        start_day, end_day, idx = 11, 20, 2
    else:
        start_day, idx = 21, 3
        end_day = calendar.monthrange(current.year, current.month)[1]
    start = date(current.year, current.month, start_day)
    end = date(current.year, current.month, end_day)
    key = f"{current.year:04d}-{current.month:02d}-D{idx}"
    title = f"{idx}-я декада: {start.day}-{end.day} {MONTH_NAMES[current.month]}"
    return idx, start, end, key, title



def is_admin_telegram(telegram_id: int) -> bool:
    return telegram_id in ADMIN_TELEGRAM_IDS


def is_user_blocked(db_user: dict | None) -> bool:
    return bool(db_user and DatabaseManager.is_user_blocked(db_user["id"]))


def subscription_expires_at_for_user(db_user: dict | None) -> datetime | None:
    if not db_user:
        return None
    if is_admin_telegram(int(db_user["telegram_id"])):
        return None
    raw = DatabaseManager.get_subscription_expires_at(db_user["id"])
    if not raw:
        return None
    try:
        expires = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=LOCAL_TZ)
    return expires


def ensure_trial_subscription(db_user: dict | None, days: int = TRIAL_DAYS) -> datetime | None:
    if not db_user or is_admin_telegram(int(db_user["telegram_id"])):
        return None
    expires = subscription_expires_at_for_user(db_user)
    if expires:
        return expires
    expires = now_local() + timedelta(days=days)
    DatabaseManager.set_subscription_expires_at(db_user["id"], expires.isoformat())
    return expires


def is_subscription_active(db_user: dict | None) -> bool:
    if not db_user:
        return False
    if is_admin_telegram(int(db_user["telegram_id"])):
        return True
    expires = ensure_trial_subscription(db_user)
    if not expires:
        return False
    return now_local() <= expires


def resolve_user_access(telegram_id: int, context: CallbackContext | None = None) -> tuple[dict | None, bool, bool]:
    db_user = DatabaseManager.get_user(telegram_id)
    if not db_user:
        return None, False, False

    blocked = is_user_blocked(db_user)
    if blocked:
        return db_user, True, False

    if context is not None:
        sync_price_mode_by_schedule(context, db_user["id"])

    ensure_trial_subscription(db_user)
    subscription_active = is_subscription_active(db_user)
    return db_user, False, subscription_active


def main_menu_for_db_user(db_user: dict | None, subscription_active: bool | None = None) -> ReplyKeyboardMarkup:
    has_active_shift = bool(db_user and DatabaseManager.get_active_shift(db_user['id']))
    if subscription_active is None:
        subscription_active = bool(db_user and is_subscription_active(db_user))
    return create_main_reply_keyboard(has_active_shift, bool(subscription_active))


def build_settings_keyboard(db_user: dict | None, is_admin: bool) -> InlineKeyboardMarkup:
    decade_goal_enabled = bool(db_user and DatabaseManager.is_goal_enabled(db_user["id"]))
    decade_label = "📆 Цель декады: ВКЛ" if decade_goal_enabled else "📆 Цель декады: ВЫКЛ"
    keyboard = [
        [InlineKeyboardButton(decade_label, callback_data="change_decade_goal")],
        [InlineKeyboardButton("🗓️ Изменить основные смены", callback_data="calendar_rebase")],
        [InlineKeyboardButton("🧩 Комбо", callback_data="combo_settings")],
        [InlineKeyboardButton("🗑️ Сбросить ВСЕ данные", callback_data="reset_data")],
    ]
    if is_admin:
        keyboard.append([InlineKeyboardButton("🛡️ Админ-панель", callback_data="admin_panel")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    return InlineKeyboardMarkup(keyboard)


def format_subscription_until(expires_at: datetime | None) -> str:
    if not expires_at:
        return "∞"
    return expires_at.astimezone(LOCAL_TZ).strftime("%d.%m.%Y %H:%M")


def get_subscription_expired_text() -> str:
    return (
        "⛔ Подписка закончилась.\n\n"
        "Доступен только раздел 👤 Профиль.\n"
        f"Стоимость подписки: {SUBSCRIPTION_PRICE_TEXT}.\n"
        f"Для продления напишите: {SUBSCRIPTION_CONTACT}"
    )


def is_allowed_when_expired_menu(text: str) -> bool:
    return text in {MENU_ACCOUNT}


def is_allowed_when_expired_callback(data: str) -> bool:
    return data in {"subscription_info", "account_info", "back"}


def activate_subscription_days(user_id: int, days: int) -> datetime:
    expires_at = now_local() + timedelta(days=max(1, int(days)))
    DatabaseManager.set_subscription_expires_at(user_id, expires_at.isoformat())
    return expires_at


def ensure_trial_for_existing_users() -> list[dict]:
    activated = []
    for row in DatabaseManager.get_all_users_with_stats():
        if is_admin_telegram(int(row["telegram_id"])):
            continue
        user_db = DatabaseManager.get_user_by_id(int(row["id"]))
        if not user_db:
            continue
        if subscription_expires_at_for_user(user_db):
            continue
        expires = activate_subscription_days(user_db["id"], TRIAL_DAYS)
        activated.append({"id": user_db["id"], "telegram_id": user_db["telegram_id"], "expires_at": expires})
    return activated


def parse_iso_date(value: str) -> date | None:
    try:
        return datetime.fromisoformat(value).date()
    except Exception:
        return None


def get_work_day_type(db_user: dict, target_day: date, overrides: dict[str, str] | None = None) -> str:
    overrides = overrides or DatabaseManager.get_calendar_overrides(db_user["id"])
    day_key = target_day.isoformat()
    forced = overrides.get(day_key)
    if forced == "planned":
        return "planned"
    if forced == "extra":
        return "extra"
    if forced == "off":
        return "off"

    anchor = parse_iso_date(DatabaseManager.get_work_anchor_date(db_user["id"]))
    if not anchor:
        return "off"

    delta = (target_day - anchor).days
    mod = delta % 4
    return "planned" if mod in {0, 1} else "off"


def build_price_text() -> str:
    lines = ["💰 Прайс (день / ночь)", ""]
    for service_id in sorted(SERVICES.keys()):
        service = SERVICES[service_id]
        if service.get("hidden"):
            continue
        if service.get("kind") == "group":
            continue
        name = plain_service_name(service.get("name", ""))
        if service.get("kind") == "distance":
            lines.append(f"{name} - {service.get('rate_per_km', 0)}₽/км")
            continue
        lines.append(f"{name} - {service.get('day_price', 0)}₽ / {service.get('night_price', 0)}₽")
    return "\n".join(lines)


def month_title(year: int, month: int) -> str:
    return f"{MONTH_NAMES_NOMINATIVE[month]} {year}"


def build_work_calendar_keyboard(db_user: dict, year: int, month: int, setup_mode: bool = False, setup_selected: list[str] | None = None, edit_mode: bool = False) -> InlineKeyboardMarkup:
    setup_selected = setup_selected or []
    shifts_days = {row["day"] for row in DatabaseManager.get_days_for_month(db_user["id"], f"{year:04d}-{month:02d}")}
    overrides = DatabaseManager.get_calendar_overrides(db_user["id"])

    keyboard: list[list[InlineKeyboardButton]] = []
    keyboard.append([
        InlineKeyboardButton("◀️", callback_data=f"calendar_nav_{year}_{month}_prev"),
        InlineKeyboardButton(month_title(year, month), callback_data="noop"),
        InlineKeyboardButton("▶️", callback_data=f"calendar_nav_{year}_{month}_next"),
    ])

    weekday_header = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    keyboard.append([InlineKeyboardButton(day, callback_data="noop") for day in weekday_header])

    weeks = calendar.monthcalendar(year, month)
    for week in weeks:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="noop"))
                continue
            current_day = date(year, month, day)
            day_key = current_day.isoformat()
            if setup_mode:
                mark = "✅" if day_key in setup_selected else "▫️"
                row.append(InlineKeyboardButton(f"{mark}{day:02d}", callback_data=f"calendar_setup_pick_{day_key}"))
                continue

            day_type = get_work_day_type(db_user, current_day, overrides)
            if day_key in shifts_days and day_type == "off":
                day_type = "extra"
            prefix = "🔴" if day_type == "planned" else ("🟡" if day_type == "extra" else "⚪")
            row.append(InlineKeyboardButton(f"{prefix}{day:02d}", callback_data=f"calendar_day_{day_key}"))
        keyboard.append(row)

    if setup_mode:
        keyboard.append([InlineKeyboardButton("✅ Сохранить базовые дни", callback_data=f"calendar_setup_save_{year}_{month}")])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="back")])
    else:
        edit_label = "✏️ Редакт.: ВКЛ" if edit_mode else "✏️ Редакт.: ВЫКЛ"
        keyboard.append([
            InlineKeyboardButton("🗓️ Изменить смены", callback_data="calendar_rebase"),
            InlineKeyboardButton(edit_label, callback_data=f"calendar_edit_toggle_{year}_{month}"),
        ])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    return InlineKeyboardMarkup(keyboard)


def build_work_calendar_text(db_user: dict, year: int, month: int, setup_mode: bool = False, edit_mode: bool = False) -> str:
    if setup_mode:
        return (
            f"📅 Календарь — {month_title(year, month)}\n\n"
            "Первый запуск: выберите 2 подряд идущих основных рабочих дня.\n"
            "После сохранения график 2/2 будет рассчитан автоматически."
        )
    return (
        f"📅 {month_title(year, month)}\n"
        "Обозначения: 🔴 основная, 🟡 доп., ⚪ выходной."
    )


def short_amount(amount: int) -> str:
    if amount >= 1000:
        return f"{amount / 1000:.1f}к".replace(".0", "")
    return str(amount)


def get_decade_index_for_day(day: int) -> int:
    if day <= 10:
        return 1
    if day <= 20:
        return 2
    return 3


def build_short_goal_line(user_id: int) -> str:
    goal = DatabaseManager.get_daily_goal(user_id)
    if goal <= 0:
        return "🎯 Цель не задана"
    today_total = DatabaseManager.get_user_total_for_date(user_id, now_local().strftime("%Y-%m-%d"))
    percent = calculate_percent(today_total, goal)
    filled = min(percent // 20, 5)
    bar = "█" * filled + "░" * (5 - filled)
    return f"🎯 {format_money(today_total)}/{format_money(goal)} {percent}% {bar}"


def format_decade_title(year: int, month: int, decade_index: int) -> str:
    if decade_index == 1:
        start_day, end_day = 1, 10
    elif decade_index == 2:
        start_day, end_day = 11, 20
    else:
        start_day = 21
        end_day = calendar.monthrange(year, month)[1]
    return f"{start_day:02d}-{end_day:02d} {MONTH_NAMES[month]} {year}"


def get_decade_range_by_index(year: int, month: int, decade_index: int) -> tuple[date, date]:
    if decade_index == 1:
        return date(year, month, 1), date(year, month, 10)
    if decade_index == 2:
        return date(year, month, 11), date(year, month, 20)
    return date(year, month, 21), date(year, month, calendar.monthrange(year, month)[1])


def build_decade_goal_hint(db_user: dict, year: int, month: int) -> str:
    today = now_local().date()
    decade_index = 1 if today.day <= 10 else 2 if today.day <= 20 else 3
    if not (today.year == year and today.month == month):
        decade_index = 1

    start_d, end_d = get_decade_range_by_index(year, month, decade_index)
    overrides = DatabaseManager.get_calendar_overrides(db_user["id"])
    month_days = DatabaseManager.get_days_for_month(db_user["id"], f"{year:04d}-{month:02d}")
    actual_shift_days = {
        str(row.get("day"))
        for row in month_days
        if int(row.get("shifts_count", 0) or 0) > 0
    }

    main_days = 0
    extra_days = 0
    cursor = start_d
    while cursor <= end_d:
        day_key = cursor.isoformat()
        day_type = get_work_day_type(db_user, cursor, overrides)
        if day_type == "planned":
            main_days += 1
        elif day_type == "extra" or (day_type == "off" and day_key in actual_shift_days):
            extra_days += 1
        cursor += timedelta(days=1)

    total_work_days = main_days + extra_days
    decade_goal = DatabaseManager.get_decade_goal(db_user["id"])
    if decade_goal <= 0:
        return (
            f"🎯 {decade_index}-я декада ({format_decade_range(start_d, end_d)}): цель не задана\n"
            f"Смены: осн. {main_days}, доп. {extra_days}."
        )
    earned = DatabaseManager.get_user_total_between_dates(db_user["id"], start_d.isoformat(), end_d.isoformat())
    remaining_amount = max(decade_goal - earned, 0)
    remaining_days = 0
    today = now_local().date()
    cursor = max(today, start_d)
    while cursor <= end_d:
        day_key = cursor.isoformat()
        day_type = get_work_day_type(db_user, cursor, overrides)
        if day_type in {"planned", "extra"} or (day_type == "off" and day_key in actual_shift_days):
            remaining_days += 1
        cursor += timedelta(days=1)

    per_shift = int(remaining_amount / remaining_days) if remaining_days else 0
    return (
        f"🎯 {decade_index}-я декада ({format_decade_range(start_d, end_d)}): {format_money(decade_goal)}\n"
        f"Смены: осн. {main_days}, доп. {extra_days}, всего {total_work_days}.\n"
        f"Сделано: {format_money(earned)} | Осталось: {format_money(remaining_amount)}\n"
        f"Осталось рабочих смен: {remaining_days}. Цель на смену: {format_money(per_shift)}"
    )

# ========== КЛАВИАТУРЫ ==========

MENU_SHIFT_OPEN = "🟢 Открыть смену"
MENU_SHIFT_CLOSE = "🔚 Закрыть смену"
MENU_ADD_CAR = "🚗 Добавить машину"
MENU_CURRENT_SHIFT = "📊 Дашборд"
MENU_SETTINGS = "🧰 Инструменты"
MENU_LEADERBOARD = "🏆 Топ героев"
MENU_FAQ = "❓ FAQ"
MENU_PRICE = "💰 Прайс"
MENU_CALENDAR = "🗓️ Календарь"
MENU_ACCOUNT = "👤 Профиль"

TOOLS_PRICE = "💰 Прайс"
TOOLS_CALENDAR = "🗓️ Календарь"
TOOLS_HISTORY = "📚 История"
TOOLS_COMBO = "🧩 Комбо"
TOOLS_DECADE_GOAL = "🎯 Цель декады"
TOOLS_RESET = "🗑️ Сброс всех данных"
TOOLS_ADMIN = "🛡️ Админ панель"
TOOLS_BACK = "🔙 Назад"


def create_main_reply_keyboard(has_active_shift: bool = False, subscription_active: bool = True) -> ReplyKeyboardMarkup:
    """Главное меню под полем ввода"""
    keyboard = []

    if not subscription_active:
        keyboard.append([KeyboardButton(MENU_ACCOUNT)])
        return ReplyKeyboardMarkup(
            keyboard,
            resize_keyboard=True,
            one_time_keyboard=False,
            input_field_placeholder="Выбери действие"
        )

    shift_button = MENU_SHIFT_CLOSE if has_active_shift else MENU_SHIFT_OPEN
    keyboard.append([KeyboardButton(MENU_ADD_CAR), KeyboardButton(shift_button)])
    keyboard.append([KeyboardButton(MENU_CURRENT_SHIFT), KeyboardButton(MENU_LEADERBOARD)])
    keyboard.append([KeyboardButton(MENU_FAQ), KeyboardButton(MENU_ACCOUNT)])
    keyboard.append([KeyboardButton(MENU_SETTINGS)])

    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выбери действие"
    )


def create_tools_reply_keyboard(is_admin: bool = False) -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(TOOLS_PRICE), KeyboardButton(TOOLS_CALENDAR)],
        [KeyboardButton(TOOLS_HISTORY), KeyboardButton(TOOLS_COMBO)],
        [KeyboardButton(TOOLS_DECADE_GOAL), KeyboardButton(TOOLS_RESET)],
    ]
    if is_admin:
        keyboard.append([KeyboardButton(TOOLS_ADMIN)])
    keyboard.append([KeyboardButton(TOOLS_BACK)])
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выбери инструмент"
    )

def get_service_order(user_id: int | None = None) -> List[int]:
    visible = [
        (service_id, service)
        for service_id, service in SERVICES.items()
        if not service.get("hidden")
    ]

    usage = DatabaseManager.get_user_service_usage(user_id) if user_id else {}
    visible.sort(
        key=lambda item: (
            -usage.get(item[0], 0),
            item[1].get("priority", 999),
            item[1].get("order", 999),
            item[0],
        )
    )
    return [service_id for service_id, _ in visible]

def chunk_buttons(buttons: List[InlineKeyboardButton], columns: int) -> List[List[InlineKeyboardButton]]:
    return [buttons[i:i + columns] for i in range(0, len(buttons), columns)]


def create_services_keyboard(
    car_id: int,
    page: int = 0,
    is_edit_mode: bool = False,
    mode: str = "day",
    user_id: int | None = None,
    history_day: str | None = None,
) -> InlineKeyboardMarkup:
    """Клавиатура выбора услуг (3 колонки, 12 услуг на страницу)."""
    service_ids = get_service_order(user_id)

    per_page = 12
    max_page = max((len(service_ids) - 1) // per_page, 0)
    page = max(0, min(page, max_page))

    start = page * per_page
    end = start + per_page
    page_ids = service_ids[start:end]

    def compact(text: str, limit: int = 14) -> str:
        value = (text or "").strip()
        return value if len(value) <= limit else (value[:limit - 1] + "…")

    buttons = []
    for service_id in page_ids:
        service = SERVICES[service_id]
        clean_name = plain_service_name(service['name'])
        if service.get("kind") == "group":
            text = f"{clean_name} (выбор)"
        elif service.get("kind") == "distance":
            text = "Дальняк"
        else:
            text = clean_name
        buttons.append(InlineKeyboardButton(compact(text), callback_data=f"service_{service_id}_{car_id}_{page}"))

    keyboard = []

    combos = DatabaseManager.get_user_combos(user_id) if user_id else []
    if combos:
        top_combo = combos[0]
        keyboard.append([
            InlineKeyboardButton(
                f"🧩 {top_combo['name'][:28]}",
                callback_data=f"combo_apply_{top_combo['id']}_{car_id}_{page}",
            )
        ])

    keyboard.extend(chunk_buttons(buttons, 3))

    nav = [InlineKeyboardButton(f"Стр {page + 1}/{max_page + 1}", callback_data="noop")]
    if page > 0:
        nav.insert(0, InlineKeyboardButton("⬅️ Назад", callback_data=f"service_page_{car_id}_{page-1}"))
    if page < max_page:
        nav.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"service_page_{car_id}_{page+1}"))
    keyboard.append(nav)

    keyboard.append([
        InlineKeyboardButton("🔎 Поиск", callback_data=f"service_search_{car_id}_{page}"),
        InlineKeyboardButton("🧹 Очистить", callback_data=f"clear_{car_id}_{page}"),
        InlineKeyboardButton("💾 Сохранить", callback_data=f"save_{car_id}"),
    ])

    if history_day:
        keyboard.append([
            InlineKeyboardButton("🗑️ Удалить машину", callback_data=f"delcar_{car_id}_{history_day}"),
            InlineKeyboardButton("🔙 К машинам дня", callback_data=f"cleanup_day_{history_day}"),
        ])

    return InlineKeyboardMarkup(keyboard)

def build_history_keyboard(shifts) -> InlineKeyboardMarkup:
    """Простая клавиатура для блока истории."""
    del shifts  # оставляем для совместимости, пока пагинация не используется
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])

def parse_datetime(value):
    if isinstance(value, datetime):
        return value
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
    return None



def render_bar(percent: int, width: int = 10) -> str:
    percent = max(0, min(percent, 100))
    filled = round((percent / 100) * width)
    return "█" * filled + "░" * (width - filled)


def calculate_percent(value: int, total: int) -> int:
    if total <= 0:
        return 0
    percent = int((value * 100) / total + 0.5)
    return max(0, min(percent, 100))


def build_shift_metrics(shift: dict, cars: list[dict], total: int) -> dict:
    start_time = parse_datetime(shift.get("start_time"))
    end_time = parse_datetime(shift.get("end_time")) or now_local()
    hours = max((end_time - start_time).total_seconds() / 3600, 0.01) if start_time else 0.01
    rate_hours = max(hours, 1.0)
    cars_count = len(cars)
    avg_check = int(total / cars_count) if cars_count else 0
    return {
        "start_time": start_time,
        "hours": hours,
        "cars_count": cars_count,
        "avg_check": avg_check,
        "cars_per_hour": cars_count / rate_hours,
        "money_per_hour": total / rate_hours,
    }


def build_current_shift_dashboard(user_id: int, shift: dict, cars: list[dict], total: int) -> str:
    today = now_local().date()
    day_key = today.isoformat()

    today_cars = len([car for car in cars if str(car.get("created_at", "")).startswith(day_key)])
    today_income = DatabaseManager.get_user_total_for_date(user_id, day_key)

    daily_goal = DatabaseManager.get_daily_goal(user_id) if DatabaseManager.is_goal_enabled(user_id) else 0
    daily_percent = calculate_percent(today_income, daily_goal) if daily_goal > 0 else 0
    progress_bar = render_bar(daily_percent, 10)

    _, start_d, end_d, _, _ = get_decade_period(today)
    total_days = max((end_d - start_d).days + 1, 1)
    passed_days = max((today - start_d).days + 1, 1)

    decade_goal = DatabaseManager.get_decade_goal(user_id) if DatabaseManager.is_goal_enabled(user_id) else 0
    earned_decade = DatabaseManager.get_user_total_between_dates(user_id, start_d.isoformat(), end_d.isoformat())

    remaining_days = max(total_days - passed_days, 1)
    need_per_day = int(max(decade_goal - earned_decade, 0) / remaining_days) if decade_goal > 0 else 0
    expected_today = int((decade_goal / total_days) * passed_days) if decade_goal > 0 else 0
    lag_today = earned_decade - expected_today
    runrate = 0
    if expected_today > 0:
        runrate = int(((earned_decade - expected_today) / expected_today) * 100)

    today_line = f"{format_money(today_income)} / {format_money(daily_goal)} план" if daily_goal > 0 else format_money(today_income)
    decade_line = f"{format_money(earned_decade)} / {format_money(decade_goal)}" if decade_goal > 0 else f"{format_money(earned_decade)} / —"

    return (
        "📅 Сегодня:\n"
        f"Машин: {today_cars}\n"
        f"Доход: {today_line}\n"
        f"% выполнения: {daily_percent}%\n"
        f"{progress_bar}\n\n"
        "🎯 План декады:\n"
        f"Всего заработано: {decade_line}\n"
        f"Нужно в день: {format_money(need_per_day)}\n"
        f"Отставание на текущий день: {format_money(lag_today)}\n\n"
        f"⚡ Ранрейт: {runrate:+d}%"
    )


def build_closed_shift_dashboard(shift: dict, cars: list[dict], total: int) -> str:
    metrics = build_shift_metrics(shift, cars, total)
    tax = round(total * 0.06)
    net = total - tax
    stars = "⭐" * (1 if total < 3000 else 2 if total < 7000 else 3 if total < 12000 else 4)

    start_time = parse_datetime(shift.get("start_time"))
    end_time = parse_datetime(shift.get("end_time"))
    start_label = start_time.strftime("%H:%M") if start_time else "—"
    end_label = end_time.strftime("%H:%M") if end_time else now_local().strftime("%H:%M")

    top_services = DatabaseManager.get_shift_top_services(shift["id"], limit=3)
    top_block = ""
    if top_services:
        top_rows = [
            f"• {plain_service_name(item['service_name'])} — {item['total_count']} шт. ({format_money(int(item['total_amount']))})"
            for item in top_services
        ]
        top_block = "\n\n🏆 Топ услуг смены:\n" + "\n".join(top_rows)

    return (
        f"📘 <b>Итог смены</b> {stars}\n"
        f"🗓 Дата: {now_local().strftime('%d.%m.%Y')}\n"
        f"🕒 Время: {start_label} — {end_label} ({metrics['hours']:.1f} ч)\n\n"
        f"🚗 Машин: <b>{metrics['cars_count']}</b>\n"
        f"💰 Выручка: <b>{format_money(total)}</b>\n"
        f"📈 Средний чек: {format_money(metrics['avg_check'])}\n"
        f"⚡ Машин/час: {metrics['cars_per_hour']:.2f}\n"
        f"💸 Доход/час: {format_money(int(metrics['money_per_hour']))}\n"
        f"🧾 Налог 6%: {format_money(tax)}\n"
        f"✅ К выплате: <b>{format_money(net)}</b>"
        f"{top_block}"
    )


def build_shift_repeat_report_text(shift_id: int) -> str:
    rows = DatabaseManager.get_shift_repeated_services(shift_id)
    if not rows:
        return (
            "📋 Отчёт повторок\n\n"
            "За эту смену не найдено услуг с повтором (x2 и более) на одной машине."
        )

    grouped: dict[str, list[str]] = {}
    for row in rows:
        car_number = row["car_number"]
        grouped.setdefault(car_number, []).append(
            f"{plain_service_name(row['service_name'])} x{int(row['total_count'])}"
        )

    lines = ["📋 <b>Отчёт повторок по смене</b>", ""]
    for car_number, items in grouped.items():
        lines.append(f"🚗 {car_number}")
        for item in items:
            lines.append(f"• {item}")
        lines.append("")
    lines.append(f"Итого машин с повторами: {len(grouped)}")
    return "\n".join(lines)


def build_period_summary_text(user_id: int, start_d: date, end_d: date, title: str) -> str:
    total = DatabaseManager.get_user_total_between_dates(user_id, start_d.isoformat(), end_d.isoformat())
    shifts_count = DatabaseManager.get_shifts_count_between_dates(user_id, start_d.isoformat(), end_d.isoformat())
    cars_count = DatabaseManager.get_cars_count_between_dates(user_id, start_d.isoformat(), end_d.isoformat())
    avg_check = int(total / cars_count) if cars_count else 0
    top_services = DatabaseManager.get_top_services_between_dates(user_id, start_d.isoformat(), end_d.isoformat(), limit=3)

    lines = [
        f"📘 <b>{title}</b>",
        f"Период: {format_decade_range(start_d, end_d)}",
        "",
        f"🧮 Смен: {shifts_count}",
        f"🚗 Машин: {cars_count}",
        f"💰 Выручка: <b>{format_money(int(total or 0))}</b>",
        f"📈 Средний чек: {format_money(avg_check)}",
    ]

    if top_services:
        lines.append("\n🏆 Топ услуг:")
        for item in top_services:
            lines.append(f"• {plain_service_name(item['service_name'])} — {int(item['total_count'])} шт.")
    return "\n".join(lines)

def get_goal_text(user_id: int) -> str:
    if not DatabaseManager.is_goal_enabled(user_id):
        return ""

    goal = DatabaseManager.get_daily_goal(user_id)
    if goal <= 0:
        return ""
    today_total = DatabaseManager.get_user_total_for_date(user_id, now_local().date().isoformat())
    percent = calculate_percent(today_total, goal)
    bar = render_bar(percent, 10)
    return f"Цель: {format_money(today_total)}/{format_money(goal)} {bar}"


def calculate_current_decade_daily_goal(db_user: dict) -> int:
    today = now_local().date()
    decade_index = 1 if today.day <= 10 else 2 if today.day <= 20 else 3
    start_d, end_d = get_decade_range_by_index(today.year, today.month, decade_index)
    overrides = DatabaseManager.get_calendar_overrides(db_user["id"])
    month_days = DatabaseManager.get_days_for_month(db_user["id"], f"{today.year:04d}-{today.month:02d}")
    actual_shift_days = {
        str(row.get("day"))
        for row in month_days
        if int(row.get("shifts_count", 0) or 0) > 0
    }
    work_days = 0
    remaining_days = 0
    cursor = start_d
    today = now_local().date()
    while cursor <= end_d:
        day_key = cursor.isoformat()
        day_type = get_work_day_type(db_user, cursor, overrides)
        if day_type in {"planned", "extra"} or (day_type == "off" and day_key in actual_shift_days):
            work_days += 1
            if cursor >= today:
                remaining_days += 1
        cursor += timedelta(days=1)
    decade_goal = DatabaseManager.get_decade_goal(db_user["id"])
    if decade_goal <= 0 or work_days <= 0:
        return 0
    earned = DatabaseManager.get_user_total_between_dates(db_user["id"], start_d.isoformat(), end_d.isoformat())
    remaining_amount = max(decade_goal - earned, 0)
    if remaining_days <= 0:
        return 0
    return int(remaining_amount / remaining_days)


def get_edit_mode(context: CallbackContext, car_id: int) -> bool:
    return context.user_data.get(f"edit_mode_{car_id}", False)

def toggle_edit_mode(context: CallbackContext, car_id: int) -> bool:
    new_value = not context.user_data.get(f"edit_mode_{car_id}", False)
    context.user_data[f"edit_mode_{car_id}"] = new_value
    return new_value

def build_decade_summary(user_id: int) -> str:
    today = now_local().date()
    year = today.year
    month = today.month
    current_decade = 1 if today.day <= 10 else 2 if today.day <= 20 else 3

    decades = [
        (1, date(year, month, 1), date(year, month, 10)),
        (2, date(year, month, 11), date(year, month, 20)),
        (3, date(year, month, 21), date(year, month, calendar.monthrange(year, month)[1])),
    ]

    lines = [f"📆 <b>Зарплата по декадам — {MONTH_NAMES[month].capitalize()} {year}</b>", ""]
    for idx, start_d, end_d in decades:
        if idx > current_decade:
            continue
        total = DatabaseManager.get_user_total_between_dates(user_id, start_d.isoformat(), end_d.isoformat())
        row = f"{idx}-я декада {MONTH_NAMES[month]}: {format_money(total)}"
        lines.append(f"<b>{row}</b>" if idx == current_decade else row)

    return "\n".join(lines)


def build_csv_report(user_id: int) -> str:
    rows = DatabaseManager.get_shift_report_rows(user_id)
    if not rows:
        return ""

    reports_dir = "reports"
    os.makedirs(reports_dir, exist_ok=True)
    filename = f"report_{now_local().strftime('%Y%m%d_%H%M%S')}.csv"
    path = os.path.join(reports_dir, filename)

    with open(path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["shift_id", "start_time", "end_time", "car_number", "services", "total_amount"])
        for row in rows:
            writer.writerow([
                row.get("shift_id"),
                row.get("start_time"),
                row.get("end_time") or "",
                row.get("car_number") or "",
                row.get("services") or "",
                row.get("total_amount") or 0,
            ])
    return path

def create_db_backup() -> str:
    if not os.path.exists(DB_PATH):
        return ""
    backups_dir = "backups"
    os.makedirs(backups_dir, exist_ok=True)
    filename = f"backup_{now_local().strftime('%Y%m%d_%H%M%S')}.db"
    path = os.path.join(backups_dir, filename)
    shutil.copy2(DB_PATH, path)
    return path

async def ensure_goal_message_pinned(context: CallbackContext, chat_id: int, message_id: int) -> None:
    """Пытаемся закрепить сообщение с целью в любом чате, где это поддерживается."""
    try:
        await context.bot.pin_chat_message(
            chat_id=chat_id,
            message_id=message_id,
            disable_notification=True,
        )
    except Exception:
        # Для чатов/ролей без прав на закреп просто пропускаем.
        pass


async def send_goal_status(update: Update | None, context: CallbackContext, user_id: int, source_message=None):
    """Обновить закреп по цели, только если цель включена пользователем."""
    goal_text = get_goal_text(user_id)
    if not goal_text:
        return

    source_message = source_message or (update.message if update and update.message else None) or (
        update.callback_query.message if update and update.callback_query else None
    )
    if not source_message:
        return

    chat_id = source_message.chat_id
    bind_chat_id, bind_message_id = DatabaseManager.get_goal_message_binding(user_id)

    if bind_chat_id and int(bind_chat_id) != int(chat_id):
        chat_id = int(bind_chat_id)

    if bind_chat_id and bind_message_id:
        try:
            await context.bot.edit_message_text(chat_id=bind_chat_id, message_id=bind_message_id, text=goal_text)
            await ensure_goal_message_pinned(context, int(bind_chat_id), int(bind_message_id))
            return
        except Exception:
            DatabaseManager.clear_goal_message_binding(user_id)

    # если биндинг есть, но сообщение удалено/не доступно — пытаемся опубликовать в том же чате
    target_chat_id = int(bind_chat_id) if bind_chat_id else int(chat_id)
    send_target = source_message
    if target_chat_id != int(chat_id):
        class _ChatProxy:
            def __init__(self, bot, chat_id):
                self.bot = bot
                self.chat_id = chat_id

            async def reply_text(self, text):
                return await self.bot.send_message(chat_id=self.chat_id, text=text)

        send_target = _ChatProxy(context.bot, target_chat_id)

    message = await send_target.reply_text(goal_text)
    DatabaseManager.set_goal_message_binding(user_id, target_chat_id, message.message_id)
    await ensure_goal_message_pinned(context, message.chat_id, message.message_id)


async def disable_goal_status(context: CallbackContext, user_id: int) -> None:
    chat_id, message_id = DatabaseManager.get_goal_message_binding(user_id)
    if chat_id and message_id:
        try:
            await context.bot.unpin_chat_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass
    DatabaseManager.clear_goal_message_binding(user_id)

# ========== ОСНОВНЫЕ КОМАНДЫ ==========

async def start_command(update: Update, context: CallbackContext):
    """Обработчик команды /start"""
    user = update.effective_user

    if update.message:
        db_user = DatabaseManager.get_user(user.id)

        is_new_user = False
        if not db_user:
            name = " ".join(part for part in [user.first_name, user.last_name] if part) or user.username or "Пользователь"
            DatabaseManager.register_user(user.id, name)
            db_user = DatabaseManager.get_user(user.id)
            is_new_user = True

        if not db_user:
            await update.message.reply_text("❌ Не удалось зарегистрировать пользователя. Повторите /start")
            return
        if is_user_blocked(db_user):
            await update.message.reply_text("⛔ Доступ к боту закрыт администратором.")
            return

        expires_at = ensure_trial_subscription(db_user)
        subscription_active = is_subscription_active(db_user)

        context.user_data["price_mode"] = sync_price_mode_by_schedule(context, db_user["id"])

        has_active = DatabaseManager.get_active_shift(db_user['id']) is not None

        if is_new_user and not is_admin_telegram(user.id):
            await update.message.reply_text(
                "🎉 Аккаунт активирован на 7 дней!\n"
                f"Доступ до: {format_subscription_until(expires_at)}\n"
                "Приятного пользования ботом."
            )

        if not subscription_active:
            await update.message.reply_text(
                get_subscription_expired_text(),
                reply_markup=create_main_reply_keyboard(False, False)
            )
            return

        await update.message.reply_text(
            f"👋 Привет, {user.first_name or db_user.get('name', 'пользователь')}!\n"
            f"На связи Делибабос.\n\n"
            f"Версия: {APP_VERSION}",
            reply_markup=create_main_reply_keyboard(has_active, subscription_active)
        )
        await update.message.reply_text(
            "Хочешь пройти быстрый тур? Он покажет базовый процесс работы за 1 минуту.",
            reply_markup=onboarding_start_keyboard(),
        )
        await send_goal_status(update, context, db_user['id'])
        await send_period_reports_for_user(context.application, db_user)

async def menu_command(update: Update, context: CallbackContext):
    user = update.effective_user
    db_user, blocked, subscription_active = resolve_user_access(user.id, context)
    if not db_user:
        await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
        return
    if blocked:
        await update.message.reply_text("⛔ Доступ к боту закрыт администратором.")
        return
    if not subscription_active:
        await update.message.reply_text(
            get_subscription_expired_text(),
            reply_markup=create_main_reply_keyboard(False, False)
        )
        return

    await update.message.reply_text(
        "Главное меню открыто.",
        reply_markup=main_menu_for_db_user(db_user, subscription_active)
    )
    await send_period_reports_for_user(context.application, db_user)

def create_tools_inline_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("💰 Прайс", callback_data="show_price")],
        [InlineKeyboardButton("🗓️ Календарь", callback_data="calendar_open")],
        [InlineKeyboardButton("📚 История", callback_data="history_decades")],
        [InlineKeyboardButton("🧩 Комбо", callback_data="combo_settings")],
        [InlineKeyboardButton("🎯 Цель декады", callback_data="change_decade_goal")],
        [InlineKeyboardButton("🗑️ Сброс всех данных", callback_data="reset_data")],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton("🛡️ Админ панель", callback_data="admin_panel")])
    rows.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    return InlineKeyboardMarkup(rows)


async def shift_hub_message(update: Update, context: CallbackContext):
    db_user = DatabaseManager.get_user(update.effective_user.id)
    if not db_user:
        await update.message.reply_text("❌ Пользователь не найден. Напиши /start")
        return
    if DatabaseManager.get_active_shift(db_user['id']):
        await current_shift_message(update, context)
    else:
        await open_shift_message(update, context)


async def history_hub_message(update: Update, context: CallbackContext):
    await update.message.reply_text(
        "📚 История по декадам:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📚 Открыть историю", callback_data="history_decades")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back")],
        ]),
    )


async def tools_hub_message(update: Update, context: CallbackContext):
    context.user_data["tools_menu_active"] = True
    push_screen(context, Screen(name="tools_menu", kind="reply"))
    await update.message.reply_text(
        "🧰 Инструменты\nВыбери нужный раздел.",
        reply_markup=create_tools_reply_keyboard(is_admin=is_admin_telegram(update.effective_user.id)),
    )


async def help_hub_message(update: Update, context: CallbackContext):
    await send_faq(update.message, context)


async def nav_shift_callback(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    if DatabaseManager.get_active_shift(db_user['id']):
        await current_shift(query, context)
    else:
        await open_shift(query, context)


async def nav_history_callback(query, context):
    await history_decades(query, context)


async def nav_tools_callback(query, context):
    await query.edit_message_text(
        "🧰 Инструменты",
        reply_markup=create_tools_inline_keyboard(is_admin=is_admin_telegram(query.from_user.id)),
    )


async def nav_help_callback(query, context):
    await query.edit_message_text(
        "🎓 Центр обучения\n\n"
        "Здесь можно пройти интерактивный обзор всех ключевых функций.",
        reply_markup=create_faq_topics_keyboard(get_faq_topics(), is_admin=is_admin_telegram(query.from_user.id)),
    )

async def nav_navigator_callback(query, context):
    await query.edit_message_text(
        "Главное меню уже внизу 👇\nНажми нужную кнопку.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🧰 Инструменты", callback_data="nav_tools")]])
    )



async def handle_media_message(update: Update, context: CallbackContext):
    user = update.effective_user
    db_user_for_access, blocked, _ = resolve_user_access(user.id, context)
    if blocked:
        return

    if is_admin_telegram(user.id) and db_user_for_access:
        section = context.user_data.get("awaiting_admin_section_photo")
        if section:
            photo = update.message.photo[-1] if update.message.photo else None
            if not photo:
                await update.message.reply_text("Пришлите фото (изображение).")
                return
            set_section_photo_file_id(section, photo.file_id)
            context.user_data.pop("awaiting_admin_section_photo", None)
            await update.message.reply_text("✅ Фото сохранено для раздела.")
            return

        if context.user_data.get("awaiting_admin_faq_video") and update.message.video:
            video = update.message.video
            DatabaseManager.set_app_content("faq_video_file_id", video.file_id)
            DatabaseManager.set_app_content("faq_video_source_chat_id", str(update.message.chat_id))
            DatabaseManager.set_app_content("faq_video_source_message_id", str(update.message.message_id))
            context.user_data.pop("awaiting_admin_faq_video", None)
            await update.message.reply_text("✅ Видео FAQ обновлено. Пользователи будут получать его как полноценное видео.")
            return


async def handle_message(update: Update, context: CallbackContext):
    """Обработка текстовых сообщений"""
    user = update.effective_user
    text = (update.message.text or "").strip()
    db_user_for_access, blocked, subscription_active = resolve_user_access(user.id, context)
    if blocked:
        await update.message.reply_text("⛔ Доступ к боту закрыт администратором.")
        return

    if await demo_handle_car_text(update, context):
        return

    # Быстрый ввод: "номер + сокращения услуг"
    if db_user_for_access and subscription_active:
        active_shift = DatabaseManager.get_active_shift(db_user_for_access['id'])
        if active_shift:
            fast_car_number, fast_services = parse_fast_car_with_services(text)
            if fast_car_number and fast_services:
                car_id = DatabaseManager.add_car(active_shift['id'], fast_car_number)
                mode = get_price_mode(context, db_user_for_access["id"])
                for service_id in fast_services:
                    service = SERVICES.get(service_id)
                    if not service:
                        continue
                    DatabaseManager.add_service_to_car(
                        car_id,
                        service_id,
                        plain_service_name(service['name']),
                        get_current_price(service_id, mode),
                    )

                car = DatabaseManager.get_car(car_id)
                await update.message.reply_text(
                    f"🚗 Быстро добавлено: {fast_car_number}\n"
                    f"Услуг: {len(fast_services)}\n"
                    f"Сумма: {format_money(int(car['total_amount']) if car else 0)}"
                )
                await send_goal_status(update, context, db_user_for_access['id'])
                return

    if is_admin_telegram(user.id) and db_user_for_access:
        if await process_admin_broadcast(update, context, db_user_for_access):
            return

        awaiting_days_for_user = context.user_data.get("awaiting_admin_subscription_days")
        if awaiting_days_for_user:
            raw_days = text.strip()
            if not raw_days.isdigit() or int(raw_days) <= 0:
                await update.message.reply_text("Введите количество дней числом, например: 30")
                return
            target_user = DatabaseManager.get_user_by_id(int(awaiting_days_for_user))
            context.user_data.pop("awaiting_admin_subscription_days", None)
            if not target_user:
                await update.message.reply_text("❌ Пользователь не найден")
                return
            expires = activate_subscription_days(target_user["id"], int(raw_days))
            await update.message.reply_text(
                f"✅ Подписка активирована на {int(raw_days)} дн. (до {format_subscription_until(expires)})."
            )
            try:
                await context.bot.send_message(
                    chat_id=target_user["telegram_id"],
                    text=(
                        f"✅ Ваш аккаунт активирован на {int(raw_days)} дн.!\n"
                        f"Доступ до: {format_subscription_until(expires)}\n"
                        "Приятного пользования ботом."
                    )
                )
            except Exception:
                pass
            return

        if context.user_data.pop("awaiting_admin_faq_text", None):
            DatabaseManager.set_app_content("faq_text", update.message.text.strip())
            await update.message.reply_text("✅ Текст FAQ обновлён.")
            return

        if context.user_data.pop("awaiting_admin_faq_topic_add", None):
            if "|" not in text:
                await update.message.reply_text("Неверный формат. Используйте: Тема | Текст ответа")
                return
            title, body = [part.strip() for part in text.split("|", 1)]
            if not title or not body:
                await update.message.reply_text("И тема, и текст ответа должны быть заполнены.")
                return
            topics = get_faq_topics()
            topic_id = str(int(now_local().timestamp() * 1000))
            topics.append({"id": topic_id, "title": title, "text": body})
            save_faq_topics(topics)
            await update.message.reply_text(f"✅ Тема добавлена: {title}")
            return

        editing_topic_id = context.user_data.get("awaiting_admin_faq_topic_edit")
        if editing_topic_id:
            if "|" not in text:
                await update.message.reply_text("Неверный формат. Используйте: Новое название | Новый текст")
                return
            title, body = [part.strip() for part in text.split("|", 1)]
            topics = get_faq_topics()
            updated = False
            for topic in topics:
                if topic["id"] == editing_topic_id:
                    topic["title"] = title
                    topic["text"] = body
                    updated = True
                    break
            context.user_data.pop("awaiting_admin_faq_topic_edit", None)
            if not updated:
                await update.message.reply_text("❌ Тема не найдена.")
                return
            save_faq_topics(topics)
            await update.message.reply_text("✅ Тема FAQ обновлена.")
            return

    # Если ожидаем номер машины, но пользователь нажал меню — отменяем ввод
    if context.user_data.get('awaiting_car_number') and text in {
        MENU_ADD_CAR,
        MENU_SHIFT_OPEN,
        MENU_SHIFT_CLOSE,
        MENU_CURRENT_SHIFT,
        MENU_SETTINGS,
        MENU_LEADERBOARD,
        MENU_FAQ,
        MENU_ACCOUNT,
    }:
        context.user_data.pop('awaiting_car_number', None)
        await update.message.reply_text("Ок, ввод номера отменён.")
        # Продолжаем обработку выбранного пункта меню

    # Ожидание номера машины
    if context.user_data.get('awaiting_car_number'):
        # Проверяем валидность номера
        is_valid, normalized_number, error_msg = validate_car_number(text)
        
        if not is_valid:
            await update.message.reply_text(
                f"❌ Ошибка: {error_msg}\n\n"
                f"Введите номер ещё раз:"
            )
            return
        
        # Получаем активную смену
        db_user = DatabaseManager.get_user(user.id)
        if not db_user:
            await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
            context.user_data.pop('awaiting_car_number', None)
            return
        active_shift = DatabaseManager.get_active_shift(db_user['id'])
        
        if not active_shift:
            await update.message.reply_text(
                "❌ Нет активной смены! Сначала откройте смену."
            )
            context.user_data.pop('awaiting_car_number', None)
            await update.message.reply_text(
        "Номер ТС можно указывать в любом формате:\n"
        "Х340РУ797, х340ру или даже хру340.\n\n"
        "Бот сам приведет номер к формату Х340РУ797, используя по умолчанию 797 регион"
    )
            return
        
        # Добавляем машину
        car_id = DatabaseManager.add_car(active_shift['id'], normalized_number)
        
        context.user_data.pop('awaiting_car_number', None)
        context.user_data['current_car'] = car_id
        
        await update.message.reply_text(
            f"🚗 Машина: {normalized_number}\n"
            f"Выберите услуги:",
            reply_markup=create_services_keyboard(car_id, 0, False, get_price_mode(context, db_user["id"]), db_user["id"])
        )
        return

    if context.user_data.get("awaiting_decade_goal"):
        raw_value = text.replace(" ", "").replace("₽", "")
        if not raw_value.isdigit():
            context.user_data.pop("awaiting_decade_goal", None)
            await update.message.reply_text("❌ Ввод цели отменён: нужно было ввести только цифры.")
            return
        goal_value = int(raw_value)
        db_user = DatabaseManager.get_user(user.id)
        if not db_user:
            await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
            return
        DatabaseManager.set_decade_goal(db_user["id"], goal_value)
        DatabaseManager.set_goal_enabled(db_user["id"], True)
        daily_goal = calculate_current_decade_daily_goal(db_user)
        DatabaseManager.set_daily_goal(db_user["id"], daily_goal)
        context.user_data.pop("awaiting_decade_goal", None)
        has_active = DatabaseManager.get_active_shift(db_user['id']) is not None
        await update.message.reply_text(
            "✅ Цель дня обновлена.",
            reply_markup=create_main_reply_keyboard(has_active)
        )
        await send_goal_status(update, context, db_user['id'])
        return

    awaiting_combo_name = context.user_data.get("awaiting_combo_name")
    if awaiting_combo_name:
        name = text.strip()
        if not name:
            await update.message.reply_text("Название не может быть пустым")
            return
        db_user = DatabaseManager.get_user(user.id)
        if not db_user:
            await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
            return
        service_ids = awaiting_combo_name.get("service_ids", [])
        if not service_ids:
            context.user_data.pop("awaiting_combo_name", None)
            await update.message.reply_text("❌ Список услуг пуст, начните заново.")
            return
        DatabaseManager.save_user_combo(db_user['id'], name, service_ids)
        context.user_data.pop("awaiting_combo_name", None)
        await update.message.reply_text(f"✅ Комбо «{name}» сохранено")
        return

    if context.user_data.get('awaiting_service_search'):
        query_text = text.lower().strip()
        payload = context.user_data.get('awaiting_service_search')
        if not payload:
            await update.message.reply_text("Поиск отменён. Нажмите 🔎 Поиск снова.")
            return
        car_id = payload["car_id"]
        page = payload["page"]
        db_user = DatabaseManager.get_user(user.id)
        user_id = db_user['id'] if db_user else None

        matches = []
        for service_id in get_service_order(user_id):
            service = SERVICES.get(service_id, {})
            name = plain_service_name(service.get("name", ""))
            if query_text in name.lower():
                matches.append((service_id, service))
            if len(matches) >= 12:
                break

        if not matches:
            await update.message.reply_text("Ничего не найдено. Попробуйте другое слово.")
            return

        keyboard = []
        for service_id, service in matches:
            name = plain_service_name(service["name"])
            keyboard.append([InlineKeyboardButton(name, callback_data=f"service_{service_id}_{car_id}_{page}")])
        keyboard.append([InlineKeyboardButton("❌ Отмена поиска", callback_data=f"search_cancel_{car_id}_{page}")])

        await update.message.reply_text(
            "Результаты поиска:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if context.user_data.get("tools_menu_active") and text in {
        TOOLS_PRICE,
        TOOLS_CALENDAR,
        TOOLS_HISTORY,
        TOOLS_COMBO,
        TOOLS_DECADE_GOAL,
        TOOLS_RESET,
        TOOLS_ADMIN,
        TOOLS_BACK,
    }:
        db_user = DatabaseManager.get_user(user.id)
        if text == TOOLS_BACK:
            context.user_data.pop("tools_menu_active", None)
            await update.message.reply_text("Главное меню:", reply_markup=main_menu_for_db_user(db_user, subscription_active))
            return
        if text == TOOLS_PRICE:
            await price_message(update, context)
            return
        if text == TOOLS_CALENDAR:
            await calendar_message(update, context)
            return
        if text == TOOLS_HISTORY:
            await history_message(update, context)
            return
        if text == TOOLS_COMBO:
            await combo_settings_menu_for_message(update, context)
            return
        if text == TOOLS_DECADE_GOAL:
            context.user_data["awaiting_decade_goal"] = True
            await update.message.reply_text(
                "Вы можете указать денежную цель для каждой декады.\n"
                "Исходя из этой цели бот автоматически рассчитает сколько нужно зарабатывть каждую смену чтобы к концу декады вышла эта сумма.\n\n"
                "Бот из указанной цели вычитает уже заработанную сумму за эту декаду, делит на количество оставшихся рабочих дней указанных в календаре для текущей декады (как основных, так и запланированных доп. смен) и дает динамичный расчет цели дня.\n\n"
                "При открытии смены в закрепленном сообщении будет появляться цель дня, та самая рассчитая сумма по формуле выше.\n\n"
                "Укажите цель декады. Например: 35000"
            )
            return
        if text == TOOLS_RESET:
            await update.message.reply_text("Подтверди сброс:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🗑️ Сброс всех данных", callback_data="reset_data")]]))
            return
        if text == TOOLS_ADMIN and is_admin_telegram(user.id):
            await send_admin_panel_for_message(update)
            return

    # Обработка кнопок главного меню (reply клавиатура)
    if text in {
        MENU_ADD_CAR,
        MENU_SHIFT_OPEN,
        MENU_SHIFT_CLOSE,
        MENU_CURRENT_SHIFT,
        MENU_SETTINGS,
        MENU_LEADERBOARD,
        MENU_FAQ,
        MENU_ACCOUNT,
    }:
        context.user_data.pop("tools_menu_active", None)
        if text == MENU_ADD_CAR:
            await add_car_message(update, context)
        elif text in {MENU_SHIFT_OPEN, MENU_SHIFT_CLOSE}:
            await toggle_shift_message(update, context)
        elif text == MENU_CURRENT_SHIFT:
            await current_shift_message(update, context)
        elif text == MENU_SETTINGS:
            await tools_hub_message(update, context)
        elif text == MENU_LEADERBOARD:
            await leaderboard_message(update, context)
        elif text == MENU_FAQ:
            await faq_message(update, context)
        elif text == MENU_ACCOUNT:
            await account_message(update, context)
        return

    if not subscription_active and not is_allowed_when_expired_menu(text):
        await update.message.reply_text(
            get_subscription_expired_text(),
            reply_markup=create_main_reply_keyboard(False, False)
        )
        return

    if context.user_data.get('awaiting_distance'):
        raw_value = text.replace(" ", "").replace("км", "")
        if not raw_value.isdigit():
            await update.message.reply_text("❌ Введите километраж цифрами. Например: 45")
            return
        km = int(raw_value)
        payload = context.user_data.pop('awaiting_distance')
        car_id = payload["car_id"]
        service_id = payload["service_id"]
        page = payload["page"]
        service = SERVICES.get(service_id)
        if not service:
            await update.message.reply_text("❌ Услуга не найдена.")
            return
        price = km * service.get("rate_per_km", 0)
        service_name = f"{plain_service_name(service['name'])} — {km} км"
        DatabaseManager.add_service_to_car(car_id, service_id, service_name, price)
        car = DatabaseManager.get_car(car_id)
        db_user = DatabaseManager.get_user(user.id)
        if car:
            await update.message.reply_text(
                f"✅ Добавлено: {service_name} ({format_money(price)})\n"
                f"Текущая сумма по машине: {format_money(car['total_amount'])}",
                reply_markup=create_services_keyboard(car_id, page, get_edit_mode(context, car_id), get_price_mode(context, db_user["id"] if db_user else None), db_user["id"] if db_user else None)
            )
        return
    
    if db_user_for_access:
        active_shift = DatabaseManager.get_active_shift(db_user_for_access['id'])
        if active_shift:
            is_valid, normalized_number, _ = validate_car_number(text)
            if is_valid:
                car_id = DatabaseManager.add_car(active_shift['id'], normalized_number)
                context.user_data['current_car'] = car_id
                await update.message.reply_text(
                    f"🚗 Машина: {normalized_number}\n"
                    f"Выберите услуги:",
                    reply_markup=create_services_keyboard(
                        car_id,
                        0,
                        False,
                        get_price_mode(context, db_user_for_access["id"]),
                        db_user_for_access["id"],
                    )
                )
                return

    await update.message.reply_text(
        "Используйте кнопки меню для работы с ботом.\n"
        "Напишите /start для начала."
    )

# ========== ОБРАБОТЧИКИ КНОПОК ==========

async def dispatch_exact_callback(data: str, query, context) -> bool:
    exact_handlers = {
        "open_shift": open_shift,
        "add_car": add_car,
        "current_shift": current_shift,
        "history_0": history,
        "settings": settings,
        "change_decade_goal": change_decade_goal,
        "calendar_rebase": calendar_rebase_callback,
        "leaderboard": leaderboard,
        "export_csv": export_csv,
        "backup_db": backup_db,
        "reset_data": reset_data_prompt,
        "reset_data_yes": reset_data_confirm_yes,
        "reset_data_no": reset_data_confirm_no,
        "toggle_price": toggle_price_mode,
        "combo_settings": combo_settings_menu,
        "combo_create_settings": combo_builder_start,
        "admin_panel": admin_panel,
        "admin_users": admin_users,
        "admin_broadcast_menu": admin_broadcast_menu,
        "admin_broadcast_all": lambda q, c: admin_broadcast_prepare(q, c, "all"),
        "admin_broadcast_expiring_1d": lambda q, c: admin_broadcast_prepare(q, c, "expiring_1d"),
        "admin_broadcast_expired": lambda q, c: admin_broadcast_prepare(q, c, "expired"),
        "admin_broadcast_pick_user": admin_broadcast_pick_user,
        "admin_broadcast_cancel": admin_broadcast_cancel,
        "faq": faq_callback,
        "nav_shift": nav_shift_callback,
        "nav_navigator": nav_navigator_callback,
        "nav_history": nav_history_callback,
        "nav_tools": nav_tools_callback,
        "nav_help": nav_help_callback,
        "subscription_info": subscription_info_callback,
        "subscription_info_photo": subscription_info_photo_callback,
        "account_info": account_info_callback,
        "show_price": show_price_callback,
        "calendar_open": calendar_callback,
        "faq_overview": faq_overview_callback,
        "faq_start_demo": demo_start,
        "demo_step_shift": demo_step_shift_callback,
        "demo_step_services": lambda q, c: demo_render_card(q, c, "services"),
        "demo_step_services_adv": lambda q, c: demo_render_card(q, c, "services_adv"),
        "demo_step_calendar": lambda q, c: demo_render_card(q, c, "calendar"),
        "demo_step_leaderboard": lambda q, c: demo_render_card(q, c, "leaderboard"),
        "demo_step_done": lambda q, c: demo_render_card(q, c, "done"),
        "demo_exit": demo_exit_callback,
        "onb:start": onboarding_start,
        "onb:skip": onboarding_skip,
        "onb:exit": onboarding_exit,
        "onb:step_shift": onboarding_step_shift,
        "onb:step_car": onboarding_step_car,
        "onb:step_services": onboarding_step_services,
        "onb:save_services": onboarding_save_services,
        "onb:step_dashboard": onboarding_step_dashboard,
        "onb:step_top": onboarding_step_top,
        "onb:finish": onboarding_finish,
        "nav:back": nav_back_callback,
        "admin_faq_menu": admin_faq_menu,
        "admin_media_menu": admin_media_menu,
        "admin_media_set_profile": lambda q, c: admin_media_set_target(q, c, "profile"),
        "admin_media_set_leaderboard": lambda q, c: admin_media_set_target(q, c, "leaderboard"),
        "admin_media_clear_profile": lambda q, c: admin_media_clear_target(q, c, "profile"),
        "admin_media_clear_leaderboard": lambda q, c: admin_media_clear_target(q, c, "leaderboard"),
        "admin_faq_set_text": admin_faq_set_text,
        "admin_faq_set_video": admin_faq_set_video,
        "admin_faq_preview": admin_faq_preview,
        "admin_faq_clear_video": admin_faq_clear_video,
        "admin_faq_topics": admin_faq_topics,
        "admin_faq_topic_add": admin_faq_topic_add,
        "combo_builder_save": combo_builder_save,
        "history_decades": history_decades,
        "back": go_back,
        "cleanup_data": cleanup_data_menu,
        "cancel_add_car": cancel_add_car_callback,
        "noop": noop_callback,
    }

    handler = exact_handlers.get(data)
    if not handler:
        return False
    await handler(query, context)
    return True


async def demo_step_shift_callback(query, context):
    context.user_data["demo_mode"] = True
    context.user_data["demo_waiting_car"] = True
    await demo_render_card(query, context, "shift")


async def demo_exit_callback(query, context):
    context.user_data.pop("demo_mode", None)
    context.user_data.pop("demo_waiting_car", None)
    context.user_data.pop("demo_payload", None)
    await query.edit_message_text("Демо завершено. Нажми ❓ FAQ, чтобы пройти снова.")


async def nav_back_callback(query, context):
    pop_screen(context)
    prev = get_current_screen(context)
    if not prev:
        db_user = DatabaseManager.get_user(query.from_user.id)
        has_active = bool(db_user and DatabaseManager.get_active_shift(db_user['id']))
        await query.edit_message_text("Главное меню уже внизу 👇")
        await query.message.reply_text("Выбери действие:", reply_markup=create_main_reply_keyboard(has_active))
        return

    name = prev.name
    if name == "onboarding_start":
        await onboarding_start(query, context)
    elif name == "onboarding_shift":
        await onboarding_step_shift(query, context)
    elif name == "onboarding_car":
        await onboarding_step_car(query, context)
    elif name == "onboarding_services":
        await onboarding_step_services(query, context)
    elif name == "onboarding_dashboard":
        await onboarding_step_dashboard(query, context)
    else:
        await query.edit_message_text("Главное меню уже внизу 👇")


def _onb_state(context):
    return context.user_data.setdefault("onboarding_state", {"mode": "demo", "services": [], "cars": 0, "amount": 0})


async def onboarding_start(query, context):
    push_screen(context, Screen(name="onboarding_start", kind="inline"))
    st = _onb_state(context)
    st["mode"] = "demo"
    st["services"] = []
    st["cars"] = 0
    st["amount"] = 0
    await query.edit_message_text(
        "🚀 Быстрый тур\n\n"
        "Шаг 1/4: Открываем смену в демо-режиме (реальные данные не изменяются).",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Открыть смену (демо)", callback_data="onb:step_shift")],
            [InlineKeyboardButton("✖️ Выйти из тура", callback_data="onb:exit")],
        ])
    )


async def onboarding_skip(query, context):
    await query.edit_message_text("Окей, пропускаем тур. Можешь начать работу с меню ниже 👇")


async def onboarding_exit(query, context):
    context.user_data.pop("onboarding_state", None)
    await query.edit_message_text("Тур завершён. В любой момент можно запустить снова из FAQ.")


async def onboarding_step_shift(query, context):
    push_screen(context, Screen(name="onboarding_shift", kind="inline"))
    await query.edit_message_text(
        "✅ Шаг 1/4: Смена в демо открыта.\n\n"
        "Теперь добавим машину.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Добавить машину (демо)", callback_data="onb:step_car")],
            [InlineKeyboardButton("🔙 Назад", callback_data="nav:back")],
            [InlineKeyboardButton("✖️ Выйти из тура", callback_data="onb:exit")],
        ])
    )


async def onboarding_step_car(query, context):
    push_screen(context, Screen(name="onboarding_car", kind="inline"))
    st = _onb_state(context)
    st["cars"] = 1
    await query.edit_message_text(
        "🚗 Шаг 2/4: Машина добавлена в демо.\n\n"
        "Выбери 2-3 услуги как в реальном процессе.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🧼 Проверка", callback_data="onb:svc_1"), InlineKeyboardButton("⛽ Заправка", callback_data="onb:svc_2")],
            [InlineKeyboardButton("🧴 Омывайка", callback_data="onb:svc_3"), InlineKeyboardButton("🅿️ Перепарковка", callback_data="onb:svc_14")],
            [InlineKeyboardButton("💾 Сохранить (демо)", callback_data="onb:save_services")],
            [InlineKeyboardButton("🔙 Назад", callback_data="nav:back")],
            [InlineKeyboardButton("✖️ Выйти из тура", callback_data="onb:exit")],
        ])
    )


async def onboarding_step_services(query, context):
    st = _onb_state(context)
    selected = st.get("services", [])
    amount = sum(get_current_price(sid, "day") for sid in selected)
    st["amount"] = amount
    await query.edit_message_text(
        f"🧾 Шаг 2/4: Услуги выбраны.\nВыбрано: {len(selected)}\nСумма: {format_money(amount)}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💾 Сохранить (демо)", callback_data="onb:save_services")],
            [InlineKeyboardButton("🔙 Назад", callback_data="nav:back")],
            [InlineKeyboardButton("✖️ Выйти из тура", callback_data="onb:exit")],
        ])
    )


async def onboarding_toggle_service(query, context, data):
    sid = int(data.replace("onb:svc_", ""))
    st = _onb_state(context)
    selected = st.get("services", [])
    if sid in selected:
        selected.remove(sid)
    else:
        selected.append(sid)
    st["services"] = selected
    context.user_data["onboarding_state"] = st
    await onboarding_step_services(query, context)


async def onboarding_save_services(query, context):
    push_screen(context, Screen(name="onboarding_services", kind="inline"))
    await onboarding_step_dashboard(query, context)


async def onboarding_step_dashboard(query, context):
    push_screen(context, Screen(name="onboarding_dashboard", kind="inline"))
    st = _onb_state(context)
    cars = st.get("cars", 1)
    total = st.get("amount", 0)
    avg = int(total / max(cars, 1))
    await query.edit_message_text(
        "📊 Шаг 3/4: Дашборд демо\n\n"
        f"Машин: {cars}\n"
        f"Сумма: {format_money(total)}\n"
        f"Средний чек: {format_money(avg)}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🏆 Показать топ (демо)", callback_data="onb:step_top")],
            [InlineKeyboardButton("🔙 Назад", callback_data="nav:back")],
            [InlineKeyboardButton("✖️ Выйти из тура", callback_data="onb:exit")],
        ])
    )


async def onboarding_step_top(query, context):
    leaders = [
        {"name": "Вы", "total_amount": 16500, "shift_count": 3, "telegram_id": query.from_user.id},
        {"name": "Коллега 1", "total_amount": 18900, "shift_count": 4, "telegram_id": 0},
        {"name": "Коллега 2", "total_amount": 17200, "shift_count": 3, "telegram_id": 0},
        {"name": "Коллега 3", "total_amount": 14900, "shift_count": 3, "telegram_id": 0},
    ]
    status = await send_status(update=type("U", (), {"callback_query": None, "message": query.message, "effective_chat": query.message.chat})(), context=context, text="🖼 Собираю красивую картинку…")
    avatars = {1: await get_avatar_image_async(context.bot, query.from_user.id, 140, fallback_name="Вы")}
    img = build_leaderboard_image_bytes("Демо-декада", leaders, highlight_name="Вы", top3_avatars=avatars)
    await done_status(status, "✅ Готово! Вот ваш демо-топ.", attach_photo_bytes=img, filename="demo_top.png", caption="🏆 Демо-топ")
    await query.message.reply_text(
        "🎉 Шаг 4/4 завершён.\nГотово! Теперь вы умеете всё базовое.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Открыть главное меню", callback_data="back")],
            [InlineKeyboardButton("Открыть инструменты", callback_data="nav_tools")],
        ])
    )


async def onboarding_finish(query, context):
    await query.edit_message_text("Тур завершён.")


async def cancel_add_car_callback(query, context):
    context.user_data.pop('awaiting_car_number', None)
    await query.edit_message_text("Ок, добавление машины отменено.")
    db_user = DatabaseManager.get_user(query.from_user.id)
    await query.message.reply_text(
        "Выбери действие:",
        reply_markup=main_menu_for_db_user(db_user)
    )


async def noop_callback(query, context):
    del query, context


async def handle_callback(update: Update, context: CallbackContext):
    """Главный обработчик callback-кнопок"""
    query = update.callback_query
    await query.answer()

    data = query.data
    user = query.from_user

    logger.info(f"Callback: {data} from {user.id}")

    _, blocked, subscription_active = resolve_user_access(user.id, context)
    if blocked:
        await query.edit_message_text("⛔ Доступ к боту закрыт администратором.")
        return

    if not subscription_active and not is_allowed_when_expired_callback(data):
        await query.edit_message_text(get_subscription_expired_text())
        await query.message.reply_text(
            "Доступные действия:",
            reply_markup=create_main_reply_keyboard(False, False)
        )
        return

    if await dispatch_exact_callback(data, query, context):
        return

    prefix_handlers = getattr(handle_callback, "_prefix_handlers", None)
    if prefix_handlers is None:
        prefix_handlers = [
            ("service_page_", change_services_page),
        ("toggle_price_car_", toggle_price_mode_for_car),
        ("repeat_prev_", repeat_prev_services),
        ("service_search_", start_service_search),
        ("search_text_", search_enter_text_mode),
        ("search_cancel_", search_cancel),
        ("combo_menu_", show_combo_menu),
        ("combo_apply_", apply_combo_to_car),
        ("combo_save_from_car_", save_combo_from_car),
        ("combo_delete_prompt_", delete_combo_prompt),
        ("combo_delete_confirm_", delete_combo),
        ("combo_edit_", combo_edit_menu),
        ("combo_rename_", combo_start_rename),
        ("childsvc_", add_group_child_service),
        ("back_to_services_", back_to_services),
        ("service_", add_service),
        ("clear_", clear_services_prompt),
        ("confirm_clear_", clear_services),
        ("save_", save_car),
        ("shift_repeats_", export_shift_repeats),
        ("combo_builder_toggle_", combo_builder_toggle),
        ("admin_user_", admin_user_card),
        ("admin_toggle_block_", admin_toggle_block),
        ("admin_activate_month_", admin_activate_month),
        ("admin_activate_days_prompt_", admin_activate_days_prompt),
        ("admin_broadcast_user_", lambda q, c, d: admin_broadcast_prepare(q, c, d.replace("admin_broadcast_user_", ""))),
        ("calendar_nav_", calendar_nav_callback),
        ("calendar_day_", calendar_day_callback),
        ("calendar_setup_pick_", calendar_setup_pick_callback),
        ("calendar_setup_save_", calendar_setup_save_callback),
        ("calendar_edit_toggle_", calendar_edit_toggle_callback),
        ("demo_service_", demo_toggle_service_callback),
        ("demo_calendar_", demo_toggle_calendar_day_callback),
        ("faq_topic_", faq_topic_callback),
        ("admin_faq_topic_edit_", admin_faq_topic_edit),
        ("admin_faq_topic_del_", admin_faq_topic_del),
        ("onb:svc_", onboarding_toggle_service),
        ("history_decades_page_", history_decades_page),
        ("history_decade_", history_decade_days),
        ("history_day_", history_day_cars),
        ("history_edit_car_", history_edit_car),
        ("cleanup_month_", cleanup_month),
        ("cleanup_day_", cleanup_day),
        ("day_repeats_", day_repeats_callback),
        ("delcar_", delete_car_callback),
        ("delday_prompt_", delete_day_prompt),
        ("delday_confirm_", delete_day_callback),
        ("toggle_edit_", toggle_edit),
        ("close_confirm_yes_", close_shift_confirm_yes),
        ("close_confirm_no_", close_shift_confirm_no),
        ("close_", close_shift_confirm_prompt),
        ]
        handle_callback._prefix_handlers = prefix_handlers

    for prefix, handler in prefix_handlers:
        if data.startswith(prefix):
            try:
                if prefix == "close_confirm_no_":
                    await handler(query, context)
                else:
                    await handler(query, context, data)
            except (ValueError, IndexError) as exc:
                logger.warning(f"Некорректный callback payload {data}: {exc}")
                await query.answer("Некорректные данные кнопки", show_alert=True)
            return

    await query.edit_message_text("❌ Неизвестная команда")


async def demo_toggle_calendar_day_callback(query, context, data):
    key = data.replace("demo_calendar_", "")
    payload = context.user_data.get("demo_payload", {"services": [], "calendar_days": []})
    selected = payload.get("calendar_days", [])
    if key in selected:
        selected.remove(key)
    else:
        selected.append(key)
    payload["calendar_days"] = selected
    context.user_data["demo_payload"] = payload
    await demo_render_card(query, context, "calendar")


async def demo_toggle_service_callback(query, context, data):
    sid = int(data.replace("demo_service_", ""))
    payload = context.user_data.get("demo_payload", {"services": []})
    selected = payload.get("services", [])
    if sid in selected:
        selected.remove(sid)
    else:
        selected.append(sid)
    payload["services"] = selected
    context.user_data["demo_payload"] = payload
    await demo_render_card(query, context, "services")




def open_shift_core(db_user: dict) -> tuple[bool, str, bool]:
    active_shift = DatabaseManager.get_active_shift(db_user['id'])
    if active_shift:
        start_time = parse_datetime(active_shift['start_time'])
        time_text = start_time.strftime('%H:%M %d.%m') if start_time else "неизвестно"
        return False, f"❌ У вас уже есть активная смена!\nНачата: {time_text}", False

    DatabaseManager.start_shift(db_user['id'])
    today = now_local().date()
    marked_extra = False
    if get_work_day_type(db_user, today) == "off":
        DatabaseManager.set_calendar_override(db_user["id"], today.isoformat(), "extra")
        marked_extra = True

    message = (
        f"✅ Смена открыта!\n"
        f"Время: {now_local().strftime('%H:%M %d.%m.%Y')}\n\n"
        f"Теперь можно добавлять машины."
    )
    if marked_extra:
        message += "\n\n🟡 День отмечен как доп. смена в календаре."
    return True, message, marked_extra


async def open_shift(query, context):
    """Открытие смены"""
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)

    if not db_user:
        await query.edit_message_text("❌ Ошибка: пользователь не найден")
        return

    opened, message, _ = open_shift_core(db_user)
    await query.edit_message_text(message)
    await query.message.reply_text(
        "Выбери действие:",
        reply_markup=main_menu_for_db_user(db_user, True)
    )
    if DatabaseManager.is_goal_enabled(db_user["id"]):
        daily_goal = calculate_current_decade_daily_goal(db_user)
        DatabaseManager.set_daily_goal(db_user["id"], daily_goal)
        await send_goal_status(None, context, db_user['id'], source_message=query.message)

async def add_car(query, context):
    """Добавление машины"""
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    
    if not db_user:
        await query.edit_message_text("❌ Ошибка: пользователь не найден")
        return
    
    # Проверяем активную смену
    active_shift = DatabaseManager.get_active_shift(db_user['id'])
    if not active_shift:
        await query.edit_message_text(
            "❌ Нет активной смены!\n"
            "Сначала откройте смену."
        )
        await query.message.reply_text(
            "Выбери действие:",
            reply_markup=create_main_reply_keyboard(False)
        )
        return
    
    context.user_data['awaiting_car_number'] = True
    await query.edit_message_text("Чтобы добавить машину введите номер ТС в свободном формате.\n\nКнопку, кстати, для этого нажимать не обязательно.")

async def current_shift(query, context):
    """Текущая смена"""
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)

    if not db_user:
        await query.edit_message_text("❌ Ошибка: пользователь не найден")
        return

    active_shift = DatabaseManager.get_active_shift(db_user['id'])
    if not active_shift:
        await query.edit_message_text(
            "📭 Нет активной смены.\n"
            "Откройте смену для начала работы."
        )
        await query.message.reply_text(
            "Выбери действие:",
            reply_markup=create_main_reply_keyboard(False)
        )
        return

    cars = DatabaseManager.get_shift_cars(active_shift['id'])
    total = DatabaseManager.get_shift_total(active_shift['id'])
    message = build_current_shift_dashboard(db_user['id'], active_shift, cars, total)

    await query.edit_message_text(
        message,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 В меню", callback_data="back")],
        ]),
    )
    await query.message.reply_text(
        "Выбери действие:",
        reply_markup=create_main_reply_keyboard(True)
    )

async def history(query, context):
    await history_decades(query, context)


async def settings(query, context):
    """Настройки"""
    db_user = DatabaseManager.get_user(query.from_user.id)
    await query.edit_message_text(
        f"⚙️ НАСТРОЙКИ\n\nВерсия: {APP_VERSION}\nОбновлено: {APP_UPDATED_AT}\n\nВыберите параметр:",
        reply_markup=build_settings_keyboard(db_user, is_admin_telegram(query.from_user.id))
    )

async def combo_builder_start(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    context.user_data["combo_builder"] = {"selected": [], "page": 0}
    await combo_builder_render(query, context, db_user["id"])


async def combo_builder_render(query, context, user_id: int):
    payload = context.user_data.get("combo_builder", {"selected": [], "page": 0})
    selected = payload.get("selected", [])
    page = payload.get("page", 0)
    service_ids = get_service_order(user_id)
    per_page = 8
    max_page = max((len(service_ids) - 1) // per_page, 0)
    page = max(0, min(page, max_page))
    payload["page"] = page
    context.user_data["combo_builder"] = payload

    chunk = service_ids[page * per_page:(page + 1) * per_page]
    keyboard = []
    for sid in chunk:
        mark = "✅" if sid in selected else "▫️"
        keyboard.append([InlineKeyboardButton(f"{mark} {plain_service_name(SERVICES[sid]['name'])}", callback_data=f"combo_builder_toggle_{sid}")])

    nav = [InlineKeyboardButton(f"Стр {page + 1}/{max_page + 1}", callback_data="noop")]
    if page > 0:
        nav.insert(0, InlineKeyboardButton("⬅️", callback_data="combo_builder_toggle_prev"))
    if page < max_page:
        nav.append(InlineKeyboardButton("➡️", callback_data="combo_builder_toggle_next"))
    keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("💾 Сохранить комбо", callback_data="combo_builder_save")])
    keyboard.append([InlineKeyboardButton("🔙 В настройки", callback_data="settings")])

    text = f"🧩 Конструктор комбо\nВыбрано услуг: {len(selected)}\nОтметьте нужные услуги и нажмите «Сохранить комбо»."
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def combo_builder_toggle(query, context, data):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    payload = context.user_data.get("combo_builder", {"selected": [], "page": 0})
    selected = payload.get("selected", [])
    if data.endswith("_prev"):
        payload["page"] = max(payload.get("page", 0) - 1, 0)
    elif data.endswith("_next"):
        payload["page"] = payload.get("page", 0) + 1
    else:
        sid = int(data.replace("combo_builder_toggle_", ""))
        if sid in selected:
            selected.remove(sid)
        else:
            selected.append(sid)
        payload["selected"] = selected
    context.user_data["combo_builder"] = payload
    await combo_builder_render(query, context, db_user["id"])


async def combo_builder_save(query, context):
    payload = context.user_data.get("combo_builder")
    if not payload or not payload.get("selected"):
        await query.answer("Сначала выберите хотя бы одну услугу")
        return
    context.user_data["awaiting_combo_name"] = {"service_ids": payload["selected"], "car_id": None, "page": 0}
    await query.edit_message_text("Введите название нового комбо в чат")


async def admin_panel(query, context):
    if not is_admin_telegram(query.from_user.id):
        await query.edit_message_text("⛔ Доступно только администратору")
        return
    keyboard = [
        [InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton("📣 Рассылка", callback_data="admin_broadcast_menu")],
        [InlineKeyboardButton("❓ Редактировать FAQ", callback_data="admin_faq_menu")],
        [InlineKeyboardButton("🖼 Медиа разделов", callback_data="admin_media_menu")],
        [InlineKeyboardButton("🔙 В настройки", callback_data="settings")],
    ]
    await query.edit_message_text("🛡️ Админ-панель\nВыберите раздел:", reply_markup=InlineKeyboardMarkup(keyboard))




async def send_admin_panel_for_message(update: Update):
    keyboard = [
        [InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton("📣 Рассылка", callback_data="admin_broadcast_menu")],
        [InlineKeyboardButton("❓ Редактировать FAQ", callback_data="admin_faq_menu")],
        [InlineKeyboardButton("🖼 Медиа разделов", callback_data="admin_media_menu")],
        [InlineKeyboardButton("🔙 В настройки", callback_data="settings")],
    ]
    await update.message.reply_text("🛡️ Админ-панель\nВыберите раздел:", reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_users(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    users = DatabaseManager.get_all_users_with_stats()
    keyboard = []
    for row in users[:30]:
        status = "⛔" if int(row.get("is_blocked", 0)) else "✅"
        keyboard.append([InlineKeyboardButton(f"{status} {row['name']} ({row['telegram_id']})", callback_data=f"admin_user_{row['id']}")])
    keyboard.append([InlineKeyboardButton("🔙 В админку", callback_data="admin_panel")])
    await query.edit_message_text("👥 Пользователи:", reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_user_card(query, context, data):
    if not is_admin_telegram(query.from_user.id):
        return
    user_id = int(data.replace("admin_user_", ""))
    users = {u["id"]: u for u in DatabaseManager.get_all_users_with_stats()}
    row = users.get(user_id)
    if not row:
        await query.answer("Пользователь не найден")
        return
    blocked = bool(int(row.get("is_blocked", 0)))
    target_user = DatabaseManager.get_user_by_id(user_id)
    expires = subscription_expires_at_for_user(target_user) if target_user else None
    sub_status = "♾️ Админ" if is_admin_telegram(int(row["telegram_id"])) else (
        f"до {format_subscription_until(expires)}" if expires and now_local() <= expires else "истекла"
    )
    keyboard = [
        [InlineKeyboardButton("🔓 Открыть доступ" if blocked else "⛔ Закрыть доступ", callback_data=f"admin_toggle_block_{user_id}")],
        [InlineKeyboardButton("🗓️ Активировать на месяц", callback_data=f"admin_activate_month_{user_id}")],
        [InlineKeyboardButton("✍️ Активировать на N дней", callback_data=f"admin_activate_days_prompt_{user_id}")],
        [InlineKeyboardButton("🔙 К пользователям", callback_data="admin_users")],
    ]
    await query.edit_message_text(
        f"👤 {row['name']}\nTelegram ID: {row['telegram_id']}\n"
        f"Смен: {row['shifts_count']}\nСумма: {format_money(int(row['total_amount'] or 0))}\n"
        f"Статус: {'Заблокирован' if blocked else 'Активен'}\n"
        f"Подписка: {sub_status}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def admin_toggle_block(query, context, data):
    if not is_admin_telegram(query.from_user.id):
        return
    user_id = int(data.replace("admin_toggle_block_", ""))
    users = {u["id"]: u for u in DatabaseManager.get_all_users_with_stats()}
    row = users.get(user_id)
    if not row:
        await query.answer("Пользователь не найден")
        return
    new_state = not bool(int(row.get("is_blocked", 0)))
    DatabaseManager.set_user_blocked(user_id, new_state)
    await admin_user_card(query, context, f"admin_user_{user_id}")


async def admin_activate_month(query, context, data):
    if not is_admin_telegram(query.from_user.id):
        return
    user_id = int(data.replace("admin_activate_month_", ""))
    target_user = DatabaseManager.get_user_by_id(user_id)
    if not target_user:
        await query.answer("Пользователь не найден")
        return
    expires = activate_subscription_days(user_id, 30)
    await query.answer("Подписка на 30 дней активирована")
    try:
        await context.bot.send_message(
            chat_id=target_user["telegram_id"],
            text=(
                "✅ Ваш аккаунт активирован на 30 дн.!\n"
                f"Доступ до: {format_subscription_until(expires)}\n"
                "Приятного пользования ботом."
            )
        )
    except Exception:
        pass
    await admin_user_card(query, context, f"admin_user_{user_id}")


async def admin_activate_days_prompt(query, context, data):
    if not is_admin_telegram(query.from_user.id):
        return
    user_id = int(data.replace("admin_activate_days_prompt_", ""))
    context.user_data["awaiting_admin_subscription_days"] = user_id
    await query.edit_message_text(
        "Введите количество дней для активации (например, 45)."
    )


def get_broadcast_recipients(target: str, admin_db_user: dict) -> list[int]:
    users = DatabaseManager.get_all_users_with_stats()
    now_dt = now_local()
    recipients: list[int] = []

    for row in users:
        telegram_id = int(row["telegram_id"])
        if telegram_id == admin_db_user["telegram_id"]:
            continue
        if int(row.get("is_blocked", 0)) == 1:
            continue

        user_db = DatabaseManager.get_user_by_id(int(row["id"]))
        expires_at = subscription_expires_at_for_user(user_db) if user_db else None

        if target == "all":
            recipients.append(telegram_id)
        elif target == "expiring_1d":
            if expires_at and now_dt <= expires_at <= now_dt + timedelta(days=1):
                recipients.append(telegram_id)
        elif target == "expired":
            if expires_at and expires_at < now_dt:
                recipients.append(telegram_id)
        else:
            try:
                if telegram_id == int(target):
                    recipients.append(telegram_id)
            except ValueError:
                continue

    return recipients


async def admin_broadcast_menu(query, context):
    if not is_admin_telegram(query.from_user.id):
        await query.edit_message_text("⛔ Доступно только администратору")
        return
    keyboard = [
        [InlineKeyboardButton("📢 Всем пользователям", callback_data="admin_broadcast_all")],
        [InlineKeyboardButton("⏳ Истекает за 1 день", callback_data="admin_broadcast_expiring_1d")],
        [InlineKeyboardButton("🚫 Подписка истекла", callback_data="admin_broadcast_expired")],
        [InlineKeyboardButton("👤 Выбрать одного", callback_data="admin_broadcast_pick_user")],
        [InlineKeyboardButton("🔙 В админку", callback_data="admin_panel")],
    ]
    await query.edit_message_text("📣 Рассылка\nВыберите получателей:", reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_broadcast_pick_user(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    users = DatabaseManager.get_all_users_with_stats()
    keyboard = []
    for row in users[:30]:
        keyboard.append([InlineKeyboardButton(f"{row['name']} ({row['telegram_id']})", callback_data=f"admin_broadcast_user_{row['telegram_id']}")])
    keyboard.append([InlineKeyboardButton("🔙 К рассылке", callback_data="admin_broadcast_menu")])
    await query.edit_message_text("Выбери пользователя:", reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_broadcast_prepare(query, context, target: str):
    if not is_admin_telegram(query.from_user.id):
        return
    context.user_data["awaiting_admin_broadcast"] = target
    await query.edit_message_text(
        "Введите текст рассылки одним сообщением.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_broadcast_cancel")]])
    )


async def admin_broadcast_cancel(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    context.user_data.pop("awaiting_admin_broadcast", None)
    await admin_broadcast_menu(query, context)


async def process_admin_broadcast(update: Update, context: CallbackContext, admin_db_user: dict):
    target = context.user_data.pop("awaiting_admin_broadcast", None)
    if not target:
        return False

    text = (update.message.text or "").strip()
    recipients = get_broadcast_recipients(target, admin_db_user)

    sent = 0
    failed = 0
    for telegram_id in recipients:
        if telegram_id == admin_db_user["telegram_id"]:
            continue
        try:
            await context.bot.send_message(chat_id=telegram_id, text=text)
            sent += 1
        except Exception:
            failed += 1

    has_active = DatabaseManager.get_active_shift(admin_db_user['id']) is not None
    await update.message.reply_text(
        f"📣 Рассылка завершена.\nОтправлено: {sent}\nОшибок: {failed}",
        reply_markup=create_main_reply_keyboard(has_active)
    )
    return True


async def show_price_callback(query, context):
    await query.edit_message_text(
        build_price_text(),
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
    )


async def price_message(update: Update, context: CallbackContext):
    db_user = DatabaseManager.get_user(update.effective_user.id)
    if not db_user:
        await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
        return
    await update.message.reply_text(
        build_price_text(),
        reply_markup=create_main_reply_keyboard(
            bool(DatabaseManager.get_active_shift(db_user['id'])),
            is_subscription_active(db_user),
        )
    )


async def calendar_message(update: Update, context: CallbackContext):
    db_user = DatabaseManager.get_user(update.effective_user.id)
    if not db_user:
        await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
        return
    today = now_local().date()
    year, month = today.year, today.month
    anchor_set = bool(DatabaseManager.get_work_anchor_date(db_user["id"]))
    context.user_data["calendar_month"] = (year, month)
    context.user_data.setdefault("calendar_edit_mode", False)
    context.user_data.setdefault("calendar_setup_days", [])

    await update.message.reply_text(
        build_work_calendar_text(db_user, year, month, setup_mode=not anchor_set, edit_mode=context.user_data.get("calendar_edit_mode", False)),
        reply_markup=build_work_calendar_keyboard(
            db_user,
            year,
            month,
            setup_mode=not anchor_set,
            setup_selected=context.user_data.get("calendar_setup_days", []),
            edit_mode=context.user_data.get("calendar_edit_mode", False),
        )
    )


async def calendar_callback(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    today = now_local().date()
    year, month = context.user_data.get("calendar_month", (today.year, today.month))
    anchor_set = bool(DatabaseManager.get_work_anchor_date(db_user["id"]))
    setup_mode = not anchor_set
    await query.edit_message_text(
        build_work_calendar_text(db_user, year, month, setup_mode=setup_mode, edit_mode=context.user_data.get("calendar_edit_mode", False)),
        reply_markup=build_work_calendar_keyboard(
            db_user,
            year,
            month,
            setup_mode=setup_mode,
            setup_selected=context.user_data.get("calendar_setup_days", []),
            edit_mode=context.user_data.get("calendar_edit_mode", False),
        )
    )


async def calendar_nav_callback(query, context, data):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    _, _, y, m, direction = data.split("_")
    year, month = int(y), int(m)
    if direction == "prev":
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
    else:
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    context.user_data["calendar_month"] = (year, month)
    anchor_set = bool(DatabaseManager.get_work_anchor_date(db_user["id"]))
    setup_mode = not anchor_set
    await query.edit_message_text(
        build_work_calendar_text(db_user, year, month, setup_mode=setup_mode, edit_mode=context.user_data.get("calendar_edit_mode", False)),
        reply_markup=build_work_calendar_keyboard(
            db_user,
            year,
            month,
            setup_mode=setup_mode,
            setup_selected=context.user_data.get("calendar_setup_days", []),
            edit_mode=context.user_data.get("calendar_edit_mode", False),
        )
    )


async def calendar_setup_pick_callback(query, context, data):
    day = data.replace("calendar_setup_pick_", "")
    selected = context.user_data.get("calendar_setup_days", [])
    if day in selected:
        selected.remove(day)
    else:
        if len(selected) >= 2:
            selected.pop(0)
        selected.append(day)
    context.user_data["calendar_setup_days"] = selected

    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    year, month = context.user_data.get("calendar_month", (now_local().year, now_local().month))
    await query.edit_message_text(
        build_work_calendar_text(db_user, year, month, setup_mode=True),
        reply_markup=build_work_calendar_keyboard(
            db_user,
            year,
            month,
            setup_mode=True,
            setup_selected=selected,
            edit_mode=False,
        )
    )


async def calendar_setup_save_callback(query, context, data):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    selected = sorted(context.user_data.get("calendar_setup_days", []))
    if len(selected) != 2:
        await query.answer("Выбери 2 дня", show_alert=True)
        return

    d1 = parse_iso_date(selected[0])
    d2 = parse_iso_date(selected[1])
    if not d1 or not d2 or abs((d2 - d1).days) != 1:
        await query.answer("Нужно выбрать 2 подряд идущих дня", show_alert=True)
        return

    anchor = min(d1, d2).isoformat()
    DatabaseManager.set_work_anchor_date(db_user["id"], anchor)
    context.user_data["calendar_setup_days"] = []
    year, month = context.user_data.get("calendar_month", (now_local().year, now_local().month))
    await query.edit_message_text(
        build_work_calendar_text(db_user, year, month, setup_mode=False, edit_mode=context.user_data.get("calendar_edit_mode", False)),
        reply_markup=build_work_calendar_keyboard(
            db_user,
            year,
            month,
            setup_mode=False,
            setup_selected=[],
            edit_mode=context.user_data.get("calendar_edit_mode", False),
        )
    )


async def calendar_edit_toggle_callback(query, context, data):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    context.user_data["calendar_edit_mode"] = not context.user_data.get("calendar_edit_mode", False)
    _, _, _, y, m = data.split("_")
    year, month = int(y), int(m)
    context.user_data["calendar_month"] = (year, month)
    await query.edit_message_text(
        build_work_calendar_text(db_user, year, month, setup_mode=False, edit_mode=context.user_data.get("calendar_edit_mode", False)),
        reply_markup=build_work_calendar_keyboard(
            db_user,
            year,
            month,
            setup_mode=False,
            setup_selected=[],
            edit_mode=context.user_data.get("calendar_edit_mode", False),
        )
    )


async def render_calendar_day_card(query, context, db_user: dict, day: str):
    target = parse_iso_date(day)
    if not target:
        await query.answer("Некорректная дата")
        return

    day_type = get_work_day_type(db_user, target)

    month_key = day[:7]
    month_days = DatabaseManager.get_days_for_month(db_user["id"], month_key)
    has_day = any(row.get("day") == day and int(row.get("shifts_count", 0)) > 0 for row in month_days)
    if has_day and day_type == "off":
        day_type = "extra"

    day_type_text = {
        "planned": "🔴 Основная смена",
        "extra": "🟡 Доп. смена",
        "off": "⚪ Выходной",
    }.get(day_type, "⚪ Выходной")

    text = (
        f"📅 Карточка дня: {day}\n"
        f"План: {day_type_text}\n"
        f"Факт: {'есть смены' if has_day else 'смен нет'}"
    )
    keyboard = []
    if has_day:
        keyboard.append([InlineKeyboardButton("📂 Открыть историю дня", callback_data=f"history_day_{day}")])
    keyboard.append([
        InlineKeyboardButton("✅ Сделать рабочим", callback_data=f"calendar_set_planned_{day}"),
        InlineKeyboardButton("🚫 Сделать выходным", callback_data=f"calendar_set_off_{day}"),
    ])
    keyboard.append([InlineKeyboardButton("➕ Сделать доп. сменой", callback_data=f"calendar_set_extra_{day}")])
    keyboard.append([InlineKeyboardButton("♻️ Сбросить ручную правку", callback_data=f"calendar_set_reset_{day}")])
    keyboard.append([InlineKeyboardButton("🔙 К месяцу", callback_data=f"calendar_back_month_{day[:7]}")])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def calendar_set_day_type_callback(query, context, data):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    body = data.replace("calendar_set_", "")
    mode, day = body.split("_", 1)
    if mode == "planned":
        DatabaseManager.set_calendar_override(db_user["id"], day, "planned")
    elif mode == "off":
        DatabaseManager.set_calendar_override(db_user["id"], day, "off")
    elif mode == "extra":
        DatabaseManager.set_calendar_override(db_user["id"], day, "extra")
    else:
        DatabaseManager.set_calendar_override(db_user["id"], day, "")

    await render_calendar_day_card(query, context, db_user, day)


async def calendar_back_month_callback(query, context, data):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    ym = data.replace("calendar_back_month_", "")
    year_s, month_s = ym.split("-")
    year, month = int(year_s), int(month_s)
    context.user_data["calendar_month"] = (year, month)
    anchor_set = bool(DatabaseManager.get_work_anchor_date(db_user["id"]))
    await query.edit_message_text(
        build_work_calendar_text(db_user, year, month, setup_mode=not anchor_set, edit_mode=context.user_data.get("calendar_edit_mode", False)),
        reply_markup=build_work_calendar_keyboard(
            db_user,
            year,
            month,
            setup_mode=not anchor_set,
            setup_selected=context.user_data.get("calendar_setup_days", []),
            edit_mode=context.user_data.get("calendar_edit_mode", False),
        )
    )


async def calendar_day_callback(query, context, data):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    day = data.replace("calendar_day_", "")

    if context.user_data.get("calendar_edit_mode", False):
        target = parse_iso_date(day)
        if target:
            overrides = DatabaseManager.get_calendar_overrides(db_user["id"])
            base_type = get_work_day_type(db_user, target, {})
            current_override = overrides.get(day)
            if base_type == "planned":
                DatabaseManager.set_calendar_override(db_user["id"], day, "" if current_override == "off" else "off")
            else:
                DatabaseManager.set_calendar_override(db_user["id"], day, "" if current_override == "extra" else "extra")

        year, month = context.user_data.get("calendar_month", (now_local().year, now_local().month))
        if DatabaseManager.is_goal_enabled(db_user["id"]):
            daily_goal = calculate_current_decade_daily_goal(db_user)
            DatabaseManager.set_daily_goal(db_user["id"], daily_goal)
            await send_goal_status(None, context, db_user["id"], source_message=query.message)
        await query.edit_message_text(
            build_work_calendar_text(db_user, year, month, setup_mode=False, edit_mode=True),
            reply_markup=build_work_calendar_keyboard(
                db_user,
                year,
                month,
                setup_mode=False,
                setup_selected=[],
                edit_mode=True,
            )
        )
        return

    await query.answer("Редактирование доступно только в режиме редактирования")


async def subscription_message(update: Update, context: CallbackContext):
    db_user = DatabaseManager.get_user(update.effective_user.id)
    if not db_user:
        await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
        return

    expires_at = subscription_expires_at_for_user(db_user)
    if is_admin_telegram(update.effective_user.id):
        status = "♾️ Бессрочный доступ (админ)"
    elif is_subscription_active(db_user):
        status = f"✅ Подписка активна до {format_subscription_until(expires_at)}"
    else:
        status = "⛔ Подписка истекла"

    await update.message.reply_text(
        f"💳 Продление подписки\n\n"
        f"{status}\n"
        f"Стоимость: {SUBSCRIPTION_PRICE_TEXT}\n\n"
        f"Для продления напишите: {SUBSCRIPTION_CONTACT}",
        reply_markup=create_main_reply_keyboard(
            bool(DatabaseManager.get_active_shift(db_user['id'])),
            is_subscription_active(db_user),
        )
    )


def build_profile_text(db_user: dict, telegram_id: int) -> str:
    expires_at = subscription_expires_at_for_user(db_user)
    expires_text = format_subscription_until(expires_at) if expires_at else "—"
    status_text = "✅ Подписка активна" if is_subscription_active(db_user) else "⛔ Подписка неактивна"
    total_cars = DatabaseManager.get_cars_count_between_dates(db_user["id"], "2000-01-01", "2100-01-01")
    total_earned = DatabaseManager.get_user_total_between_dates(db_user["id"], "2000-01-01", "2100-01-01")
    return (
        f"👤 Профиль: {db_user.get('name', 'Пользователь')}\n"
        f"ID: {telegram_id}\n\n"
        f"Статус: {status_text}\n"
        f"Действует до: {expires_text}\n\n"
        f"Всего сделано машин: {total_cars}\n"
        f"Всего заработано: {format_money(total_earned)}"
    )


def build_profile_keyboard(db_user: dict, telegram_id: int) -> InlineKeyboardMarkup | None:
    callback = "subscription_info_photo" if get_section_photo_file_id("profile") else "subscription_info"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Купить подписку", callback_data=callback)],
    ])


SECTION_MEDIA_KEYS = {
    "profile": "media_profile_photo_file_id",
    "leaderboard": "media_leaderboard_photo_file_id",
}


def get_section_photo_file_id(section: str) -> str:
    key = SECTION_MEDIA_KEYS.get(section, "")
    if not key:
        return ""
    return DatabaseManager.get_app_content(key, "")


def set_section_photo_file_id(section: str, file_id: str) -> None:
    key = SECTION_MEDIA_KEYS.get(section, "")
    if not key:
        return
    DatabaseManager.set_app_content(key, file_id or "")


async def send_text_with_optional_photo(chat_target, context: CallbackContext, text: str, reply_markup=None, section: str = ""):
    file_id = get_section_photo_file_id(section) if section else ""
    if file_id:
        await context.bot.send_photo(
            chat_id=chat_target.chat_id,
            photo=file_id,
            caption=text[:1024],
            reply_markup=reply_markup,
        )
        return
    await chat_target.reply_text(text, reply_markup=reply_markup)


async def account_message(update: Update, context: CallbackContext):
    db_user = DatabaseManager.get_user(update.effective_user.id)
    if not db_user:
        await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
        return

    await send_text_with_optional_photo(
        update.message,
        context,
        build_profile_text(db_user, update.effective_user.id),
        reply_markup=build_profile_keyboard(db_user, update.effective_user.id),
        section="profile",
    )


async def account_info_callback(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    profile_text = build_profile_text(db_user, query.from_user.id)
    profile_keyboard = build_profile_keyboard(db_user, query.from_user.id)
    profile_photo = get_section_photo_file_id("profile")

    if profile_photo:
        try:
            await query.edit_message_media(
                media=InputMediaPhoto(media=profile_photo, caption=profile_text[:1024]),
                reply_markup=profile_keyboard,
            )
            return
        except Exception:
            await send_text_with_optional_photo(
                query.message,
                context,
                profile_text,
                reply_markup=profile_keyboard,
                section="profile",
            )
            return

    await query.edit_message_text(profile_text, reply_markup=profile_keyboard)


async def subscription_info_callback(query, context):
    await query.edit_message_text(
        "Стоимость подписки 200₽/мес.\nЗа покупкой стучаться к @dakonoplev2",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад в профиль", callback_data="account_info")]]),
    )


async def subscription_info_photo_callback(query, context):
    text = "Стоимость подписки 200₽/мес.\nЗа покупкой стучаться к @dakonoplev2"
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("Назад в профиль", callback_data="account_info")]])
    try:
        await query.edit_message_caption(caption=text, reply_markup=keyboard)
    except Exception:
        await query.edit_message_text(text, reply_markup=keyboard)


def get_faq_topics() -> list[dict]:
    raw = DatabaseManager.get_app_content("faq_topics_json", "")
    if not raw:
        return [
            {
                "id": "add_car",
                "title": "Как добавить машину",
                "text": "Номер ТС можно указывать в любом формате:\nХ340РУ797, х340ру или даже хру340.\n\nБот сам приведет номер к формату Х340РУ797, используя по умолчанию 797 регион",
            },
            {"id": "calendar", "title": "Календарь смен", "text": "Открой 🧰 Инструменты → 🗓️ Календарь. Там можно менять основные смены и редактировать отдельные дни."},
            {"id": "combo", "title": "Комбо-наборы", "text": "Открой 🧰 Инструменты → 🧩 Комбо, собери набор услуг и применяй его в один тап."},
            {"id": "history", "title": "История", "text": "Открой 🧰 Инструменты → 📚 История, чтобы посмотреть декады и дни."},
            {
                "id": "fast_input",
                "title": "Быстрый ввод",
                "text": (
                    "Можно отправить номер и услуги одной строкой, например:\n"
                    "хру340 пров запр зо\n\n"
                    "Поддерживаемые сокращения:\n"
                    "• Проверка: проверка, пров, провер, чек\n"
                    "• Заправка: заправка, запр, топливо, бенз\n"
                    "• Омывайка: омыв, омывка, омывайка, зали, зо, заливка\n"
                    "• Перепарковка: перепарковка, перепарк, парковка, некорректная, некк, нек, некорр"
                ),
            },
        ]
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    result = []
    for item in data:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        body = str(item.get("text", "")).strip()
        item_id = str(item.get("id", "")).strip()
        if title and body and item_id:
            result.append({"id": item_id, "title": title, "text": body})
    return result


def save_faq_topics(topics: list[dict]) -> None:
    DatabaseManager.set_app_content("faq_topics_json", json.dumps(topics, ensure_ascii=False))


def create_faq_demo_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Запустить обучение", callback_data="faq_start_demo")]])


def create_faq_topics_keyboard(topics: list[dict], is_admin: bool = False) -> InlineKeyboardMarkup:
    icon_map = {
        "add_car": "🚗",
        "calendar": "🗓️",
        "combo": "🧩",
        "history": "📚",
        "fast_input": "⚡",
    }
    keyboard = [
        [InlineKeyboardButton(f"{icon_map.get(topic.get('id'), '📘')} {topic['title']}", callback_data=f"faq_topic_{topic['id']}")]
        for topic in topics
    ]
    keyboard.append([InlineKeyboardButton("🚀 Запустить обучение", callback_data="faq_start_demo")])
    keyboard.append([InlineKeyboardButton("🛠️ Управление FAQ", callback_data="admin_faq_menu")])
    return InlineKeyboardMarkup(keyboard)


async def send_faq(chat_target, context: CallbackContext):
    faq_text = DatabaseManager.get_app_content("faq_text", "")
    faq_video = DatabaseManager.get_app_content("faq_video_file_id", "")
    source_chat_id = DatabaseManager.get_app_content("faq_video_source_chat_id", "")
    source_message_id = DatabaseManager.get_app_content("faq_video_source_message_id", "")
    topics = get_faq_topics()

    header = faq_text or (
        "🎓 Обучение и FAQ\n"
        "Выбери раздел с гайдом или запусти обучение."
    )

    if faq_video:
        if source_chat_id and source_message_id:
            try:
                await context.bot.copy_message(
                    chat_id=chat_target.chat_id,
                    from_chat_id=int(source_chat_id),
                    message_id=int(source_message_id),
                    caption=header[:1024] if header else None,
                )
            except Exception:
                await context.bot.send_video(chat_id=chat_target.chat_id, video=faq_video, caption=header[:1024])
        else:
            await context.bot.send_video(chat_id=chat_target.chat_id, video=faq_video, caption=header[:1024])

    if topics:
        await chat_target.reply_text(
            "Выбери раздел FAQ:",
            reply_markup=create_faq_topics_keyboard(topics, False),
        )
        return

    await chat_target.reply_text(
        "Выбери раздел FAQ:",
        reply_markup=create_faq_topics_keyboard([], False),
    )


def build_feature_overview_text() -> str:
    return (
        "🗺️ Полный обзор функций\n\n"
        "1) 🚘 Работа в смене\n"
        "• Открываешь смену\n"
        "• Вводишь номер ТС\n"
        "• Выбираешь услуги кнопками, поиск или комбо\n"
        "• Фиксируешь сумму по машине и смене\n\n"
        "2) 📊 Аналитика и отчёты\n"
        "• История по декадам\n"
        "• Эффективность текущей декады\n"
        "• Топ героев\n"
        "• Экспорт PDF/XLSX\n\n"
        "3) 🧰 Инструменты\n"
        "• Прайс день/ночь\n"
        "• Календарь смен и план\n"
        "• Настройки и комбо\n\n"
        "4) 👤 Профиль и доступ\n"
        "• Статус подписки\n"
        "• Продление\n"
        "• Управление аккаунтом\n\n"
        "Хочешь быстро освоиться — запусти интерактивное обучение ниже."
    )


async def faq_overview_callback(query, context):
    await query.edit_message_text(
        "Раздел отключён. Используй кнопки FAQ.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="faq")]]),
    )


async def demo_render_card(query, context, step: str):
    payload = context.user_data.get("demo_payload", {"services": [], "calendar_days": [], "car_number": ""})
    services = payload.get("services", [])
    car_number = payload.get("car_number", "Х340РУ797")
    calendar_days = payload.get("calendar_days", [])

    if step == "start":
        text = (
            "👋 Добро пожаловать в интерактивное обучение.\n\n"
            "Это тренажёр на реальном интерфейсе бота (без сохранения в историю).\n\n"
            "Шаги:\n"
            "1) Открытие смены и ввод номера\n"
            "2) Добавление услуг (как в боевом режиме)\n"
            "3) Выбор рабочих дней в календаре\n"
            "4) Просмотр итогов и переход к реальной работе"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Начать демо", callback_data="demo_step_shift")]])
    elif step == "shift":
        text = (
            "✅ Шаг 1/4: Смена открыта (демо).\n"
            "Отправьте номер авто в чат — как в обычной работе.\n"
            "Например: Х340РУ"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Использовать пример номера", callback_data="demo_step_services")]])
        context.user_data["demo_waiting_car"] = True
    elif step == "services":
        total = sum(get_current_price(sid, "day") for sid in services)
        text = (
            f"🚗 Шаг 2/4: Машина {car_number}\n"
            "Добавьте услуги так же, как в реальной смене.\n"
            "Ничего не сохранится в историю.\n\n"
            f"Выбрано услуг: {len(services)}\n"
            f"Сумма по машине: {format_money(total)}"
        )
        rows = []
        for sid in [1, 2, 3, 4, 5, 6, 7, 8]:
            mark = "✅" if sid in services else "▫️"
            rows.append([InlineKeyboardButton(f"{mark} {plain_service_name(SERVICES[sid]['name'])}", callback_data=f"demo_service_{sid}")])
        rows.append([InlineKeyboardButton("➡️ Ещё услуги", callback_data="demo_step_services_adv")])
        rows.append([InlineKeyboardButton("📅 К календарю демо", callback_data="demo_step_calendar")])
        kb = InlineKeyboardMarkup(rows)
    elif step == "services_adv":
        total = sum(get_current_price(sid, "day") for sid in services)
        text = (
            f"🚗 Шаг 2/4: Машина {car_number} (доп. услуги)\n"
            "Редкие услуги из того же прайса.\n\n"
            f"Выбрано услуг: {len(services)}\n"
            f"Сумма по машине: {format_money(total)}"
        )
        rows = []
        for sid in [9, 12, 13, 14, 16, 18, 19, 21]:
            mark = "✅" if sid in services else "▫️"
            rows.append([InlineKeyboardButton(f"{mark} {plain_service_name(SERVICES[sid]['name'])}", callback_data=f"demo_service_{sid}")])
        rows.append([InlineKeyboardButton("⬅️ К основным", callback_data="demo_step_services")])
        rows.append([InlineKeyboardButton("📅 К календарю демо", callback_data="demo_step_calendar")])
        kb = InlineKeyboardMarkup(rows)
    elif step == "calendar":
        today = now_local().date()
        week_dates = [today + timedelta(days=i) for i in range(7)]
        selected_count = len(calendar_days)
        selected_hint = ", ".join(d[-5:] for d in calendar_days[:5]) if calendar_days else "не выбраны"
        text = (
            "📅 Шаг 3/4: Календарь (тренажёр).\n"
            "Выберите рабочие дни на ближайшую неделю.\n"
            "Это поможет понять логику планирования смен.\n\n"
            f"Отмечено дней: {selected_count}\n"
            f"Выбрано: {selected_hint}\n\n"
            "ℹ️ В демо календарь не меняет реальные данные аккаунта."
        )
        rows = []
        for d in week_dates:
            key = d.isoformat()
            mark = "✅" if key in calendar_days else "▫️"
            rows.append([InlineKeyboardButton(f"{mark} {d.strftime('%a %d.%m')}", callback_data=f"demo_calendar_{key}")])
        rows.append([
            InlineKeyboardButton("⬅️ К услугам", callback_data="demo_step_services_adv"),
            InlineKeyboardButton("⏭ Дальше", callback_data="demo_step_leaderboard"),
        ])
        kb = InlineKeyboardMarkup(rows)
    elif step == "leaderboard":
        today = now_local().date()
        idx, _, _, _, decade_title = get_decade_period(today)
        decade_leaders = DatabaseManager.get_decade_leaderboard(today.year, today.month, idx)
        top_block = "\n".join(
            f"{place}. {row['name']} — {format_money(int(row['total_amount'] or 0))}"
            for place, row in enumerate(decade_leaders[:5], start=1)
        ) if decade_leaders else "Пока нет данных по декаде."
        text = (
            "📊 Шаг 4/4: Итог демо и аналитика.\n"
            f"Декада: {decade_title}\n"
            f"Машина: {car_number}\n"
            f"Услуг в демо: {len(services)}\n"
            f"Рабочих дней в демо-календаре: {len(calendar_days)}\n\n"
            f"{top_block}\n\n"
            "В реальном режиме данные сохраняются в историю, отчёты и рейтинг."
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Завершить демо", callback_data="demo_step_done")]])
    elif step == "done":
        total = sum(get_current_price(sid, "day") for sid in services)
        text = (
            "🎉 Отлично! Вы прошли демо.\n\n"
            f"Услуг выбрано: {len(services)}\n"
            f"Сумма: {format_money(total)}\n"
            f"Отмечено смен в календаре: {len(calendar_days)}\n\n"
            "Теперь можно перейти к реальной работе в боте."
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К FAQ", callback_data="faq")],
            [InlineKeyboardButton("✖️ Выйти из демо", callback_data="demo_exit")],
        ])
    else:
        text = "Демо завершено."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К FAQ", callback_data="faq")]])

    await query.edit_message_text(text, reply_markup=kb)


async def demo_start(query, context):
    context.user_data["demo_mode"] = True
    context.user_data["demo_payload"] = {"services": [], "calendar_days": [], "car_number": "Х340РУ797"}
    context.user_data["demo_waiting_car"] = False
    await demo_render_card(query, context, "start")


async def demo_handle_car_text(update: Update, context: CallbackContext):
    if not context.user_data.get("demo_mode"):
        return False
    if context.user_data.get("demo_waiting_car") is not True:
        return False

    raw = (update.message.text or "").strip()
    is_valid, normalized, error = validate_car_number(raw)
    if not is_valid:
        await update.message.reply_text(f"❌ В демо не распознал номер: {error}\nПопробуй ещё раз.")
        return True

    payload = context.user_data.get("demo_payload", {"services": [], "calendar_days": []})
    payload["car_number"] = normalized
    payload["services"] = []
    context.user_data["demo_waiting_car"] = False
    context.user_data["demo_payload"] = payload
    await update.message.reply_text(
        f"✅ Номер распознан: {normalized}\nОткрываю демо-выбор услуг.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🧪 Перейти к услугам (демо)", callback_data="demo_step_services")],
        ]),
    )
    return True


async def faq_message(update: Update, context: CallbackContext):
    has_active = False
    db_user = DatabaseManager.get_user(update.effective_user.id)
    if db_user:
        has_active = DatabaseManager.get_active_shift(db_user['id']) is not None
    await send_faq(update.message, context)


async def faq_callback(query, context):
    await query.edit_message_text(
        "❓ FAQ\nВыбери раздел:",
        reply_markup=create_faq_topics_keyboard(get_faq_topics(), is_admin=is_admin_telegram(query.from_user.id)),
    )


async def admin_media_menu(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    keyboard = [
        [InlineKeyboardButton("👤 Фото для «Профиль»", callback_data="admin_media_set_profile")],
        [InlineKeyboardButton("🏆 Фото для «Топ героев»", callback_data="admin_media_set_leaderboard")],
        [InlineKeyboardButton("🗑 Убрать фото «Профиль»", callback_data="admin_media_clear_profile")],
        [InlineKeyboardButton("🗑 Убрать фото «Топ героев»", callback_data="admin_media_clear_leaderboard")],
        [InlineKeyboardButton("🔙 В админку", callback_data="admin_panel")],
    ]
    await query.edit_message_text(
        "🖼 Управление фото для разделов.\n"
        "Нажмите нужный пункт, затем отправьте фото в чат.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def admin_media_set_target(query, context, section: str):
    if not is_admin_telegram(query.from_user.id):
        return
    context.user_data["awaiting_admin_section_photo"] = section
    labels = {"profile": "Профиль", "leaderboard": "Топ героев"}
    await query.edit_message_text(
        f"Отправьте фото для раздела: {labels.get(section, section)}.\n"
        "Будет использован Telegram file_id, поэтому загрузить нужно один раз.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К медиа", callback_data="admin_media_menu")]]),
    )


async def admin_media_clear_target(query, context, section: str):
    if not is_admin_telegram(query.from_user.id):
        return
    set_section_photo_file_id(section, "")
    context.user_data.pop("awaiting_admin_section_photo", None)
    await query.answer("Фото удалено")
    await admin_media_menu(query, context)


async def admin_faq_menu(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    keyboard = [
        [InlineKeyboardButton("✏️ Изменить вступительный текст", callback_data="admin_faq_set_text")],
        [InlineKeyboardButton("🧩 Темы FAQ", callback_data="admin_faq_topics")],
        [InlineKeyboardButton("➕ Добавить тему", callback_data="admin_faq_topic_add")],
        [InlineKeyboardButton("🎬 Загрузить/обновить видео", callback_data="admin_faq_set_video")],
        [InlineKeyboardButton("👁️ Предпросмотр FAQ", callback_data="admin_faq_preview")],
        [InlineKeyboardButton("🗑️ Удалить видео", callback_data="admin_faq_clear_video")],
        [InlineKeyboardButton("🔙 В админку", callback_data="admin_panel")],
    ]
    await query.edit_message_text("Управление FAQ:", reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_faq_set_text(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    context.user_data["awaiting_admin_faq_text"] = True
    await query.edit_message_text("Отправьте новый текст FAQ одним сообщением.")


async def admin_faq_set_video(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    context.user_data["awaiting_admin_faq_video"] = True
    await query.edit_message_text("Отправьте видео в чат (как video). Я сохраню его и буду отправлять пользователям как полноценное видео.")


async def admin_faq_preview(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    await send_faq(query.message, context)


async def admin_faq_clear_video(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    DatabaseManager.set_app_content("faq_video_file_id", "")
    DatabaseManager.set_app_content("faq_video_source_chat_id", "")
    DatabaseManager.set_app_content("faq_video_source_message_id", "")
    await query.edit_message_text(
        "✅ Видео FAQ удалено.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 В админку", callback_data="admin_panel")]])
    )


async def faq_topic_callback(query, context, data):
    topic_id = data.replace("faq_topic_", "")
    topics = get_faq_topics()
    topic = next((t for t in topics if t["id"] == topic_id), None)
    if not topic:
        await query.edit_message_text("❌ Тема FAQ не найдена.")
        return
    await query.edit_message_text(
        f"❓ {topic['title']}\n\n{topic['text']}",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К FAQ", callback_data="faq")]])
    )


async def admin_faq_topics(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    topics = get_faq_topics()
    keyboard = []
    for topic in topics:
        keyboard.append([InlineKeyboardButton(f"✏️ {topic['title']}", callback_data=f"admin_faq_topic_edit_{topic['id']}")])
        keyboard.append([InlineKeyboardButton(f"🗑️ Удалить: {topic['title']}", callback_data=f"admin_faq_topic_del_{topic['id']}")])
    keyboard.append([InlineKeyboardButton("➕ Добавить тему", callback_data="admin_faq_topic_add")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_faq_menu")])
    await query.edit_message_text("Темы FAQ:", reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_faq_topic_add(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    context.user_data["awaiting_admin_faq_topic_add"] = True
    await query.edit_message_text("Отправьте тему и ответ в формате:\nТема | Текст ответа")


async def admin_faq_topic_edit(query, context, data):
    if not is_admin_telegram(query.from_user.id):
        return
    topic_id = data.replace("admin_faq_topic_edit_", "")
    context.user_data["awaiting_admin_faq_topic_edit"] = topic_id
    await query.edit_message_text("Отправьте новый текст для темы в формате:\nНовое название | Новый текст")


async def admin_faq_topic_del(query, context, data):
    if not is_admin_telegram(query.from_user.id):
        return
    topic_id = data.replace("admin_faq_topic_del_", "")
    topics = [t for t in get_faq_topics() if t["id"] != topic_id]
    save_faq_topics(topics)
    await admin_faq_topics(query, context)


def resolve_history_page_for_current_decade(decades: list[dict]) -> int:
    today = now_local().date()
    current_idx, _, _, _, _ = get_decade_period(today)
    for i, item in enumerate(decades):
        if int(item["year"]) == today.year and int(item["month"]) == today.month and int(item["decade_index"]) == current_idx:
            return i // 5
    return 0


def build_history_decades_page(db_user: dict, page: int = 0) -> tuple[str, InlineKeyboardMarkup] | tuple[None, None]:
    decades = DatabaseManager.get_decades_with_data(db_user["id"], limit=120)
    if not decades:
        return None, None

    if page < 0:
        page = 0
    max_page = max((len(decades) - 1) // 5, 0)
    page = min(page, max_page)

    start_idx = page * 5
    chunk = decades[start_idx:start_idx + 5]
    keyboard = []
    message = "📜 История по декадам\n\n"
    for d in chunk:
        title = format_decade_title(int(d["year"]), int(d["month"]), int(d["decade_index"]))
        message += f"• {title}: {format_money(int(d['total_amount']))} (машин: {d['cars_count']})\n"
        keyboard.append([InlineKeyboardButton(title, callback_data=f"history_decade_{d['year']}_{d['month']}_{d['decade_index']}")])

    if max_page > 0:
        nav = []
        if page < max_page:
            nav.append(InlineKeyboardButton("⬅️ Старее", callback_data=f"history_decades_page_{page + 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1}/{max_page + 1}", callback_data="noop"))
        if page > 0:
            nav.append(InlineKeyboardButton("Новее ➡️", callback_data=f"history_decades_page_{page - 1}"))
        keyboard.append(nav)

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    return message, InlineKeyboardMarkup(keyboard)


async def history_decades(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    if "history_decades_page" not in context.user_data:
        decades = DatabaseManager.get_decades_with_data(db_user["id"], limit=120)
        context.user_data["history_decades_page"] = resolve_history_page_for_current_decade(decades)
    page = int(context.user_data.get("history_decades_page", 0))
    message, markup = build_history_decades_page(db_user, page)
    if not message or not markup:
        await query.edit_message_text("📜 История пуста")
        return
    await query.edit_message_text(message, reply_markup=markup)


async def history_decades_page(query, context, data):
    try:
        page = int(data.replace("history_decades_page_", ""))
    except ValueError:
        page = 0
    context.user_data["history_decades_page"] = max(page, 0)
    await history_decades(query, context)


async def history_decade_days(query, context, data):
    _, _, year_s, month_s, decade_s = data.split("_")
    year = int(year_s)
    month = int(month_s)
    decade_index = int(decade_s)
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    days = DatabaseManager.get_days_for_decade(db_user["id"], year, month, decade_index)
    title = format_decade_title(year, month, decade_index)
    total = sum(int(d["total_amount"] or 0) for d in days)
    message = f"📆 {title}\nИтого: {format_money(total)}\n\n"
    keyboard = []
    if not days:
        message += "Данных за эту декаду пока нет.\n"
    for d in days:
        day = d["day"]
        message += f"• {day}: {format_money(int(d['total_amount']))} (машин: {d['cars_count']})\n"
        keyboard.append([InlineKeyboardButton(f"{day} — {format_money(int(d['total_amount']))}", callback_data=f"history_day_{day}")])
    keyboard.append([InlineKeyboardButton("🔙 К декадам", callback_data="history_decades")])
    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))


async def history_day_cars(query, context, data):
    day = data.replace("history_day_", "")
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    cars = DatabaseManager.get_cars_for_day(db_user["id"], day)
    if not cars:
        back_callback = context.user_data.pop("history_back_callback", "history_decades")
        back_title = "🔙 К календарю" if back_callback.startswith("calendar_back_month_") else "🔙 К декадам"
        await query.edit_message_text(
            "Машин за день нет",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(back_title, callback_data=back_callback)]])
        )
        return
    message = f"🚗 Машины за {day}\n\n"
    keyboard = []
    subscription_active = is_subscription_active(db_user)
    for car in cars:
        message += f"• #{car['id']} {car['car_number']} — {format_money(int(car['total_amount']))}\n"
        if subscription_active:
            keyboard.append([
                InlineKeyboardButton(
                    f"✏️ Редактировать {car['car_number']}",
                    callback_data=f"history_edit_car_{car['id']}_{day}",
                )
            ])
    if subscription_active:
        keyboard.append([InlineKeyboardButton("🧹 Редактировать этот день", callback_data=f"cleanup_day_{day}")])
    else:
        message += "\nℹ️ Режим чтения: редактирование доступно после продления подписки.\n"
        keyboard.append([InlineKeyboardButton("💳 Продлить подписку", callback_data="subscription_info")])
    back_callback = context.user_data.pop("history_back_callback", "history_decades")
    back_title = "🔙 К календарю" if back_callback.startswith("calendar_back_month_") else "🔙 К декадам"
    keyboard.append([InlineKeyboardButton(back_title, callback_data=back_callback)])
    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))

async def history_edit_car(query, context, data):
    body = data.replace("history_edit_car_", "")
    car_id_s, day = body.split("_", 1)
    car_id = int(car_id_s)

    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    if not is_subscription_active(db_user):
        await query.edit_message_text(get_subscription_expired_text())
        return

    car = DatabaseManager.get_car(car_id)
    if not car:
        await query.edit_message_text("❌ Машина не найдена")
        return

    cars_for_day = DatabaseManager.get_cars_for_day(db_user["id"], day)
    if not any(item["id"] == car_id for item in cars_for_day):
        await query.edit_message_text("❌ Машина не найдена в выбранном дне")
        return

    context.user_data[f"history_day_for_car_{car_id}"] = day
    await show_car_services(query, context, car_id, page=0, history_day=day)

async def add_service(query, context, data):
    """Добавление услуги"""
    context.user_data.pop('awaiting_service_search', None)
    parts = data.split('_')
    if len(parts) < 4:
        return

    service_id = int(parts[1])
    car_id = int(parts[2])
    page = int(parts[3])

    service = SERVICES.get(service_id)
    if not service:
        return

    if service.get("kind") == "group":
        await show_group_service_options(query, context, service_id, car_id, page)
        return

    if service.get("kind") == "distance" and not get_edit_mode(context, car_id):
        context.user_data['awaiting_distance'] = {
            "car_id": car_id,
            "service_id": service_id,
            "page": page,
        }
        await query.message.reply_text(
            f"Введите километраж для услуги «{plain_service_name(service['name'])}».\n"
            "Пример: 45"
        )
        return

    db_user = DatabaseManager.get_user(query.from_user.id)
    price = get_current_price(service_id, get_price_mode(context, db_user["id"] if db_user else None))

    if get_edit_mode(context, car_id):
        DatabaseManager.remove_service_from_car(car_id, service_id)
    else:
        clean_name = plain_service_name(service['name'])
        DatabaseManager.add_service_to_car(car_id, service_id, clean_name, price)


    await show_car_services(query, context, car_id, page)


async def show_group_service_options(query, context, group_service_id: int, car_id: int, page: int):
    group_service = SERVICES.get(group_service_id)
    if not group_service:
        return

    children = group_service.get("children", [])
    db_user = DatabaseManager.get_user(query.from_user.id)
    mode = get_price_mode(context, db_user["id"] if db_user else None)
    keyboard = []
    for child_id in children:
        child = SERVICES.get(child_id)
        if not child:
            continue
        child_name = plain_service_name(child['name'])
        child_price = get_current_price(child_id, mode)
        keyboard.append([
            InlineKeyboardButton(
                f"{child_name} ({child_price}₽)",
                callback_data=f"childsvc_{child_id}_{car_id}_{page}"
            )
        ])

    keyboard.append([InlineKeyboardButton("⬅️ К услугам", callback_data=f"back_to_services_{car_id}_{page}")])
    await query.edit_message_text(
        f"Выберите вариант: {plain_service_name(group_service['name'])}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def add_group_child_service(query, context, data):
    parts = data.split('_')
    if len(parts) < 4:
        return
    service_id = int(parts[1])
    car_id = int(parts[2])
    page = int(parts[3])

    service = SERVICES.get(service_id)
    if not service:
        return

    if get_edit_mode(context, car_id):
        DatabaseManager.remove_service_from_car(car_id, service_id)
    else:
        db_user = DatabaseManager.get_user(query.from_user.id)
        price = get_current_price(service_id, get_price_mode(context, db_user["id"] if db_user else None))
        DatabaseManager.add_service_to_car(car_id, service_id, plain_service_name(service['name']), price)

    await show_car_services(query, context, car_id, page)


async def back_to_services(query, context, data):
    context.user_data.pop('awaiting_service_search', None)
    parts = data.split('_')
    if len(parts) < 5:
        return
    car_id = int(parts[3])
    page = int(parts[4])
    await show_car_services(query, context, car_id, page)




async def toggle_price_mode_for_car(query, context, data):
    parts = data.split('_')
    if len(parts) < 5:
        return
    car_id = int(parts[3])
    page = int(parts[4])

    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        return

    current = get_price_mode(context, db_user['id'])
    new_mode = "night" if current == "day" else "day"
    set_manual_price_mode(context, db_user['id'], new_mode)
    await show_car_services(query, context, car_id, page)


async def start_service_search(query, context, data):
    parts = data.split('_')
    if len(parts) < 4:
        return
    car_id = int(parts[2])
    page = int(parts[3])

    context.user_data['awaiting_service_search'] = {"car_id": car_id, "page": page}
    context.user_data["search_message_id"] = query.message.message_id
    context.user_data["search_chat_id"] = query.message.chat_id

    keyboard = [
        [InlineKeyboardButton("❌ Отмена поиска", callback_data=f"search_cancel_{car_id}_{page}")],
    ]

    await query.edit_message_text(
        "🔎 Поиск услуг\n\nВведите в чат часть названия услуги.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def apply_search_pick(query, context, data):
    parts = data.split('_')
    if len(parts) < 5:
        return
    service_id = int(parts[2])
    car_id = int(parts[3])
    page = int(parts[4])
    await add_service(query, context, f"service_{service_id}_{car_id}_{page}")


async def search_enter_text_mode(query, context, data):
    parts = data.split('_')
    if len(parts) < 4:
        return
    car_id = int(parts[2])
    page = int(parts[3])
    context.user_data['awaiting_service_search'] = {"car_id": car_id, "page": page}
    context.user_data["search_message_id"] = query.message.message_id
    context.user_data["search_chat_id"] = query.message.chat_id
    await query.edit_message_text(
        "🔎 Поиск услуг\n\nВведите в чат часть названия услуги.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Отмена поиска", callback_data=f"search_cancel_{car_id}_{page}")],
        ])
    )


async def repeat_prev_services(query, context, data):
    parts = data.split('_')
    if len(parts) < 4:
        return
    car_id = int(parts[2])
    page = int(parts[3])

    car = DatabaseManager.get_car(car_id)
    if not car:
        return
    prev_car = DatabaseManager.get_previous_car_with_services(car["shift_id"], car_id)
    if not prev_car:
        await query.answer("Нет предыдущей машины с услугами", show_alert=True)
        return

    services = DatabaseManager.get_car_services(prev_car["id"])
    DatabaseManager.clear_car_services(car_id)
    for service in services:
        qty = int(service.get("quantity", 1) or 1)
        for _ in range(max(1, qty)):
            DatabaseManager.add_service_to_car(
                car_id,
                int(service["service_id"]),
                str(service["service_name"]),
                int(service["price"]),
            )
    await show_car_services(query, context, car_id, page)


async def search_cancel(query, context, data):
    parts = data.split("_")
    if len(parts) < 4:
        return
    car_id = int(parts[2])
    page = int(parts[3])
    context.user_data.pop("awaiting_service_search", None)
    await show_car_services(query, context, car_id, page)


async def show_combo_menu(query, context, data):
    parts = data.split('_')
    if len(parts) < 4:
        return
    car_id = int(parts[2])
    page = int(parts[3])

    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    combos = DatabaseManager.get_user_combos(db_user['id'])
    keyboard = []
    for combo in combos:
        keyboard.append([
            InlineKeyboardButton(
                f"▶️ {combo['name']}",
                callback_data=f"combo_apply_{combo['id']}_{car_id}_{page}",
            ),
            InlineKeyboardButton(
                "✏️",
                callback_data=f"combo_edit_{combo['id']}_{car_id}_{page}",
            ),
        ])

    keyboard.append([InlineKeyboardButton("⬅️ К услугам", callback_data=f"back_to_services_{car_id}_{page}")])
    text_msg = "🧩 У вас пока нет сохранённых комбо.\nСоздайте их в настройках: «Мои комбинации»." if not combos else "🧩 Выберите комбинацию для применения:"
    await query.edit_message_text(text_msg, reply_markup=InlineKeyboardMarkup(keyboard))


async def apply_combo_to_car(query, context, data):
    parts = data.split('_')
    if len(parts) < 5:
        return
    combo_id = int(parts[2])
    car_id = int(parts[3])
    page = int(parts[4])
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    combo = DatabaseManager.get_combo(combo_id, db_user['id'])
    if not combo:
        await query.answer("Комбо не найдено", show_alert=True)
        return

    mode = get_price_mode(context, db_user['id'])
    for sid in combo.get('service_ids', []):
        service = SERVICES.get(int(sid))
        if not service or service.get('kind') in {'group', 'distance'}:
            continue
        DatabaseManager.add_service_to_car(car_id, int(sid), service['name'], get_current_price(int(sid), mode))

    await show_car_services(query, context, car_id, page)


async def save_combo_from_car(query, context, data):
    parts = data.split('_')
    if len(parts) < 4:
        return
    car_id = int(parts[3])
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    services = DatabaseManager.get_car_services(car_id)
    service_ids = [int(s['service_id']) for s in services if int(s.get('service_id', 0)) in SERVICES]
    service_ids = sorted(set(service_ids))
    if not service_ids:
        await query.answer("Сначала добавьте услуги машине", show_alert=True)
        return
    name = f"Комбо {now_local().strftime('%d.%m %H:%M')}"
    DatabaseManager.save_user_combo(db_user['id'], name, service_ids)
    await query.answer("✅ Комбо сохранено", show_alert=True)


async def delete_combo_prompt(query, context, data):
    combo_id = int(data.replace('combo_delete_prompt_', '').split('_')[0])
    await query.edit_message_text(
        "Удалить это комбо?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, удалить", callback_data=f"combo_delete_confirm_{combo_id}")],
            [InlineKeyboardButton("🔙 Назад", callback_data="combo_settings")],
        ])
    )


async def delete_combo(query, context, data):
    combo_id = int(data.replace('combo_delete_confirm_', '').split('_')[0])
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    DatabaseManager.delete_combo(combo_id, db_user['id'])
    await combo_settings_menu(query, context)


async def combo_edit_menu(query, context, data):
    parts = data.split('_')
    if len(parts) < 3:
        return
    combo_id = int(parts[2])
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    combo = DatabaseManager.get_combo(combo_id, db_user['id'])
    if not combo:
        await query.edit_message_text("❌ Комбо не найдено")
        return
    await query.edit_message_text(
        f"🧩 {combo['name']}\nУслуг: {len(combo.get('service_ids', []))}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Переименовать", callback_data=f"combo_rename_{combo_id}")],
            [InlineKeyboardButton("🗑️ Удалить", callback_data=f"combo_delete_prompt_{combo_id}")],
            [InlineKeyboardButton("🔙 Назад", callback_data="combo_settings")],
        ])
    )


async def combo_start_rename(query, context, data):
    combo_id = int(data.replace('combo_rename_', '').split('_')[0])
    context.user_data['awaiting_combo_rename'] = combo_id
    await query.edit_message_text("Введите новое название комбо в чат.")


async def combo_settings_menu(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    combos = DatabaseManager.get_user_combos(db_user['id'])
    if not combos:
        await query.edit_message_text(
            "🧩 Комбо\n"
            "Собери набор услуг для быстрого выбора в один тап.\n\n"
            "У вас пока нет сохранённых комбо.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Создать комбо", callback_data="combo_create_settings")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back")],
            ])
        )
        return
    keyboard = []
    for combo in combos:
        keyboard.append([
            InlineKeyboardButton(combo['name'], callback_data=f"combo_edit_{combo['id']}_0_0"),
        ])
    keyboard.append([InlineKeyboardButton("➕ Создать комбо", callback_data="combo_create_settings")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    await query.edit_message_text(
        "🧩 Комбо\n"
        "Собери набор услуг для быстрого выбора в один тап.\n\n"
        "Мои комбинации:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def combo_settings_menu_for_message(update: Update, context: CallbackContext):
    db_user = DatabaseManager.get_user(update.effective_user.id)
    if not db_user:
        await update.message.reply_text("❌ Пользователь не найден")
        return
    combo_intro = (
        "Здесь вы можете создать любую комбинацию из услуг для быстрого ввода.\n\n"
        "После создания первого комбо при добавлении услуг в машину появится кнопка с названием вашего комбо."
    )
    combos = DatabaseManager.get_user_combos(db_user['id'])
    if not combos:
        await update.message.reply_text(
            f"{combo_intro}\n\n🧩 У вас пока нет сохранённых комбо.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Создать комбо", callback_data="combo_create_settings")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back")],
            ])
        )
        return
    keyboard = []
    for combo in combos:
        keyboard.append([InlineKeyboardButton(combo['name'], callback_data=f"combo_edit_{combo['id']}_0_0")])
    keyboard.append([InlineKeyboardButton("➕ Создать комбо", callback_data="combo_create_settings")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    await update.message.reply_text(f"{combo_intro}\n\n🧩 Мои комбинации:", reply_markup=InlineKeyboardMarkup(keyboard))


async def export_csv(query, context):
    await query.edit_message_text("Экспорт CSV временно недоступен.")


async def backup_db(query, context):
    path = create_db_backup()
    if not path:
        await query.edit_message_text("❌ Бэкап недоступен")
        return
    with open(path, 'rb') as f:
        await query.message.reply_document(document=f, filename=os.path.basename(path), caption='Бэкап базы')


async def export_decade_pdf(query, context, data):
    _, _, _, y, m, d = data.split('_')
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    path = create_decade_pdf(db_user['id'], int(y), int(m), int(d))
    with open(path, 'rb') as f:
        await query.message.reply_document(document=f, filename=os.path.basename(path), caption='PDF отчёт')


async def export_decade_xlsx(query, context, data):
    _, _, _, y, m, d = data.split('_')
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    path = create_decade_xlsx(db_user['id'], int(y), int(m), int(d))
    with open(path, 'rb') as f:
        await query.message.reply_document(document=f, filename=os.path.basename(path), caption='XLSX отчёт')


async def clear_services_prompt(query, context, data):
    parts = data.split('_')
    if len(parts) < 3:
        return
    car_id = int(parts[1])
    page = int(parts[2])
    keyboard = [
        [InlineKeyboardButton("✅ Да, очистить", callback_data=f"confirm_clear_{car_id}_{page}")],
        [InlineKeyboardButton("⬅️ Отмена", callback_data=f"back_to_services_{car_id}_{page}")],
    ]
    await query.edit_message_text("Подтвердите очистку всех услуг у этой машины", reply_markup=InlineKeyboardMarkup(keyboard))


async def clear_services(query, context, data):
    """Очистка услуг"""
    parts = data.split('_')
    if len(parts) < 4:
        return

    car_id = int(parts[2])
    page = int(parts[3])

    DatabaseManager.clear_car_services(car_id)
    context.user_data.pop(f"edit_mode_{car_id}", None)
    await show_car_services(query, context, car_id, page)

async def change_services_page(query, context, data):
    """Перелистывание услуг"""
    parts = data.split('_')
    if len(parts) < 4:
        return
    car_id = int(parts[2])
    page = int(parts[3])
    await show_car_services(query, context, car_id, page)

async def toggle_edit(query, context, data):
    parts = data.split('_')
    if len(parts) < 4:
        return
    car_id = int(parts[2])
    page = int(parts[3])
    toggle_edit_mode(context, car_id)
    await show_car_services(query, context, car_id, page)

async def save_car_by_id(query, context, car_id: int):
    car = DatabaseManager.get_car(car_id)
    if not car:
        await query.edit_message_text("❌ Машина не найдена")
        return

    services = DatabaseManager.get_car_services(car_id)
    if not services:
        await query.edit_message_text(
            f"❌ Машина {car['car_number']} не сохранена.\n"
            f"Не выбрано ни одной услуги."
        )
        await query.message.reply_text("Выбери действие:", reply_markup=create_main_reply_keyboard(True))
        return

    await query.edit_message_text(
        f"✅ Машина {car['car_number']} сохранена!\n"
        f"Сумма: {format_money(car['total_amount'])}\n\n"
        "Отправьте следующий номер авто в чат."
    )
    context.user_data.pop(f"edit_mode_{car_id}", None)
    context.user_data.pop(f"history_day_for_car_{car_id}", None)
    db_user = DatabaseManager.get_user(query.from_user.id)
    if db_user:
        await send_goal_status(None, context, db_user['id'], source_message=query.message)




async def save_car(query, context, data):
    """Сохранение машины"""
    parts = data.split('_')
    if len(parts) < 2:
        return
    car_id = int(parts[1])
    await save_car_by_id(query, context, car_id)
    await query.message.reply_text(
        "Выбери действие:",
        reply_markup=create_main_reply_keyboard(True)
    )

async def close_shift(query, context, data):
    """Старая точка входа: теперь только подтверждение"""
    await close_shift_confirm_prompt(query, context, data)


async def close_shift_confirm_prompt(query, context, data):
    parts = data.split('_')
    if len(parts) < 2:
        return

    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await query.edit_message_text("❌ Ошибка: пользователь не найден")
        return

    shift_id = int(parts[1])
    shift = DatabaseManager.get_shift(shift_id) if shift_id > 0 else None
    if not shift:
        shift = DatabaseManager.get_active_shift(db_user['id'])
    if not shift or shift['user_id'] != db_user['id']:
        await query.edit_message_text("❌ Смена не найдена")
        return

    shift_id = int(shift['id'])

    if shift['status'] != 'active':
        await query.edit_message_text("ℹ️ Эта смена уже закрыта.")
        return

    keyboard = [
        [InlineKeyboardButton("✅ Да, закрыть", callback_data=f"close_confirm_yes_{shift_id}")],
        [InlineKeyboardButton("❌ Нет, оставить открытой", callback_data=f"close_confirm_no_{shift_id}")],
    ]
    await query.edit_message_text(
        "Вы точно хотите закрыть смену?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def close_shift_confirm_yes(query, context, data):
    parts = data.split('_')
    if len(parts) < 4:
        return
    shift_id = int(parts[3])

    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await query.edit_message_text("❌ Ошибка: пользователь не найден")
        return

    shift = DatabaseManager.get_shift(shift_id)
    if not shift or shift['user_id'] != db_user['id']:
        await query.edit_message_text("❌ Смена не найдена")
        return
    if shift['status'] != 'active':
        await query.edit_message_text("ℹ️ Эта смена уже закрыта.")
        return

    total = DatabaseManager.get_shift_total(shift_id)
    DatabaseManager.close_shift(shift_id)
    DatabaseManager.clear_goal_message_binding(db_user['id'])
    closed_shift = DatabaseManager.get_shift(shift_id) or shift
    cars = DatabaseManager.get_shift_cars(shift_id)
    message = build_closed_shift_dashboard(closed_shift, cars, total)

    await query.edit_message_text(
        message,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 В меню", callback_data="back")],
        ]),
    )
    await query.message.reply_text(build_shift_repeat_report_text(shift_id))
    await query.message.reply_text(
        "Выбери действие:",
        reply_markup=create_main_reply_keyboard(False)
    )


async def close_shift_confirm_no(query, context):
    await query.edit_message_text("Ок, смена остаётся открытой ✅")
    await query.message.reply_text(
        "Выбери действие:",
        reply_markup=create_main_reply_keyboard(True)
    )

async def go_back(query, context):
    """Возврат в главное меню"""
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    has_active = False
    subscription_active = False

    if db_user:
        has_active = DatabaseManager.get_active_shift(db_user['id']) is not None
        subscription_active = is_subscription_active(db_user)

    await query.edit_message_text("↩️ Возврат в главное меню")
    await query.message.reply_text(
        "Главное меню:",
        reply_markup=create_main_reply_keyboard(has_active, subscription_active)
    )

async def change_goal(query, context):
    """Запрос цели дня"""
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user or not DatabaseManager.get_active_shift(db_user['id']):
        await query.edit_message_text("🎯 Цель дня доступна только при открытой смене.")
        return
    context.user_data['awaiting_goal'] = True
    await query.edit_message_text(
        "Введи цель дня суммой, например: 5000"
    )

async def change_decade_goal(query, context):
    """Тоггл цели декады: если включена — выключаем, иначе просим сумму."""
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    if DatabaseManager.is_goal_enabled(db_user["id"]):
        DatabaseManager.set_goal_enabled(db_user["id"], False)
        DatabaseManager.set_daily_goal(db_user["id"], 0)
        await disable_goal_status(context, db_user["id"])
        await query.edit_message_text(
            "✅ Цель декады выключена.",
            reply_markup=build_settings_keyboard(db_user, is_admin_telegram(query.from_user.id))
        )
        return

    context.user_data["awaiting_decade_goal"] = True
    await query.edit_message_text("Введи цель декады суммой, например: 35000")


async def calendar_rebase_callback(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    today = now_local().date()
    context.user_data["calendar_month"] = (today.year, today.month)
    context.user_data["calendar_setup_days"] = []
    DatabaseManager.set_work_anchor_date(db_user["id"], "")
    await query.edit_message_text(
        (
            f"📅 Календарь — {month_title(today.year, today.month)}\n\n"
            "Выберите 2 подряд идущих основных рабочих дня.\n"
            "Это обновит базовый график 2/2."
        ),
        reply_markup=build_work_calendar_keyboard(
            db_user,
            today.year,
            today.month,
            setup_mode=True,
            setup_selected=[],
            edit_mode=False,
        ),
    )


def build_leaderboard_text(decade_title: str, decade_leaders: list[dict]) -> str:
    header = [f"🏆 Топ героев", f"📆 Декада: {decade_title}"]
    if not decade_leaders:
        return "\n".join(header + ["Пока нет данных за декаду"])
    lines = []
    for place, leader in enumerate(decade_leaders, start=1):
        total = format_money(int(leader.get("total_amount", 0)))
        shifts = int(leader.get("shift_count", 0))
        lines.append(f"{place}. {leader.get('name', '—')} — {total} ({shifts} смен)")
    return "\n".join(header + [""] + lines)


def _load_rank_font(image_font, size: int):
    for path in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    ):
        try:
            return image_font.truetype(path, size=size)
        except Exception:
            continue
    try:
        return image_font.load_default()
    except Exception:
        return None


def _build_fallback_avatar(size: int, initials: str):
    if importlib.util.find_spec("PIL") is None:
        return None
    from PIL import Image, ImageDraw, ImageFont

    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img, "RGBA")
    for y in range(size):
        t = y / max(size - 1, 1)
        c1 = (34, 56, 98)
        c2 = (90, 65, 138)
        draw.line((0, y, size, y), fill=(int(c1[0] + (c2[0] - c1[0]) * t), int(c1[1] + (c2[1] - c1[1]) * t), int(c1[2] + (c2[2] - c1[2]) * t), 255))
    font = _load_rank_font(ImageFont, max(18, int(size * 0.33)))
    text = (initials or "?")[:2].upper()
    box = draw.textbbox((0, 0), text, font=font)
    draw.text(((size - (box[2] - box[0])) / 2, (size - (box[3] - box[1])) / 2), text, fill="#EAF0FF", font=font)
    return img
def build_leaderboard_image_bytes(decade_title: str, decade_leaders: list[dict], highlight_name: str | None = None, top3_avatars: dict[int, object] | None = None) -> BytesIO | None:
    if importlib.util.find_spec("PIL") is None:
        return None

    from PIL import Image, ImageDraw, ImageFont, ImageFilter

    width = 1200
    padding = 34
    header_h = 140
    podium_h = 320
    gap = 20
    row_h = 64
    list_rows = max(len(decade_leaders) - 3, 0)
    two_cols = list_rows > 14
    list_lines = max((list_rows + (2 if two_cols else 1) - 1) // (2 if two_cols else 1), 1 if list_rows else 0)
    list_h = 90 if list_rows == 0 else (64 + list_lines * row_h + 20)
    height = padding * 2 + header_h + podium_h + gap + list_h

    img = Image.new("RGBA", (width, height), "#071023")
    draw = ImageDraw.Draw(img, "RGBA")

    title_font = _load_rank_font(ImageFont, 48)
    sec_font = _load_rank_font(ImageFont, 24)
    card_font = _load_rank_font(ImageFont, 28)
    amount_font = _load_rank_font(ImageFont, 32)
    small_font = _load_rank_font(ImageFont, 20)

    def _rounded_card(x1, y1, x2, y2, fill=(17, 27, 50, 170), outline=(120, 146, 198, 80), r=24):
        draw.rounded_rectangle((x1, y1, x2, y2), radius=r, fill=fill, outline=outline, width=2)

    def _initials(name: str) -> str:
        parts = [p for p in str(name or "").strip().split() if p]
        if not parts:
            return "?"
        return (parts[0][0] + (parts[1][0] if len(parts) > 1 else "")).upper()

    def _username(leader: dict) -> str:
        username = str(leader.get("username") or leader.get("telegram_username") or "").strip()
        if not username:
            return ""
        return username if username.startswith("@") else f"@{username}"

    def _fit_text(text: str, max_width: int, base_size: int, min_size: int = 22) -> tuple[str, object]:
        current_size = base_size
        while current_size >= min_size:
            fnt = _load_rank_font(ImageFont, current_size)
            if draw.textbbox((0, 0), text, font=fnt)[2] <= max_width:
                return text, fnt
            current_size -= 1

        fnt = _load_rank_font(ImageFont, min_size)
        if draw.textbbox((0, 0), text, font=fnt)[2] <= max_width:
            return text, fnt
        cut = text
        while len(cut) > 1 and draw.textbbox((0, 0), cut + "…", font=fnt)[2] > max_width:
            cut = cut[:-1]
        return (cut + "…") if cut else "…", fnt

    # Background gradient
    for y in range(height):
        t = y / max(height - 1, 1)
        r = int(7 + (16 - 7) * t)
        g = int(16 + (26 - 16) * t)
        b = int(35 + (56 - 35) * t)
        draw.line((0, y, width, y), fill=(r, g, b, 255))

    # Blurred light spots
    glow = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow, "RGBA")
    glow_draw.ellipse((70, 20, 460, 360), fill=(87, 123, 255, 65))
    glow_draw.ellipse((760, 70, 1160, 470), fill=(247, 201, 72, 45))
    glow_draw.ellipse((420, 300, 900, 860), fill=(57, 199, 163, 32))
    glow = glow.filter(ImageFilter.GaussianBlur(55))
    img.alpha_composite(glow)

    # Light grain/noise
    for y in range(0, height, 4):
        for x in range((y * 3) % 11, width, 11):
            draw.point((x, y), fill=(255, 255, 255, 9))

    _rounded_card(padding - 4, padding - 4, width - padding + 4, height - padding + 4, fill=(12, 20, 39, 145), outline=(169, 180, 204, 60), r=28)

    # Header
    draw.text((padding + 18, padding + 18), f"Топ героев — {decade_title}", fill="#EAF0FF", font=title_font)
    draw.text((padding + 18, padding + 78), f"Сформировано: {now_local().strftime('%d.%m.%Y %H:%M')} МСК", fill="#A9B4CC", font=sec_font)

    y = padding + header_h

    # Podium background card
    _rounded_card(padding, y, width - padding, y + podium_h, fill=(19, 30, 56, 170), outline=(169, 180, 204, 70), r=26)

    ring_colors = [(247, 201, 72, 255), (196, 201, 214, 255), (205, 127, 50, 255)]
    top3 = decade_leaders[:3]
    col_gap = 18
    col_x1 = padding + 24
    col_x2 = width - padding - 24
    col_w = (col_x2 - col_x1 - col_gap * 2) // 3
    col_rects = [(col_x1 + i * (col_w + col_gap), col_x1 + (i + 1) * col_w + i * col_gap) for i in range(3)]
    # visual order: [#2, #1, #3]
    slot_place_order = [2, 1, 3]
    tile_h_small = podium_h - 34
    tile_h_large = int(tile_h_small * 1.2)

    # Soft extra glow for #1 only (inside Top-3 card)
    first_glow = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    fg_draw = ImageDraw.Draw(first_glow, "RGBA")
    center_col_left, center_col_right = col_rects[1]
    fg_draw.ellipse((center_col_left - 40, y + 14, center_col_right + 40, y + 224), fill=(247, 201, 72, 34))
    first_glow = first_glow.filter(ImageFilter.GaussianBlur(30))
    img.alpha_composite(first_glow)

    def _circle_mask(sz: int):
        from PIL import Image, ImageDraw
        m = Image.new("L", (sz, sz), 0)
        md = ImageDraw.Draw(m)
        md.ellipse((0, 0, sz - 1, sz - 1), fill=255)
        return m

    for slot_idx, place in enumerate(slot_place_order):
        leader_idx = place - 1
        if leader_idx >= len(top3):
            continue
        leader = top3[leader_idx]
        col_left, col_right = col_rects[slot_idx]
        cx = (col_left + col_right) // 2
        is_first = place == 1
        avatar_r = 70 if is_first else 56

        tile_h = tile_h_large if is_first else tile_h_small
        tile_top = y + (12 if is_first else 28)
        tile_bottom = min(tile_top + tile_h, y + podium_h - 10)
        tile_fill = (29, 43, 78, 196) if is_first else (24, 37, 66, 176)
        draw.rounded_rectangle((col_left + 6, tile_top, col_right - 6, tile_bottom), radius=22, fill=tile_fill, outline=(169, 180, 204, 90), width=2)

        cy = tile_top + (120 if is_first else 102)

        name = str(leader.get("name", "—"))
        total = format_money(int(leader.get("total_amount", 0)))
        uname = _username(leader)

        accent = ring_colors[leader_idx]

        # Avatar
        avatar_size = avatar_r * 2
        avatar_raw = (top3_avatars or {}).get(place)
        if avatar_raw is None:
            avatar_raw = _build_fallback_avatar(avatar_size, _initials(name))
        if avatar_raw is not None:
            mask = _circle_mask(avatar_size)
            avatar_round = Image.new("RGBA", (avatar_size, avatar_size), (0, 0, 0, 0))
            avatar_round.paste(avatar_raw.resize((avatar_size, avatar_size)), (0, 0), mask)
            img.alpha_composite(avatar_round, (int(cx - avatar_r), int(cy - avatar_r)))
        else:
            draw.ellipse((cx - avatar_r, cy - avatar_r, cx + avatar_r, cy + avatar_r), fill=(26, 39, 71, 235))
            initials = _initials(name)
            init_font = _load_rank_font(ImageFont, 34 if is_first else 28)
            iw = draw.textbbox((0, 0), initials, font=init_font)
            draw.text((cx - (iw[2] - iw[0]) / 2, cy - (iw[3] - iw[1]) / 2), initials, fill="#EAF0FF", font=init_font)
        draw.ellipse((cx - avatar_r, cy - avatar_r, cx + avatar_r, cy + avatar_r), outline=ring_colors[leader_idx], width=6 if is_first else 5)

        # Rank badge (no emoji)
        badge_w = 58
        badge_h = 32
        bx1 = col_right - 10 - badge_w
        by1 = tile_top + 10
        draw.rounded_rectangle((bx1, by1, bx1 + badge_w, by1 + badge_h), radius=16, fill=(accent[0], accent[1], accent[2], 235), outline=(255, 255, 255, 80), width=1)
        btxt = f"#{place}"
        bw = draw.textbbox((0, 0), btxt, font=small_font)
        draw.text((bx1 + (badge_w - (bw[2] - bw[0])) / 2, by1 + 6), btxt, fill="#0A1020", font=small_font)

        safe_w = col_w - 36
        name_text, name_font = _fit_text(name, safe_w, 30 if is_first else 27, min_size=22)
        nw = draw.textbbox((0, 0), name_text, font=name_font)
        name_y = cy + avatar_r + 12
        draw.text((cx - (nw[2] - nw[0]) / 2, name_y), name_text, fill="#EAF0FF", font=name_font)

        amount_text, amount_fit_font = _fit_text(total, safe_w, 42 if is_first else 33, min_size=24)
        aw = draw.textbbox((0, 0), amount_text, font=amount_fit_font)
        amount_y = name_y + (nw[3] - nw[1]) + 8
        draw.text((cx - (aw[2] - aw[0]) / 2, amount_y), amount_text, fill="#F7C948", font=amount_fit_font)

        if uname and amount_y + (aw[3] - aw[1]) + 26 <= tile_bottom - 8:
            uname_text, uname_font = _fit_text(uname, safe_w, 20, min_size=18)
            uw = draw.textbbox((0, 0), uname_text, font=uname_font)
            draw.text((cx - (uw[2] - uw[0]) / 2, amount_y + (aw[3] - aw[1]) + 6), uname_text, fill="#A9B4CC", font=uname_font)

    y += podium_h + gap

    # List card
    _rounded_card(padding, y, width - padding, y + list_h, fill=(18, 28, 52, 168), outline=(169, 180, 204, 70), r=24)
    draw.text((padding + 20, y + 16), "Остальные места", fill="#A9B4CC", font=sec_font)

    rest = decade_leaders[3:]
    if not rest:
        draw.text((padding + 20, y + 52), "Пока недостаточно данных для списка 4..N", fill="#A9B4CC", font=small_font)
    else:
        columns = 2 if two_cols else 1
        col_gap = 18
        content_x1 = padding + 16
        content_x2 = width - padding - 16
        content_y = y + 52
        col_w = (content_x2 - content_x1 - col_gap * (columns - 1)) // columns
        highlight_norm = (highlight_name or "").strip().lower()

        for idx, leader in enumerate(rest, start=4):
            local = idx - 4
            col = local // list_lines if columns == 2 else 0
            row = local % list_lines if columns == 2 else local
            x = content_x1 + col * (col_w + col_gap)
            yy = content_y + row * row_h
            name = str(leader.get("name", "—"))
            total = format_money(int(leader.get("total_amount", 0)))
            is_me = bool(highlight_norm and name.strip().lower() == highlight_norm)
            fill = (54, 40, 84, 200) if is_me else ((24, 36, 66, 185) if idx % 2 else (20, 32, 58, 175))

            draw.rounded_rectangle((x, yy, x + col_w, yy + row_h - 8), radius=14, fill=fill, outline=(132, 146, 173, 80), width=1)
            draw.text((x + 14, yy + 14), f"{idx}.", fill="#A9B4CC", font=small_font)
            avx = x + 56
            avy = yy + 26
            draw.ellipse((avx - 14, avy - 14, avx + 14, avy + 14), fill=(32, 49, 88, 255), outline=(90, 115, 173, 200), width=2)
            init = _initials(name)
            draw.text((avx - 7, avy - 10), init[:2], fill="#EAF0FF", font=small_font)
            draw.text((x + 84, yy + 14), name[:20], fill="#EAF0FF", font=small_font)
            tw = draw.textbbox((0, 0), total, font=small_font)
            draw.text((x + col_w - (tw[2] - tw[0]) - 14, yy + 14), total, fill="#F7C948", font=small_font)

    out = BytesIO()
    out.name = "leaderboard.png"
    img.convert("RGB").save(out, format="PNG")
    out.seek(0)
    return out


async def send_leaderboard_output(chat_target, context: CallbackContext, decade_title: str, decade_leaders: list[dict], reply_markup=None, highlight_name: str | None = None):
    text_message = build_leaderboard_text(decade_title, decade_leaders)
    # live statuses
    class _U:
        callback_query = None
        message = chat_target
        effective_chat = chat_target.chat

    fake_update = _U()
    st = await send_status(fake_update, context, STATUS_LEADERBOARD[0])
    await edit_status(st, STATUS_LEADERBOARD[1])

    top3_avatars: dict[int, object] = {}
    try:
        tasks = []
        top3 = decade_leaders[:3]
        for place, leader in enumerate(top3, start=1):
            uid = int(leader.get("telegram_id") or 0)
            name = str(leader.get("name", ""))
            tasks.append(get_avatar_image_async(context.bot, uid, 140 if place == 1 else 112, fallback_name=name))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for place, res in enumerate(results, start=1):
            if not isinstance(res, Exception):
                top3_avatars[place] = res
    except Exception:
        await edit_status(st, "⚠️ Не смог получить часть данных, показал то, что есть.")

    await edit_status(st, STATUS_LEADERBOARD[2])
    image = build_leaderboard_image_bytes(decade_title, decade_leaders, highlight_name, top3_avatars=top3_avatars)
    if image is not None:
        await done_status(
            st,
            STATUS_LEADERBOARD[3],
            attach_photo_bytes=image,
            filename="leaderboard.png",
            caption=f"🏆 Топ героев\n📆 Декада: {decade_title}"[:1024],
        )
        # user position line
        rank_line = "Пока данных мало, но топ уже формируется."
        if highlight_name and decade_leaders:
            pos = next((i for i, row in enumerate(decade_leaders, start=1) if str(row.get("name", "")).strip().lower() == highlight_name.strip().lower()), None)
            if pos:
                top3_amount = int(decade_leaders[2]["total_amount"]) if len(decade_leaders) >= 3 else int(decade_leaders[0]["total_amount"])
                me_amount = int(decade_leaders[pos - 1]["total_amount"])
                diff = max(0, top3_amount - me_amount)
                rank_line = f"Твоя позиция: #{pos} · До #3 осталось {format_money(diff)}"
        await context.bot.send_message(chat_id=chat_target.chat_id, text=rank_line)
        if isinstance(reply_markup, ReplyKeyboardMarkup):
            await context.bot.send_message(
                chat_id=chat_target.chat_id,
                text="Выбери действие:",
                reply_markup=reply_markup,
            )
        return

    await send_text_with_optional_photo(
        chat_target,
        context,
        text_message,
        reply_markup=reply_markup,
        section="leaderboard",
    )


async def leaderboard(query, context):
    """Топ героев: лидеры текущей декады"""
    today = now_local().date()
    idx, _, _, _, decade_title = get_decade_period(today)
    decade_leaders = DatabaseManager.get_decade_leaderboard(today.year, today.month, idx)

    db_user = DatabaseManager.get_user(query.from_user.id)
    has_active = bool(db_user and DatabaseManager.get_active_shift(db_user['id']))
    highlight_name = db_user["name"] if db_user else (query.from_user.first_name or "")
    await query.edit_message_text("🏆 Формирую рейтинг...")
    await send_leaderboard_output(
        query.message,
        context,
        decade_title,
        decade_leaders,
        reply_markup=create_main_reply_keyboard(has_active),
        highlight_name=highlight_name,
    )


async def reset_data_prompt(query, context):
    await query.edit_message_text(
        "⚠️ Вы точно хотите полностью сбросить аккаунт?\n\n"

        "Будут удалены: все смены, машины, услуги, комбо, цель дня и история.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, удалить всё", callback_data="reset_data_yes")],
            [InlineKeyboardButton("❌ Нет", callback_data="reset_data_no")],
        ])
    )


async def reset_data_confirm_yes(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    DatabaseManager.reset_user_data(db_user['id'])
    context.user_data.clear()
    await query.edit_message_text("✅ Все ваши данные удалены.")
    await query.message.reply_text("Выбери действие:", reply_markup=create_main_reply_keyboard(False))


async def reset_data_confirm_no(query, context):
    await go_back(query, context)


async def toggle_shift_message(update: Update, context: CallbackContext):
    db_user = DatabaseManager.get_user(update.effective_user.id)
    if not db_user:
        await update.message.reply_text("❌ Пользователь не найден. Напиши /start")
        return
    if DatabaseManager.get_active_shift(db_user['id']):
        await close_shift_message(update, context)
    else:
        await open_shift_message(update, context)


async def open_shift_message(update: Update, context: CallbackContext):
    user = update.effective_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await update.message.reply_text("❌ Ошибка: пользователь не найден")
        return

    _, message, _ = open_shift_core(db_user)
    await update.message.reply_text(
        message + "\n\n💡 Теперь просто отправляйте номер авто в чат в любой момент — машина добавится автоматически.",
        reply_markup=main_menu_for_db_user(db_user, True)
    )

async def add_car_message(update: Update, context: CallbackContext):
    user = update.effective_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await update.message.reply_text("❌ Ошибка: пользователь не найден")
        return

    active_shift = DatabaseManager.get_active_shift(db_user['id'])
    if not active_shift:
        await update.message.reply_text(
            "❌ Нет активной смены!\nСначала откройте смену.",
            reply_markup=create_main_reply_keyboard(False)
        )
        return

    context.user_data['awaiting_car_number'] = True
    await update.message.reply_text(
        "Чтобы добавить машину введите номер ТС в свободном формате.\n\n"
        "Кнопку, кстати, для этого нажимать не обязательно."
    )

async def history_message(update: Update, context: CallbackContext):
    user = update.effective_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await update.message.reply_text("❌ Ошибка: пользователь не найден")
        return

    shifts = DatabaseManager.get_user_shifts(db_user['id'], limit=10)
    if not shifts:
        await update.message.reply_text(
            "📜 У вас ещё нет смен.\nОткройте первую смену!",
            reply_markup=create_main_reply_keyboard(False)
        )
        return

    today = now_local().date()
    idx, _, _, _, _ = get_decade_period(today)
    context.user_data["history_decades_page"] = max((idx - 1), 0)
    message, markup = build_history_decades_page(db_user, context.user_data["history_decades_page"])
    if not message or not markup:
        await update.message.reply_text("📜 История пуста")
        return
    await update.message.reply_text(message, reply_markup=markup)


async def current_shift_message(update: Update, context: CallbackContext):
    user = update.effective_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await update.message.reply_text("❌ Ошибка: пользователь не найден")
        return

    active_shift = DatabaseManager.get_active_shift(db_user['id'])
    if not active_shift:
        await update.message.reply_text(
            "📭 Нет активной смены.\nОткройте смену для начала работы.",
            reply_markup=create_main_reply_keyboard(False)
        )
        return

    cars = DatabaseManager.get_shift_cars(active_shift['id'])
    total = DatabaseManager.get_shift_total(active_shift['id'])
    message = build_current_shift_dashboard(db_user['id'], active_shift, cars, total)
    await update.message.reply_text(
        message,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 В меню", callback_data="back")],
        ])
    )


async def close_shift_message(update: Update, context: CallbackContext):
    user = update.effective_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await update.message.reply_text("❌ Ошибка: пользователь не найден")
        return

    active_shift = DatabaseManager.get_active_shift(db_user['id'])
    if not active_shift:
        await update.message.reply_text(
            "📭 Нет активной смены для закрытия.",
            reply_markup=create_main_reply_keyboard(False)
        )
        return

    await update.message.reply_text(
        "Вы точно хотите закрыть смену?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, закрыть", callback_data=f"close_confirm_yes_{active_shift['id']}")],
            [InlineKeyboardButton("❌ Нет, оставить открытой", callback_data=f"close_confirm_no_{active_shift['id']}")],
        ]),
    )

async def settings_message(update: Update, context: CallbackContext):
    db_user = DatabaseManager.get_user(update.effective_user.id)
    await update.message.reply_text(
        f"⚙️ НАСТРОЙКИ\n\nВерсия: {APP_VERSION}\nОбновлено: {APP_UPDATED_AT}\n\nВыберите параметр:",
        reply_markup=build_settings_keyboard(db_user, is_admin_telegram(update.effective_user.id))
    )

async def leaderboard_message(update: Update, context: CallbackContext):
    today = now_local().date()
    idx, _, _, _, decade_title = get_decade_period(today)
    decade_leaders = DatabaseManager.get_decade_leaderboard(today.year, today.month, idx)

    db_user = DatabaseManager.get_user(update.effective_user.id)
    has_active = bool(db_user and DatabaseManager.get_active_shift(db_user['id']))
    highlight_name = db_user["name"] if db_user else (update.effective_user.first_name or "")
    await send_leaderboard_output(
        update.message,
        context,
        decade_title,
        decade_leaders,
        reply_markup=create_main_reply_keyboard(has_active),
        highlight_name=highlight_name,
    )


async def decade_message(update: Update, context: CallbackContext):
    user = update.effective_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await update.message.reply_text("❌ Ошибка: пользователь не найден")
        return
    message = build_decade_summary(db_user['id'])
    await update.message.reply_text(
        message,
        parse_mode="HTML",
        reply_markup=create_main_reply_keyboard(True)
    )

async def stats_message(update: Update, context: CallbackContext):
    user = update.effective_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await update.message.reply_text("❌ Ошибка: пользователь не найден")
        return
    message = build_stats_summary(db_user['id'])
    await update.message.reply_text(
        message,
        reply_markup=create_main_reply_keyboard(True)
    )

async def show_car_services(
    query,
    context: CallbackContext,
    car_id: int,
    page: int = 0,
    history_day: str | None = None,
):
    """Показать услуги машины"""
    car = DatabaseManager.get_car(car_id)
    if not car:
        return None, None

    if not history_day:
        history_day = context.user_data.get(f"history_day_for_car_{car_id}")

    services = DatabaseManager.get_car_services(car_id)
    services_text = ""
    for service in services:
        services_text += f"• {plain_service_name(service['service_name'])} ({service['price']}₽) ×{service['quantity']}\n"

    if not services_text:
        services_text = "Нет выбранных услуг\n"

    edit_mode = get_edit_mode(context, car_id)
    mode_text = "✏️ Режим: удаление" if edit_mode else "➕ Режим: добавление"

    db_user = DatabaseManager.get_user(query.from_user.id)
    current_mode = get_price_mode(context, db_user["id"] if db_user else None)
    price_text = "🌞 Прайс: день" if current_mode == "day" else "🌙 Прайс: ночь"

    header = f"🚗 Машина: {car['car_number']}\n"
    if history_day:
        header += f"📅 День: {history_day}\n"

    message = (
        f"{header}"
        f"Итог: {format_money(car['total_amount'])}\n\n"
        f"{mode_text}\n{price_text}\n\n"
        f"Услуги:\n{services_text}\n"
        f"Выберите ещё:"
    )

    await query.edit_message_text(
        message,
        reply_markup=create_services_keyboard(
            car_id,
            page,
            edit_mode,
            current_mode,
            db_user["id"] if db_user else None,
            history_day
        )
    )


async def export_shift_repeats(query, context, data):
    shift_id = int(data.replace("shift_repeats_", ""))
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    shift = DatabaseManager.get_shift(shift_id)
    if not shift or shift["user_id"] != db_user["id"]:
        await query.edit_message_text("❌ Смена не найдена")
        return

    await query.edit_message_text(
        build_shift_repeat_report_text(shift_id),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 В меню", callback_data="back")]])
    )


def get_previous_decade_period(target_day: date | None = None) -> tuple[date, date, int, int, int]:
    current = target_day or now_local().date()
    if current.day <= 10:
        prev_month = current.month - 1 or 12
        prev_year = current.year - 1 if current.month == 1 else current.year
        prev_end_day = calendar.monthrange(prev_year, prev_month)[1]
        return date(prev_year, prev_month, 21), date(prev_year, prev_month, prev_end_day), prev_year, prev_month, 3
    if current.day <= 20:
        return date(current.year, current.month, 1), date(current.year, current.month, 10), current.year, current.month, 1
    return date(current.year, current.month, 11), date(current.year, current.month, 20), current.year, current.month, 2


async def notify_decade_change_if_needed(application: Application, db_user: dict):
    _, _, _, current_key, _ = get_decade_period(now_local().date())
    last_key = DatabaseManager.get_last_decade_notified(db_user["id"])
    if not last_key:
        DatabaseManager.set_last_decade_notified(db_user["id"], current_key)
        return
    if last_key == current_key:
        return

    prev_start, prev_end, year, month, idx = get_previous_decade_period(now_local().date())
    text = build_period_summary_text(
        db_user["id"], prev_start, prev_end, f"Итог {idx}-й декады {MONTH_NAMES[month]} {year}"
    )
    try:
        await application.bot.send_message(
            chat_id=db_user["telegram_id"],
            text="🔔 Декада завершилась!\n\n" + text,
            parse_mode="HTML",
        )
    except Exception as exc:
        logger.warning(f"Не удалось отправить декадный отчёт {db_user['telegram_id']}: {exc}")
    finally:
        DatabaseManager.set_last_decade_notified(db_user["id"], current_key)


async def export_month_xlsx_callback(query, context, data):
    body = data.replace("export_month_xlsx_", "")
    year_s, month_s = body.split("_")
    year, month = int(year_s), int(month_s)
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    path = create_month_xlsx(db_user["id"], year, month)
    with open(path, "rb") as file:
        await query.message.reply_document(
            document=file,
            filename=os.path.basename(path),
            caption=f"XLSX отчёт за {MONTH_NAMES[month].capitalize()} {year}",
        )


async def notify_month_end_if_needed(application: Application, db_user: dict):
    now_dt = now_local()
    if now_dt.day != 1:
        return
    prev_day = now_dt.date() - timedelta(days=1)
    month_key = f"{prev_day.year:04d}-{prev_day.month:02d}"
    sent_key = f"month_report_sent_{db_user['id']}"
    if DatabaseManager.get_app_content(sent_key, "") == month_key:
        return

    start_d = date(prev_day.year, prev_day.month, 1)
    text = build_period_summary_text(
        db_user["id"],
        start_d,
        prev_day,
        f"Итог месяца: {MONTH_NAMES[prev_day.month].capitalize()} {prev_day.year}",
    )
    try:
        await application.bot.send_message(
            chat_id=db_user["telegram_id"],
            text="🗓 Месяц завершён!\n\n" + text,
            parse_mode="HTML",
        )
    except Exception as exc:
        logger.warning(f"Не удалось отправить месячный отчёт {db_user['telegram_id']}: {exc}")
    finally:
        DatabaseManager.set_app_content(sent_key, month_key)


async def send_period_reports_for_user(application: Application, db_user: dict):
    await notify_decade_change_if_needed(application, db_user)
    await notify_month_end_if_needed(application, db_user)


async def notify_subscription_events(application: Application):
    today = now_local().date()
    users = DatabaseManager.get_all_users_with_stats()
    for row in users:
        telegram_id = int(row["telegram_id"])
        if is_admin_telegram(telegram_id) or int(row.get("is_blocked", 0)) == 1:
            continue

        db_user = DatabaseManager.get_user_by_id(int(row["id"]))
        expires_at = subscription_expires_at_for_user(db_user) if db_user else None
        if not expires_at:
            continue

        expires_date = expires_at.astimezone(LOCAL_TZ).date()
        days_left = (expires_date - today).days

        if days_left == 1:
            key = f"sub_notice_1d_{row['id']}_{expires_date.isoformat()}"
            if DatabaseManager.get_app_content(key, "") != "1":
                try:
                    await application.bot.send_message(
                        chat_id=telegram_id,
                        text=(
                            "⏳ До окончания подписки остался 1 день.\n"
                            f"Доступ до: {format_subscription_until(expires_at)}\n\n"
                            f"Продление: {SUBSCRIPTION_PRICE_TEXT}. Напишите: {SUBSCRIPTION_CONTACT}"
                        ),
                    )
                except Exception:
                    pass
                DatabaseManager.set_app_content(key, "1")

        if days_left < 0:
            key = f"sub_notice_expired_{row['id']}_{expires_date.isoformat()}"
            if DatabaseManager.get_app_content(key, "") != "1":
                try:
                    await application.bot.send_message(
                        chat_id=telegram_id,
                        text=(
                            "⛔ Подписка закончилась.\n"
                            "Аккаунт деактивирован, доступен только раздел «👤 Профиль».\n\n"
                            f"Чтобы продлить ({SUBSCRIPTION_PRICE_TEXT}), напишите: {SUBSCRIPTION_CONTACT}"
                        ),
                    )
                except Exception:
                    pass
                DatabaseManager.set_app_content(key, "1")


async def scheduled_subscription_notifications_job(context: CallbackContext):
    await notify_subscription_events(context.application)


async def notify_shift_close_prompts(application: Application):
    now_dt = now_local()
    users = DatabaseManager.get_all_users_with_stats()
    for row in users:
        db_user = DatabaseManager.get_user_by_id(int(row["id"]))
        if not db_user:
            continue
        active_shift = DatabaseManager.get_active_shift(db_user["id"])
        if not active_shift:
            continue

        start_dt = parse_datetime(active_shift.get("start_time"))
        if not start_dt:
            continue
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=LOCAL_TZ)

        hours_open = (now_dt - start_dt).total_seconds() / 3600
        if hours_open < 12:
            continue

        key = f"shift_close_prompt_{active_shift['id']}"
        if DatabaseManager.get_app_content(key, "") == "1":
            continue

        try:
            await application.bot.send_message(
                chat_id=db_user["telegram_id"],
                text=(
                    "⏱ Смена открыта уже 12+ часов.\nЗакрыть её сейчас?"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Закрыть смену", callback_data=f"close_confirm_yes_{active_shift['id']}")],
                    [InlineKeyboardButton("❌ Оставить открытой", callback_data=f"close_confirm_no_{active_shift['id']}")],
                ]),
            )
            DatabaseManager.set_app_content(key, "1")
        except Exception:
            continue


async def scheduled_shift_close_prompts_job(context: CallbackContext):
    await notify_shift_close_prompts(context.application)


async def scheduled_period_reports(application: Application):
    users = DatabaseManager.get_all_users_with_stats()
    for row in users:
        db_user = DatabaseManager.get_user_by_id(int(row["id"]))
        if not db_user or is_user_blocked(db_user):
            continue
        await send_period_reports_for_user(application, db_user)


async def scheduled_period_reports_job(context: CallbackContext):
    await scheduled_period_reports(context.application)



async def toggle_price_mode(query, context):
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    current = get_price_mode(context, db_user['id'])
    new_mode = "night" if current == "day" else "day"
    set_manual_price_mode(context, db_user['id'], new_mode)
    label = "🌙 Ночной" if new_mode == "night" else "☀️ Дневной"
    await query.edit_message_text(
        f"✅ Прайс переключен: {label}\n"
        "Откройте машину и добавляйте услуги в этом режиме."
    )


async def cleanup_data_menu(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    months = DatabaseManager.get_user_months_with_data(db_user["id"], limit=18)
    if not months:
        await query.edit_message_text("🧹 Нет данных для очистки.")
        return

    keyboard = []
    for ym in months:
        year, month = ym.split('-')
        month_i = int(month)
        keyboard.append([
            InlineKeyboardButton(
                f"{MONTH_NAMES[month_i].capitalize()} {year}",
                callback_data=f"cleanup_month_{ym}",
            )
        ])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="settings")])
    await query.edit_message_text(
        "🧹 Выберите месяц для редактирования:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def cleanup_month(query, context, data):
    ym = data.replace("cleanup_month_", "")
    year, month = ym.split('-')
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    days = DatabaseManager.get_month_days_with_totals(db_user['id'], int(year), int(month))
    if not days:
        await query.edit_message_text("В этом месяце нет данных.")
        return

    keyboard = []
    for day_info in days:
        day_value = day_info['day']
        keyboard.append([
            InlineKeyboardButton(
                f"{day_value} • машин: {day_info['cars_count']} • {format_money(day_info['total_amount'])}",
                callback_data=f"cleanup_day_{day_value}",
            )
        ])
    keyboard.append([InlineKeyboardButton("🔙 К месяцам", callback_data="cleanup_data")])
    await query.edit_message_text("Выберите день:", reply_markup=InlineKeyboardMarkup(keyboard))


async def cleanup_day(query, context, data):
    day = data.replace("cleanup_day_", "")
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    cars = DatabaseManager.get_cars_for_day(db_user['id'], day)
    if not cars:
        await query.edit_message_text("За этот день машин нет.")
        return

    message = f"🗓️ {day}\n\n"
    keyboard = []
    for car in cars:
        message += f"• #{car['id']} {car['car_number']} — {format_money(car['total_amount'])}\n"
        keyboard.append([
            InlineKeyboardButton(
                f"🗑️ Удалить {car['car_number']}",
                callback_data=f"delcar_{car['id']}_{day}",
            )
        ])

    keyboard.append([InlineKeyboardButton("📋 Отчёт повторок", callback_data=f"day_repeats_{day}")])
    keyboard.append([InlineKeyboardButton("⚠️ Удалить весь день", callback_data=f"delday_prompt_{day}")])
    keyboard.append([InlineKeyboardButton("🔙 К дням", callback_data=f"cleanup_month_{day[:7]}")])
    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))


def build_day_repeat_report_text(user_id: int, day: str) -> str:
    cars = DatabaseManager.get_cars_for_day(user_id, day)
    if not cars:
        return f"📋 Отчёт повторок за {day}\n\nПовторов нет."

    lines = []
    for car in cars:
        services = DatabaseManager.get_car_services(int(car["id"]))
        repeats = [svc for svc in services if int(svc.get("quantity", 0)) > 1]
        if not repeats:
            continue
        lines.append(f"🚗 {car['car_number']}:")
        for svc in repeats:
            lines.append(f"• {plain_service_name(svc['service_name'])} ×{svc['quantity']}")

    if not lines:
        return f"📋 Отчёт повторок за {day}\n\nПовторов нет."
    return f"📋 Отчёт повторок за {day}\n\n" + "\n".join(lines)


async def day_repeats_callback(query, context, data):
    day = data.replace("day_repeats_", "")
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    await query.answer()
    await query.message.reply_text(build_day_repeat_report_text(db_user['id'], day))


async def delete_car_callback(query, context, data):
    body = data.replace("delcar_", "")
    car_id_s, day = body.split("_", 1)
    car_id = int(car_id_s)
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    ok = DatabaseManager.delete_car_for_user(db_user['id'], car_id)
    DatabaseManager.prune_empty_shifts_for_user(db_user['id'])
    if ok:
        await query.answer("Машина удалена")
    await cleanup_day(query, context, f"cleanup_day_{day}")


async def delete_day_prompt(query, context, data):
    day = data.replace("delday_prompt_", "")
    keyboard = [
        [InlineKeyboardButton("✅ Да, удалить день", callback_data=f"delday_confirm_{day}")],
        [InlineKeyboardButton("⬅️ Отмена", callback_data=f"cleanup_month_{day[:7]}")],
    ]
    await query.edit_message_text(
        f"Удалить все машины за {day}?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def delete_day_callback(query, context, data):
    day = data.replace("delday_confirm_", "")
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    deleted = DatabaseManager.delete_day_data(db_user['id'], day)
    removed_shifts = DatabaseManager.prune_empty_shifts_for_user(db_user['id'])
    await query.edit_message_text(
        f"✅ Удалено машин за день {day}: {deleted}\n"
        f"Пустых смен удалено: {removed_shifts}"
    )
    await cleanup_month(query, context, f"cleanup_month_{day[:7]}")


# ========== ОБРАБОТЧИК ОШИБОК ==========

async def error_handler(update: Update, context: CallbackContext):
    """Обработчик ошибок"""
    logger.error(f"Ошибка: {context.error}", exc_info=context.error)
    
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "❌ Произошла ошибка.\n"
                "Попробуйте ещё раз или перезапустите бота командой /start"
            )
        except Exception:
            pass

async def on_startup(application: Application):
    if application.job_queue:
        application.job_queue.run_daily(
            scheduled_period_reports_job,
            time=datetime.strptime("23:59", "%H:%M").time().replace(tzinfo=LOCAL_TZ),
            name="period_reports_daily",
        )
        application.job_queue.run_repeating(
            scheduled_subscription_notifications_job,
            interval=3600,
            first=30,
            name="subscription_notifications_hourly",
        )
        application.job_queue.run_repeating(
            scheduled_shift_close_prompts_job,
            interval=3600,
            first=60,
            name="shift_close_prompts_hourly",
        )

    rollout_done = DatabaseManager.get_app_content("trial_rollout_done", "")
    if rollout_done == APP_VERSION:
        await notify_subscription_events(application)
        await notify_shift_close_prompts(application)
        return

    activated = ensure_trial_for_existing_users()
    for row in activated:
        try:
            await application.bot.send_message(
                chat_id=row["telegram_id"],
                text=(
                    "🎉 Ваш аккаунт активирован на 7 дней!\n"
                    f"Доступ до: {format_subscription_until(row['expires_at'])}\n"
                    "Приятного пользования ботом."
                )
            )
        except Exception:
            continue

    DatabaseManager.set_app_content("trial_rollout_done", APP_VERSION)
    await notify_subscription_events(application)
    await notify_shift_close_prompts(application)


# ========== ГЛАВНАЯ ФУНКЦИЯ ==========

def main():
    """Запуск бота"""
    application = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()
    
    # Регистрация команд
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("menu", menu_command))
    
    # Обработчик callback-кнопок
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    # Обработчик медиа и текстовых сообщений
    application.add_handler(MessageHandler((filters.PHOTO | filters.VIDEO) & ~filters.COMMAND, handle_media_message))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Обработчик ошибок
    application.add_error_handler(error_handler)
    
    # Запуск бота
    logger.info(f"🤖 Бот запускается... Версия: {APP_VERSION}")
    print("=" * 60)
    print("🚀 БОТ ДЛЯ УЧЁТА УСЛУГ - УПРОЩЕННАЯ ВЕРСИЯ")
    print(f"🔖 Версия: {APP_VERSION}")
    print(f"🛠 Обновлено: {APP_UPDATED_AT}")
    print(f"🕒 Часовой пояс: {APP_TIMEZONE}")
    print("✅ Просто работает")
    print("=" * 60)
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
