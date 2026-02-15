"""
🤖 БОТ ДЛЯ УЧЁТА УСЛУГ 
"""

import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import csv
import os
import shutil
import calendar
import re
from typing import List

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
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

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
APP_VERSION = "2026.02.16-hotfix-13"
APP_UPDATED_AT = "2026-02-16 00:30 (Europe/Moscow)"
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
    has_active_shift = bool(db_user and DatabaseManager.get_active_shift(db_user['id']))
    keyboard = [
        *([[InlineKeyboardButton("🎯 Цель дня", callback_data="change_goal")]] if has_active_shift else []),
        [InlineKeyboardButton("📜 История по декадам", callback_data="history_decades")],
        [InlineKeyboardButton("🧩 Мои комбинации", callback_data="combo_settings")],
        [InlineKeyboardButton("➕ Создать комбо", callback_data="combo_create_settings")],
        [InlineKeyboardButton("💰 Прайс", callback_data="show_price")],
        [InlineKeyboardButton("📅 Рабочий календарь", callback_data="calendar_open")],
        [InlineKeyboardButton("❓ FAQ", callback_data="faq")],
        [InlineKeyboardButton("🗑️ Сбросить данные", callback_data="reset_data")],
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
        "Доступны только: 📜 История смен и 💳 Продлить подписку.\n"
        f"Стоимость подписки: {SUBSCRIPTION_PRICE_TEXT}.\n"
        f"Для продления напишите: {SUBSCRIPTION_CONTACT}"
    )


def is_allowed_when_expired_menu(text: str) -> bool:
    return text in {MENU_HISTORY, MENU_SUBSCRIPTION, MENU_FAQ}


def is_allowed_when_expired_callback(data: str) -> bool:
    return (
        data in {"faq", "history_decades", "subscription_info", "back", "calendar_open"}
        or data.startswith("history_decade_")
        or data.startswith("history_day_")
        or data.startswith("calendar_nav_")
        or data.startswith("calendar_day_")
        or data.startswith("calendar_set_")
        or data.startswith("calendar_back_month_")
    )


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
    return f"{MONTH_NAMES[month].capitalize()} {year}"


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
            suffix = "📂" if day_key in shifts_days else ""
            row.append(InlineKeyboardButton(f"{prefix}{day:02d}{suffix}", callback_data=f"calendar_day_{day_key}"))
        keyboard.append(row)

    if setup_mode:
        keyboard.append([InlineKeyboardButton("✅ Сохранить базовые дни", callback_data=f"calendar_setup_save_{year}_{month}")])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="back")])
    else:
        mode_label = "✏️ Режим редактирования: ВКЛ" if edit_mode else "✏️ Режим редактирования: ВЫКЛ"
        keyboard.append([InlineKeyboardButton(mode_label, callback_data=f"calendar_edit_toggle_{year}_{month}")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    return InlineKeyboardMarkup(keyboard)


def build_work_calendar_text(db_user: dict, year: int, month: int, setup_mode: bool = False, edit_mode: bool = False) -> str:
    if setup_mode:
        return (
            f"📅 Рабочий календарь — {month_title(year, month)}\n\n"
            "Первый запуск: выберите 2 подряд идущих основных рабочих дня.\n"
            "После сохранения график 2/2 будет рассчитан автоматически."
        )
    mode = "редактирование" if edit_mode else "просмотр"
    return (
        f"📅 Рабочий календарь — {month_title(year, month)}\n"
        "Легенда: 🔴 основная смена, 🟡 доп. смена, ⚪ выходной, 📂 есть смена.\n"
        f"Режим: {mode}."
    )


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

# ========== КЛАВИАТУРЫ ==========

MENU_OPEN_SHIFT = "📅 Открыть смену"
MENU_ADD_CAR = "🚗 Добавить машину"
MENU_CURRENT_SHIFT = "📊 Текущая смена"
MENU_CLOSE_SHIFT = "🔚 Закрыть смену"
MENU_HISTORY = "📜 История смен"
MENU_SETTINGS = "⚙️ Настройки и данные"
MENU_LEADERBOARD = "🏆 Топ героев"
MENU_DECADE = "📆 Зарплата (декады)"
MENU_STATS = "📈 Статистика"
MENU_FAQ = "❓ FAQ"
MENU_SUBSCRIPTION = "💳 Продлить подписку"
MENU_PRICE = "💰 Прайс"
MENU_CALENDAR = "📅 Рабочий календарь"


def create_main_reply_keyboard(has_active_shift: bool = False, subscription_active: bool = True) -> ReplyKeyboardMarkup:
    """Главное меню под полем ввода"""
    keyboard = []

    if not subscription_active:
        keyboard.append([KeyboardButton(MENU_HISTORY)])
        keyboard.append([KeyboardButton(MENU_SUBSCRIPTION), KeyboardButton(MENU_FAQ)])
        return ReplyKeyboardMarkup(
            keyboard,
            resize_keyboard=True,
            one_time_keyboard=False,
            input_field_placeholder="Выберите действие ниже"
        )

    if has_active_shift:
        keyboard.append([KeyboardButton(MENU_ADD_CAR), KeyboardButton(MENU_CURRENT_SHIFT)])
        keyboard.append([KeyboardButton(MENU_CLOSE_SHIFT)])
    else:
        keyboard.append([KeyboardButton(MENU_OPEN_SHIFT)])

    keyboard.append([KeyboardButton(MENU_HISTORY), KeyboardButton(MENU_LEADERBOARD)])
    keyboard.append([KeyboardButton(MENU_DECADE), KeyboardButton(MENU_STATS)])
    keyboard.append([KeyboardButton(MENU_PRICE), KeyboardButton(MENU_CALENDAR)])
    keyboard.append([KeyboardButton(MENU_FAQ), KeyboardButton(MENU_SETTINGS)])

    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выберите действие ниже"
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
    """Клавиатура выбора услуг (с колонками и перелистыванием)"""
    service_ids = get_service_order(user_id)
    per_page = 10
    max_page = max((len(service_ids) - 1) // per_page, 0)
    page = max(0, min(page, max_page))

    start = page * per_page
    end = start + per_page
    page_ids = service_ids[start:end]

    buttons = []
    for service_id in page_ids:
        service = SERVICES[service_id]
        clean_name = plain_service_name(service['name'])
        if service.get("kind") == "group":
            text = f"{clean_name} (выбор)"
        elif service.get("kind") == "distance":
            text = "Дальняк"
            text = "Дальняк"
        else:
            text = clean_name
        buttons.append(InlineKeyboardButton(text, callback_data=f"service_{service_id}_{car_id}_{page}"))

    keyboard = chunk_buttons(buttons, 3)

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"service_page_{car_id}_{page - 1}"))
    nav_buttons.append(InlineKeyboardButton(f"Стр {page + 1}/{max_page + 1}", callback_data="noop"))
    if page < max_page:
        nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"service_page_{car_id}_{page + 1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)

    mode_label = "🌞 День" if mode == "day" else "🌙 Ночь"
    keyboard.append([
        InlineKeyboardButton("🔎 Поиск", callback_data=f"service_search_{car_id}_{page}"),
        InlineKeyboardButton(f"🔁 {mode_label}", callback_data=f"toggle_price_car_{car_id}_{page}"),
    ])
    if user_id:
        combos = DatabaseManager.get_user_combos(user_id)
        for combo in combos[:5]:
            keyboard.append([InlineKeyboardButton(f"🧩 {combo['name']}", callback_data=f"combo_apply_{combo['id']}_{car_id}_{page}")])

    edit_text = "✅ Готово" if is_edit_mode else "✏️ Изменить"
    keyboard.append([
        InlineKeyboardButton(edit_text, callback_data=f"toggle_edit_{car_id}_{page}"),
        InlineKeyboardButton("🗑️ Очистить", callback_data=f"clear_{car_id}_{page}"),
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
    metrics = build_shift_metrics(shift, cars, total)
    goal = DatabaseManager.get_daily_goal(user_id) if DatabaseManager.is_goal_enabled(user_id) else 0
    percent = calculate_percent(total, goal) if goal > 0 else 0
    goal_line = (
        f"🎯 Цель: {format_money(total)}/{format_money(goal)} {percent}% {render_bar(percent, 8)}"
        if goal > 0 else ""
    )

    top_services = DatabaseManager.get_shift_top_services(shift["id"], limit=3)
    top_block = ""
    if top_services:
        top_rows = [
            f"• {plain_service_name(item['service_name'])} — {item['total_count']}"
            for item in top_services
        ]
        top_block = "\n🔥 Топ услуг:\n" + "\n".join(top_rows)

    start_label = metrics["start_time"].strftime("%H:%M %d.%m.%Y") if metrics["start_time"] else "неизвестно"
    return (
        "✨ <b>Дашборд текущей смены</b>\n\n"
        f"🕒 Старт: {start_label}\n"
        f"🚗 Машин: {metrics['cars_count']}\n"
        f"💰 Выручка: <b>{format_money(total)}</b>\n"
        f"📈 Средний чек: {format_money(metrics['avg_check'])}\n"
        f"⚡ Машин/час: {metrics['cars_per_hour']:.2f}\n"
        f"💸 Доход/час: {format_money(int(metrics['money_per_hour']))}\n"
        f"{goal_line}{top_block}"
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

    active_shift = DatabaseManager.get_active_shift(user_id)
    if not active_shift:
        return ""

    goal = DatabaseManager.get_daily_goal(user_id)
    if goal <= 0:
        return ""

    shift_total = DatabaseManager.get_shift_total(active_shift['id'])
    percent = calculate_percent(shift_total, goal)
    filled = min(percent // 10, 10)
    bar = "🟩" * filled + "⬜" * (10 - filled)
    return (
        f"🎯 Цель смены: {format_money(goal)}\n"
        f"Сделано: {format_money(shift_total)} ({percent}%)\n"
        f"{bar}"
    )


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
        total = int(DatabaseManager.get_user_total_between_dates(user_id, start_d.isoformat(), end_d.isoformat()) or 0)
        shifts = DatabaseManager.get_shifts_count_between_dates(user_id, start_d.isoformat(), end_d.isoformat())
        marker = "👉 " if idx == current_decade else ""
        lines.append(
            f"{marker}<b>{idx}-я декада</b> ({format_decade_range(start_d, end_d)}): {format_money(total)} · смен: {shifts}"
        )

    return "\n".join(lines)


def create_db_backup() -> str:
    if not os.path.exists(DB_PATH):
        return ""
    backups_dir = "backups"
    os.makedirs(backups_dir, exist_ok=True)
    filename = f"backup_{now_local().strftime('%Y%m%d_%H%M%S')}.db"
    path = os.path.join(backups_dir, filename)
    shutil.copy2(DB_PATH, path)
    return path

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

    if bind_chat_id and bind_message_id:
        try:
            await context.bot.edit_message_text(chat_id=bind_chat_id, message_id=bind_message_id, text=goal_text)
            return
        except Exception:
            DatabaseManager.clear_goal_message_binding(user_id)

    message = await source_message.reply_text(goal_text)
    DatabaseManager.set_goal_message_binding(user_id, chat_id, message.message_id)
    try:
        await context.bot.pin_chat_message(
            chat_id=message.chat_id,
            message_id=message.message_id,
            disable_notification=True
        )
    except Exception:
        pass

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
            f"👋 Привет!\n"
            f"Я бот для учёта выполненных услуг.\n\n"
            f"Версия: {APP_VERSION}\n"
            f"Выберите действие:",
            reply_markup=create_main_reply_keyboard(has_active, subscription_active)
        )
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
        "Меню открыто.",
        reply_markup=main_menu_for_db_user(db_user, subscription_active)
    )
    await send_period_reports_for_user(context.application, db_user)

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

        if context.user_data.get("awaiting_admin_faq_video"):
            video = update.message.video
            if not video:
                await update.message.reply_text("Пришлите именно видео сообщением Telegram (формат video).")
                return
            DatabaseManager.set_app_content("faq_video_file_id", video.file_id)
            DatabaseManager.set_app_content("faq_video_source_chat_id", str(update.message.chat_id))
            DatabaseManager.set_app_content("faq_video_source_message_id", str(update.message.message_id))
            context.user_data.pop("awaiting_admin_faq_video", None)
            await update.message.reply_text("✅ Видео FAQ обновлено. Пользователи будут получать его как полноценное видео.")
            return

    # Если ожидаем номер машины, но пользователь нажал меню — отменяем ввод
    if context.user_data.get('awaiting_car_number') and text in {
        MENU_OPEN_SHIFT,
        MENU_ADD_CAR,
        MENU_CURRENT_SHIFT,
        MENU_CLOSE_SHIFT,
        MENU_HISTORY,
        MENU_SETTINGS,
        MENU_LEADERBOARD,
        MENU_DECADE,
        MENU_STATS,
        MENU_FAQ,
        MENU_SUBSCRIPTION,
        MENU_PRICE,
        MENU_CALENDAR,
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
        "Введите номер машины:\n\n"
        "Примеры:\n"
        "• А123ВС777\n"
        "• Х340РУ797\n"
        "• В567ТХ799\n\n"
        "Можно вводить русскими или английскими буквами."
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

    # Ожидание цели дня
    if context.user_data.get('awaiting_goal'):
        raw_value = text.replace(" ", "").replace("₽", "")
        if not raw_value.isdigit():
            await update.message.reply_text("❌ Введите сумму цифрами. Например: 5000")
            return
        goal_value = int(raw_value)
        db_user = DatabaseManager.get_user(user.id)
        if not db_user:
            await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
            return
        DatabaseManager.set_daily_goal(db_user['id'], goal_value)
        context.user_data.pop('awaiting_goal', None)
        has_active = DatabaseManager.get_active_shift(db_user['id']) is not None
        await update.message.reply_text(
            f"✅ Цель смены обновлена: {format_money(goal_value)}",
            reply_markup=create_main_reply_keyboard(has_active)
        )
        if has_active:
            await send_goal_status(update, context, db_user['id'])
        await send_period_reports_for_user(context.application, db_user)
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
        keyboard.append([InlineKeyboardButton("⬅️ К списку услуг", callback_data=f"back_to_services_{car_id}_{page}")])

        search_message_id = context.user_data.get("search_message_id")
        db_user = DatabaseManager.get_user(user.id)
        if not db_user:
            await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
            return
        if not new_name:
            await update.message.reply_text("Название не может быть пустым")
            return
        ok = DatabaseManager.update_combo_name(combo_id, db_user['id'], new_name)
        if ok:
            await update.message.reply_text(f"✅ Комбо переименовано: {new_name}")
        else:
            await update.message.reply_text("❌ Не удалось переименовать комбо")
        return

    # Обработка кнопок главного меню (reply клавиатура)
    if text in {
        MENU_OPEN_SHIFT,
        MENU_ADD_CAR,
        MENU_CURRENT_SHIFT,
        MENU_CLOSE_SHIFT,
        MENU_HISTORY,
        MENU_SETTINGS,
        MENU_LEADERBOARD,
        MENU_DECADE,
        MENU_STATS,
        MENU_FAQ,
        MENU_SUBSCRIPTION,
        MENU_PRICE,
        MENU_CALENDAR,
    }:
        if text == MENU_OPEN_SHIFT:
            await open_shift_message(update, context)
        elif text == MENU_ADD_CAR:
            await add_car_message(update, context)
        elif text == MENU_CURRENT_SHIFT:
            await current_shift_message(update, context)
        elif text == MENU_CLOSE_SHIFT:
            await close_shift_message(update, context)
        elif text == MENU_HISTORY:
            await history_message(update, context)
        elif text == MENU_SETTINGS:
            await settings_message(update, context)
        elif text == MENU_LEADERBOARD:
            await leaderboard_message(update, context)
        elif text == MENU_DECADE:
            await decade_message(update, context)
        elif text == MENU_STATS:
            await stats_message(update, context)
        elif text == MENU_FAQ:
            await faq_message(update, context)
        elif text == MENU_SUBSCRIPTION:
            await subscription_message(update, context)
        elif text == MENU_PRICE:
            await price_message(update, context)
        elif text == MENU_CALENDAR:
            await calendar_message(update, context)
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
    exact_handlers = getattr(dispatch_exact_callback, "_handlers", None)
    if exact_handlers is None:
        exact_handlers = {
        "open_shift": lambda: open_shift(query, context),
        "add_car": lambda: add_car(query, context),
        "current_shift": lambda: current_shift(query, context),
        "history_0": lambda: history(query, context),
        "settings": lambda: settings(query, context),
        "change_goal": lambda: change_goal(query, context),
        "leaderboard": lambda: leaderboard(query, context),
        "decade": lambda: decade_callback(query, context),
        "stats": lambda: stats_callback(query, context),
        "export_csv": lambda: export_csv(query, context),
        "backup_db": lambda: backup_db(query, context),
        "reset_data": lambda: reset_data(query, context),
        "toggle_price": lambda: toggle_price_mode(query, context),
        "combo_settings": lambda: combo_settings_menu(query, context),
        "combo_create_settings": lambda: combo_builder_start(query, context),
        "admin_panel": lambda: admin_panel(query, context),
        "admin_broadcast_menu": lambda: admin_broadcast_menu(query, context),
        "admin_broadcast_all": lambda: admin_broadcast_prepare(query, context, "all"),
        "admin_broadcast_expiring_1d": lambda: admin_broadcast_prepare(query, context, "expiring_1d"),
        "admin_broadcast_expired": lambda: admin_broadcast_prepare(query, context, "expired"),
        "admin_broadcast_pick_user": lambda: admin_broadcast_pick_user(query, context),
        "faq": lambda: faq_callback(query, context),
        "subscription_info": lambda: subscription_info_callback(query, context),
        "show_price": lambda: show_price_callback(query, context),
        "calendar_open": lambda: calendar_callback(query, context),
        "faq_start_demo": lambda: demo_start(query, context),
        "demo_step_shift": lambda: demo_step_shift_callback(query, context),
        "demo_step_services": lambda: demo_render_card(query, context, "services"),
        "demo_step_save": lambda: demo_render_card(query, context, "done"),
        "demo_exit": lambda: demo_exit_callback(query, context),
        "admin_faq_menu": lambda: admin_faq_menu(query, context),
        "admin_faq_set_text": lambda: admin_faq_set_text(query, context),
        "admin_faq_set_video": lambda: admin_faq_set_video(query, context),
        "admin_faq_preview": lambda: admin_faq_preview(query, context),
        "history_decades": lambda: history_decades(query, context),
        "back": lambda: go_back(query, context),
        "cancel_add_car": lambda: cancel_add_car_callback(query, context),
        "noop": lambda: query.answer(),
        }
        dispatch_exact_callback._handlers = exact_handlers

    handler = exact_handlers.get(data)
    if not handler:
        return False
    await handler()
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


async def cancel_add_car_callback(query, context):
    context.user_data.pop('awaiting_car_number', None)
    await query.edit_message_text("Ок, добавление машины отменено.")
    db_user = DatabaseManager.get_user(query.from_user.id)
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=main_menu_for_db_user(db_user)
    )


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
        ("export_decade_pdf_", export_decade_pdf),
        ("export_decade_xlsx_", export_decade_xlsx),
        ("export_month_xlsx_", export_month_xlsx_callback),
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
        ("calendar_set_", calendar_set_day_type_callback),
        ("calendar_back_month_", calendar_back_month_callback),
        ("demo_service_", demo_toggle_service_callback),
        ("history_decade_", history_decade_days),
        ("history_day_", history_day_cars),
        ("history_edit_car_", history_edit_car),
        ("cleanup_month_", cleanup_month),
        ("cleanup_day_", cleanup_day),
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
        "Выберите действие:",
        reply_markup=main_menu_for_db_user(db_user, True)
    )

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
            "Выберите действие:",
            reply_markup=create_main_reply_keyboard(False)
        )
        return
    
    context.user_data['awaiting_car_number'] = True
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
            "Выберите действие:",
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
            [InlineKeyboardButton("📋 Создать отчёт повторок", callback_data=f"shift_repeats_{shift_id}")],
            [InlineKeyboardButton("🔙 В меню", callback_data="back")],
        ]),
    )
    await query.message.reply_text(
        "Выберите действие:",
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
    per_page = 10
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
    users = DatabaseManager.get_all_users_with_stats()
    keyboard = []
    for row in users[:20]:
        status = "⛔" if int(row.get("is_blocked", 0)) else "✅"
        keyboard.append([InlineKeyboardButton(f"{status} {row['name']} ({row['telegram_id']})", callback_data=f"admin_user_{row['id']}")])
    keyboard.append([InlineKeyboardButton("📣 Рассылка", callback_data="admin_broadcast_menu")])
    keyboard.append([InlineKeyboardButton("❓ Редактировать FAQ", callback_data="admin_faq_menu")])
    keyboard.append([InlineKeyboardButton("🔙 В настройки", callback_data="settings")])
    await query.edit_message_text("🛡️ Админ-панель\nПользователи:", reply_markup=InlineKeyboardMarkup(keyboard))


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
        [InlineKeyboardButton("🔙 К пользователям", callback_data="admin_panel")],
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
    await query.edit_message_text("Выберите пользователя:", reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_broadcast_prepare(query, context, target: str):
    if not is_admin_telegram(query.from_user.id):
        return
    context.user_data["awaiting_admin_broadcast"] = target
    await query.edit_message_text(
        "Введите текст рассылки одним сообщением.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_broadcast_cancel")]])
    )


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
        await query.answer("Выберите 2 дня", show_alert=True)
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
    day_type_text = {
        "planned": "🔴 Основная смена",
        "extra": "🟡 Доп. смена",
        "off": "⚪ Выходной",
    }.get(day_type, "⚪ Выходной")

    month_key = day[:7]
    month_days = DatabaseManager.get_days_for_month(db_user["id"], month_key)
    has_day = any(row.get("day") == day and int(row.get("shifts_count", 0)) > 0 for row in month_days)

    text = (
        f"📅 Карточка дня: {day}\n"
        f"План: {day_type_text}\n"
        f"Факт: {'есть смены' if has_day else 'смен нет'}"
    )
    keyboard = []
    if has_day:
        keyboard.append([InlineKeyboardButton("📂 Открыть историю дня", callback_data=f"history_day_{day}")])
    keyboard.append([
        InlineKeyboardButton("🔴 Сделать рабочим", callback_data=f"calendar_set_planned_{day}"),
        InlineKeyboardButton("⚪ Сделать выходным", callback_data=f"calendar_set_off_{day}"),
    ])
    keyboard.append([InlineKeyboardButton("🟡 Сделать доп. сменой", callback_data=f"calendar_set_extra_{day}")])
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
        DatabaseManager.set_calendar_override(db_user["id"], day, "")
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

    month_key = day[:7]
    month_days = DatabaseManager.get_days_for_month(db_user["id"], month_key)
    has_day = any(row.get("day") == day and int(row.get("shifts_count", 0)) > 0 for row in month_days)
    if has_day:
        await history_day_cars(query, context, f"history_day_{day}")
        return

    await render_calendar_day_card(query, context, db_user, day)


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


async def subscription_info_callback(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    expires_at = subscription_expires_at_for_user(db_user)
    if is_admin_telegram(query.from_user.id):
        status = "♾️ Бессрочный доступ (админ)"
    elif is_subscription_active(db_user):
        status = f"✅ Подписка активна до {format_subscription_until(expires_at)}"
    else:
        status = "⛔ Подписка истекла"

    await query.edit_message_text(
        f"💳 Продление подписки\n\n"
        f"{status}\n"
        f"Стоимость: {SUBSCRIPTION_PRICE_TEXT}\n\n"
        f"Для продления напишите: {SUBSCRIPTION_CONTACT}",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
    )


async def send_faq(chat_target, context: CallbackContext):
    faq_text = DatabaseManager.get_app_content("faq_text", "")
    faq_video = DatabaseManager.get_app_content("faq_video_file_id", "")
    source_chat_id = DatabaseManager.get_app_content("faq_video_source_chat_id", "")
    source_message_id = DatabaseManager.get_app_content("faq_video_source_message_id", "")

    if faq_video:
        # Предпочитаем copy_message, чтобы пользователю приходило именно видео-сообщение,
        # а не ссылка или иной формат.
        if source_chat_id and source_message_id:
            try:
                await context.bot.copy_message(
                    chat_id=chat_target.chat_id,
                    from_chat_id=int(source_chat_id),
                    message_id=int(source_message_id),
                    caption=faq_text[:1024] if faq_text else None,
                )
                return
            except Exception:
                pass

        caption = faq_text[:1024] if faq_text else "📘 FAQ по боту"
        await context.bot.send_video(chat_id=chat_target.chat_id, video=faq_video, caption=caption)
        return

    if faq_text:
        await chat_target.reply_text(faq_text)
        return

    await chat_target.reply_text("FAQ пока не заполнен администратором.")


def create_faq_demo_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🚀 Запустить мини-демо", callback_data="faq_start_demo")]])


async def demo_render_card(query, context, step: str):
    payload = context.user_data.get("demo_payload", {"services": []})
    services = payload.get("services", [])

    if step == "start":
        text = """👋 Привет! Коротко покажу, как пользоваться ботом.

1) Открываешь смену.
2) Пока смена открыта — отправляешь номер ТС в любом формате.
3) Выбираешь услуги и сохраняешь машину.

Готов потренироваться?"""
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("1️⃣ Открыть смену (демо)", callback_data="demo_step_shift")],
            [InlineKeyboardButton("❌ Выход", callback_data="demo_exit")],
        ])
    elif step == "shift":
        text = """✅ Смена (демо) открыта.
Теперь отправь в чат номер ТС в любом виде.
Например: ХРУ340 или Х340РУ"""
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🧾 Ввёл номер, дальше", callback_data="demo_step_services")],
            [InlineKeyboardButton("❌ Выход", callback_data="demo_exit")],
        ])
        context.user_data["demo_waiting_car"] = True
    elif step == "services":
        total = sum(get_current_price(sid, "day") for sid in services)
        text = "🧪 Демо-услуги: нажми несколько услуг, потом сохрани.\n"
        text += f"Выбрано: {len(services)} | Сумма: {format_money(total)}"
        rows = []
        for sid in [1, 2, 3, 6]:
            mark = "✅" if sid in services else "▫️"
            rows.append([
                InlineKeyboardButton(
                    f"{mark} {plain_service_name(SERVICES[sid]['name'])}",
                    callback_data=f"demo_service_{sid}",
                )
            ])
        rows.append([InlineKeyboardButton("💾 Сохранить машину (демо)", callback_data="demo_step_save")])
        rows.append([InlineKeyboardButton("❌ Выход", callback_data="demo_exit")])
        kb = InlineKeyboardMarkup(rows)
    elif step == "done":
        total = sum(get_current_price(sid, "day") for sid in services)
        text = (
            "🎉 Готово! Ты прошёл мини-демо.\n\n"
            f"В демо выбрано услуг: {len(services)}\n"
            f"Сумма: {format_money(total)}\n\n"
            "Теперь можешь работать в реальной смене."
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К FAQ", callback_data="faq")],
            [InlineKeyboardButton("❌ Выход", callback_data="demo_exit")],
        ])
    else:
        text = "Демо завершено."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К FAQ", callback_data="faq")]])

    await query.edit_message_text(text, reply_markup=kb)


async def demo_start(query, context):
    context.user_data["demo_mode"] = True
    context.user_data["demo_payload"] = {"services": []}
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

    context.user_data["demo_waiting_car"] = False
    context.user_data["demo_payload"] = {"services": []}
    await update.message.reply_text(
        f"✅ Номер распознан: {normalized}\nОткрываю демо-выбор услуг.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🧪 Перейти к услугам (демо)", callback_data="demo_step_services")],
            [InlineKeyboardButton("❌ Выход", callback_data="demo_exit")],
        ]),
    )
    return True


async def faq_message(update: Update, context: CallbackContext):
    has_active = False
    db_user = DatabaseManager.get_user(update.effective_user.id)
    if db_user:
        has_active = DatabaseManager.get_active_shift(db_user['id']) is not None
    await send_faq(update.message, context)
    await update.message.reply_text("Можно потренироваться в мини-демо:", reply_markup=create_faq_demo_keyboard())
    await update.message.reply_text("Выберите действие:", reply_markup=create_main_reply_keyboard(has_active))


async def faq_callback(query, context):
    await send_faq(query.message, context)
    await query.message.reply_text("Можно потренироваться в мини-демо:", reply_markup=create_faq_demo_keyboard())


async def admin_faq_menu(query, context):
    if not is_admin_telegram(query.from_user.id):
        return
    keyboard = [
        [InlineKeyboardButton("✏️ Изменить текст FAQ", callback_data="admin_faq_set_text")],
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


async def history_decades(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    decades = DatabaseManager.get_decades_with_data(db_user["id"])
    if not decades:
        await query.edit_message_text("📜 История пуста")
        return
    keyboard = []
    message = "📜 История по декадам\n\n"
    for d in decades:
        title = format_decade_title(int(d["year"]), int(d["month"]), int(d["decade_index"]))
        message += f"• {title}: {format_money(int(d['total_amount']))} (машин: {d['cars_count']})\n"
        keyboard.append([InlineKeyboardButton(title, callback_data=f"history_decade_{d['year']}_{d['month']}_{d['decade_index']}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))


async def history_decade_days(query, context, data):
    _, _, year_s, month_s, decade_s = data.split("_")
    year = int(year_s)
    month = int(month_s)
    days = DatabaseManager.get_days_for_decade(db_user["id"], year, month, decade_index)
    title = format_decade_title(year, month, decade_index)
    total = sum(int(d["total_amount"] or 0) for d in days)
    message = f"📆 {title}\nИтого: {format_money(total)}\n\n"
    keyboard = []
    for d in days:
        day = d["day"]
        message += f"• {day}: {format_money(int(d['total_amount']))} (машин: {d['cars_count']})\n"
        keyboard.append([InlineKeyboardButton(f"{day} — {format_money(int(d['total_amount']))}", callback_data=f"history_day_{day}")])
    keyboard.append([InlineKeyboardButton("📄 Экспорт PDF", callback_data=f"export_decade_pdf_{year}_{month}_{decade_index}")])
    keyboard.append([InlineKeyboardButton("📊 Экспорт XLSX", callback_data=f"export_decade_xlsx_{year}_{month}_{decade_index}")])
    keyboard.append([InlineKeyboardButton("🔙 К декадам", callback_data="history_decades")])
    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))


async def history_day_cars(query, context, data):
    day = data.replace("history_day_", "")
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    cars = DatabaseManager.get_cars_for_day(db_user["id"], day)
    if not cars:
        await query.edit_message_text("Машин за день нет", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К декадам", callback_data="history_decades")]]))
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
    keyboard.append([InlineKeyboardButton("🔙 К декадам", callback_data="history_decades")])
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
    if len(parts) < 4:
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
        [InlineKeyboardButton("⬅️ К услугам", callback_data=f"back_to_services_{car_id}_{page}")],
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
            [InlineKeyboardButton("⬅️ К услугам", callback_data=f"back_to_services_{car_id}_{page}")],
        ])
    )


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

    keyboard.append([InlineKeyboardButton("➕ Новый комбо", callback_data=f"combo_create_{car_id}_{page}")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"back_to_services_{car_id}_{page}")])

    await query.edit_message_text("🧩 Выберите комбо:", reply_markup=InlineKeyboardMarkup(keyboard))


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

async def save_car(query, context, data):
    """Сохранение машины"""
    parts = data.split('_')
    if len(parts) < 2:
        return
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
        await query.message.reply_text(
            "Выберите действие:",
            reply_markup=create_main_reply_keyboard(True)
        )
        return
    
    await query.edit_message_text(
        f"✅ Машина {car['car_number']} сохранена!\n"
        f"Сумма: {format_money(car['total_amount'])}\n\n"
        f"Можете добавить следующую машину."
    )
    context.user_data.pop(f"edit_mode_{car_id}", None)
    context.user_data.pop(f"history_day_for_car_{car_id}", None)
    db_user = DatabaseManager.get_user(query.from_user.id)
    if db_user:
        await send_goal_status(None, context, db_user['id'], source_message=query.message)
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=create_main_reply_keyboard(True)
    )

async def close_shift(query, context, data):
    """Старая точка входа: теперь только подтверждение"""
    await close_shift_confirm_prompt(query, context, data)


async def close_shift_confirm_prompt(query, context, data):
    parts = data.split('_')
    if len(parts) < 2:
        return

    shift_id = int(parts[1])
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await query.edit_message_text("❌ Ошибка: пользователь не найден")
        return

    shift = DatabaseManager.get_shift(shift_id)
    if not shift or shift['user_id'] != db_user['id']:
        await query.edit_message_text("❌ Смена не найдена")
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Закрыть смену", callback_data=f"close_shift_confirm_yes_{shift_id}")],
        [InlineKeyboardButton("❌ Отмена", callback_data="back")],
    ])
    await query.edit_message_text("Закрыть смену?", reply_markup=keyboard)


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
            [InlineKeyboardButton("📋 Создать отчёт повторок", callback_data=f"shift_repeats_{shift_id}")],
            [InlineKeyboardButton("🔙 В меню", callback_data="back")],
        ]),
    )
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=create_main_reply_keyboard(False)
    )


async def close_shift_confirm_no(query, context):
    await query.edit_message_text("Ок, смена остаётся открытой ✅")
    await query.message.reply_text(
        "Выберите действие:",
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

    await query.edit_message_text("Ок, возвращаюсь в меню.")
    await query.message.reply_text(
        "Выберите действие:",
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

async def leaderboard(query, context):
    """Топ героев: лидеры декады и активной смены"""
    today = now_local().date()
    idx, _, _, _, decade_title = get_decade_period(today)
    decade_leaders = DatabaseManager.get_decade_leaderboard(today.year, today.month, idx)
    active_leaders = DatabaseManager.get_active_leaderboard()

    message = "🏆 ТОП ГЕРОЕВ\n\n"
    message += f"📆 Лидеры декады ({decade_title}):\n"
    if decade_leaders:
        for place, leader in enumerate(decade_leaders, start=1):
            message += f"{place}. {leader['name']} — {format_money(leader['total_amount'])} (смен: {leader['shift_count']})\n"
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=create_main_reply_keyboard(True)
    )

async def reset_data(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    DatabaseManager.reset_user_data(db_user['id'])
    context.user_data.clear()
    await query.edit_message_text("✅ Все ваши данные сброшены: смены, машины, услуги, комбо и цель дня.")
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=create_main_reply_keyboard(False)
    )

async def open_shift_message(update: Update, context: CallbackContext):
    user = update.effective_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await update.message.reply_text("❌ Ошибка: пользователь не найден")
        return

    _, message, _ = open_shift_core(db_user)
    await update.message.reply_text(
        message,
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
        "Введите номер машины:\n\n"
        "Примеры:\n"
        "• А123ВС777\n"
        "• Х340РУ797\n"
        "• В567ТХ799\n\n"
        "Можно вводить русскими или английскими буквами."
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

    await update.message.reply_text(
        "📜 История теперь по декадам. Выберите нужную декаду:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📆 Открыть декады", callback_data="history_decades")], [InlineKeyboardButton("🔙 Назад", callback_data="back")]])
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
    active_leaders = DatabaseManager.get_active_leaderboard()

    message = "🏆 ТОП ГЕРОЕВ\n\n"
    message += f"📆 Лидеры декады ({decade_title}):\n"
    if decade_leaders:
        for place, leader in enumerate(decade_leaders, start=1):
            message += f"{place}. {leader['name']} — {format_money(leader['total_amount'])} (смен: {leader['shift_count']})\n"
    else:
        message += "Пока нет данных за декаду.\n"

    message += "\n⚡ Лидеры смены (активные):\n"
    if active_leaders:
        for place, leader in enumerate(active_leaders, start=1):
            message += f"{place}. {leader['name']} — {format_money(leader['total_amount'])} (смен: {leader['shift_count']})\n"
    else:
        message += "Пока нет активных смен."

    db_user = DatabaseManager.get_user(update.effective_user.id)
    has_active = bool(db_user and DatabaseManager.get_active_shift(db_user['id']))
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
            history_day,
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
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📄 Экспорт PDF", callback_data=f"export_decade_pdf_{year}_{month}_{idx}")],
        [InlineKeyboardButton("📊 Экспорт XLSX", callback_data=f"export_decade_xlsx_{year}_{month}_{idx}")],
    ])
    try:
        await application.bot.send_message(
            chat_id=db_user["telegram_id"],
            text="🔔 Декада завершилась!\n\n" + text,
            parse_mode="HTML",
            reply_markup=keyboard,
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
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Экспорт месяца XLSX", callback_data=f"export_month_xlsx_{prev_day.year}_{prev_day.month}")],
    ])
    try:
        await application.bot.send_message(
            chat_id=db_user["telegram_id"],
            text="🗓 Месяц завершён!\n\n" + text,
            parse_mode="HTML",
            reply_markup=keyboard,
        )
    except Exception as exc:
        logger.warning(f"Не удалось отправить месячный отчёт {db_user['telegram_id']}: {exc}")
    finally:
        DatabaseManager.set_app_content(sent_key, month_key)


async def send_period_reports_for_user(application: Application, db_user: dict):
    await notify_decade_change_if_needed(application, db_user)
    await notify_month_end_if_needed(application, db_user)


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
    await query.edit_message_text("🧹 Очистка данных временно недоступна в этой версии.")


async def cleanup_month(query, context, data):
    await query.edit_message_text("🧹 Очистка по месяцу временно недоступна.")


async def delete_day_prompt(query, context, data):
    await query.edit_message_text("🧹 Удаление дня временно недоступно.")


async def delete_day_callback(query, context, data):
    await query.edit_message_text("🧹 Удаление дня временно недоступно.")



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

    rollout_done = DatabaseManager.get_app_content("trial_rollout_done", "")
    if rollout_done == APP_VERSION:
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


# ========== ГЛАВНАЯ ФУНКЦИЯ ==========

def main():
    """Запуск бота"""
    application = Application.builder().token(BOT_TOKEN).post_init(on_startup).build()
    
    # Регистрация команд
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("menu", menu_command))
    
    # Обработчик callback-кнопок
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    # Обработчик текстовых сообщений
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
