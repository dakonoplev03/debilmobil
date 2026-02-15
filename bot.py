"""
🤖 БОТ ДЛЯ УЧЁТА УСЛУГ 
"""

import logging
from datetime import datetime, date
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
from exports import create_decade_pdf, create_decade_xlsx

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
APP_VERSION = "2026.02.11-hotfix-5"
APP_UPDATED_AT = "2026-02-11 22:40 (Europe/Moscow)"
APP_TIMEZONE = "Europe/Moscow"
LOCAL_TZ = ZoneInfo(APP_TIMEZONE)
ADMIN_TELEGRAM_IDS = {8379101989}

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


def get_price_mode(context: CallbackContext, user_id: int | None = None) -> str:
    mode = context.user_data.get("price_mode")
    if mode in {"day", "night"}:
        return mode
    if user_id:
        mode = DatabaseManager.get_price_mode(user_id)
        context.user_data["price_mode"] = mode
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


def build_short_goal_line(user_id: int) -> str:
    goal = DatabaseManager.get_daily_goal(user_id)
    if goal <= 0:
        return "🎯 Цель не задана"
    today_total = DatabaseManager.get_user_total_for_date(user_id, now_local().strftime("%Y-%m-%d"))
    percent = min(int((today_total / goal) * 100) if goal else 0, 100)
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

def create_main_reply_keyboard(has_active_shift: bool = False) -> ReplyKeyboardMarkup:
    """Главное меню под полем ввода"""
    keyboard = []

    if has_active_shift:
        keyboard.append([KeyboardButton(MENU_ADD_CAR), KeyboardButton(MENU_CURRENT_SHIFT)])
        keyboard.append([KeyboardButton(MENU_CLOSE_SHIFT)])
    else:
        keyboard.append([KeyboardButton(MENU_OPEN_SHIFT)])

    keyboard.append([KeyboardButton(MENU_HISTORY), KeyboardButton(MENU_LEADERBOARD)])
    keyboard.append([KeyboardButton(MENU_DECADE), KeyboardButton(MENU_STATS)])
    keyboard.append([KeyboardButton(MENU_SETTINGS)])

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
    keyboard.append([InlineKeyboardButton("🔎 Поиск", callback_data=f"service_search_{car_id}_{page}")])
    if user_id:
        combos = DatabaseManager.get_user_combos(user_id)
        for combo in combos[:5]:
            keyboard.append([InlineKeyboardButton(f"🧩 {combo['name']}", callback_data=f"combo_apply_{combo['id']}_{car_id}_{page}")])
    keyboard.append([InlineKeyboardButton(f"🔁 Изменить прайс: {mode_label}", callback_data=f"toggle_price_car_{car_id}_{page}")])

    edit_text = "✅ Готово" if is_edit_mode else "✏️ Изменить"
    keyboard.append([
        InlineKeyboardButton(edit_text, callback_data=f"toggle_edit_{car_id}_{page}"),
        InlineKeyboardButton("🗑️ Очистить", callback_data=f"clear_{car_id}_{page}"),
        InlineKeyboardButton("💾 Сохранить", callback_data=f"save_{car_id}"),
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
    goal = DatabaseManager.get_daily_goal(user_id)
    percent = min(int((total / goal) * 100), 100) if goal > 0 else 0
    goal_line = (
        f"🎯 Цель: {format_money(total)}/{format_money(goal)} {percent}% {render_bar(percent, 8)}"
        if goal > 0 else "🎯 Цель дня не задана"
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

    top_services = DatabaseManager.get_shift_top_services(shift["id"], limit=3)
    top_block = ""
    if top_services:
        top_rows = [
            f"• {plain_service_name(item['service_name'])} — {format_money(int(item['total_amount']))}"
            for item in top_services
        ]
        top_block = "\n🏆 Лучшие услуги смены:\n" + "\n".join(top_rows)

    return (
        f"🎉 <b>Смена закрыта!</b> {stars}\n\n"
        f"💰 Выручка: <b>{format_money(total)}</b>\n"
        f"🧾 Налог 6%: {format_money(tax)}\n"
        f"✅ К выплате: <b>{format_money(net)}</b>\n"
        f"⏱ Длительность: {metrics['hours']:.1f} ч\n"
        f"🚗 Машин: {metrics['cars_count']}\n"
        f"📈 Средний чек: {format_money(metrics['avg_check'])}\n"
        f"⚡ Машин/час: {metrics['cars_per_hour']:.2f}\n"
        f"💸 Доход/час: {format_money(int(metrics['money_per_hour']))}{top_block}"
    )

def get_goal_text(user_id: int) -> str:
    if not DatabaseManager.get_active_shift(user_id):
        return "🎯 Цель дня доступна только при открытой смене."

    goal = DatabaseManager.get_daily_goal(user_id)
    if goal <= 0:
        return "🎯 Укажи денежную цель смены."

    today_total = DatabaseManager.get_user_total_for_date(user_id, now_local().strftime("%Y-%m-%d"))
    percent = min(int((today_total / goal) * 100) if goal else 0, 100)
    filled = min(percent // 10, 10)
    bar = "🟩" * filled + "⬜" * (10 - filled)
    return (
        f"🎯 Цель дня: {format_money(goal)}\n"
        f"Сделано: {format_money(today_total)} ({percent}%)\n"
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
        if idx > current_decade:
            continue
        total = DatabaseManager.get_user_total_between_dates(user_id, start_d.isoformat(), end_d.isoformat())
        row = f"{idx}-я декада {MONTH_NAMES[month]}: {format_money(total)}"
        lines.append(f"<b>{row}</b>" if idx == current_decade else row)

    return "\n".join(lines)


def build_stats_summary(user_id: int) -> str:
    services = DatabaseManager.get_service_stats(user_id)
    cars = DatabaseManager.get_car_stats(user_id)

    message = "📈 СТАТИСТИКА\n\n"
    if services:
        message += "Топ услуг:\n"
        for item in services:
            message += (
                f"• {item['service_name']} — {item['total_count']} шт. "
                f"({format_money(item['total_amount'])})\n"
            )
    else:
        message += "Топ услуг: пока нет данных.\n"

    message += "\n"
    if cars:
        message += "Топ машин:\n"
        for item in cars:
            message += (
                f"• {item['car_number']} — {item['visits']} раз "
                f"({format_money(item['total_amount'])})\n"
            )
    else:
        message += "Топ машин: пока нет данных.\n"

    return message

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

async def send_goal_status(update: Update, context: CallbackContext, user_id: int):
    """Отправить и попытаться закрепить цель дня"""
    goal_text = get_goal_text(user_id)
    if update.message:
        message = await update.message.reply_text(goal_text)
    elif update.callback_query and update.callback_query.message:
        message = await update.callback_query.message.reply_text(goal_text)
    else:
        return

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
        # Автоматическая регистрация
        db_user = DatabaseManager.get_user(user.id)
        context.user_data["price_mode"] = DatabaseManager.get_price_mode(db_user["id"]) if db_user else "day"
        
        if not db_user:
            name = user.first_name or user.username or "Пользователь"
            DatabaseManager.register_user(user.id, name)
            db_user = DatabaseManager.get_user(user.id)

        if not db_user:
            await update.message.reply_text("❌ Не удалось зарегистрировать пользователя. Повторите /start")
            return
        if is_user_blocked(db_user):
            await update.message.reply_text("⛔ Доступ к боту закрыт администратором.")
            return

        # Простое приветствие
        has_active = False
        if db_user:
            has_active = DatabaseManager.get_active_shift(db_user['id']) is not None

        await update.message.reply_text(
            f"👋 Привет!\n"
            f"Я бот для учёта выполненных услуг.\n\n"
            f"Версия: {APP_VERSION}\n"
            f"Выберите действие:",
            reply_markup=create_main_reply_keyboard(has_active)
        )
        if has_active:
            await send_goal_status(update, context, db_user['id'])
        await notify_decade_change_if_needed(update, context, db_user['id'])

async def menu_command(update: Update, context: CallbackContext):
    user = update.effective_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
        return
    if is_user_blocked(db_user):
        await update.message.reply_text("⛔ Доступ к боту закрыт администратором.")
        return
    has_active = DatabaseManager.get_active_shift(db_user['id']) is not None
    await update.message.reply_text(
        "Меню открыто.",
        reply_markup=create_main_reply_keyboard(has_active)
    )
    context.user_data["price_mode"] = DatabaseManager.get_price_mode(db_user["id"])
    await notify_decade_change_if_needed(update, context, db_user["id"])

async def handle_message(update: Update, context: CallbackContext):
    """Обработка текстовых сообщений"""
    user = update.effective_user
    text = update.message.text.strip()
    db_user_for_access = DatabaseManager.get_user(user.id)
    if is_user_blocked(db_user_for_access):
        await update.message.reply_text("⛔ Доступ к боту закрыт администратором.")
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
                "Выберите действие:",
                reply_markup=create_main_reply_keyboard(False)
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
        await send_goal_status(update, context, db_user["id"])
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
            f"✅ Цель дня обновлена: {format_money(goal_value)}\n\n{get_goal_text(db_user['id'])}",
            reply_markup=create_main_reply_keyboard(has_active)
        )
        if has_active:
            await send_goal_status(update, context, db_user['id'])
        await notify_decade_change_if_needed(update, context, db_user['id'])
        return

    if context.user_data.get('awaiting_service_search'):
        query_text = text.lower().strip()
        payload = context.user_data.pop('awaiting_service_search')
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
        search_chat_id = context.user_data.get("search_chat_id")
        if search_message_id and search_chat_id:
            try:
                await context.bot.edit_message_text(
                    chat_id=search_chat_id,
                    message_id=search_message_id,
                    text="Результаты поиска:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )
                return
            except Exception:
                pass

        await update.message.reply_text("Результаты поиска:", reply_markup=InlineKeyboardMarkup(keyboard))
        return


    if context.user_data.get('awaiting_combo_name'):
        combo_name = text.strip()
        payload = context.user_data.pop('awaiting_combo_name')
        service_ids = payload.get("service_ids", [])
        car_id = payload.get("car_id")
        page = payload.get("page", 0)
        db_user = DatabaseManager.get_user(user.id)
        if not db_user:
            await update.message.reply_text("❌ Пользователь не найден. Напишите /start")
            return
        if not combo_name:
            await update.message.reply_text("Название не может быть пустым")
            return
        if not service_ids:
            await update.message.reply_text("В этой машине нет услуг для сохранения комбо.")
            return

        DatabaseManager.save_user_combo(db_user['id'], combo_name, service_ids)
        await update.message.reply_text(f"✅ Комбо «{combo_name}» сохранено.")
        if car_id:
            await update.message.reply_text(
                "Возвращаю список услуг:",
                reply_markup=create_services_keyboard(
                    car_id,
                    page,
                    get_edit_mode(context, car_id),
                    get_price_mode(context, db_user['id']),
                    db_user['id'],
                ),
            )
        return

    if context.user_data.get('awaiting_combo_rename'):
        new_name = text.strip()
        combo_id = context.user_data.pop('awaiting_combo_rename')
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
                reply_markup=create_services_keyboard(
                    car_id,
                    page,
                    get_edit_mode(context, car_id),
                    get_price_mode(context, db_user["id"] if db_user else None),
                    db_user["id"] if db_user else None,
                )
            )
        return
    
    await update.message.reply_text(
        "Используйте кнопки меню для работы с ботом.\n"
        "Напишите /start для начала."
    )

# ========== ОБРАБОТЧИКИ КНОПОК ==========

async def handle_callback(update: Update, context: CallbackContext):
    """Главный обработчик callback-кнопок"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user = query.from_user
    
    logger.info(f"Callback: {data} from {user.id}")

    db_user_access = DatabaseManager.get_user(user.id)
    if is_user_blocked(db_user_access):
        await query.edit_message_text("⛔ Доступ к боту закрыт администратором.")
        return

    # Основные действия
    if data == "open_shift":
        await open_shift(query, context)
    elif data == "add_car":
        await add_car(query, context)
    elif data == "current_shift":
        await current_shift(query, context)
    elif data == "history_0":
        await history(query, context)
    elif data == "settings":
        await settings(query, context)
    elif data.startswith("service_page_"):
        await change_services_page(query, context, data)
    elif data.startswith("toggle_price_car_"):
        await toggle_price_mode_for_car(query, context, data)
    elif data.startswith("service_search_"):
        await start_service_search(query, context, data)
    elif data.startswith("search_text_"):
        await search_enter_text_mode(query, context, data)
    elif data.startswith("combo_menu_"):
        await show_combo_menu(query, context, data)
    elif data.startswith("combo_apply_"):
        await apply_combo_to_car(query, context, data)
    elif data.startswith("combo_save_from_car_"):
        await save_combo_from_car(query, context, data)
    elif data.startswith("combo_delete_prompt_"):
        await delete_combo_prompt(query, context, data)
    elif data.startswith("combo_delete_confirm_"):
        await delete_combo(query, context, data)
    elif data.startswith("combo_edit_"):
        await combo_edit_menu(query, context, data)
    elif data.startswith("combo_rename_"):
        await combo_start_rename(query, context, data)
    elif data.startswith("childsvc_"):
        await add_group_child_service(query, context, data)
    elif data.startswith("back_to_services_"):
        await back_to_services(query, context, data)
    elif data.startswith("service_"):
        await add_service(query, context, data)
    elif data.startswith("clear_"):
        await clear_services_prompt(query, context, data)
    elif data.startswith("confirm_clear_"):
        await clear_services(query, context, data)
    elif data.startswith("save_"):
        await save_car(query, context, data)
    elif data == "change_goal":
        await change_goal(query, context)
    elif data == "leaderboard":
        await leaderboard(query, context)
    elif data == "decade":
        await decade_callback(query, context)
    elif data == "stats":
        await stats_callback(query, context)
    elif data == "export_csv":
        await export_csv(query, context)
    elif data.startswith("export_decade_pdf_"):
        await export_decade_pdf(query, context, data)
    elif data.startswith("export_decade_xlsx_"):
        await export_decade_xlsx(query, context, data)
    elif data == "backup_db":
        await backup_db(query, context)
    elif data == "reset_data":
        await reset_data(query, context)
    elif data == "toggle_price":
        await toggle_price_mode(query, context)
    elif data == "cleanup_data":
        await cleanup_data_menu(query, context)
    elif data == "combo_settings":
        await combo_settings_menu(query, context)
    elif data == "combo_create_settings":
        await combo_builder_start(query, context)
    elif data.startswith("combo_builder_toggle_"):
        await combo_builder_toggle(query, context, data)
    elif data == "combo_builder_save":
        await combo_builder_save(query, context)
    elif data == "admin_panel":
        await admin_panel(query, context)
    elif data.startswith("admin_user_"):
        await admin_user_card(query, context, data)
    elif data.startswith("admin_toggle_block_"):
        await admin_toggle_block(query, context, data)
    elif data == "history_decades":
        await history_decades(query, context)
    elif data.startswith("history_decade_"):
        await history_decade_days(query, context, data)
    elif data.startswith("history_day_"):
        await history_day_cars(query, context, data)
    elif data.startswith("cleanup_month_"):
        await cleanup_month(query, context, data)
    elif data.startswith("cleanup_day_"):
        await cleanup_day(query, context, data)
    elif data.startswith("delcar_"):
        await delete_car_callback(query, context, data)
    elif data.startswith("delday_prompt_"):
        await delete_day_prompt(query, context, data)
    elif data.startswith("delday_confirm_"):
        await delete_day_callback(query, context, data)
    elif data.startswith("toggle_edit_"):
        await toggle_edit(query, context, data)
    elif data == "noop":
        return
    elif data.startswith("close_confirm_yes_"):
        await close_shift_confirm_yes(query, context, data)
    elif data.startswith("close_confirm_no_"):
        await close_shift_confirm_no(query, context)
    elif data.startswith("close_"):
        await close_shift_confirm_prompt(query, context, data)
    elif data == "back":
        await go_back(query, context)
    elif data == "cancel_add_car":
        context.user_data.pop('awaiting_car_number', None)
        await query.edit_message_text("Ок, добавление машины отменено.")
        db_user = DatabaseManager.get_user(user.id)
        has_active = bool(db_user and DatabaseManager.get_active_shift(db_user['id']))
        await query.message.reply_text(
            "Выберите действие:",
            reply_markup=create_main_reply_keyboard(has_active)
        )
    else:
        await query.edit_message_text("❌ Неизвестная команда")

async def open_shift(query, context):
    """Открытие смены"""
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    
    if not db_user:
        await query.edit_message_text("❌ Ошибка: пользователь не найден")
        return
    
    # Проверяем активную смену
    active_shift = DatabaseManager.get_active_shift(db_user['id'])
    if active_shift:
        start_time = parse_datetime(active_shift['start_time'])
        time_text = start_time.strftime('%H:%M %d.%m') if start_time else "неизвестно"
        await query.edit_message_text(
            f"❌ У вас уже есть активная смена!\n"
            f"Начата: {time_text}"
        )
        await query.message.reply_text(
            "Выберите действие:",
            reply_markup=create_main_reply_keyboard(True)
        )
        return
    
    # Создаём новую смену
    DatabaseManager.start_shift(db_user['id'])
    
    await query.edit_message_text(
        f"✅ Смена открыта!\n"
        f"Время: {now_local().strftime('%H:%M %d.%m.%Y')}\n\n"
        f"Теперь можно добавлять машины."
    )
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=create_main_reply_keyboard(True)
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
    
    await query.edit_message_text(
        "Введите номер машины:\n\n"
        "Пример:\n"
        "• А123ВС777\n"
        "• х340ру\n\n"
        "Можно вводить русскими или английскими буквами, можно даже маленькими. Если регион не указан, по умолчанию будет 797.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Отмена", callback_data="cancel_add_car")]]
        )
    )

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

    await query.edit_message_text(message, parse_mode="HTML")
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=create_main_reply_keyboard(True)
    )

async def history(query, context):
    await history_decades(query, context)


async def settings(query, context):
    """Настройки"""
    db_user = DatabaseManager.get_user(query.from_user.id)
    has_active_shift = bool(db_user and DatabaseManager.get_active_shift(db_user['id']))
    keyboard = [
        *([[InlineKeyboardButton("🎯 Цель дня", callback_data="change_goal")]] if has_active_shift else []),
        [InlineKeyboardButton("📆 Зарплата (декады)", callback_data="decade")],
        [InlineKeyboardButton("📈 Статистика", callback_data="stats")],
        [InlineKeyboardButton("📜 История по декадам", callback_data="history_decades")],
        [InlineKeyboardButton("📤 Экспорт CSV", callback_data="export_csv")],
        [InlineKeyboardButton("🗄️ Резервная копия", callback_data="backup_db")],
        [InlineKeyboardButton("🧩 Мои комбинации", callback_data="combo_settings")],
        [InlineKeyboardButton("➕ Создать комбо", callback_data="combo_create_settings")],
        [InlineKeyboardButton("🧹 Редактировать данные", callback_data="cleanup_data")],
        [InlineKeyboardButton("🗑️ Сбросить данные", callback_data="reset_data")],
    ]
    if is_admin_telegram(query.from_user.id):
        keyboard.append([InlineKeyboardButton("🛡️ Админ-панель", callback_data="admin_panel")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])

    await query.edit_message_text(
        f"⚙️ НАСТРОЙКИ\n\nВерсия: {APP_VERSION}\nОбновлено: {APP_UPDATED_AT}\n\nВыберите параметр:",
        reply_markup=InlineKeyboardMarkup(keyboard)
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
    keyboard = [
        [InlineKeyboardButton("🔓 Открыть доступ" if blocked else "⛔ Закрыть доступ", callback_data=f"admin_toggle_block_{user_id}")],
        [InlineKeyboardButton("🔙 К пользователям", callback_data="admin_panel")],
    ]
    await query.edit_message_text(
        f"👤 {row['name']}\nTelegram ID: {row['telegram_id']}\n"
        f"Смен: {row['shifts_count']}\nСумма: {format_money(int(row['total_amount'] or 0))}\n"
        f"Статус: {'Заблокирован' if blocked else 'Активен'}",
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
    decade_index = int(decade_s)
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
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
    for car in cars:
        message += f"• #{car['id']} {car['car_number']} — {format_money(int(car['total_amount']))}\n"
    keyboard = [[InlineKeyboardButton("🧹 Редактировать этот день", callback_data=f"cleanup_day_{day}")], [InlineKeyboardButton("🔙 К декадам", callback_data="history_decades")]]
    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))

async def add_service(query, context, data):
    """Добавление услуги"""
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

    price = get_current_price(service_id, get_price_mode(context))

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
    mode = get_price_mode(context)
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
        price = get_current_price(service_id, get_price_mode(context))
        DatabaseManager.add_service_to_car(car_id, service_id, plain_service_name(service['name']), price)

    await show_car_services(query, context, car_id, page)


async def back_to_services(query, context, data):
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
    context.user_data["price_mode"] = new_mode
    DatabaseManager.set_price_mode(db_user['id'], new_mode)
    await show_car_services(query, context, car_id, page)


async def start_service_search(query, context, data):
    parts = data.split('_')
    if len(parts) < 4:
        return
    car_id = int(parts[2])
    page = int(parts[3])

    keyboard = [
        [InlineKeyboardButton("✍️ Введите текст", callback_data=f"search_text_{car_id}_{page}")],
        [InlineKeyboardButton("⬅️ К услугам", callback_data=f"back_to_services_{car_id}_{page}")],
    ]

    context.user_data["search_message_id"] = query.message.message_id
    context.user_data["search_chat_id"] = query.message.chat_id
    await query.edit_message_text(
        "🔎 Поиск услуг\n\n"
        "Нажмите «Введите текст» и отправьте часть названия услуги.",
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
    await query.answer("Введите текст в чат")


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
            InlineKeyboardButton(
                "🗑️",
                callback_data=f"combo_delete_prompt_{combo['id']}_{car_id}_{page}",
            ),
        ])

    keyboard.append([
        InlineKeyboardButton(
            "💾 Сохранить текущее как комбо",
            callback_data=f"combo_save_from_car_{car_id}_{page}",
        )
    ])
    keyboard.append([InlineKeyboardButton("⬅️ К услугам", callback_data=f"back_to_services_{car_id}_{page}")])

    text = "🧩 Комбинации услуг\n\nВыберите комбо для применения или сохраните текущее."
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def combo_edit_menu(query, context, data):
    parts = data.split('_')
    if len(parts) < 5:
        return
    combo_id = int(parts[2])
    car_id = int(parts[3])
    page = int(parts[4])

    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    combo = DatabaseManager.get_combo(combo_id, db_user['id'])
    if not combo:
        await query.answer("Комбо не найдено")
        return

    services = []
    for sid in combo.get("service_ids", []):
        service = SERVICES.get(int(sid))
        if service:
            services.append(plain_service_name(service['name']))
    services_preview = ", ".join(services[:8]) if services else "нет услуг"

    text = (
        f"🧩 Редактор комбо\n\n"
        f"Название: {combo['name']}\n"
        f"Услуг: {len(combo.get('service_ids', []))}\n"
        f"Состав: {services_preview}"
    )
    keyboard = [
        [InlineKeyboardButton("✏️ Переименовать", callback_data=f"combo_rename_{combo_id}_{car_id}_{page}")],
        [InlineKeyboardButton("🗑️ Удалить", callback_data=f"combo_delete_prompt_{combo_id}_{car_id}_{page}")],
        [InlineKeyboardButton("⬅️ Назад к комбо", callback_data=f"combo_menu_{car_id}_{page}")],
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def combo_start_rename(query, context, data):
    parts = data.split('_')
    if len(parts) < 5:
        return
    combo_id = int(parts[2])
    context.user_data['awaiting_combo_rename'] = combo_id
    await query.answer("Введите новое название в чат")


async def apply_combo_to_car(query, context, data):
    parts = data.split('_')
    if len(parts) < 5:
        return
    combo_id = int(parts[2])
    car_id = int(parts[3])
    page = int(parts[4])

    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return
    combo = DatabaseManager.get_combo(combo_id, db_user['id'])
    if not combo:
        await query.answer("Комбо не найдено")
        return

    mode = get_price_mode(context, db_user['id'])
    added = 0
    skipped = 0
    for service_id in combo.get("service_ids", []):
        service = SERVICES.get(int(service_id))
        if not service:
            skipped += 1
            continue
        if service.get("kind") == "distance":
            skipped += 1
            continue
        price = get_current_price(int(service_id), mode)
        DatabaseManager.add_service_to_car(car_id, int(service_id), plain_service_name(service['name']), price)
        added += 1

    await query.answer(f"Добавлено: {added}, пропущено: {skipped}")
    await show_car_services(query, context, car_id, page)


async def save_combo_from_car(query, context, data):
    parts = data.split('_')
    if len(parts) < 6:
        return
    car_id = int(parts[4])
    page = int(parts[5])

    services = DatabaseManager.get_car_services(car_id)
    service_ids = []
    for item in services:
        qty = int(item.get("quantity", 1))
        service_ids.extend([int(item["service_id"])] * max(1, qty))

    context.user_data['awaiting_combo_name'] = {
        "service_ids": service_ids,
        "car_id": car_id,
        "page": page,
    }
    await query.message.reply_text("Введите название для новой комбинации услуг")


async def delete_combo_prompt(query, context, data):
    parts = data.split('_')
    if len(parts) < 6:
        return
    combo_id = int(parts[3])
    car_id = int(parts[4])
    page = int(parts[5])
    keyboard = [
        [InlineKeyboardButton("✅ Да, удалить", callback_data=f"combo_delete_confirm_{combo_id}_{car_id}_{page}")],
        [InlineKeyboardButton("⬅️ Отмена", callback_data=f"combo_menu_{car_id}_{page}")],
    ]
    await query.edit_message_text("Подтвердите удаление комбо", reply_markup=InlineKeyboardMarkup(keyboard))


async def delete_combo(query, context, data):
    parts = data.split('_')
    if len(parts) < 6:
        return
    combo_id = int(parts[3])
    car_id = int(parts[4])
    page = int(parts[5])

    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        return

    ok = DatabaseManager.delete_combo(combo_id, db_user['id'])
    await query.answer("Удалено" if ok else "Не найдено")
    await show_combo_menu(query, context, f"combo_menu_{car_id}_{page}")


async def combo_settings_menu(query, context):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    combos = DatabaseManager.get_user_combos(db_user['id'])
    text = "🧩 Мои комбинации\n\n"
    keyboard = []
    if not combos:
        text += "Пока нет сохранённых комбинаций.\nСоздайте первое через кнопку «➕ Создать комбо» в настройках."
    else:
        for combo in combos[:10]:
            text += f"• {combo['name']} ({len(combo.get('service_ids', []))} услуг)\n"
    keyboard.append([InlineKeyboardButton("➕ Создать комбо", callback_data="combo_create_settings")])
    keyboard.append([InlineKeyboardButton("🔙 В настройки", callback_data="settings")])

    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


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
    
    car_id = int(parts[1])
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
    db_user = DatabaseManager.get_user(query.from_user.id)
    if db_user:
        await query.message.reply_text(get_goal_text(db_user['id']))
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

    if shift['status'] != 'active':
        await query.edit_message_text("ℹ️ Эта смена уже закрыта.")
        return

    cars = DatabaseManager.get_shift_cars(shift_id)
    total = DatabaseManager.get_shift_total(shift_id)
    dashboard = build_current_shift_dashboard(db_user['id'], shift, cars, total)

    keyboard = [
        [InlineKeyboardButton("✅ Да, закрыть", callback_data=f"close_confirm_yes_{shift_id}")],
        [InlineKeyboardButton("❌ Нет, оставить открытой", callback_data=f"close_confirm_no_{shift_id}")],
    ]
    await query.edit_message_text(
        dashboard + "\n\n⚠️ Вы точно хотите закрыть смену?",
        parse_mode="HTML",
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
    DatabaseManager.set_daily_goal(db_user['id'], 0)
    DatabaseManager.set_daily_goal(db_user['id'], 0)
    closed_shift = DatabaseManager.get_shift(shift_id) or shift
    cars = DatabaseManager.get_shift_cars(shift_id)
    message = build_closed_shift_dashboard(closed_shift, cars, total)

    await query.edit_message_text(message, parse_mode="HTML")
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

    if db_user:
        has_active = DatabaseManager.get_active_shift(db_user['id']) is not None

    await query.edit_message_text("Ок, возвращаюсь в меню.")
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=create_main_reply_keyboard(has_active)
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
    else:
        message += "Пока нет данных за декаду.\n"

    message += "\n⚡ Лидеры смены (активные):\n"
    if active_leaders:
        for place, leader in enumerate(active_leaders, start=1):
            message += f"{place}. {leader['name']} — {format_money(leader['total_amount'])} (смен: {leader['shift_count']})\n"
    else:
        message += "Пока нет активных смен."

    await query.edit_message_text(message)
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=create_main_reply_keyboard(True)
    )

async def decade_callback(query, context):
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await query.edit_message_text("❌ Ошибка: пользователь не найден")
        return
    message = build_decade_summary(db_user['id'])
    await query.edit_message_text(message, parse_mode="HTML")
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=create_main_reply_keyboard(True)
    )

async def stats_callback(query, context):
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await query.edit_message_text("❌ Ошибка: пользователь не найден")
        return
    message = build_stats_summary(db_user['id'])
    await query.edit_message_text(message)
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=create_main_reply_keyboard(True)
    )

async def export_csv(query, context):
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await query.edit_message_text("❌ Ошибка: пользователь не найден")
        return
    path = build_csv_report(db_user['id'])
    if not path:
        await query.edit_message_text("❌ Не получилось создать CSV отчёт.")
        return
    with open(path, "rb") as report_file:
        await query.message.reply_document(
            document=report_file,
            filename=os.path.basename(path),
            caption="📤 Ваш CSV отчёт"
        )
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=create_main_reply_keyboard(True)
    )

async def export_decade_pdf(query, context, data):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    body = data.replace("export_decade_pdf_", "")
    year_s, month_s, decade_s = body.split("_")
    year, month, decade_index = int(year_s), int(month_s), int(decade_s)
    path = create_decade_pdf(db_user['id'], year, month, decade_index)
    with open(path, "rb") as file_obj:
        await query.message.reply_document(
            document=file_obj,
            filename=os.path.basename(path),
            caption=f"PDF отчёт по декаде {format_decade_title(year, month, decade_index)}",
        )
    await query.answer("PDF отчёт отправлен")


async def export_decade_xlsx(query, context, data):
    db_user = DatabaseManager.get_user(query.from_user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return
    body = data.replace("export_decade_xlsx_", "")
    year_s, month_s, decade_s = body.split("_")
    year, month, decade_index = int(year_s), int(month_s), int(decade_s)
    path = create_decade_xlsx(db_user['id'], year, month, decade_index)
    with open(path, "rb") as file_obj:
        await query.message.reply_document(
            document=file_obj,
            filename=os.path.basename(path),
            caption=f"XLSX отчёт по декаде {format_decade_title(year, month, decade_index)}",
        )
    await query.answer("XLSX отчёт отправлен")


async def backup_db(query, context):
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await query.edit_message_text("❌ Ошибка: пользователь не найден")
        return
    backup_path = create_db_backup()
    if not backup_path:
        await query.edit_message_text("❌ Не получилось сделать резервную копию.")
        return
    with open(backup_path, "rb") as backup_file:
        await query.message.reply_document(
            document=backup_file,
            filename=os.path.basename(backup_path),
            caption="🗄️ Резервная копия базы"
        )
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

    active_shift = DatabaseManager.get_active_shift(db_user['id'])
    if active_shift:
        start_time = parse_datetime(active_shift['start_time'])
        time_text = start_time.strftime('%H:%M %d.%m') if start_time else "неизвестно"
        await update.message.reply_text(
            f"❌ У вас уже есть активная смена!\n"
            f"Начата: {time_text}",
            reply_markup=create_main_reply_keyboard(True)
        )
        return

    DatabaseManager.start_shift(db_user['id'])
    await update.message.reply_text(
        f"✅ Смена открыта!\n"
        f"Время: {now_local().strftime('%H:%M %d.%m.%Y')}\n\n"
        f"Теперь можно добавлять машины.",
        reply_markup=create_main_reply_keyboard(True)
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
        reply_markup=create_main_reply_keyboard(True)
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

    shift_id = active_shift['id']
    cars = DatabaseManager.get_shift_cars(shift_id)
    total = DatabaseManager.get_shift_total(shift_id)
    dashboard = build_current_shift_dashboard(db_user['id'], active_shift, cars, total)

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Да, закрыть", callback_data=f"close_confirm_yes_{shift_id}")],
        [InlineKeyboardButton("❌ Нет, оставить открытой", callback_data=f"close_confirm_no_{shift_id}")],
    ])

    await update.message.reply_text(
        dashboard + "\n\n⚠️ Вы точно хотите закрыть смену?",
        parse_mode="HTML",
        reply_markup=keyboard,
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
    has_active_shift = bool(db_user and DatabaseManager.get_active_shift(db_user['id']))
    keyboard = [
        *([[InlineKeyboardButton("🎯 Цель дня", callback_data="change_goal")]] if has_active_shift else []),
        [InlineKeyboardButton("📜 История по декадам", callback_data="history_decades")],
        [InlineKeyboardButton("🧩 Мои комбинации", callback_data="combo_settings")],
        [InlineKeyboardButton("➕ Создать комбо", callback_data="combo_create_settings")],
        [InlineKeyboardButton("🧹 Редактировать данные", callback_data="cleanup_data")],
        [InlineKeyboardButton("🗑️ Сбросить данные", callback_data="reset_data")],
    ]
    if is_admin_telegram(update.effective_user.id):
        keyboard.append([InlineKeyboardButton("🛡️ Админ-панель", callback_data="admin_panel")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back")])
    await update.message.reply_text(
        f"⚙️ НАСТРОЙКИ\n\nВерсия: {APP_VERSION}\nОбновлено: {APP_UPDATED_AT}\n\nВыберите параметр:",
        reply_markup=InlineKeyboardMarkup(keyboard)
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
    await update.message.reply_text(
        message,
        reply_markup=create_main_reply_keyboard(has_active)
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

async def show_car_services(query, context: CallbackContext, car_id: int, page: int = 0):
    """Показать услуги машины"""
    car = DatabaseManager.get_car(car_id)
    if not car:
        return None, None

    services = DatabaseManager.get_car_services(car_id)
    services_text = ""
    for service in services:
        services_text += f"• {plain_service_name(service['service_name'])} ({service['price']}₽) ×{service['quantity']}\n"

    if not services_text:
        services_text = "Нет выбранных услуг\n"

    edit_mode = get_edit_mode(context, car_id)
    mode_text = "✏️ Режим: удаление" if edit_mode else "➕ Режим: добавление"
    price_text = "🌞 Прайс: день" if get_price_mode(context) == "day" else "🌙 Прайс: ночь"
    
    message = (
        f"🚗 Машина: {car['car_number']}\n"
        f"Итог: {format_money(car['total_amount'])}\n\n"
        f"{mode_text}\n{price_text}\n\n"
        f"Услуги:\n{services_text}\n"
        f"Выберите ещё:"
    )
    
    db_user = DatabaseManager.get_user(query.from_user.id)
    await query.edit_message_text(
        message,
        reply_markup=create_services_keyboard(car_id, page, edit_mode, get_price_mode(context), db_user["id"] if db_user else None)
    )


async def notify_decade_change_if_needed(update: Update, context: CallbackContext, user_id: int):
    current_idx, current_start, current_end, current_key, _ = get_decade_period(now_local().date())
    last_key = DatabaseManager.get_last_decade_notified(user_id)

    if not last_key:
        DatabaseManager.set_last_decade_notified(user_id, current_key)
        return

    if last_key == current_key:
        return

    try:
        year_s, month_s, decade_s = last_key.split("-")
        year = int(year_s)
        month = int(month_s)
        idx = int(decade_s.replace("D", ""))
    except Exception:
        DatabaseManager.set_last_decade_notified(user_id, current_key)
        return

    if idx == 1:
        start_day, end_day = 1, 10
    elif idx == 2:
        start_day, end_day = 11, 20
    else:
        start_day = 21
        end_day = calendar.monthrange(year, month)[1]

    prev_start = date(year, month, start_day)
    prev_end = date(year, month, end_day)
    total = DatabaseManager.get_user_total_between_dates(user_id, prev_start.isoformat(), prev_end.isoformat())

    cars_total = DatabaseManager.get_top_cars_between_dates(user_id, prev_start.isoformat(), prev_end.isoformat(), limit=1)
    services_top = DatabaseManager.get_top_services_between_dates(user_id, prev_start.isoformat(), prev_end.isoformat(), limit=1)
    best_car = cars_total[0]["car_number"] if cars_total else "—"
    top_service = plain_service_name(services_top[0]["service_name"]) if services_top else "—"
    text = (
        "🔔 Декада закрыта!\n"
        f"Период: {format_decade_range(prev_start, prev_end)}\n"
        f"Итог: {format_money(total)}\n"
        f"Топ услуга: {top_service}\n"
        f"Топ машина: {best_car}\n\n"
        f"Новая декада: {format_decade_range(current_start, current_end)}"
    )



async def toggle_price_mode(query, context):
    user = query.from_user
    db_user = DatabaseManager.get_user(user.id)
    if not db_user:
        await query.edit_message_text("❌ Пользователь не найден")
        return

    current = get_price_mode(context, db_user['id'])
    new_mode = "night" if current == "day" else "day"
    context.user_data["price_mode"] = new_mode
    DatabaseManager.set_price_mode(db_user['id'], new_mode)
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

    months = DatabaseManager.get_user_months_with_data(db_user['id'])
    if not months:
        await query.edit_message_text("Пока нет данных для редактирования.")
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

    keyboard.append([InlineKeyboardButton("⚠️ Удалить весь день", callback_data=f"delday_prompt_{day}")])
    keyboard.append([InlineKeyboardButton("🔙 К дням", callback_data=f"cleanup_month_{day[:7]}")])
    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))


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

# ========== ГЛАВНАЯ ФУНКЦИЯ ==========

def main():
    """Запуск бота"""
    application = Application.builder().token(BOT_TOKEN).build()
    
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

if __name__ == "__main__":
    main()
