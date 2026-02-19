from __future__ import annotations

from io import BytesIO
from telegram import Update
from telegram.ext import CallbackContext


async def send_status(update: Update, context: CallbackContext, text: str, *, reply_markup=None):
    if update.callback_query:
        return await update.callback_query.message.reply_text(text, reply_markup=reply_markup)
    if update.message:
        return await update.message.reply_text(text, reply_markup=reply_markup)
    return await context.bot.send_message(chat_id=update.effective_chat.id, text=text, reply_markup=reply_markup)


async def edit_status(msg, text: str, *, reply_markup=None) -> None:
    try:
        await msg.edit_text(text=text, reply_markup=reply_markup)
    except Exception:
        await msg.reply_text(text, reply_markup=reply_markup)


async def done_status(msg, text: str, *, attach_photo_bytes: bytes | BytesIO | None = None, filename: str = "top.png", caption: str | None = None, reply_markup=None) -> None:
    if attach_photo_bytes is None:
        await edit_status(msg, text, reply_markup=reply_markup)
        return

    photo = attach_photo_bytes
    if isinstance(attach_photo_bytes, bytes):
        b = BytesIO(attach_photo_bytes)
        b.name = filename
        photo = b

    try:
        await msg.delete()
    except Exception:
        pass
    await msg.reply_photo(photo=photo, caption=caption or text, reply_markup=reply_markup)
