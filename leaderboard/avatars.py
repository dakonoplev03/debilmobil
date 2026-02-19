from __future__ import annotations

import asyncio
from io import BytesIO
from pathlib import Path
from datetime import datetime

from PIL import Image, ImageDraw, ImageFont
from telegram import Bot

AVATAR_CACHE_DIR = Path("cache/avatars")
AVATAR_TTL_DAYS = 7
MAX_PARALLEL_DOWNLOADS = 4
DOWNLOAD_TIMEOUT = 5

_semaphore = asyncio.Semaphore(MAX_PARALLEL_DOWNLOADS)


def is_cache_valid(path: Path) -> bool:
    if not path.exists():
        return False
    age = datetime.now().timestamp() - path.stat().st_mtime
    return age < AVATAR_TTL_DAYS * 86400


def _fallback(size: int, initials: str) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d = ImageDraw.Draw(img, "RGBA")
    for y in range(size):
        t = y / max(size - 1, 1)
        d.line((0, y, size, y), fill=(int(31 + 70 * t), int(47 + 25 * t), int(88 + 55 * t), 255))
    fnt = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", max(16, size // 3))
    text = (initials or "?")[:2].upper()
    b = d.textbbox((0, 0), text, font=fnt)
    d.text(((size - (b[2]-b[0]))/2, (size - (b[3]-b[1]))/2), text, fill="#EAF0FF", font=fnt)
    return img


def _crop_square(image: Image.Image) -> Image.Image:
    w, h = image.size
    s = min(w, h)
    return image.crop(((w-s)//2, (h-s)//2, (w+s)//2, (h+s)//2))


async def fetch_avatar_bytes(bot: Bot, user_id: int) -> bytes | None:
    try:
        async with _semaphore:
            photos = await asyncio.wait_for(bot.get_user_profile_photos(user_id=user_id, limit=1), timeout=DOWNLOAD_TIMEOUT)
            if not photos or not photos.photos:
                return None
            file_id = photos.photos[0][-1].file_id
            file = await asyncio.wait_for(bot.get_file(file_id), timeout=DOWNLOAD_TIMEOUT)
            data = await asyncio.wait_for(file.download_as_bytearray(), timeout=DOWNLOAD_TIMEOUT)
            return bytes(data)
    except Exception:
        return None


async def get_avatar_image(bot: Bot, user_id: int, size: int, fallback_name: str = "") -> Image.Image:
    AVATAR_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache = AVATAR_CACHE_DIR / f"{int(user_id)}.jpg"
    initials = "".join(p[:1] for p in fallback_name.split()[:2]).upper() or "?"

    raw: bytes | None = None
    if user_id and is_cache_valid(cache):
        try:
            raw = cache.read_bytes()
        except Exception:
            raw = None

    if raw is None and user_id:
        raw = await fetch_avatar_bytes(bot, user_id)
        if raw:
            try:
                cache.write_bytes(raw)
            except Exception:
                pass

    if not raw:
        return _fallback(size, initials)

    try:
        img = Image.open(BytesIO(raw)).convert("RGBA")
        return _crop_square(img).resize((size, size), Image.Resampling.LANCZOS)
    except Exception:
        return _fallback(size, initials)
