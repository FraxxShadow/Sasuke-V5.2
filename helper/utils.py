import os
import re
import math
import time
import asyncio
import logging
import shutil  # Added missing import
from datetime import datetime
from pytz import timezone
from config import Config, Txt
from pyrogram.errors import FloodWait
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

__all__ = [
    'progress_for_pyrogram',
    'humanbytes',
    'convert',
    'TimeFormatter',
    'handle_floodwait',
    'add_prefix_suffix',
    'safe_delete',
    'send_log'
]

LAST_UPDATE_TIMES = {}
USER_SEMAPHORES = {}

max_retries = Config.FLOODWAIT_RETRIES
initial_wait = Config.FLOODWAIT_WAIT

def handle_floodwait():
    def decorator(func):
        async def wrapper(*args, **kwargs):
            retries = 0
            wait_time = initial_wait
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except FloodWait as e:
                    wait = min(e.value + 2, 60)
                    logger.warning(f"FloodWait: Sleeping {wait}s (Attempt {retries+1}/{max_retries})")
                    await asyncio.sleep(wait)
                    retries += 1
                    wait_time *= 1.5
            raise Exception(f"Max retries ({max_retries}) reached for FloodWait")
        return wrapper
    return decorator

async def progress_for_pyrogram(current, total, ud_type, message, start):
    now = time.time()
    message_id = message.id
    last_update = LAST_UPDATE_TIMES.get(message_id, 0)

    if current == total or (now - last_update >= Config.UPDATE_TIME):
        percentage = current * 100 / total
        speed = current / (now - start) if (now - start) > 0 else 0

        progress = "".join(
            "■" if i < math.floor(percentage / 5) else "□" 
            for i in range(20)
        )

        if speed > 0:
            eta_seconds = (total - current) / speed
            eta_formatted = TimeFormatter(eta_seconds * 1000)
        else:
            eta_formatted = "0s"

        progress_text = Txt.PROGRESS_BAR.format(
            round(percentage, 2),
            humanbytes(current),
            humanbytes(total),
            humanbytes(speed),
            eta_formatted
        )

        tmp = f"{progress}\n{progress_text}"

        try:
            user_id = message.chat.id
            semaphore = USER_SEMAPHORES.setdefault(user_id, asyncio.Semaphore(1))
            async with semaphore:
                await message.edit(
                    text=f"{ud_type}\n\n{tmp}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("• Cᴀɴᴄᴇʟ •", callback_data="close")
                    ]])
                )
                LAST_UPDATE_TIMES[message_id] = now

                if current == total:
                    await asyncio.sleep(3)

        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            LAST_UPDATE_TIMES[message_id] = now + e.value + 1
        except Exception as e:
            logger.error(f"Progress update error: {e}", exc_info=True)
        finally:
            if current == total:
                LAST_UPDATE_TIMES.pop(message_id, None)

def humanbytes(size: int) -> str:
    if not size:
        return "0 B"
    
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units)-1:
        size /= 1024
        unit_index += 1
        
    return f"{size:.2f} {units[unit_index]}"

def TimeFormatter(milliseconds: int) -> str:
    seconds, milliseconds = divmod(int(milliseconds), 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    elif seconds > 0:
        return f"{seconds}s"
    return f"{milliseconds}ms"

def convert(seconds: int) -> str:
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    return f"{hour:02d}:{minutes:02d}:{seconds:02d}"

@handle_floodwait()
async def send_log(bot, user):
    if not Config.LOG_CHANNEL:
        return
        
    try:
        curr = datetime.now(timezone("Asia/Kolkata"))
        await bot.send_message(
            Config.LOG_CHANNEL,
            f"**Nᴇᴡ Usᴇʀ Aᴄᴛɪᴠɪᴛʏ**\n\n"
            f"**➤ Usᴇʀ: {user.mention}**\n"
            f"**➤ ID: `{user.id}`**\n"
            f"**➤ Usᴇʀɴᴀᴍᴇ: @{user.username if user.username else 'N/A'}**\n\n"
            f"**➤ Dᴀᴛᴇ: {curr.strftime('%d %B, %Y')}**\n"
            f"**➤ Tɪᴍᴇ: {curr.strftime('%I:%M:%S %p')}**\n\n"
            f"**➤ Bᴏᴛ: {bot.mention}**",
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Logging error: {e}", exc_info=True)

def add_prefix_suffix(input_string, prefix='', suffix=''):
    pattern = r'(?P<filename>.*?)(\.\w+)?$'
    match = re.search(pattern, input_string)
    if match:
        filename = match.group('filename')
        extension = match.group(2) or ''
        return f"{prefix}{filename}{suffix}{extension}"
    return input_string

async def safe_delete(*paths):
    for path in paths:
        try:
            if path and os.path.exists(path):
                if os.path.isfile(path):
                    os.remove(path)
                else:
                    shutil.rmtree(path)
        except Exception as e:
            logger.error(f"Error deleting {path}: {e}")
