from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from helper.database import DARKXSIDE78
from datetime import datetime, timedelta
from pyrogram.types import CallbackQuery
import pytz

@Client.on_message(filters.private & filters.command("autorename"))
async def auto_rename_command(client, message):
    user_id = message.from_user.id

    command_parts = message.text.split(maxsplit=1)
    if len(command_parts) < 2 or not command_parts[1].strip():
        await message.reply_text(
            "**Pʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ɴᴇᴡ ɴᴀᴍᴇ ᴀғᴛᴇʀ ᴛʜᴇ ᴄᴏᴍᴍᴀɴᴅ /autorename**\n\n"
            "Hᴇʀᴇ's ʜᴏᴡ ᴛᴏ ᴜsᴇ ɪᴛ:\n"
            "**Exᴀᴍᴘʟᴇ ғᴏʀᴍᴀᴛ:** `/autorename [S{season}-{episode}] Attack on Titan [{quality}] [{audio}] @GenAnimeOfc`"
        )
        return

    format_template = command_parts[1].strip()

    await DARKXSIDE78.set_format_template(user_id, format_template)

    await message.reply_text(
        f"**Fᴀɴᴛᴀsᴛɪᴄ! Yᴏᴜ'ʀᴇ ʀᴇᴀᴅʏ ᴛᴏ ᴀᴜᴛᴏ-ʀᴇɴᴀᴍᴇ ʏᴏᴜʀ ғɪʟᴇs.**\n\n"
        "Sɪᴍᴘʟʏ sᴇɴᴅ ᴛʜᴇ ғɪʟᴇ(s) ʏᴏᴜ ᴡᴀɴᴛ ᴛᴏ ʀᴇɴᴀᴍᴇ.\n\n"
        f"**Yᴏᴜʀ sᴀᴠᴇᴅ ᴛᴇᴍᴘʟᴀᴛᴇ:** `{format_template}`\n\n"
        "**Rᴇᴍᴇᴍʙᴇʀ, ɪᴛ ᴍɪɢʜᴛ ᴛᴀᴋᴇ sᴏᴍᴇ ᴛɪᴍᴇ, ʙᴜᴛ I'ʟʟ ᴇɴsᴜʀᴇ ʏᴏᴜʀ ғɪʟᴇs ᴀʀᴇ ʀᴇɴᴀᴍᴇᴅ ᴘᴇʀғᴇᴄᴛʟʏ!**"
    )


@Client.on_message(filters.private & filters.command("setmedia"))
async def set_media_command(client, message):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Dᴏᴄᴜᴍᴇɴᴛ", callback_data="setmedia_document")],
        [InlineKeyboardButton("Vɪᴅᴇᴏs", callback_data="setmedia_video")],
        [InlineKeyboardButton("Aᴜᴅɪᴏ", callback_data="setmedia_audio")],
    ])

    await message.reply_text(
        "**Cʜᴏᴏsᴇ Yᴏᴜʀ Mᴇᴅɪᴀ Vɪʙᴇ**\n"
        "**Sᴇʟᴇᴄᴛ ᴛʜᴇ ᴛʏᴘᴇ ᴏғ ᴍᴇᴅɪᴀ ʏᴏᴜ'ᴅ ʟɪᴋᴇ ᴛᴏ sᴇᴛ ᴀs ʏᴏᴜʀ ᴘʀᴇғᴇʀᴇɴᴄᴇ:**",
        reply_markup=keyboard,
        quote=True
    )

@Client.on_callback_query(filters.regex(r"^setmedia_"))
async def handle_media_selection(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    media_type = callback_query.data.split("_", 1)[1].capitalize()

    try:
        await DARKXSIDE78.set_media_preference(user_id, media_type.lower())

        await callback_query.answer(f"Lᴏᴄᴋᴇᴅ ɪɴ: {media_type}")
        await callback_query.message.edit_text(
            f"**Mᴇᴅɪᴀ Pʀᴇғᴇʀᴇɴᴄᴇ Uᴘᴅᴀᴛᴇᴅ**\n"
            f"**Yᴏᴜʀ ᴠɪʙᴇ ɪs ɴᴏᴡ sᴇᴛ ᴛᴏ:** **{media_type}**\n"
            f"**Rᴇᴀᴅʏ ᴛᴏ ʀᴏʟʟ ᴡɪᴛʜ ʏᴏᴜʀ ᴄʜᴏɪᴄᴇ!**"
        )
    except Exception as e:
        await callback_query.answer("Oᴏᴘs, sᴏᴍᴇᴛʜɪɴɢ ᴡᴇɴᴛ ᴡʀᴏɴɢ!")
        await callback_query.message.edit_text(
            f"**Eʀʀᴏʀ Sᴇᴛᴛɪɴɢ Pʀᴇғᴇʀᴇɴᴄᴇ**\n"
            f"**Cᴏᴜʟᴅɴ’ᴛ sᴇᴛ {media_type} ʀɪɢʜᴛ ɴᴏᴡ. Tʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!**\n"
            f"**Dᴇᴛᴀɪʟs: {str(e)}**"
        )
