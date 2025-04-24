from pyrogram import Client, filters 
from helper.database import DARKXSIDE78

@Client.on_message(filters.private & filters.command('set_caption'))
async def add_caption(client, message):
    if len(message.command) == 1:
       return await message.reply_text("**Gɪᴠᴇ Cᴀᴘᴛɪᴏɴ\n\nExᴀᴍᴘʟᴇ: `/set_caption Nᴀᴍᴇ ➠ : {filename} \n\nSɪᴢᴇ ➠ : {filesize} \n\nDᴜʀᴀᴛɪᴏɴ ➠ : {duration}`**")
    caption = message.text.split(" ", 1)[1]
    await DARKXSIDE78.set_caption(message.from_user.id, caption=caption)
    await message.reply_text("**Yᴏᴜʀ Cᴀᴘᴛɪᴏɴ ʜᴀs ʙᴇᴇɴ Sᴜᴄᴄᴇssғᴜʟʟʏ Sᴇᴛ...**")

@Client.on_message(filters.private & filters.command('del_caption'))
async def delete_caption(client, message):
    caption = await madflixbotz.get_caption(message.from_user.id)  
    if not caption:
       return await message.reply_text("**Yᴏᴜ Dᴏɴ'ᴛ Hᴀᴠᴇ Aɴʏ Cᴀᴘᴛɪᴏɴ.**")
    await DARKXSIDE78.set_caption(message.from_user.id, caption=None)
    await message.reply_text("**Yᴏᴜʀ Cᴀᴘᴛɪᴏɴ ʜᴀs ʙᴇᴇɴ Sᴜᴄᴄᴇssғᴜʟʟʏ Dᴇʟᴇᴛᴇᴅ...**")

@Client.on_message(filters.private & filters.command(['see_caption', 'view_caption']))
async def see_caption(client, message):
    caption = await DARKXSIDE78.get_caption(message.from_user.id)  
    if caption:
       await message.reply_text(f"**Cᴜʀʀᴇɴᴛ Cᴀᴘᴛɪᴏɴ:**\n\n`{caption}`")
    else:
       await message.reply_text("**Yᴏᴜ Dᴏɴ'ᴛ Hᴀᴠᴇ Aɴʏ Cᴀᴘᴛɪᴏɴ.**")


@Client.on_message(filters.private & filters.command(['view_thumb', 'viewthumb']))
async def viewthumb(client, message):    
    thumb = await DARKXSIDE78.get_thumbnail(message.from_user.id)
    if thumb:
       await client.send_photo(chat_id=message.chat.id, photo=thumb)
    else:
        await message.reply_text("**Yᴏᴜ Dᴏɴ'ᴛ Hᴀᴠᴇ Aɴʏ Tʜᴜᴍʙɴᴀɪʟ.**") 

@Client.on_message(filters.private & filters.command(['del_thumb', 'delthumb']))
async def removethumb(client, message):
    await DARKXSIDE78.set_thumbnail(message.from_user.id, file_id=None)
    await message.reply_text("**Yᴏᴜʀ Tʜᴜᴍʙɴᴀɪʟ ʜᴀs ʙᴇᴇɴ Sᴜᴄᴄᴇssғᴜʟʟʏ Dᴇʟᴇᴛᴇᴅ.**")

@Client.on_message(filters.private & filters.photo)
async def addthumbs(client, message):
    mkn = await message.reply_text("Pʟᴇᴀsᴇ Wᴀɪᴛ ᴀ ᴍᴏᴍᴇɴᴛ...")
    await DARKXSIDE78.set_thumbnail(message.from_user.id, file_id=message.photo.file_id)                
    await mkn.edit("**Tʜᴜᴍʙɴᴀɪʟ ʜᴀs ʙᴇᴇɴ Sᴀᴠᴇᴅ Sᴜᴄᴄᴇssғᴜʟʟʏ.**")
