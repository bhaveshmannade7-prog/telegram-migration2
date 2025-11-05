import os
import re
import asyncio
import asyncpg
from aiohttp import web

# --- Telebot Imports ---
from telebot.async_telebot import AsyncTeleBot
from telebot import types

# --- Pyrogram Imports ---
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageNotModified

# --- CONFIG (ENV VARS) ---
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# === CHANGE 1: ADMIN LIST ===
# Purane ADMIN_ID ko aur naye ID ko ek list mein daal diya hai
MAIN_ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
ADMIN_IDS = [MAIN_ADMIN_ID, 920892710] 
# === END CHANGE 1 ===

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
SOURCE_CHANNEL_ID = int(os.environ.get("SOURCE_CHANNEL_ID", 0))

CAPTION_FOOTER = "\n\n@THEGREATMOVIESL9\n@MOVIEMAZASU"
USERNAME_WHITELIST = ["@THEGREATMOVIESL9", "@MOVIEMAZASU"]
BLACKLIST_WORDS = ["18+", "adult", "hot", "sexy"]
BATCH_SLEEP_TIME = 2

db_pool = None
batch_job_lock = asyncio.Lock()

# Init Bots
bot = AsyncTeleBot(BOT_TOKEN, parse_mode='Markdown')
app = Client("indexer", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)

print("\n============================")
print("🤖 HYBRID BOT STARTING...")
print("============================\n")

# ---------- DATABASE ----------
async def init_database():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS indexed_movies (
                    source_message_id BIGINT PRIMARY KEY,
                    file_unique_id TEXT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_uid ON indexed_movies (file_unique_id);
            """)
        print("✅ DB Connected")
    except Exception as e:
        print(f"❌ DB Error: {e}")
        db_pool = None

# ---------- HELPERS ----------
def clean_caption(txt):
    if not txt:
        return ""
    cleaned = txt
    for pattern in [r'https?://\S+', r'www\.\S+', r't\.me/\S+']:
        cleaned = re.sub(pattern, "", cleaned)
    for user in re.findall(r'@\S+', cleaned):
        if user not in USERNAME_WHITELIST:
            cleaned = cleaned.replace(user, "")
    cleaned = "\n".join([l.strip() for l in cleaned.splitlines() if l.strip()])
    return cleaned

def get_file_id(m):
    if m.video:
        return m.video.file_unique_id
    if m.document:
        return m.document.file_unique_id
    return None

def get_main_menu():
    m = types.InlineKeyboardMarkup(row_width=1)
    m.add(
        types.InlineKeyboardButton("📊 Stats", callback_data="show_stats"),
        types.InlineKeyboardButton("⏳ Full Index", callback_data="info_index"),
        types.InlineKeyboardButton("🧹 Clean All", callback_data="info_clean"),
        types.InlineKeyboardButton("🔄 Refresh", callback_data="info_refresh")
    )
    return m

# ---------- TELEBOT ----------
# === CHANGE 2: TELEBOT HANDLERS ===
@bot.message_handler(commands=['start', 'help'])
async def start_cmd(message):
    # Check if user is in the admin list
    if message.from_user.id not in ADMIN_IDS:
        return await bot.reply_to(message, "⛔ Not Authorized")
    await bot.reply_to(message,
        "👋 *Admin Menu*",
        reply_markup=get_main_menu()
    )

@bot.callback_query_handler(func=lambda c: True)
async def cb(call):
    # Check if user is in the admin list
    if call.from_user.id not in ADMIN_IDS:
        return await bot.answer_callback_query(call.id, "⛔ Not Allowed", show_alert=True)

    if call.data == "show_stats":
        total = 0
        if db_pool:
            async with db_pool.acquire() as conn:
                total = await conn.fetchval("SELECT COUNT(*) FROM indexed_movies")
        await bot.send_message(call.message.chat.id, f"📊 Total Movies in DB: `{total}`")

    elif call.data == "info_index":
        await bot.send_message(call.message.chat.id, "Go to *Saved Messages* and send `/index`")

    elif call.data == "info_clean":
        await bot.send_message(call.message.chat.id, "Go to *Saved Messages* and send `/cleanall`")

    elif call.data == "info_refresh":
        await bot.send_message(call.message.chat.id, "Reply a movie in channel & send `/refresh`")
# === END CHANGE 2 ===


# ---------- INDEXER AUTO HANDLER ----------
async def process_post(msg):
    if not db_pool:
        return
    file_uid = get_file_id(msg)
    if not file_uid:
        return

    lower = (msg.caption or "").lower()
    for w in BLACKLIST_WORDS:
        if w in lower:
            await msg.delete()
            print(f"🚫 Blacklisted Removed: {msg.id}")
            return

    async with db_pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM indexed_movies WHERE file_unique_id=$1 LIMIT 1", file_uid)
        if exists:
            await msg.delete()
            print(f"♻️ Duplicate Removed: {msg.id}")
            return

        await conn.execute("INSERT INTO indexed_movies VALUES ($1,$2) ON CONFLICT DO NOTHING", msg.id, file_uid)
        print(f"✅ Indexed: {msg.id}")

@app.on_message(filters.chat(SOURCE_CHANNEL_ID) & (filters.video | filters.document) & filters.channel)
async def new_post(client, message):
    await process_post(message)

@app.on_edited_message(filters.chat(SOURCE_CHANNEL_ID) & (filters.video | filters.document) & filters.channel)
async def edited_post(client, message):
    await process_post(message)

# ---------- MANUAL COMMANDS ----------
# === CHANGE 3: PYROGRAM HANDLERS ===
@app.on_message(filters.command("index") & filters.user(ADMIN_IDS))
async def full_index(client, message):
    if db_pool is None:
        return await message.reply("❌ DB Not Connected")

    if batch_job_lock.locked():
        return await message.reply("⏳ Another job running")

    async with batch_job_lock:
        status = await message.reply("⏳ Indexing...")
        count_new = 0
        async for m in app.get_chat_history(SOURCE_CHANNEL_ID):
            uid = get_file_id(m)
            if uid:
                async with db_pool.acquire() as conn:
                    exists = await conn.fetchval("SELECT 1 FROM indexed_movies WHERE file_unique_id=$1", uid)
                    if not exists:
                        await conn.execute("INSERT INTO indexed_movies VALUES ($1,$2) ON CONFLICT DO NOTHING", m.id, uid)
                        count_new += 1
            await asyncio.sleep(0.1)
        await status.edit(f"✅ Done. Added: `{count_new}`")

@app.on_message(filters.command("cleanall") & filters.user(ADMIN_IDS))
async def clean_all(client, message):
    if db_pool is None:
        return await message.reply("❌ DB Not Connected")

    if batch_job_lock.locked():
        return await message.reply("⏳ Another job running")

    async with batch_job_lock:
        status = await message.reply("🧹 Cleaning...")
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT source_message_id FROM indexed_movies")

        for r in rows:
            try:
                msg = await app.get_messages(SOURCE_CHANNEL_ID, r["source_message_id"])
                new_cap = clean_caption(msg.caption) + CAPTION_FOOTER
                await msg.edit_caption(new_cap)
            except MessageNotModified:
                pass
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except:
                pass
            await asyncio.sleep(BATCH_SLEEP_TIME)

        await status.edit("✅ Clean Done!")

@app.on_message(filters.command("refresh") & filters.user(ADMIN_IDS) & filters.chat(SOURCE_CHANNEL_ID))
async def refresh(client, message):
    if not message.reply_to_message:
        return await message.reply("Reply a movie")
    msg = message.reply_to_message
    new_cap = clean_caption(msg.caption) + CAPTION_FOOTER
    await msg.edit_caption(new_cap)
    await message.reply("✅ Refreshed")
# === END CHANGE 3 ===

# ---------- WEB SERVER ----------
async def web_server():
    app_web = web.Application()
    app_web.router.add_get("/", lambda r: web.Response(text="Bot Alive ✅"))
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 8080)))
    await site.start()
    await asyncio.Event().wait()

# ---------- MAIN ----------
async def main():
    for v in [BOT_TOKEN, API_HASH, SESSION_STRING, DATABASE_URL]:
        if not v:
            print("❌ Missing ENV VARS")
            exit(1)
            
    if not ADMIN_IDS or ADMIN_IDS == [0, 920892710]:
        print("❌ ADMIN_ID environment variable is missing or invalid!")
        exit(1)

    await init_database()

    await app.start()
    await bot.delete_webhook(drop_pending_updates=True) 

    print("✅ Bots Running...\n")
    await asyncio.gather(
        bot.polling(non_stop=True, timeout=60),
        web_server()
    )

if __name__ == "__main__":
    asyncio.run(main())
