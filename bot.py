import os
import re
import asyncio
import asyncpg
from aiohttp import web

# --- Telebot Imports ---
from telebot.async_telebot import AsyncTeleBot
from telebot import types

# --- Pyrogram Imports ---
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, MessageNotModified

# --- CONFIG (ENV VARS) ---
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# Admin List
MAIN_ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
ADMIN_IDS = [MAIN_ADMIN_ID, 920892710] 

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
SOURCE_CHANNEL_ID = int(os.environ.get("SOURCE_CHANNEL_ID", 0))

# --- NEW: Target Channel Config ---
TARGET_FORWARD_CHANNEL = -1002417767287
FORWARD_BATCH_SIZE = 100
FORWARD_BATCH_SLEEP = 7  # 7 second gap between batches
FORWARD_MSG_SLEEP = 2    # 2 second gap between each movie

CAPTION_FOOTER = "\n\n@THEGREATMOVIESL9\n@MOVIEMAZASU"
USERNAME_WHITELIST = ["@THEGREATMOVIESL9", "@MOVIEMAZASU"]
BLACKLIST_WORDS = ["18+", "adult", "hot", "sexy"]
BATCH_SLEEP_TIME = 2

db_pool = None
batch_job_lock = asyncio.Lock()

# =================================================================
# === 'PARSE ENTITIES' WALA FIX ===
# Default parse_mode ko hata diya gaya hai. Ab default PLAIN TEXT hai.
# =================================================================
bot = AsyncTeleBot(BOT_TOKEN) 
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
            # Table for Source Channel
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS indexed_movies (
                    source_message_id BIGINT PRIMARY KEY,
                    file_unique_id TEXT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_uid ON indexed_movies (file_unique_id);
            """)
            
            # NEW: Table for Target Channel (@MAZABACKUP01)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS target_channel_files (
                    file_unique_id TEXT PRIMARY KEY,
                    target_message_id BIGINT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_target_file_uid ON target_channel_files (file_unique_id);
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

# NEW: Updated Main Menu
def get_main_menu():
    m = types.InlineKeyboardMarkup(row_width=1)
    m.add(
        types.InlineKeyboardButton("📊 Stats", callback_data="show_stats"),
        types.InlineKeyboardButton("⏳ Full Index (Source)", callback_data="info_index"),
        types.InlineKeyboardButton("FORWARDER ➡️ @MAZABACKUP01", callback_data="fwd_channel_start"),
        types.InlineKeyboardButton("CLEAN ♻️ @MAZABACKUP01", callback_data="dedupe_target_start"),
        types.InlineKeyboardButton("🧹 Clean All (Source)", callback_data="info_clean"),
        types.InlineKeyboardButton("🔄 Refresh (Source)", callback_data="info_refresh")
    )
    return m

# ---------- TELEBOT ----------
@bot.message_handler(commands=['start', 'help'])
async def start_cmd(message):
    if message.from_user.id not in ADMIN_IDS:
        return await bot.reply_to(message, "⛔ Not Authorized")
    
    # parse_mode="Markdown" alag se add kiya gaya
    await bot.reply_to(message,
        "👋 *Admin Menu*\n\n"
        "Aap `/whoami` command type karke bhej sakte hain "
        "yeh check karne ke liye ki `SESSION_STRING` kis account ka hai.",
        reply_markup=get_main_menu(),
        parse_mode="Markdown" 
    )

# =================================================================
# === NAYA DIAGNOSTIC COMMAND (FIXED) ===
# =================================================================
@bot.message_handler(commands=['whoami'])
async def whoami_cmd(message):
    if message.from_user.id not in ADMIN_IDS:
        return await bot.reply_to(message, "⛔ Not Authorized")
    
    status_msg = await bot.reply_to(message, "⏳ Fetching `SESSION_STRING` account details...")
    
    try:
        me = await app.get_me() 
        
        response_text = (
            f"ℹ️ **SESSION_STRING Account Details**\n\n"
            f"First Name: `{me.first_name or 'N/A'}`\n"
            f"Last Name: `{me.last_name or 'N/A'}`\n"
            f"Username: `@{me.username or 'N/A'}`\n"
            f"User ID: `{me.id}`\n\n"
            f"👉 Ab check karein ki yeh account (`@{me.username or 'N/A'}`) "
            f"channel `{SOURCE_CHANNEL_ID}` mein **Admin** hai ya nahi."
        )
        
        # parse_mode="Markdown" alag se add kiya gaya
        await bot.edit_message_text(response_text, chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
        
    except Exception as e:
        # Ab yahaan koi parse_mode nahi hai. Yeh PLAIN TEXT jayega.
        await bot.edit_message_text(
            f"❌ /whoami command fail ho gaya:\n{e}", # Simple text
            chat_id=status_msg.chat.id, 
            message_id=status_msg.message_id,
            parse_mode=None # Force plain text
        )
# =================================================================
# === NAYA COMMAND END ===
# =================================================================

@bot.callback_query_handler(func=lambda c: True)
async def cb(call):
    if call.from_user.id not in ADMIN_IDS:
        return await bot.answer_callback_query(call.id, "⛔ Not Allowed", show_alert=True)

    if call.data == "show_stats":
        total_source = 0
        total_target = 0
        if db_pool:
            async with db_pool.acquire() as conn:
                total_source = await conn.fetchval("SELECT COUNT(*) FROM indexed_movies")
                total_target = await conn.fetchval("SELECT COUNT(*) FROM target_channel_files")
        
        # parse_mode="Markdown" alag se add kiya gaya
        await bot.send_message(
            call.message.chat.id, 
            f"📊 **Database Stats**\n\n"
            f"Source Channel Movies: `{total_source}`\n"
            f"Target Channel (@MAZABACKUP01) Movies: `{total_target}`",
            parse_mode="Markdown"
        )

    elif call.data == "info_index":
        await bot.answer_callback_query(call.id, "🚀 Starting Full Index...")
        asyncio.create_task(run_index_job_for_telebot(call))

    # Other callbacks...
    elif call.data == "fwd_channel_start":
        await bot.answer_callback_query(call.id)
        await bot.send_message(
            call.message.chat.id, 
            "REPLY TO THIS MESSAGE:\n\n"
            "Please send the **Source Channel ID** or **Username** (e.g., -100... or @channelname):", 
            reply_markup=types.ForceReply(selective=True),
            parse_mode="Markdown"
        )
    elif call.data == "dedupe_target_start":
        await bot.answer_callback_query(call.id, "🚀 Starting Duplicate Cleanup for @MAZABACKUP01...")
        asyncio.create_task(run_dedupe_job(call))
    elif call.data == "info_clean":
        # parse_mode="Markdown" alag se add kiya gaya
        await bot.send_message(call.message.chat.id, "Go to *Saved Messages* and send `/cleanall`", parse_mode="Markdown")
    elif call.data == "info_refresh":
        # parse_mode="Markdown" alag se add kiya gaya
        await bot.send_message(call.message.chat.id, "Reply a movie in channel & send `/refresh`", parse_mode="Markdown")

@bot.message_handler(
    func=lambda m: m.reply_to_message 
                 and "Please send the **Source Channel ID**" in m.reply_to_message.text 
                 and m.from_user.id in ADMIN_IDS,
    content_types=['text']
)
async def handle_forward_source(message):
    source_chat_id = message.text.strip()
    if not (source_chat_id.startswith('@') or source_chat_id.startswith('-100')):
        await bot.reply_to(message, "Invalid format. Must be `@username` or `-100...`")
        return
    if source_chat_id.startswith('-100'):
        try:
            source_chat_id = int(source_chat_id)
        except ValueError:
            await bot.reply_to(message, "Invalid Channel ID. Must be a number.")
            return
    # parse_mode="Markdown" alag se add kiya gaya
    await bot.reply_to(message, f"Got it. Starting to forward from `{source_chat_id}` to @MAZABACKUP01...", parse_mode="Markdown")
    asyncio.create_task(run_forwarding_job(message, source_chat_id))

async def run_forwarding_job(message, source_chat_id):
    if db_pool is None: return await bot.send_message(message.chat.id, "❌ DB Not Connected")
    if batch_job_lock.locked(): return await bot.send_message(message.chat.id, "⏳ Another job is already running. Please wait.")

    # parse_mode="Markdown" alag se add kiya gaya
    status_msg = await bot.send_message(message.chat.id, f"⏳ Starting forward job from `{source_chat_id}`...", parse_mode="Markdown")
    total_forwarded = 0
    total_skipped = 0
    batch_count = 0
    
    async with batch_job_lock:
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT file_unique_id FROM target_channel_files")
                existing_uids = {r['file_unique_id'] for r in rows}
            
            # parse_mode="Markdown" alag se add kiya gaya
            await bot.edit_message_text(
                f"Found `{len(existing_uids)}` movies already in @MAZABACKUP01 (DB). \n"
                f"⏳ Starting scan of `{source_chat_id}`...",
                chat_id=status_msg.chat.id, message_id=status_msg.message_id,
                parse_mode="Markdown"
            )
            
            async for msg in app.get_chat_history(source_chat_id):
                file_uid = get_file_id(msg)
                if not file_uid: continue
                if file_uid in existing_uids: total_skipped += 1; continue
                
                try:
                    fwded_msg = await msg.forward(TARGET_FORWARD_CHANNEL)
                    existing_uids.add(file_uid)
                    async with db_pool.acquire() as conn:
                        await conn.execute("INSERT INTO target_channel_files (file_unique_id, target_message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", file_uid, fwded_msg.id)
                    
                    total_forwarded += 1
                    batch_count += 1
                    
                    if total_forwarded % 20 == 0:
                        # parse_mode="Markdown" alag se add kiya gaya
                        await bot.edit_message_text(
                            f"⏳ Progress...\nForwarded: `{total_forwarded}`\nSkipped: `{total_skipped}`",
                            chat_id=status_msg.chat.id, message_id=status_msg.message_id,
                            parse_mode="Markdown"
                        )
                    
                    await asyncio.sleep(FORWARD_MSG_SLEEP)
                    
                    if batch_count == FORWARD_BATCH_SIZE:
                        # parse_mode="Markdown" alag se add kiya gaya
                        await bot.edit_message_text(
                            f"⏳ Batch of {FORWARD_BATCH_SIZE} done. Pausing for {FORWARD_BATCH_SLEEP}s...\n"
                            f"Forwarded: `{total_forwarded}`\nSkipped: `{total_skipped}`",
                            chat_id=status_msg.chat.id, message_id=status_msg.message_id,
                            parse_mode="Markdown"
                        )
                        await asyncio.sleep(FORWARD_BATCH_SLEEP)
                        batch_count = 0

                except FloodWait as e:
                    # parse_mode="Markdown" alag se add kiya gaya
                    await bot.edit_message_text(f"⏳ FloodWait... sleeping for {e.value}s", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
                    await asyncio.sleep(e.value + 5)
                except Exception as e:
                    print(f"❌ Error forwarding {msg.id}: {e}")
                    await asyncio.sleep(1)

        except Exception as e:
            # Error message ab PLAIN TEXT jayega
            await bot.edit_message_text(
                f"❌ Job Failed:\n{e}",
                chat_id=status_msg.chat.id, message_id=status_msg.message_id,
                parse_mode=None
            )
        else:
            # parse_mode="Markdown" alag se add kiya gaya
            await bot.edit_message_text(
                f"✅ Forwarding Done!\n\nTotal Forwarded: `{total_forwarded}`\nTotal Skipped (Duplicates): `{total_skipped}`",
                chat_id=status_msg.chat.id, message_id=status_msg.message_id,
                parse_mode="Markdown"
            )

async def run_dedupe_job(call):
    if db_pool is None: return await bot.send_message(call.message.chat.id, "❌ DB Not Connected")
    if batch_job_lock.locked(): return await bot.send_message(call.message.chat.id, "⏳ Another job is already running. Please wait.")

    # parse_mode="Markdown" alag se add kiya gaya
    status_msg = await bot.send_message(call.message.chat.id, f"🧹 Cleaning duplicates from @MAZABACKUP01...", parse_mode="Markdown")
    seen_uids = set()
    deleted_count = 0
    batch_count = 0
    
    async with batch_job_lock:
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT file_unique_id FROM target_channel_files")
                seen_uids = {r['file_unique_id'] for r in rows}

            # parse_mode="Markdown" alag se add kiya gaya
            await bot.edit_message_text(
                f"Found `{len(seen_uids)}` unique files in DB. \n⏳ Scanning channel @MAZABACKUP01...",
                chat_id=status_msg.chat.id, message_id=status_msg.message_id,
                parse_mode="Markdown"
            )

            async for msg in app.get_chat_history(TARGET_FORWARD_CHANNEL):
                file_uid = get_file_id(msg)
                if not file_uid: continue

                if file_uid in seen_uids:
                    try:
                        await msg.delete()
                        deleted_count += 1
                        batch_count += 1
                        await asyncio.sleep(BATCH_SLEEP_TIME)
                        
                        if batch_count == 100:
                            # parse_mode="Markdown" alag se add kiya gaya
                            await bot.edit_message_text(
                                f"⏳ Deleted {deleted_count} duplicates... \nPausing for 5s...",
                                chat_id=status_msg.chat.id, message_id=status_msg.message_id,
                                parse_mode="Markdown"
                            )
                            await asyncio.sleep(5)
                            batch_count = 0

                    except FloodWait as e:
                        # parse_mode="Markdown" alag se add kiya gaya
                        await bot.edit_message_text(f"⏳ FloodWait... sleeping for {e.value}s", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
                        await asyncio.sleep(e.value + 5)
                    except Exception as e:
                        print(f"❌ Error deleting {msg.id}: {e}")
                        await asyncio.sleep(1)
                else:
                    seen_uids.add(file_uid)
                    async with db_pool.acquire() as conn:
                        await conn.execute("INSERT INTO target_channel_files (file_unique_id, target_message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", file_uid, msg.id)

        except Exception as e:
            # Error message ab PLAIN TEXT jayega
            await bot.edit_message_text(
                f"❌ Job Failed:\n{e}", 
                chat_id=status_msg.chat.id, message_id=status_msg.message_id,
                parse_mode=None
            )
        else:
            # parse_mode="Markdown" alag se add kiya gaya
            await bot.edit_message_text(
                f"✅ Deduplication Done!\nTotal Deleted: `{deleted_count}`",
                chat_id=status_msg.chat.id, message_id=status_msg.message_id,
                parse_mode="Markdown"
            )
            
# ---------- INDEXER AUTO HANDLER ----------
async def process_post(msg):
    if not db_pool: return
    file_uid = get_file_id(msg)
    if not file_uid: return

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


# ---------- MANUAL COMMANDS / INDEXING LOGIC ----------

async def run_the_index_job():
    if db_pool is None: return "❌ DB Not Connected", -1 
    if batch_job_lock.locked(): return "⏳ Another job is already running", -1
    
    # --- Pre-flight check ---
    try:
        print(f"Checking access to channel: {SOURCE_CHANNEL_ID}")
        await app.get_chat(SOURCE_CHANNEL_ID)
        print(f"✅ Access to {SOURCE_CHANNEL_ID} successful.")
    except Exception as e:
        print(f"❌ Indexing Pre-Check Error: {e}")
        # Yeh message ab PLAIN TEXT ban jayega.
        return f"❌ Error: Could not access channel {SOURCE_CHANNEL_ID}. \n" \
               f"Reason: {e}\n\n" \
               f"👉 Please make sure your bot/account (using SESSION_STRING) is a member of this channel and the ID is correct.", -1
    # --- End of new check ---

    async with batch_job_lock:
        count_new = 0
        try:
            async for m in app.get_chat_history(SOURCE_CHANNEL_ID):
                uid = get_file_id(m)
                if uid:
                    async with db_pool.acquire() as conn:
                        exists = await conn.fetchval("SELECT 1 FROM indexed_movies WHERE file_unique_id=$1", uid)
                        if not exists:
                            await conn.execute("INSERT INTO indexed_movies VALUES ($1,$2) ON CONFLICT DO NOTHING", m.id, uid)
                            count_new += 1
                await asyncio.sleep(0.05) 
        except Exception as e:
            print(f"❌ Indexing Error during history scan: {e}")
            # Yeh message ab PLAIN TEXT ban jayega
            return f"❌ Error during index scan: {e}", -1
        
        # Success message ko Markdown V1-safe banaya
        return f"✅ Indexing Done. Added: `{count_new}` new movies.", count_new

async def run_index_job_for_telebot(call):
    status_msg = None
    try:
        status_msg = await bot.send_message(call.message.chat.id, "⏳ Indexing... Please wait. (Checking channel access first...)")
        result_msg, count = await run_the_index_job()
        
        is_success = result_msg.startswith("✅")

        # Agar success hai toh Markdown, warna PLAIN TEXT
        await bot.edit_message_text(
            result_msg, 
            chat_id=status_msg.chat.id, 
            message_id=status_msg.message_id,
            parse_mode="Markdown" if is_success else None
        )
    
    except Exception as e:
        print(f"❌ Telebot Job Error: {e}")
        if status_msg:
            # Error message ab PLAIN TEXT jayega
            await bot.edit_message_text(
                f"❌ Job failed:\n{e}", 
                chat_id=status_msg.chat.id, 
                message_id=status_msg.message_id, 
                parse_mode=None
            )
        else:
            await bot.send_message(
                call.message.chat.id, 
                f"❌ Job failed:\n{e}",
                parse_mode=None
            )

@app.on_message(filters.command("index") & filters.user(ADMIN_IDS))
async def full_index(client, message):
    status = await message.reply("⏳ Indexing... (Checking channel access first...)")
    result_msg, count = await run_the_index_job()
    
    is_success = result_msg.startswith("✅")
    
    # Agar success hai toh Markdown, warna PLAIN TEXT (DISABLED)
    await status.edit(
        result_msg,
        parse_mode=enums.ParseMode.MARKDOWN if is_success else enums.ParseMode.DISABLED
    )

@app.on_message(filters.command("cleanall") & filters.user(ADMIN_IDS))
async def clean_all(client, message):
    if db_pool is None: return await message.reply("❌ DB Not Connected")
    if batch_job_lock.locked(): return await message.reply("⏳ Another job running")

    async with batch_job_lock:
        status = await message.reply("🧹 Cleaning...")
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT source_message_id FROM indexed_movies")

        for r in rows:
            try:
                msg = await app.get_messages(SOURCE_CHANNEL_ID, r["source_message_id"])
                new_cap = clean_caption(msg.caption) + CAPTION_FOOTER
                await msg.edit_caption(new_cap)
            except MessageNotModified: pass
            except FloodWait as e: await asyncio.sleep(e.value + 5)
            except: pass
            await asyncio.sleep(BATCH_SLEEP_TIME)

        await status.edit("✅ Clean Done!")

@app.on_message(filters.command("refresh") & filters.user(ADMIN_IDS) & filters.chat(SOURCE_CHANNEL_ID))
async def refresh(client, message):
    if not message.reply_to_message: return await message.reply("Reply a movie")
    msg = message.reply_to_message
    new_cap = clean_caption(msg.caption) + CAPTION_FOOTER
    await msg.edit_caption(new_cap)
    await message.reply("✅ Refreshed")

# ---------- ============ WEBHOOK CODE (No changes needed) ============ ----------

WEBHOOK_URL_BASE = "https://maza-cleaner.onrender.com" 
WEBHOOK_URL_PATH = f"/bot/{BOT_TOKEN}"
WEBHOOK_LISTEN = '0.0.0.0'
WEBHOOK_PORT = int(os.environ.get("PORT", 8080))

async def handle_webhook(request):
    try:
        request_body_json = await request.json()
        update = types.Update.de_json(request_body_json)
        asyncio.create_task(bot.process_new_updates([update]))
        return web.Response(status=200)
    except Exception as e:
        print(f"❌ Webhook Error: {e}")
        return web.Response(status=500)

async def web_server():
    app_web = web.Application()
    app_web.router.add_get("/", lambda r: web.Response(text="Bot Alive ✅"))
    app_web.router.add_post(WEBHOOK_URL_PATH, handle_webhook)
    
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, WEBHOOK_LISTEN, WEBHOOK_PORT)
    await site.start()
    
    # =================================================================
    # === ASLI TYPO FIX YAHAN HAI ===
    # 'WEBOK_PORT' ko 'WEBHOOK_PORT' kar diya gaya hai
    # =================================================================
    print(f"✅ Web server started at {WEBHOOK_LISTEN}:{WEBHOOK_PORT}")
    await asyncio.Event().wait()

async def main():
    # ... (Environment variable checks) ...
    for v in [BOT_TOKEN, API_HASH, SESSION_STRING, DATABASE_URL]:
        if not v: print("❌ Missing ENV VARS"); exit(1)
    if not ADMIN_IDS or ADMIN_IDS == [0, 920892710]:
        print("❌ ADMIN_ID environment variable is missing or invalid!"); exit(1)

    await init_database()
    
    print("Starting Pyrogram Client (app) for handlers...")
    await app.start()
    print("✅ Pyrogram Client Running...")

    await bot.remove_webhook()
    await asyncio.sleep(0.5) 
    
    print(f"Setting webhook to: {WEBHOOK_URL_BASE}{WEBHOOK_URL_PATH}") 
    webhook_set = await bot.set_webhook(url=f"{WEBHOOK_URL_BASE}{WEBHOOK_URL_PATH}")
    
    if webhook_set:
        print("✅ Telebot Webhook Set!")
    else:
        print("❌❌ FAILED TO SET WEBHOOK! Check URL and BOT_TOKEN. ❌❌")
        await app.stop()
        exit(1)

    await web_server()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopping...")
        asyncio.run(app.stop())
        print("Pyrogram client stopped.")
