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
    await bot.reply_to(message,
        "👋 *Admin Menu*",
        reply_markup=get_main_menu()
    )

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
        await bot.send_message(
            call.message.chat.id, 
            f"📊 **Database Stats**\n\n"
            f"Source Channel Movies: `{total_source}`\n"
            f"Target Channel (@MAZABACKUP01) Movies: `{total_target}`"
        )

    elif call.data == "info_index":
        await bot.answer_callback_query(call.id, "🚀 Starting Full Index...")
        asyncio.create_task(run_index_job_for_telebot(call))

    # NEW: Forwarder Button Handler
    elif call.data == "fwd_channel_start":
        await bot.answer_callback_query(call.id)
        await bot.send_message(
            call.message.chat.id, 
            "REPLY TO THIS MESSAGE:\n\n"
            "Please send the **Source Channel ID** or **Username** (e.g., -100... or @channelname):", 
            reply_markup=types.ForceReply(selective=True)
        )

    # NEW: Dedupe Button Handler
    elif call.data == "dedupe_target_start":
        await bot.answer_callback_query(call.id, "🚀 Starting Duplicate Cleanup for @MAZABACKUP01...")
        asyncio.create_task(run_dedupe_job(call))

    elif call.data == "info_clean":
        await bot.send_message(call.message.chat.id, "Go to *Saved Messages* and send `/cleanall`")

    elif call.data == "info_refresh":
        await bot.send_message(call.message.chat.id, "Reply a movie in channel & send `/refresh`")

# NEW: Handler for ForceReply (Getting Channel ID)
@bot.message_handler(
    func=lambda m: m.reply_to_message 
                 and "Please send the **Source Channel ID**" in m.reply_to_message.text 
                 and m.from_user.id in ADMIN_IDS,
    content_types=['text']
)
async def handle_forward_source(message):
    source_chat_id = message.text.strip()
    
    # Basic validation
    if not (source_chat_id.startswith('@') or source_chat_id.startswith('-100')):
        await bot.reply_to(message, "Invalid format. Must be `@username` or `-100...`")
        return
        
    if source_chat_id.startswith('-100'):
        try:
            source_chat_id = int(source_chat_id)
        except ValueError:
            await bot.reply_to(message, "Invalid Channel ID. Must be a number.")
            return
    
    await bot.reply_to(message, f"Got it. Starting to forward from `{source_chat_id}` to @MAZABACKUP01...")
    # Pass the 'message' object to send status updates
    asyncio.create_task(run_forwarding_job(message, source_chat_id))

# NEW: Forwarding Job Logic
async def run_forwarding_job(message, source_chat_id):
    if db_pool is None:
        return await bot.send_message(message.chat.id, "❌ DB Not Connected")
    
    if batch_job_lock.locked():
        return await bot.send_message(message.chat.id, "⏳ Another job is already running. Please wait.")

    status_msg = await bot.send_message(message.chat.id, f"⏳ Starting forward job from `{source_chat_id}`...")
    
    total_forwarded = 0
    total_skipped = 0
    batch_count = 0
    
    async with batch_job_lock:
        try:
            # First, preload existing file IDs from the target DB
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT file_unique_id FROM target_channel_files")
                existing_uids = {r['file_unique_id'] for r in rows}
            
            await bot.edit_message_text(
                f"Found `{len(existing_uids)}` movies already in @MAZABACKUP01 (DB). \n"
                f"⏳ Starting scan of `{source_chat_id}`...",
                chat_id=status_msg.chat.id, message_id=status_msg.message_id
            )

            async for msg in app.get_chat_history(source_chat_id):
                file_uid = get_file_id(msg)
                if not file_uid:
                    continue

                if file_uid in existing_uids:
                    total_skipped += 1
                    continue
                
                try:
                    fwded_msg = await msg.forward(TARGET_FORWARD_CHANNEL)
                    # Add to our local set and DB
                    existing_uids.add(file_uid)
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO target_channel_files (file_unique_id, target_message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                            file_uid, fwded_msg.id
                        )
                    
                    total_forwarded += 1
                    batch_count += 1
                    
                    if total_forwarded % 20 == 0: # Update status every 20 movies
                        await bot.edit_message_text(
                            f"⏳ Progress...\n"
                            f"Forwarded: `{total_forwarded}`\n"
                            f"Skipped: `{total_skipped}`",
                            chat_id=status_msg.chat.id, message_id=status_msg.message_id
                        )
                    
                    await asyncio.sleep(FORWARD_MSG_SLEEP) # 2 sec sleep between messages
                    
                    if batch_count == FORWARD_BATCH_SIZE:
                        await bot.edit_message_text(
                            f"⏳ Batch of {FORWARD_BATCH_SIZE} done. Pausing for {FORWARD_BATCH_SLEEP}s...\n"
                            f"Forwarded: `{total_forwarded}`\n"
                            f"Skipped: `{total_skipped}`",
                            chat_id=status_msg.chat.id, message_id=status_msg.message_id
                        )
                        await asyncio.sleep(FORWARD_BATCH_SLEEP) # 7 sec batch sleep
                        batch_count = 0

                except FloodWait as e:
                    await bot.edit_message_text(
                        f"⏳ FloodWait... sleeping for {e.value}s",
                        chat_id=status_msg.chat.id, message_id=status_msg.message_id
                    )
                    await asyncio.sleep(e.value + 5) # Sleep for FloodWait time + 5s
                except Exception as e:
                    print(f"❌ Error forwarding {msg.id}: {e}")
                    await asyncio.sleep(1) # short pause on other errors

        except Exception as e:
            await bot.edit_message_text(
                f"❌ Job Failed: {e}",
                chat_id=status_msg.chat.id, message_id=status_msg.message_id
            )
        else:
            await bot.edit_message_text(
                f"✅ Forwarding Done!\n\n"
                f"Total Forwarded: `{total_forwarded}`\n"
                f"Total Skipped (Duplicates): `{total_skipped}`",
                chat_id=status_msg.chat.id, message_id=status_msg.message_id
            )

# NEW: Deduplication Job Logic
async def run_dedupe_job(call):
    if db_pool is None:
        return await bot.send_message(call.message.chat.id, "❌ DB Not Connected")

    if batch_job_lock.locked():
        return await bot.send_message(call.message.chat.id, "⏳ Another job is already running. Please wait.")

    status_msg = await bot.send_message(call.message.chat.id, f"🧹 Cleaning duplicates from @MAZABACKUP01...")
    
    seen_uids = set()
    deleted_count = 0
    batch_count = 0
    
    async with batch_job_lock:
        try:
            # We will scan the channel and keep the FIRST one we see.
            # We also preload the DB to avoid deleting "good" copies.
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT file_unique_id FROM target_channel_files")
                seen_uids = {r['file_unique_id'] for r in rows}

            await bot.edit_message_text(
                f"Found `{len(seen_uids)}` unique files in DB. \n"
                f"⏳ Scanning channel @MAZABACKUP01...",
                chat_id=status_msg.chat.id, message_id=status_msg.message_id
            )

            async for msg in app.get_chat_history(TARGET_FORWARD_CHANNEL):
                file_uid = get_file_id(msg)
                if not file_uid:
                    continue

                if file_uid in seen_uids:
                    # This is a duplicate. Delete it.
                    try:
                        await msg.delete()
                        deleted_count += 1
                        batch_count += 1
                        await asyncio.sleep(BATCH_SLEEP_TIME) # Use existing 2s sleep
                        
                        if batch_count == 100:
                            await bot.edit_message_text(
                                f"⏳ Deleted {deleted_count} duplicates... \n"
                                f"Pausing for 5s...",
                                chat_id=status_msg.chat.id, message_id=status_msg.message_id
                            )
                            await asyncio.sleep(5) # 5 sec batch sleep
                            batch_count = 0

                    except FloodWait as e:
                        await bot.edit_message_text(
                            f"⏳ FloodWait... sleeping for {e.value}s",
                            chat_id=status_msg.chat.id, message_id=status_msg.message_id
                        )
                        await asyncio.sleep(e.value + 5)
                    except Exception as e:
                        print(f"❌ Error deleting {msg.id}: {e}")
                        await asyncio.sleep(1)
                else:
                    # This is a new file not in our DB. Add it to seen list.
                    seen_uids.add(file_uid)
                    # Also add to DB so we don't delete it next time
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO target_channel_files (file_unique_id, target_message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                            file_uid, msg.id
                        )

        except Exception as e:
            await bot.edit_message_text(
                f"❌ Job Failed: {e}",
                chat_id=status_msg.chat.id, message_id=status_msg.message_id
            )
        else:
            await bot.edit_message_text(
                f"✅ Deduplication Done!\n"
                f"Total Deleted: `{deleted_count}`",
                chat_id=status_msg.chat.id, message_id=status_msg.message_id
            )
            
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


# ---------- MANUAL COMMANDS / INDEXING LOGIC ----------

async def run_the_index_job():
    if db_pool is None:
        return "❌ DB Not Connected", -1 # Error message, count

    if batch_job_lock.locked():
        return "⏳ Another job is already running", -1

    # --- NEW: Pre-flight check ---
    try:
        # Try to get chat info first. This fails faster if ID is bad.
        print(f"Checking access to channel: {SOURCE_CHANNEL_ID}")
        await app.get_chat(SOURCE_CHANNEL_ID)
        print(f"✅ Access to {SOURCE_CHANNEL_ID} successful.")
    except Exception as e:
        print(f"❌ Indexing Pre-Check Error: {e}")
        # Yahaan behtar error message dein
        return f"❌ Error: Could not access channel `{SOURCE_CHANNEL_ID}`. \n" \
               f"Reason: `{e}`\n\n" \
               f"👉 **Please make sure your bot/account (using SESSION_STRING) is a member of this channel and the ID is correct.**", -1
    # --- End of new check ---

    async with batch_job_lock:
        count_new = 0
        try:
            # This should be safe now if the check above passed
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
            # This is now a fallback error
            print(f"❌ Indexing Error during history scan: {e}")
            return f"❌ Error during index scan: {e}", -1
        
        return f"✅ Indexing Done. Added: `{count_new}` new movies.", count_new

async def run_index_job_for_telebot(call):
    status_msg = None
    try:
        status_msg = await bot.send_message(call.message.chat.id, "⏳ Indexing... Please wait. (Checking channel access first...)")
        result_msg, count = await run_the_index_job()
        await bot.edit_message_text(result_msg, chat_id=status_msg.chat.id, message_id=status_msg.message_id)
    
    except Exception as e:
        print(f"❌ Telebot Job Error: {e}")
        if status_msg:
            await bot.edit_message_text(f"❌ Job failed: {e}", chat_id=status_msg.chat.id, message_id=status_msg.message_id)
        else:
            await bot.send_message(call.message.chat.id, f"❌ Job failed: {e}")

@app.on_message(filters.command("index") & filters.user(ADMIN_IDS))
async def full_index(client, message):
    status = await message.reply("⏳ Indexing... (Checking channel access first...)")
    result_msg, count = await run_the_index_job()
    await status.edit(result_msg)


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
                await asyncio.sleep(e.value + 5)
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

# ---------- ============ NEW WEBHOOK CODE ============ ----------

# Yahan apna Render URL daalein. Aapke log se mila:
WEBHOOK_URL_BASE = "https://maza-cleaner.onrender.com" 
WEBHOOK_URL_PATH = f"/bot/{BOT_TOKEN}"
WEBHOOK_LISTEN = '0.0.0.0'
WEBHOOK_PORT = int(os.environ.get("PORT", 8080))

async def handle_webhook(request):
    """
    Telebot updates ko process karne ke liye Webhook handler.
    """
    try:
        request_body_json = await request.json()
        update = types.Update.de_json(request_body_json)
        asyncio.create_task(bot.process_new_updates([update]))
        return web.Response(status=200)
    except Exception as e:
        print(f"❌ Webhook Error: {e}")
        return web.Response(status=500)

async def web_server():
    """
    Web server jo health check ('/') aur bot webhook ('/bot/TOKEN') sunega.
    """
    app_web = web.Application()
    app_web.router.add_get("/", lambda r: web.Response(text="Bot Alive ✅"))
    app_web.router.add_post(WEBHOOK_URL_PATH, handle_webhook)
    
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, WEBHOOK_LISTEN, WEBHOOK_PORT)
    await site.start()
    
    print(f"✅ Web server started at {WEBHOOK_LISTEN}:{WEBHOOK_PORT}")
    await asyncio.Event().wait() # Server ko hamesha chalu rakhein

async def main():
    for v in [BOT_TOKEN, API_HASH, SESSION_STRING, DATABASE_URL]:
        if not v:
            print("❌ Missing ENV VARS")
            exit(1)
            
    if not ADMIN_IDS or ADMIN_IDS == [0, 920892710]:
        print("❌ ADMIN_ID environment variable is missing or invalid!")
        exit(1)

    # 1. Database shuru karein
    await init_database()

    # 2. Pyrogram Client (app) shuru karein
    await app.start()
    print("✅ Pyrogram Client Running...")

    # 3. Telebot (bot) ke liye Webhook set karein
    await bot.remove_webhook()
    await asyncio.sleep(0.5) 
    webhook_set = await bot.set_webhook(url=f"{WEBHOOK_URL_BASE}{WEBHOOK_URL_PATH}")
    
    if webhook_set:
        print("✅ Telebot Webhook Set!")
    else:
        print("❌❌ FAILED TO SET WEBHOOK! ❌❌")
        exit(1)

    # 4. Web Server shuru karein (Yeh 'bot.polling' ki jagah lega)
    await web_server()

if __name__ == "__main__":
    asyncio.run(main())
