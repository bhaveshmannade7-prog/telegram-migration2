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
from pyrogram.errors import FloodWait, MessageNotModified, ChatAdminRequired, PeerIdInvalid, UserIsBlocked

# --- CONFIG (ENV VARS) ---
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MAIN_ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
ADMIN_IDS = [MAIN_ADMIN_ID, 920892710] 
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
SOURCE_CHANNEL_ID = int(os.environ.get("SOURCE_CHANNEL_ID", 0))
TARGET_FORWARD_CHANNEL = int(os.environ.get("TARGET_FORWARD_CHANNEL", -1002417767287))
FORWARD_BATCH_SIZE = 100
FORWARD_BATCH_SLEEP = 7
FORWARD_MSG_SLEEP = 2
CAPTION_FOOTER = "\n\n@THEGREATMOVIESL9\n@MOVIEMAZASU"
USERNAME_WHITELIST = ["@THEGREATMOVIESL9", "@MOVIEMAZASU"]
BLACKLIST_WORDS = ["18+", "adult", "hot", "sexy"]
CLEAN_BATCH_SIZE = 150
FOOTER_BATCH_SIZE = 100
DEDUPE_BATCH_SLEEP = 5
BATCH_SLEEP_TIME = 2

db_pool = None
batch_job_lock = asyncio.Lock()

# 'PARSE ENTITIES' WALA FIX: Default parse_mode ko hata diya gaya hai.
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
            
            # Table for Target Channel
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

# NEW: Updated Main Menu with all features
def get_main_menu():
    m = types.InlineKeyboardMarkup(row_width=2)
    m.add(
        types.InlineKeyboardButton("📊 Stats", callback_data="show_stats"),
        types.InlineKeyboardButton("🐞 Debug Session", callback_data="run_whoami")
    )
    m.add(
        types.InlineKeyboardButton("⏳ Full Index Channel", callback_data="info_index")
    )
    m.add(
        types.InlineKeyboardButton("♻️ Dedupe Channel", callback_data="dedupe_channel_start"),
        types.InlineKeyboardButton("🧹 Clean Captions", callback_data="clean_captions_start")
    )
    m.add(
        types.InlineKeyboardButton("➕ Add Footer", callback_data="add_footer_start"),
        types.InlineKeyboardButton("➡️ Forwarder", callback_data="fwd_channel_start")
    )
    m.add(
        types.InlineKeyboardButton("🔄 Refresh (Pyrogram)", callback_data="info_refresh_pyro")
    )
    return m

# ---------- TELEBOT ----------
@bot.message_handler(commands=['start', 'help'])
async def start_cmd(message):
    if message.from_user.id not in ADMIN_IDS:
        return await bot.reply_to(message, "⛔ Not Authorized")
    
    await bot.reply_to(message,
        "👋 *Admin Menu*\n\n"
        "Agar bot kaam nahi kar raha hai, toh sabse pehle 'Debug Session' button dabayein "
        "yeh check karne ke liye ki `SESSION_STRING` valid hai ya nahi.",
        reply_markup=get_main_menu(),
        parse_mode="Markdown" 
    )

# =================================================================
# === DIAGNOSTIC COMMAND (AB 100% CRASH-PROOF) ===
# =================================================================
@bot.callback_query_handler(func=lambda c: c.data == "run_whoami")
async def whoami_cb(call):
    if call.from_user.id not in ADMIN_IDS:
        return await bot.answer_callback_query(call.id, "⛔ Not Allowed", show_alert=True)

    await bot.answer_callback_query(call.id, "⏳ Checking SESSION_STRING...")
    status_msg = await bot.send_message(call.message.chat.id, "⏳ Fetching SESSION_STRING/API details...")
    
    try:
        # Koshish karein client (app) ko chalu karne ki
        me = await app.get_me() 
        response_text = (
            f"✅ **SESSION_STRING VALID** ✅\n\n"
            f"First Name: `{me.first_name or 'N/A'}`\n"
            f"Username: `@{me.username or 'N/A'}`\n"
            f"User ID: `{me.id}`\n\n"
            f"👉 Yeh account (`@{me.username or 'N/A'}`) "
            f"`{SOURCE_CHANNEL_ID}` channel mein **Admin** hona chahiye."
        )
        await bot.edit_message_text(response_text, chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
        
    except Exception as e:
        # =================================================
        # === AAKHRI FIX (NUCLEAR OPTION) ===
        # Hum `e` (error) ko bhej hi nahi rahe hain.
        # Hum ek 100% safe, hardcoded message bhejenge.
        # =================================================
        print(f"❌ PYROGRAM/API ERROR in whoami_cb: {e}") # Asli error yahaan log karein

        hardcoded_error_message = (
            "❌ SESSION STRING / API ERROR ❌\n\n"
            "Pyrogram client start nahi ho pa raha hai.\n\n"
            "👉 Iska 99% kaaran hai:\n"
            "1. SESSION_STRING galat ya expired hai.\n"
            "2. API_ID galat hai.\n"
            "3. API_HASH galat hai.\n\n"
            "Kripya Render mein teeno environment variables ko check karein aur bot ko restart karein."
        )
        
        await bot.edit_message_text(
            hardcoded_error_message,
            chat_id=status_msg.chat.id, 
            message_id=status_msg.message_id,
            parse_mode=None # Force plain text
        )
# =================================================================
# === /whoami COMMAND END ===
# =================================================================

# BAAKI SABHI HANDLERS
@bot.callback_query_handler(func=lambda c: True)
async def cb(call):
    if call.from_user.id not in ADMIN_IDS:
        return await bot.answer_callback_query(call.id, "⛔ Not Allowed", show_alert=True)
    data = call.data
    if data == "run_whoami": return # Upar handle ho gaya
    elif data == "show_stats":
        # ... (baaki code waisa hi) ...
        total_source = 0
        total_target = 0
        if db_pool:
            async with db_pool.acquire() as conn:
                total_source = await conn.fetchval("SELECT COUNT(*) FROM indexed_movies")
                total_target = await conn.fetchval("SELECT COUNT(*) FROM target_channel_files")
        await bot.send_message(call.message.chat.id, f"📊 **Database Stats**\n\nSource Channel Movies: `{total_source}`\nTarget Channel Movies: `{total_target}`", parse_mode="Markdown")
    elif data == "info_index":
        await bot.answer_callback_query(call.id, "🚀 Starting Full Index...")
        asyncio.create_task(run_index_job_for_telebot(call))
    elif data == "fwd_channel_start":
        await bot.answer_callback_query(call.id)
        await bot.send_message(call.message.chat.id, "REPLY TO THIS MESSAGE:\n\nPlease send the **Source Channel ID** or **Username** (jahaan se forward karna hai):", reply_markup=types.ForceReply(selective=True), parse_mode="Markdown")
    elif data == "dedupe_channel_start":
        await bot.answer_callback_query(call.id, "🚀 Starting Duplicate Cleanup...")
        asyncio.create_task(run_dedupe_job(call))
    elif data == "clean_captions_start":
        await bot.answer_callback_query(call.id, "🚀 Starting Caption Cleaning...")
        asyncio.create_task(run_clean_all_job(call))
    elif data == "add_footer_start":
        await bot.answer_callback_query(call.id)
        await bot.send_message(call.message.chat.id, "REPLY TO THIS MESSAGE:\n\nPlease send the **Footer Text** you want to add (links aur @username allowed hain):", reply_markup=types.ForceReply(selective=True))
    elif data == "info_refresh_pyro":
        await bot.send_message(call.message.chat.id, "Yeh command ab Pyrogram se chalta hai.\nChannel mein jaakar kisi movie ko reply karein aur `/refresh` send karein.", parse_mode="Markdown")

# ---------- REPLY HANDLERS (waisa hi) ----------
@bot.message_handler(func=lambda m: m.reply_to_message and "jahaan se forward karna hai" in m.reply_to_message.text and m.from_user.id in ADMIN_IDS, content_types=['text'])
async def handle_forward_source(message):
    source_chat_id = message.text.strip()
    if not (source_chat_id.startswith('@') or source_chat_id.startswith('-100')):
        await bot.reply_to(message, "Invalid format. Must be `@username` or `-100...`")
        return
    if source_chat_id.startswith('-100'):
        try: source_chat_id = int(source_chat_id)
        except ValueError: await bot.reply_to(message, "Invalid Channel ID. Must be a number."); return
    await bot.reply_to(message, f"Got it. Starting to forward from `{source_chat_id}` to `{TARGET_FORWARD_CHANNEL}`...", parse_mode="Markdown")
    asyncio.create_task(run_forwarding_job(message, source_chat_id))

@bot.message_handler(func=lambda m: m.reply_to_message and "Please send the **Footer Text**" in m.reply_to_message.text and m.from_user.id in ADMIN_IDS, content_types=['text'])
async def handle_footer_text(message):
    footer_text = message.text.strip()
    if not footer_text: await bot.reply_to(message, "Footer text cannot be empty."); return
    await bot.reply_to(message, f"Got it. Adding this footer to all messages in `{SOURCE_CHANNEL_ID}`:\n\n{footer_text}")
    asyncio.create_task(run_add_footer_job(message, footer_text))

# =================================================================
# === JOB FUNCTIONS (Error handling fix) ===
# =================================================================

# Hardcoded error message function (taaki code repeat na ho)
def get_hardcoded_error_message(job_name: str, e: Exception) -> str:
    print(f"❌ ERROR in {job_name}: {e}") # Asli error log karein
    return (
        f"❌ {job_name} Job Fail Hua ❌\n\n"
        "Pyrogram client fail ho gaya.\n\n"
        "👉 Please 'Debug Session' button daba kar check karein ki "
        "SESSION_STRING, API_ID, aur API_HASH sahi hain ya nahi."
    )

async def run_add_footer_job(message, footer_text):
    if db_pool is None: return await bot.send_message(message.chat.id, "❌ DB Not Connected")
    if batch_job_lock.locked(): return await bot.send_message(message.chat.id, "⏳ Another job is already running. Please wait.")
    status_msg = await bot.send_message(message.chat.id, f"⏳ Starting Footer Add job for `{SOURCE_CHANNEL_ID}`...", parse_mode="Markdown")
    total_processed = 0; total_failed = 0
    async with batch_job_lock:
        try:
            batch_count = 0
            async for msg in app.get_chat_history(SOURCE_CHANNEL_ID):
                if not (msg.video or msg.document): continue
                try:
                    current_caption = msg.caption or ""
                    if footer_text not in current_caption:
                        new_cap = current_caption.strip() + "\n\n" + footer_text
                        await msg.edit_caption(new_cap)
                        await asyncio.sleep(BATCH_SLEEP_TIME)
                    total_processed += 1; batch_count += 1
                    if batch_count == FOOTER_BATCH_SIZE:
                        await bot.edit_message_text(f"⏳...Processed {total_processed} movies... Pausing for {FORWARD_BATCH_SLEEP}s...", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
                        await asyncio.sleep(FORWARD_BATCH_SLEEP); batch_count = 0
                except (FloodWait, MessageNotModified) as e:
                    if isinstance(e, FloodWait): await asyncio.sleep(e.value + 5)
                    pass 
                except Exception as e:
                    print(f"❌ Error adding footer to {msg.id}: {e}"); total_failed += 1; await asyncio.sleep(1)
        except Exception as e:
            error_msg = get_hardcoded_error_message("Add Footer", e)
            await bot.edit_message_text(error_msg, chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode=None)
        else:
            await bot.edit_message_text(f"✅ Footer Add Done!\n\nProcessed: `{total_processed}`\nFailed: `{total_failed}`", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")

async def run_clean_all_job(call):
    if db_pool is None: return await bot.send_message(call.message.chat.id, "❌ DB Not Connected")
    if batch_job_lock.locked(): return await bot.send_message(call.message.chat.id, "⏳ Another job is already running. Please wait.")
    status_msg = await bot.send_message(call.message.chat.id, f"🧹 Starting Powerful Caption Cleaning for `{SOURCE_CHANNEL_ID}`... Batch size: {CLEAN_BATCH_SIZE}", parse_mode="Markdown")
    total_processed = 0; total_cleaned = 0
    async with batch_job_lock:
        try:
            batch_count = 0
            async for msg in app.get_chat_history(SOURCE_CHANNEL_ID):
                if not (msg.video or msg.document): continue
                total_processed += 1
                current_caption = msg.caption or ""
                cleaned_caption = clean_caption(current_caption) 
                if current_caption != cleaned_caption:
                    try:
                        await msg.edit_caption(cleaned_caption); total_cleaned += 1; batch_count += 1
                        await asyncio.sleep(BATCH_SLEEP_TIME)
                        if batch_count == CLEAN_BATCH_SIZE:
                            await bot.edit_message_text(f"⏳...Cleaned {total_cleaned} (scanned {total_processed})... Pausing for {FORWARD_BATCH_SLEEP}s...", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
                            await asyncio.sleep(FORWARD_BATCH_SLEEP); batch_count = 0
                    except (FloodWait, MessageNotModified) as e:
                        if isinstance(e, FloodWait): await asyncio.sleep(e.value + 5)
                        pass
                    except Exception as e:
                        print(f"❌ Error cleaning {msg.id}: {e}"); await asyncio.sleep(1)
        except Exception as e:
            error_msg = get_hardcoded_error_message("Clean Captions", e)
            await bot.edit_message_text(error_msg, chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode=None)
        else:
            await bot.edit_message_text(f"✅ Caption Cleaning Done!\n\nScanned: `{total_processed}`\nCaptions Cleaned: `{total_cleaned}`", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")

# =================================================================
# === DEDUPE JOB (Problem 1 Fix) ===
# =================================================================
async def run_dedupe_job(call):
    if db_pool is None: return await bot.send_message(call.message.chat.id, "❌ DB Not Connected")
    if batch_job_lock.locked(): return await bot.send_message(call.message.chat.id, "⏳ Another job is already running. Please wait.")
    status_msg = await bot.send_message(call.message.chat.id, f"♻️ Starting Dedupe job for `{SOURCE_CHANNEL_ID}`...", parse_mode="Markdown")
    total_deleted = 0
    conn = None # Connection ko bahar define karein taaki except block mein use kar sakein
    try:
        async with batch_job_lock:
            conn = await db_pool.acquire() # Pool se connection lein
            await bot.edit_message_text("♻️ Scanning DB for duplicates...", chat_id=status_msg.chat.id, message_id=status_msg.message_id)
            query = """
                SELECT file_unique_id, array_agg(source_message_id ORDER BY source_message_id) as ids 
                FROM indexed_movies GROUP BY file_unique_id HAVING COUNT(*) > 1
            """
            duplicates = await conn.fetch(query)
            
            if not duplicates:
                await bot.edit_message_text("✅ Koi duplicates nahi mile!", chat_id=status_msg.chat.id, message_id=status_msg.message_id)
                await db_pool.release(conn) # <-- FIX 1: Sahi tareeke se release karein
                conn = None # Release ho gaya, None set karein
                return

            await bot.edit_message_text(f"♻️ Found `{len(duplicates)}` files with duplicates. Deleting extras...", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
            
            for i, row in enumerate(duplicates):
                ids = list(row['ids']); ids_to_keep = ids.pop(0); ids_to_delete = ids      
                try:
                    await app.delete_messages(chat_id=SOURCE_CHANNEL_ID, message_ids=ids_to_delete)
                    await conn.execute("DELETE FROM indexed_movies WHERE source_message_id = ANY($1::bigint[])", ids_to_delete)
                    total_deleted += len(ids_to_delete)
                    if i % 20 == 0: 
                        await bot.edit_message_text(f"♻️ Deleted `{total_deleted}` duplicate messages...\nProcessing batch {i+1}/{len(duplicates)}...", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
                    await asyncio.sleep(DEDUPE_BATCH_SLEEP) 
                except FloodWait as e:
                    await bot.edit_message_text(f"⏳ FloodWait... sleeping for {e.value}s", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
                    await asyncio.sleep(e.value + 10)
                except Exception as e:
                    print(f"❌ Error deleting {ids_to_delete}: {e}"); await asyncio.sleep(1)

            await db_pool.release(conn) # <-- FIX 2: Sahi tareeke se release karein
            conn = None # Release ho gaya, None set karein
            
            await bot.edit_message_text(f"✅ Deduplication Done!\nTotal Deleted: `{total_deleted}`", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")

    except Exception as e:
        if conn: 
            await db_pool.release(conn) # <-- FIX 3: Error aane par bhi release karein
        error_msg = get_hardcoded_error_message("Dedupe", e)
        await bot.edit_message_text(error_msg, chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode=None)

async def run_forwarding_job(message, source_chat_id):
    if db_pool is None: return await bot.send_message(message.chat.id, "❌ DB Not Connected")
    if batch_job_lock.locked(): return await bot.send_message(message.chat.id, "⏳ Another job is already running. Please wait.")
    status_msg = await bot.send_message(message.chat.id, f"⏳ Starting forward job from `{source_chat_id}`...", parse_mode="Markdown")
    total_forwarded = 0; total_skipped = 0
    async with batch_job_lock:
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT file_unique_id FROM target_channel_files")
                existing_uids = {r['file_unique_id'] for r in rows}
            await bot.edit_message_text(f"Found `{len(existing_uids)}` movies already in target (DB). \n⏳ Starting scan of `{source_chat_id}`...", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
            batch_count = 0
            async for msg in app.get_chat_history(source_chat_id):
                file_uid = get_file_id(msg)
                if not file_uid: continue
                if file_uid in existing_uids: total_skipped += 1; continue
                try:
                    fwded_msg = await msg.forward(TARGET_FORWARD_CHANNEL)
                    existing_uids.add(file_uid)
                    async with db_pool.acquire() as conn:
                        await conn.execute("INSERT INTO target_channel_files (file_unique_id, target_message_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", file_uid, fwded_msg.id)
                    total_forwarded += 1; batch_count += 1
                    if total_forwarded % 20 == 0:
                        await bot.edit_message_text(f"⏳ Progress...\nForwarded: `{total_forwarded}`\nSkipped: `{total_skipped}`", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
                    await asyncio.sleep(FORWARD_MSG_SLEEP)
                    if batch_count == FORWARD_BATCH_SIZE:
                        await bot.edit_message_text(f"⏳ Batch of {FORWARD_BATCH_SIZE} done. Pausing for {FORWARD_BATCH_SLEEP}s...\nForwarded: `{total_forwarded}`\nSkipped: `{total_skipped}`", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
                        await asyncio.sleep(FORWARD_BATCH_SLEEP); batch_count = 0
                except FloodWait as e:
                    await asyncio.sleep(e.value + 5)
                except Exception as e:
                    print(f"❌ Error forwarding {msg.id}: {e}")
        except Exception as e:
            error_msg = get_hardcoded_error_message("Forwarding", e)
            await bot.edit_message_text(error_msg, chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode=None)
        else:
            await bot.edit_message_text(f"✅ Forwarding Done!\n\nTotal Forwarded: `{total_forwarded}`\nTotal Skipped: `{total_skipped}`", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
            
# ---------- INDEXER AUTO HANDLER (waisa hi) ----------
async def process_post(msg):
    if not db_pool: return
    file_uid = get_file_id(msg)
    if not file_uid: return
    lower = (msg.caption or "").lower()
    for w in BLACKLIST_WORDS:
        if w in lower:
            try: await msg.delete(); print(f"🚫 Blacklisted Removed: {msg.id}")
            except: pass; return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("INSERT INTO indexed_movies VALUES ($1,$2) ON CONFLICT (source_message_id) DO NOTHING", msg.id, file_uid)
            print(f"✅ Auto-Indexed: {msg.id}")
    except Exception as e:
        print(f"❌ Auto-Index DB Error: {e}")

@app.on_message(filters.chat(SOURCE_CHANNEL_ID) & (filters.video | filters.document) & filters.channel)
async def new_post(client, message):
    await process_post(message)

# ---------- FULL INDEX JOB (Error handling fix) ----------
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
        # Yahaan hum 'e' bhej sakte hain kyunki yeh 'run_index_job_for_telebot' mein handle hoga
        return f"❌ Error: Could not access channel {SOURCE_CHANNEL_ID}. \n" \
               f"Reason: {e}\n\n" \
               f"👉 Please make sure your bot/account (using SESSION_STRING, API_ID, API_HASH) is correct and is a member/admin of this channel.", -1

    async with batch_job_lock:
        count_new = 0
        try:
            async for m in app.get_chat_history(SOURCE_CHANNEL_ID):
                uid = get_file_id(m)
                if uid:
                    async with db_pool.acquire() as conn:
                        result = await conn.execute("INSERT INTO indexed_movies (source_message_id, file_unique_id) VALUES ($1, $2) ON CONFLICT (source_message_id) DO NOTHING", m.id, uid)
                        if result.endswith("1"): count_new += 1
                await asyncio.sleep(0.05) 
        except Exception as e:
            print(f"❌ Indexing Error during history scan: {e}")
            return f"❌ Error during index scan: {e}", -1
        
        return f"✅ Indexing Done. Added/Checked all messages. `{count_new}` new movies added to DB.", count_new

async def run_index_job_for_telebot(call):
    status_msg = None
    try:
        status_msg = await bot.send_message(call.message.chat.id, "⏳ Indexing... Please wait. (Checking channel access first...)")
        result_msg, count = await run_the_index_job()
        is_success = result_msg.startswith("✅")
        
        if is_success:
            await bot.edit_message_text(result_msg, chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode="Markdown")
        else:
            # Error aaya, hardcoded message istemaal karein
            # Asli error 'result_msg' mein hai, use log karein
            print(f"❌ FULL INDEX JOB FAILED: {result_msg}")
            hardcoded_error = get_hardcoded_error_message("Full Index", Exception(result_msg))
            await bot.edit_message_text(hardcoded_error, chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode=None)
    
    except Exception as e:
        # Yeh tabhi aayega agar 'bot.send_message' fail ho
        print(f"❌ Telebot Job Error: {e}")
        if status_msg:
            await bot.edit_message_text(f"❌ Job failed:\n{e}", chat_id=status_msg.chat.id, message_id=status_msg.message_id, parse_mode=None)

# --- PYROGRAM-ONLY COMMANDS (waisa hi) ---
@app.on_message(filters.command("refresh") & filters.user(ADMIN_IDS) & filters.chat(SOURCE_CHANNEL_ID))
async def refresh(client, message):
    if not message.reply_to_message: return await message.reply("Reply a movie")
    msg = message.reply_to_message
    try:
        new_cap = clean_caption(msg.caption); await msg.edit_caption(new_cap)
        await message.reply("✅ Refreshed (Cleaned caption)")
    except Exception as e: await message.reply(f"❌ Error: {e}")

@app.on_message(filters.command("id") & filters.user(ADMIN_IDS))
async def get_id_cmd(client, message):
    chat_id = message.chat.id
    text = f"Chat ID: `{chat_id}`\n"
    if message.reply_to_message:
        text += f"Replied User ID: `{message.reply_to_message.from_user.id}`\n"
        if message.reply_to_message.forward_from_chat:
            text += f"Forwarded Channel ID: `{message.reply_to_message.forward_from_chat.id}`"
    await message.reply(text, parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("del") & filters.user(ADMIN_IDS) & filters.chat(SOURCE_CHANNEL_ID))
async def del_cmd(client, message):
    if not message.reply_to_message: return await message.reply("Delete karne ke liye message ko reply karein")
    try:
        await message.reply_to_message.delete(); await message.delete()
    except Exception as e: await message.reply(f"❌ Error: {e}")

# ---------- ============ WEBHOOK CODE (waisa hi) ============ ----------
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
    print(f"✅ Web server started at {WEBHOOK_LISTEN}:{WEBHOOK_PORT}")
    await asyncio.Event().wait()

async def main():
    print("Checking ENV VARS...")
    for v_name, v_val in [
        ("BOT_TOKEN", BOT_TOKEN), ("API_ID", API_ID), ("API_HASH", API_HASH), 
        ("SESSION_STRING", SESSION_STRING), ("DATABASE_URL", DATABASE_URL), ("SOURCE_CHANNEL_ID", SOURCE_CHANNEL_ID)
    ]:
        if not v_val or (isinstance(v_val, int) and v_val == 0):
            print(f"❌ Missing CRITICAL ENV VAR: {v_name}"); exit(1)
    if not ADMIN_IDS or ADMIN_IDS == [0, 920892710]:
        print("❌ ADMIN_ID environment variable is missing or invalid!"); exit(1)
    print("✅ ENV VARS seem OK.")

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
        print("❌❌ FAILED TO SET WEBHOOK! Check URL and BOT_TOKEN. ❌❌"); await app.stop(); exit(1)

    await web_server()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopping...")
        loop = asyncio.get_event_loop()
        if loop.is_running(): loop.create_task(app.stop())
        else: asyncio.run(app.stop())
        print("Pyrogram client stopped.")
