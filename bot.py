# --- Imports ---
import os
import re
import asyncio
import asyncpg  # PostgreSQL database ke liye
from threading import Thread
from pyrogram import Client, filters, enums
from pyrogram.errors import (
    FloodWait, ChatAdminRequired, PeerIdInvalid, RPCError, 
    UserNotParticipant, MessageIdInvalid, MessageAuthorRequired
)
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# 'keep_alive' ko import karo, ab yeh local file se aayega
from keep_alive import keep_alive 

# --- Environment Variables (Sab Zaroori Hain) ---
try:
    API_ID = int(os.getenv("API_ID"))
    API_HASH = os.getenv("API_HASH")
    SESSION_STRING = os.getenv("SESSION_STRING")
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID"))
    DATABASE_URL = os.getenv("DATABASE_URL")
except Exception as e:
    print(f"❌ ERROR: Zaroori Environment Variable nahi mila: {e}")
    exit(1)

if not DATABASE_URL or not SESSION_STRING:
    print("❌ ERROR: DATABASE_URL aur SESSION_STRING dono zaroori hain.")
    exit(1)

# --- Hybrid Client Setup ---
# Bot Client (Token) - Commands lene aur status dikhaane ke liye
app_bot = Client(
    "bot_session",
    api_id=API_ID, 
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# User Client (String) - Scanning aur Forwarding ke liye
app_user = Client(
    "user_session",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    no_updates=True # Bot ko crash hone se bachaane ke liye
)

# --- Runtime State ---
is_forwarding = False
is_editing = False
app = app_bot  # Global "app" ko bot client maante hain
app.pool = None  # Database connection pool
app.user_client = app_user  # User client ko access karne ke liye

# --- Regex ---
LINK_USER_REGEX = re.compile(r"t\.me/[a-zA-Z0-9_]+|@[a-zA-Z0-9_]+|https?://[^\s]+")

# --- Database Functions ---
async def setup_database():
    print("Database se connect ho raha hoon...")
    
    # DEBUG: Check karein ki URL sahi hai ya nahi (bina password dikhaaye)
    if DATABASE_URL:
        print(f"DEBUG: Istemaal ho raha DB URL (aakhri 25 char): ...{DATABASE_URL[-25:]}")
    
    try:
        # 60 Second ka timeout, kyunki free database "jaagne" mein time lete hain
        app.pool = await asyncio.wait_for(
            asyncpg.create_pool(DATABASE_URL, max_inactive_connection_lifetime=60),
            timeout=60.0
        )
        async with app.pool.acquire() as conn:
            # Zaroori tables banao
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS movies (
                    file_unique_id TEXT PRIMARY KEY,
                    source_chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    file_name TEXT,
                    file_size BIGINT,
                    caption TEXT
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS target_duplicates (
                    file_unique_id TEXT PRIMARY KEY,
                    file_name_size TEXT UNIQUE
                );
            ''')
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            ''')
        print("✅ Database tables tayyar hain.")
        return True
    except asyncio.TimeoutError:
        print("❌ DATABASE CONNECTION FAILED: 60 second mein connection time out ho gaya.")
        print("Kripya check karein ki aapka DATABASE_URL (Internal Connection String) sahi hai ya nahi, aur database 'Running' state mein hai.")
        return False
    except Exception as e:
        print(f"❌ DATABASE CONNECTION FAILED: {e}")
        print("Kripya check karein ki aapka DATABASE_URL (Internal Connection String) sahi hai ya nahi.")
        return False

async def get_caption_lock_text(conn):
    row = await conn.fetchrow("SELECT value FROM settings WHERE key = 'caption_lock'")
    return row['value'] if row else None

async def set_caption_lock_text(conn, text):
    await conn.execute(
        "INSERT INTO settings (key, value) VALUES ('caption_lock', $1) ON CONFLICT (key) DO UPDATE SET value = $1",
        text
    )

# --- Helper Function ---
def only_admin(_, __, m):
    # Check karta hai ki message bhejnewala ADMIN_ID se match karta hai ya nahi
    return m.from_user and m.from_user.id == ADMIN_ID

async def safe_reply(message, text):
    # Message ka reply bhejta hai, error aane par fail nahi hota
    try:
        await message.reply(text, parse_mode=enums.ParseMode.DISABLED, disable_web_page_preview=True)
    except Exception as e:
        print(f"CRITICAL: Error message bhejte waqt bhi error aaya: {e}")

async def resolve_chat_id(client: Client, ref: str | int, client_name: str):
    # Chat ID ya username ko resolve karne ki koshish karta hai
    try:
        chat = await client.get_chat(ref)
        return chat
    except PeerIdInvalid:
         raise RuntimeError(f"❌ Peer nahi mila. Check karein ki aapka **{client_name}** (`{client.me.username or client.me.first_name}`) uss chat (`{ref}`) mein member hai.")
    except UserNotParticipant:
         raise RuntimeError(f"❌ Main uss chat (`{ref}`) ka member nahi hoon.")
    except Exception as e:
        raise RuntimeError(f"❌ Chat resolve fail hua ({client_name}): {e}")

# --- Core Logic: Channel Scanning (User Client ka kaam) ---
async def scan_channel_history(client: Client, chat, status_message, scan_function):
    total_processed = 0
    newly_found = 0
    
    try:
        async for m in client.get_chat_history(chat.id):
            total_processed += 1
            media = m.video or m.document
            
            if not media: continue
            # Sirf video files ko process karo
            if m.document and not (media.mime_type and media.mime_type.startswith("video/")): continue
            if not getattr(media, 'file_unique_id', None): continue
            
            try:
                res = await scan_function(m, media)
                if res and res.endswith("1"): # '1' ka matlab naya item add hua
                    newly_found += 1
            except Exception as e:
                print(f"[SYNC ERR] Msg {m.id}: {e}")

            if total_processed % 500 == 0:
                try:
                    await status_message.edit(f"⏳ (Sync) Processed: {total_processed} messages\nNew Found: {newly_found} unique")
                except FloodWait: pass
                
        return total_processed, newly_found
        
    except Exception as e:
        await status_message.edit(f"❌ Full Sync Error: {e}", parse_mode=enums.ParseMode.DISABLED)
        return total_processed, newly_found

# --- Bot Commands (Bot commands leta hai, User kaam karta hai) ---
@app.on_message(filters.command("start") & filters.create(only_admin))
async def start_cmd(_, message):
    # YEH WOH LINE THI JO ERROR DE RAHI THI (FIXED)
    await message.reply(
        f"**🚀 Asli Hybrid Bot (Database+String) Active!**\nAdmin ID: `{ADMIN_ID}`\n\n"
        "**Niyam:**\n"
        "1. Aapke **User Account (String)** ko sabhi Source/Target channels mein **Member** hona zaroori hai.\n"
        "2. Aapke **Bot Account (Token)** ko sabhi Target channels mein **Admin** (Edit/Post permission) hona zaroori hai.\n\n"
        "**Manual Sync (User Client se):**\n"
        "• `/sync_source <chat_id>` - Puraane channel ko poora scan karke database mein daalta hai.\n"
        "• `/sync_target <chat_id>` - Target channel ko scan karke duplicates ki list banata hai.\n\n"
        "**Forwarding (User Client se, Bot control se):**\n"
        "• `/start_forward <src_id> <tgt_id> [limit]` - Database se tezi se forward karta hai.\n"
        "• `/stop_fwd` - Forwarding rokta hai.\n\n"
        "**Caption Editing (Bot Client se):**\n"
        "• `/clean_captions <chat_id>` - Links/usernames hataata hai.\n"
        "• `/add_caption <chat_id> [text]` - Caption add/lock karta hai.\n"
        "• `/replace_caption <chat_id> [new text]` - 'Locked' text ko badalta hai.\n"
        "• `/stop_edit` - Editing rokta hai.\n\n"
        "**Utility:**\n"
        "• `/ping` - Bot zinda hai ya nahi.",
        parse_mode=enums.ParseMode.MARKDOWN
    ) # <--- YEH PARENTHESIS MISSING THA

@app.on_message(filters.command("ping") & filters.create(only_admin))
async def ping_cmd(_, message):
    await message.reply("✅ Pong! Bot zinda hai, User Client active hai, aur Database connected hai.")

@app.on_message(filters.command("sync_source") & filters.create(only_admin))
async def sync_source_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
        if chat_ref.isdigit(): chat_ref = int(chat_ref)
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/sync_source <chat_id_ya_username>")

    try:
        chat = await resolve_chat_id(app.user_client, chat_ref, "User Account (String)")
    except Exception as e:
        return await safe_reply(message, f"❌ Error: {e}")
        
    status = await message.reply(f"⏳ Full Scan (History) shuru ho raha hai: **{chat.title}**...\nYeh kaam User Client (`{app.user_client.me.first_name or 'User'}`) karega.")
    
    try:
        async with app.pool.acquire() as conn:
            # Puraana data delete karo
            res = await conn.execute("DELETE FROM movies WHERE source_chat_id = $1", chat.id)
            await status.edit(f"⏳ Puraani {res.split()[-1]} entries delete kar di hain. Naya scan shuru ho raha hai...")
    except Exception as e:
        return await status.edit(f"❌ Database clear karne mein fail hua: {e}")

    # Database mein daalne ke liye function
    async def db_insert(m, media):
        async with app.pool.acquire() as conn:
            return await conn.execute(
                """
                INSERT INTO movies (file_unique_id, source_chat_id, message_id, file_name, file_size, caption)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (file_unique_id) DO NOTHING;
                """,
                media.file_unique_id, m.chat.id, m.id, media.file_name, media.file_size, m.caption
            )
            
    processed, found = await scan_channel_history(app.user_client, chat, status, db_insert)
                
    await status.edit(f"🎉 **Full Sync Complete!**\nChannel: **{chat.title}**\nTotal Messages Checked: {processed}\nDatabase mein **{found}** nayi unique movies add huin.")

@app.on_message(filters.command("sync_target") & filters.create(only_admin))
async def sync_target_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
        if chat_ref.isdigit(): chat_ref = int(chat_ref)
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/sync_target <chat_id_ya_username>")

    try:
        chat = await resolve_chat_id(app.user_client, chat_ref, "User Account (String)")
    except Exception as e:
        return await safe_reply(message, f"❌ Error: {e}")
        
    status = await message.reply(f"⏳ Target Duplicates ko scan kar raha hoon: **{chat.title}**...\nYeh kaam User Client (`{app.user_client.me.first_name or 'User'}`) karega.")
    
    try:
        async with app.pool.acquire() as conn:
            # Puraana duplicate data delete karo
            await conn.execute("DELETE FROM target_duplicates;")
    except Exception as e:
        return await status.edit(f"❌ Database clear karne mein fail hua: {e}")

    # Duplicate database mein daalne ke liye function
    async def db_insert(m, media):
        async with app.pool.acquire() as conn:
            return await conn.execute(
                """
                INSERT INTO target_duplicates (file_unique_id, file_name_size)
                VALUES ($1, $2)
                ON CONFLICT (file_unique_id) DO NOTHING;
                """,
                media.file_unique_id, f"{media.file_name}-{media.file_size}"
            )

    processed, found = await scan_channel_history(app.user_client, chat, status, db_insert)
                
    await status.edit(f"🎉 **Target Sync Complete!**\nChannel: **{chat.title}**\nTotal Messages Checked: {processed}\n**{found}** unique movies/duplicates database mein add huin.")

# --- Stop Buttons ---
STOP_BUTTON = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop Forwarding", callback_data="stop_fwd")]])
STOP_EDIT_BUTTON = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop Editing", callback_data="stop_edit")]])

@app.on_callback_query(filters.regex("^stop_fwd$") & filters.create(only_admin))
async def cb_stop_forward(client, query):
    global is_forwarding
    is_forwarding = False
    await query.answer("🛑 Stop request received.", show_alert=False)
    try: await query.message.edit_text("🛑 Forwarding ruk gayi hai.")
    except: pass

@app.on_callback_query(filters.regex("^stop_edit$") & filters.create(only_admin))
async def cb_stop_edit(client, query):
    global is_editing
    is_editing = False
    await query.answer("🛑 Stop request received.", show_alert=False)
    try: await query.message.edit_text("🛑 Caption editing ruk gayi hai.")
    except: pass
# --------------------

@app.on_message(filters.command("start_forward") & filters.create(only_admin))
async def start_forward_cmd(client, message):
    global is_forwarding
    if is_forwarding:
        return await safe_reply(message, "❌ Ek forwarding process pehle se hi chal raha hai.")

    try:
        parts = message.text.split(" ", 3)
        source_ref = parts[1].strip()
        target_ref = parts[2].strip()
        if source_ref.isdigit(): source_ref = int(source_ref)
        if target_ref.isdigit(): target_ref = int(target_ref)
        
        fwd_limit = None
        if len(parts) == 4:
            fwd_limit = int(parts[3].strip())
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/start_forward <source_id> <target_id> [limit]")
    except ValueError:
        return await safe_reply(message, "❌ Limit number hona chahiye.")

    status = await message.reply("✅ Command received. Channels aur Database check kar raha hoon...")

    try:
        # User Client ko dono channels resolve karne hain
        source_chat = await resolve_chat_id(app.user_client, source_ref, "User Account (String)")
        target_chat = await resolve_chat_id(app.user_client, target_ref, "User Account (String)")
        
        async with app.pool.acquire() as conn:
            # Source DB se movies fetch karo
            if fwd_limit:
                movies_to_forward = await conn.fetch("SELECT * FROM movies WHERE source_chat_id = $1 ORDER BY message_id ASC LIMIT $2", source_chat.id, fwd_limit)
            else:
                movies_to_forward = await conn.fetch("SELECT * FROM movies WHERE source_chat_id = $1 ORDER BY message_id ASC", source_chat.id)
            
            # Target DB se duplicates fetch karo
            target_dupes_rows = await conn.fetch("SELECT file_unique_id, file_name_size FROM target_duplicates")
        
        # Sets banao taaki check karna tezi se ho
        target_dupes_set = {row['file_unique_id'] for row in target_dupes_rows}
        target_dupes_compound = {row['file_name_size'] for row in target_dupes_rows}

    except Exception as e:
        return await status.edit(f"❌ Error loading DBs ya resolving chat: {e}", parse_mode=enums.ParseMode.DISABLED)

    if not movies_to_forward:
        return await status.edit(f"ℹ️ Database mein source `{source_chat.title}` ke liye koi movie nahi mili. Pehle `/sync_source` chalaayein.")

    is_forwarding = True
    forwarded_count = 0
    duplicate_count = 0
    processed_count = 0
    total_in_index = len(movies_to_forward)

    await status.edit(
        f"⏳ **Hybrid Forwarding (User String se)**\n"
        f"Source: `{source_chat.title}`\n"
        f"Target: `{target_chat.title}`\n"
        f"Total Movies: `{total_in_index}`\n"
        f"Target Duplicates (Loaded): `{len(target_dupes_set)}`",
        reply_markup=STOP_BUTTON
    )
    
    for movie in movies_to_forward:
        if not is_forwarding: break
        
        processed_count += 1
        
        try:
            unique_id = movie["file_unique_id"]
            name_size = f"{movie['file_name']}-{movie['file_size']}"
            
            # Duplicate check
            if unique_id in target_dupes_set or name_size in target_dupes_compound:
                duplicate_count += 1
                continue
                
            # User client se forward karo
            await app.user_client.copy_message(
                chat_id=target_chat.id,
                from_chat_id=movie["source_chat_id"],
                message_id=movie["message_id"]
            )
            forwarded_count += 1
            
            # Naye forward ko duplicate list me add karo
            async with app.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO target_duplicates (file_unique_id, file_name_size) VALUES ($1, $2) ON CONFLICT (file_unique_id) DO NOTHING",
                    unique_id, name_size
                )
            target_dupes_set.add(unique_id) 
            
            await asyncio.sleep(1.0) # Thoda gap rakho floodwait se bachne ke liye
            
            if (forwarded_count % 50 == 0):
                try: await status.edit(
                    f"✅ Fwd: {forwarded_count}, 🔍 Dup: {duplicate_count}\n"
                    f"⏳ Processed: {processed_count} / {total_in_index}",
                    reply_markup=STOP_BUTTON
                )
                except FloodWait: pass
                
        except FloodWait as e:
            await status.edit(f"⏳ FloodWait: User Account {e.value}s ke liye so raha hai...", reply_markup=STOP_BUTTON)
            await asyncio.sleep(e.value + 5) # Extra time do
        except (MessageIdInvalid, MessageAuthorRequired):
            print(f"[FWD ERR] Skipping deleted msg {movie['message_id']}")
        except RPCError as e:
            print(f"[FWD RPC ERR] Skipping msg {movie['message_id']}: {e}")
        except Exception as e:
            print(f"[FWD ERR] Skipping msg {movie['message_id']}: {e}")

    is_forwarding = False
    await status.edit(
        f"🎉 **Forwarding Complete!**\n"
        f"✅ Total Forwarded: `{forwarded_count}`\n"
        f"🔍 Duplicates Skipped: `{duplicate_count}`",
        reply_markup=None
    )

# --- Caption Editing Commands (Bot Token se) ---
async def batch_editor(client, message, chat_ref, edit_function):
    global is_editing
    if is_editing:
        return await safe_reply(message, "❌ Ek editing process pehle se chal raha hai.")
        
    try:
        chat = await resolve_chat_id(client, chat_ref, "Bot Account")
    except Exception as e:
        return await safe_reply(message, f"❌ Error (Bot Client): {e}")

    status = await message.reply(f"⏳ Batch editing {chat.title} mein shuru kar raha hoon...", parse_mode=enums.ParseMode.DISABLED)
    is_editing = True
    processed_count = 0
    edited_count = 0

    try:
        async for m in client.get_chat_history(chat.id):
            if not is_editing: break
            if not m.video and not m.document: continue
                
            processed_count += 1
            
            try:
                new_caption = await edit_function(m.caption or "")
                
                if new_caption != m.caption:
                    await client.edit_message_caption(
                        chat_id=chat.id,
                        message_id=m.id,
                        caption=new_caption
                    )
                    edited_count += 1
                    await asyncio.sleep(0.5) # Thoda gap
            except FloodWait as e:
                await status.edit(f"⏳ FloodWait: Editing {e.value}s ke liye ruki hai...", reply_markup=STOP_EDIT_BUTTON)
                await asyncio.sleep(e.value + 5)
            except Exception as e:
                print(f"[EDIT ERR] Msg {m.id}: {e}")

            if processed_count % 100 == 0:
                try: await status.edit(
                    f"⏳ Batch Editing...\n"
                    f"Processed: {processed_count}\n"
                    f"Edited: {edited_count}",
                    reply_markup=STOP_EDIT_BUTTON
                )
                except FloodWait: pass
                
    except ChatAdminRequired:
        await status.edit("❌ **Error: Main Admin nahi hoon!**\nMujhe channel mein 'Edit Messages' permission ke saath Admin banao.")
    except Exception as e:
        await status.edit(f"❌ Editing Error: {e}", parse_mode=enums.ParseMode.DISABLED)
    
    is_editing = False
    await status.edit(
        f"🎉 **Batch Editing Complete!**\n"
        f"Processed: {processed_count}\n"
        f"Total Edited: {edited_count}",
        reply_markup=None
    )

@app.on_message(filters.command("clean_captions") & filters.create(only_admin))
async def clean_captions_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
        if chat_ref.isdigit(): chat_ref = int(chat_ref)
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/clean_captions <target_chat_id>")

    async def _clean(caption):
        return LINK_USER_REGEX.sub("", caption).strip()

    await batch_editor(client, message, chat_ref, _clean)

@app.on_message(filters.command("add_caption") & filters.create(only_admin))
async def add_caption_cmd(client, message):
    try:
        parts = message.text.split(" ", 2)
        chat_ref = parts[1].strip()
        text_to_add = parts[2].strip()
        if chat_ref.isdigit(): chat_ref = int(chat_ref)
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/add_caption <target_chat_id> [text to add]")
        
    async with app.pool.acquire() as conn:
        await set_caption_lock_text(conn, text_to_add)
    
    await message.reply(f"✅ Caption text '{text_to_add}' ko 'lock' kar diya hai. Ab add kar raha hoon...", parse_mode=enums.ParseMode.DISABLED)

    async def _add(caption):
        if text_to_add in caption:
            return caption
        return f"{caption.strip()}\n\n{text_to_add}"

    await batch_editor(client, message, chat_ref, _add)

@app.on_message(filters.command("replace_caption") & filters.create(only_admin))
async def replace_caption_cmd(client, message):
    try:
        parts = message.text.split(" ", 2)
        chat_ref = parts[1].strip()
        new_text = parts[2].strip()
        if chat_ref.isdigit(): chat_ref = int(chat_ref) # <--- YAHAN BHI EK TYPO THA (chat__ref), FIX KAR DIYA
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/replace_caption <target_chat_id> [new text]")
        
    async with app.pool.acquire() as conn:
        old_text = await get_caption_lock_text(conn)
        if not old_text:
            return await safe_reply(message, f"❌ Koi 'locked' text nahi mila. Pehle /add_caption chalao.")
        
        await set_caption_lock_text(conn, new_text)
        
    await message.reply(f"✅ Caption '{old_text}' ko '{new_text}' se replace kar raha hoon...", parse_mode=enums.ParseMode.DISABLED)

    async def _replace(caption):
        if old_text and old_text in caption:
            return caption.replace(old_text, new_text)
        elif new_text not in caption:
            # Agar puraana text nahi mila, toh naya text add kar do
            return f"{caption.strip()}\n\n{new_text}"
        return caption

    await batch_editor(client, message, chat_ref, _replace)

@app.on_message(filters.command("stop_edit") & filters.create(only_admin))
async def stop_edit_cmd(client, message):
    global is_editing
    is_editing = False
    await message.reply("🛑 Caption editing ruk gayi hai.")

# --- Bot ko start karo ---
async def main():
    print("Bot ko start kar raha hoon...")
    
    # 1. keep_alive ko thread me start karo
    print("Keep-alive server ko background thread mein start kar raha hoon...")
    keep_alive() # Yeh function thread ko start karta hai
    
    # 2. Database setup karo
    if not await setup_database():
        print("❌ Bot band ho raha hai kyunki database connect nahi hua.")
        return # Agar database fail ho, toh start mat karo
        
    # 3. Dono clients ko start karo
    print("User Client (String) ko start kar raha hoon...")
    await app.user_client.start()
    user_name = app.user_client.me.first_name or app.user_client.me.username or "User"
    print(f"✅ User {user_name} (String) active hai.")
    
    print("Bot Client (Token) ko start kar raha hoon...")
    await app_bot.start()
    print(f"✅ Bot @{app_bot.me.username} active hai aur commands sun raha hai.")
    
    # 4. Hamesha chalu rakho
    print("\n--- Bot Ab Poori Tarah Active Hai ---")
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot band ho raha hai...")
    except Exception as e:
        print(f"❌ MAIN LOOP MEIN FATAL ERROR: {e}")
