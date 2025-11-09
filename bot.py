from keep_alive import keep_alive
keep_alive() # Bot ko zinda rakhta hai

import os, re, asyncio
import asyncpg # Naya: PostgreSQL database ke liye
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, ChatAdminRequired, PeerIdInvalid, RPCError
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- Environment Variables ---
try:
    API_ID = int(os.getenv("API_ID"))
    API_HASH = os.getenv("API_HASH")
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID"))
    DATABASE_URL = os.getenv("DATABASE_URL") # Render ka PostgreSQL URL
except Exception as e:
    print(f"❌ ERROR: Environment Variable nahi mila: {e}")
    exit(1)

if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL environment variable set nahi hai.")
    exit(1)

# Sirf Bot Token ka istemaal
app = Client(
    "bot_session",
    api_id=API_ID, 
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- Runtime State ---
is_forwarding = False
is_editing = False
app.source_channel_ids = set() # Automatic indexing ke liye source channels ki list
app.pool = None # Database connection pool

# --- Regex ---
LINK_USER_REGEX = re.compile(r"t\.me/[a-zA-Z0-9_]+|@[a-zA-Z0-9_]+|https?://[^\s]+")

# --- Database Functions ---

async def setup_database():
    """
    Bot ke start hote hi Database tables banata hai.
    """
    print("Database se connect ho raha hoon...")
    app.pool = await asyncpg.create_pool(DATABASE_URL)
    async with app.pool.acquire() as conn:
        # Source channels ko store karne ke liye
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS source_channels (
                chat_id BIGINT PRIMARY KEY,
                chat_name TEXT
            );
        ''')
        # Sabhi movies ko store karne ke liye (Data mix nahi hoga)
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
        # Target channel ke duplicates ko check karne ke liye
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS target_duplicates (
                file_unique_id TEXT PRIMARY KEY,
                file_name_size TEXT UNIQUE
            );
        ''')
        # Caption lock text ko store karne ke liye
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        ''')
    print("Database tables tayyar hain.")
    await load_sources_to_memory()

async def load_sources_to_memory():
    """
    Source channels ko memory mein load karta hai taaki auto-indexing fast ho.
    """
    if not app.pool:
        print("Database pool available nahi hai.")
        return
        
    async with app.pool.acquire() as conn:
        rows = await conn.fetch("SELECT chat_id FROM source_channels;")
        app.source_channel_ids = {row['chat_id'] for row in rows}
    print(f"Loaded {len(app.source_channel_ids)} source channels memory mein.")

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
    return m.from_user and m.from_user.id == ADMIN_ID

async def safe_reply(message, text):
    """
    Error messages ko safely bhejta hai bina Markdown crash ke.
    """
    try:
        await message.reply(text, parse_mode=enums.ParseMode.DISABLED)
    except Exception as e:
        print(f"CRITICAL: Error message bhejte waqt bhi error aaya: {e}")

async def resolve_chat_id(client: Client, ref: str | int):
    """
    Bot ke liye chat ID/username resolve karta hai.
    """
    try:
        chat = await client.get_chat(ref)
        return chat
    except PeerIdInvalid:
         raise RuntimeError(f"❌ Peer not mila. Check karein ki aapka Bot (`{client.me.username}`) uss chat (`{ref}`) mein ADMIN hai.")
    except Exception as e:
        raise RuntimeError(f"❌ Chat resolve fail hua: {e}")

# --- Automatic Indexing (Naya Feature) ---
@app.on_message(filters.channel & (filters.video | filters.document))
async def auto_indexer(client, message):
    """
    Jab bhi source channel mein nayi movie aayegi, yeh automatic database mein add kar dega.
    """
    if not app.pool: # Agar database ready nahi hai, toh ignore karo
        return 
        
    if message.chat.id not in app.source_channel_ids:
        return # Yeh humare source list mein nahi hai, ignore karo.

    media = message.video or message.document
    if not media or not getattr(media, 'file_unique_id', None):
        return
        
    # Check karo ki yeh document ek video hai
    if message.document and not (media.mime_type and media.mime_type.startswith("video/")):
        return

    file_unique_id = media.file_unique_id
    
    print(f"[Auto-Index] Nayi movie mili: {media.file_name} (Source: {message.chat.id})")
    
    try:
        async with app.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO movies (file_unique_id, source_chat_id, message_id, file_name, file_size, caption)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (file_unique_id) DO NOTHING;
                """,
                file_unique_id,
                message.chat.id,
                message.id,
                media.file_name,
                media.file_size,
                message.caption
            )
    except Exception as e:
        print(f"❌ Auto-Index Error: {e}")

# --- Bot Commands ---
@app.on_message(filters.command("start") & filters.create(only_admin))
async def start_cmd(_, message):
    await message.reply(
        f"**🚀 Full Token Database Bot Active!**\nAdmin ID: `{ADMIN_ID}`\n\n"
        "**Niyam:** Is bot ko sabhi Source aur Target channels mein **Admin** hona zaroori hai.\n\n"
        "**Source Channel Management:**\n"
        "* `/add_source <chat_id>` - Channel ko auto-indexing ke liye add karta hai.\n"
        "* `/remove_source <chat_id>` - Channel ko auto-indexing se hataata hai.\n"
        "* `/list_sources` - Sabhi source channels ki list dikhata hai.\n\n"
        "**Manual Sync (Pehli Baar ke liye):**\n"
        "* `/sync_source <chat_id>` - Puraane channel ko poora scan karke database mein daalta hai.\n"
        "* `/sync_target <chat_id>` - Target channel ko scan karke duplicates ki list banata hai.\n\n"
        "**Forwarding (Bot Token Speed):**\n"
        "* `/start_forward <src_id> <tgt_id> [limit]` - Database se tezi se forward karta hai.\n"
        "* `/stop_fwd` - Forwarding rokta hai.\n\n"
        "**Caption Editing (Batch 100):**\n"
        "* `/clean_captions <chat_id>` - Links/usernames hataata hai.\n"
        "* `/add_caption <chat_id> [text]` - Caption add/lock karta hai.\n"
        "* `/replace_caption <chat_id> [new text]` - 'Locked' text ko badalta hai.\n"
        "* `/stop_edit` - Editing rokta hai.\n\n"
        "**Utility:**\n"
        "* `/ping` - Bot zinda hai ya nahi."
    )

@app.on_message(filters.command("ping") & filters.create(only_admin))
async def ping_cmd(_, message):
    await message.reply("✅ Pong! Bot zinda hai aur Database se connected hai.")

# --- Source Management Commands ---
@app.on_message(filters.command("add_source") & filters.create(only_admin))
async def add_source_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
        if chat_ref.isdigit(): chat_ref = int(chat_ref)
            
        chat = await resolve_chat_id(client, chat_ref)
        
        async with app.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO source_channels (chat_id, chat_name) VALUES ($1, $2) ON CONFLICT (chat_id) DO UPDATE SET chat_name = $2",
                chat.id, chat.title or chat.username
            )
        await load_sources_to_memory() # Memory ko update karo
        await message.reply(f"✅ Source Channel Add Ho Gaya: **{chat.title}** (`{chat.id}`)\nAb yahaan aane waali nayi movies automatic index hongi.\nPuraane messages ke liye `/sync_source {chat.id}` chalaayein.")
    except IndexError:
        await safe_reply(message, "❌ Usage:\n/add_source <chat_id_ya_username>")
    except Exception as e:
        await safe_reply(message, f"❌ Error: {e}")

@app.on_message(filters.command("remove_source") & filters.create(only_admin))
async def remove_source_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
        if chat_ref.isdigit(): chat_ref = int(chat_ref)
        
        # ID resolve karo (bhale hi woh DB mein na ho)
        chat = await resolve_chat_id(client, chat_ref)
        
        async with app.pool.acquire() as conn:
            # Pehle channel ko list se hatao
            await conn.execute("DELETE FROM source_channels WHERE chat_id = $1", chat.id)
            # Fir uski saari movies database se hatao
            res = await conn.execute("DELETE FROM movies WHERE source_chat_id = $1", chat.id)
            
        await load_sources_to_memory() # Memory ko update karo
        await message.reply(f"✅ Source Channel Hata Diya: **{chat.title}** (`{chat.id}`)\nDatabase se uske {res.split()[-1]} movie records delete kar diye hain.")
    except IndexError:
        await safe_reply(message, "❌ Usage:\n/remove_source <chat_id_ya_username>")
    except Exception as e:
        await safe_reply(message, f"❌ Error: {e}")

@app.on_message(filters.command("list_sources") & filters.create(only_admin))
async def list_sources_cmd(client, message):
    async with app.pool.acquire() as conn:
        rows = await conn.fetch("SELECT chat_id, chat_name FROM source_channels ORDER BY chat_name;")
        
    if not rows:
        return await message.reply("ℹ️ Abhi koi source channel add nahi hua hai.")
        
    text = "📂 **Indexed Source Channels:**\n\n"
    for row in rows:
        text += f"• **{row['chat_name']}** (`{row['chat_id']}`)\n"
    
    await message.reply(text)

# --- Manual Sync Commands ---
@app.on_message(filters.command("sync_source") & filters.create(only_admin))
async def sync_source_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
        if chat_ref.isdigit(): chat_ref = int(chat_ref)
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/sync_source <chat_id_ya_username>")

    try:
        chat = await resolve_chat_id(client, chat_ref)
    except Exception as e:
        return await safe_reply(message, f"❌ Error: {e}")
        
    status = await message.reply(f"⏳ Full Scan shuru ho raha hai: **{chat.title}**...\nYeh time lega. (Stage 1: Videos)")
    
    processed_s1 = 0
    processed_s2 = 0
    found_count = 0
    
    try:
        # Stage 1: Videos
        async for m in client.search_messages(chat.id, filter=enums.MessagesFilter.VIDEO, limit=0):
            processed_s1 += 1
            try:
                if not m.video or not m.video.file_unique_id: continue
                async with app.pool.acquire() as conn:
                    res = await conn.execute(
                        """
                        INSERT INTO movies (file_unique_id, source_chat_id, message_id, file_name, file_size, caption)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (file_unique_id) DO NOTHING;
                        """,
                        m.video.file_unique_id, m.chat.id, m.id, m.video.file_name, m.video.file_size, m.caption
                    )
                    if res.endswith("1"): found_count += 1
            except Exception as e: print(f"[SYNC S1 ERR] Msg {m.id}: {e}")

            if processed_s1 % 500 == 0:
                try: await status.edit(f"⏳ (Sync S1) Processed: {processed_s1} videos\nNew Found: {found_count} unique")
                except FloodWait: pass
        
        await status.edit(f"⏳ (Sync S2) Processed: {processed_s1} videos\nNew Found: {found_count} unique")

        # Stage 2: Documents (Files)
        async for m in client.search_messages(chat.id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
            processed_s2 += 1
            try:
                if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")): continue
                
                async with app.pool.acquire() as conn:
                    res = await conn.execute(
                        """
                        INSERT INTO movies (file_unique_id, source_chat_id, message_id, file_name, file_size, caption)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (file_unique_id) DO NOTHING;
                        """,
                        m.document.file_unique_id, m.chat.id, m.id, m.document.file_name, m.document.file_size, m.caption
                    )
                    if res.endswith("1"): found_count += 1
            except Exception as e: print(f"[SYNC S2 ERR] Msg {m.id}: {e}")

            if processed_s2 % 500 == 0:
                try: await status.edit(f"⏳ (Sync S2) Processed: {processed_s2} files\nNew Found: {found_count} unique")
                except FloodWait: pass
                
        await status.edit(f"🎉 **Full Sync Complete!**\nChannel: **{chat.title}**\nDatabase mein **{found_count}** nayi unique movies add huin.")
        
    except Exception as e:
        await status.edit(f"❌ Full Sync Error: {e}", parse_mode=enums.ParseMode.DISABLED)

@app.on_message(filters.command("sync_target") & filters.create(only_admin))
async def sync_target_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
        if chat_ref.isdigit(): chat_ref = int(chat_ref)
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/sync_target <chat_id_ya_username>")

    try:
        chat = await resolve_chat_id(client, chat_ref)
    except Exception as e:
        return await safe_reply(message, f"❌ Error: {e}")
        
    status = await message.reply(f"⏳ Target Duplicates ko scan kar raha hoon: **{chat.title}**...\n(Stage 1: Videos)")
    
    processed_s1 = 0
    processed_s2 = 0
    found_count = 0
    
    try:
        # Puraani list clear karo
        async with app.pool.acquire() as conn:
            await conn.execute("DELETE FROM target_duplicates;")
            
        # Stage 1: Videos
        async for m in client.search_messages(chat.id, filter=enums.MessagesFilter.VIDEO, limit=0):
            processed_s1 += 1
            try:
                if not m.video or not m.video.file_unique_id: continue
                async with app.pool.acquire() as conn:
                    res = await conn.execute(
                        """
                        INSERT INTO target_duplicates (file_unique_id, file_name_size)
                        VALUES ($1, $2)
                        ON CONFLICT (file_unique_id) DO NOTHING;
                        """,
                        m.video.file_unique_id, f"{m.video.file_name}-{m.video.file_size}"
                    )
                    if res.endswith("1"): found_count += 1
            except Exception as e: print(f"[SYNC TGT S1 ERR] Msg {m.id}: {e}")

            if processed_s1 % 500 == 0:
                try: await status.edit(f"⏳ (Target Sync S1) Processed: {processed_s1} videos\nFound: {found_count} unique")
                except FloodWait: pass
        
        await status.edit(f"⏳ (Target Sync S2) Processed: {processed_s1} videos\nFound: {found_count} unique")

        # Stage 2: Documents
        async for m in client.search_messages(chat.id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
            processed_s2 += 1
            try:
                if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")): continue
                
                async with app.pool.acquire() as conn:
                    res = await conn.execute(
                        """
                        INSERT INTO target_duplicates (file_unique_id, file_name_size)
                        VALUES ($1, $2)
                        ON CONFLICT (file_unique_id) DO NOTHING;
                        """,
                        m.document.file_unique_id, f"{m.document.file_name}-{m.document.file_size}"
                    )
                    if res.endswith("1"): found_count += 1
            except Exception as e: print(f"[SYNC TGT S2 ERR] Msg {m.id}: {e}")

            if processed_s2 % 500 == 0:
                try: await status.edit(f"⏳ (Target Sync S2) Processed: {processed_s2} files\nFound: {found_count} unique")
                except FloodWait: pass
                
        await status.edit(f"🎉 **Target Sync Complete!**\nChannel: **{chat.title}**\n**{found_count}** unique movies/duplicates database mein add huin.")
        
    except Exception as e:
        await status.edit(f"❌ Target Sync Error: {e}", parse_mode=enums.ParseMode.DISABLED)

# --- Stop Buttons ---
STOP_BUTTON = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop", callback_data="stop_fwd")]])
STOP_EDIT_BUTTON = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop Editing", callback_data="stop_edit")]])

@app.on_callback_query(filters.regex("^stop_fwd$"))
async def cb_stop_forward(client, query):
    if query.from_user.id != ADMIN_ID: return
    global is_forwarding
    is_forwarding = False
    await query.answer("🛑 Stop request received.", show_alert=False)
    try: await query.message.edit_text("🛑 Forwarding ruk gayi hai.")
    except: pass

@app.on_callback_query(filters.regex("^stop_edit$"))
async def cb_stop_edit(client, query):
    if query.from_user.id != ADMIN_ID: return
    global is_editing
    is_editing = False
    await query.answer("🛑 Stop request received.", show_alert=False)
    try: await query.message.edit_text("🛑 Caption editing ruk gayi hai.")
    except: pass
# --------------------

@app.on_message(filters.command("start_forward") & filters.create(only_admin))
async def start_forward_cmd(client, message):
    global is_forwarding
    
    try:
        source_ref = message.text.split(" ", 3)[1].strip()
        target_ref = message.text.split(" ", 3)[2].strip()
        if source_ref.isdigit(): source_ref = int(source_ref)
        if target_ref.isdigit(): target_ref = int(target_ref)
        
        fwd_limit = None
        if len(message.text.split(" ", 3)) == 4:
            fwd_limit = int(message.text.split(" ", 3)[3].strip())
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/start_forward <source_id> <target_id> [limit]")
    except ValueError:
        return await safe_reply(message, "❌ Limit number hona chahiye.")

    status = await message.reply("✅ Command received. Channels aur Database check kar raha hoon...")

    try:
        # Bot ko dono channels resolve karne hain
        source_chat = await resolve_chat_id(client, source_ref)
        target_chat = await resolve_chat_id(client, target_ref)
        
        async with app.pool.acquire() as conn:
            # Source se movies fetch karo
            if fwd_limit:
                movies_to_forward = await conn.fetch("SELECT * FROM movies WHERE source_chat_id = $1 ORDER BY message_id ASC LIMIT $2", source_chat.id, fwd_limit)
            else:
                movies_to_forward = await conn.fetch("SELECT * FROM movies WHERE source_chat_id = $1 ORDER BY message_id ASC", source_chat.id)
            
            # Target ke duplicates fetch karo
            target_dupes_rows = await conn.fetch("SELECT file_unique_id, file_name_size FROM target_duplicates")
        
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
    total_to_forward_str = fwd_limit or "sab"

    await status.edit(
        f"⏳ **Fast Forwarding (Bot Mode)**\n"
        f"Source: `{source_chat.title}`\n"
        f"Target: `{target_chat.title}`\n"
        f"Limit: `{total_to_forward_str}`\n\n"
        f"Total Movies: `{total_in_index}`\n"
        f"Target Duplicates (Loaded): `{len(target_dupes_set)}`",
        reply_markup=STOP_BUTTON,
        parse_mode=enums.ParseMode.DISABLED
    )
    
    for movie in movies_to_forward:
        if not is_forwarding: break
        
        processed_count += 1
        
        try:
            unique_id = movie["file_unique_id"]
            name_size = f"{movie['file_name']}-{movie['file_size']}"
            
            # Duplicate Check
            if unique_id in target_dupes_set or name_size in target_dupes_compound:
                duplicate_count += 1
                continue
                
            # Copy Message
            await client.copy_message(
                chat_id=target_chat.id,
                from_chat_id=movie["source_chat_id"],
                message_id=movie["message_id"]
            )
            forwarded_count += 1
            
            # Naye forward ko bhi duplicate list mein daalo
            async with app.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO target_duplicates (file_unique_id, file_name_size) VALUES ($1, $2) ON CONFLICT (file_unique_id) DO NOTHING",
                    unique_id, name_size
                )
            target_dupes_set.add(unique_id) # Memory mein bhi update karo
            
            await asyncio.sleep(0.05) # 50ms delay
            
            if (forwarded_count % 50 == 0) or (processed_count % 500 == 0):
                try: await status.edit(
                    f"✅ Fwd: {forwarded_count}, 🔍 Dup: {duplicate_count}\n"
                    f"⏳ Processed: {processed_count} / {total_in_index}",
                    reply_markup=STOP_BUTTON
                )
                except FloodWait: pass
                
        except FloodWait as e:
            await status.edit(f"⏳ FloodWait: Bot {e.value}s ke liye so raha hai...", reply_markup=STOP_BUTTON)
            await asyncio.sleep(e.value)
        except (MessageIdInvalid, MessageAuthorRequired):
            print(f"[FWD ERR] Skipping deleted msg {movie['message_id']}")
        except RPCError as e:
            print(f"[FWD RPC ERR] Skipping msg {movie['message_id']}: {e}")
        except Exception as e:
            print(f"[FWD ERR] Skipping msg {movie['message_id']}: {e}")

    is_forwarding = False
    await status.edit(
        f"🎉 **Fast Forwarding Complete!**\n"
        f"✅ Total Forwarded: `{forwarded_count}`\n"
        f"🔍 Duplicates Skipped: `{duplicate_count}`",
        reply_markup=None,
        parse_mode=enums.ParseMode.DISABLED
    )

# --- Caption Editing Commands (Bot Token Speed) ---

async def batch_editor(client, message, chat_ref, edit_function):
    global is_editing
    try:
        chat = await resolve_chat_id(client, chat_ref)
    except Exception as e:
        return await safe_reply(message, f"❌ Error (Bot Client): {e}")

    status = await message.reply(f"⏳ Batch editing {chat.title} mein shuru kar raha hoon... (Batch 100)", parse_mode=enums.ParseMode.DISABLED)
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
                    await asyncio.sleep(0.2) 
            except FloodWait as e:
                await status.edit(f"⏳ FloodWait: Editing {e.value}s ke liye ruki hai...", reply_markup=STOP_EDIT_BUTTON)
                await asyncio.sleep(e.value)
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
        reply_markup=None,
        parse_mode=enums.ParseMode.DISABLED
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
        chat_ref = message.text.split(" ", 2)[1].strip()
        text_to_add = message.text.split(" ", 2)[2].strip()
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
        chat_ref = message.text.split(" ", 2)[1].strip()
        new_text = message.text.split(" ", 2)[2].strip()
        if chat_ref.isdigit(): chat_ref = int(chat_ref)
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
    await setup_database()
    print("Hybrid Bot (Full Token + DB) start ho raha hai...")
    await app.start()
    print(f"Bot @{app.me.username} active hai.")
    await asyncio.Event().wait() # Bot ko hamesha chalu rakho

if __name__ == "__main__":
    # FIX: Pyrogram ke wrapper 'app.run()' ka istemaal karna zaroori hai
    #      na ki 'asyncio.run()'. Yeh pichhli galti thi.
    print("Bot ke main event loop ko start kar raha hoon...")
    app.run(main())
