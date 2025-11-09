from keep_alive import keep_alive
keep_alive() # Bot ko zinda rakhta hai

import os, time, re, json
import asyncio
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, ChatAdminRequired, InviteHashExpired, InviteHashInvalid, PeerIdInvalid, UserAlreadyParticipant, MessageIdInvalid, MessageAuthorRequired, RPCError
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- Environment Variables ---
try:
    API_ID = int(os.getenv("API_ID"))
    API_HASH = os.getenv("API_HASH") # FIX: Extra ')' removed
    SESSION_STRING = os.getenv("SESSION_STRING") # FIX: Extra ')' removed
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID"))
except ValueError:
    print("ERROR: Environment variables ko integer mein set karein (API_ID, ADMIN_ID)")
    exit(1)
except Exception as e:
    print(f"ERROR: Environment variable nahi mila: {e}")
    exit(1)

# --- Hybrid Client Setup ---
# Bot Client (Token) - Yeh commands lega aur forwarding/editing karega
app_bot = Client(
    "bot_session",
    api_id=API_ID, 
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# User Client (String) - Yeh sirf private channels ko scan karega
app_user = Client(
    "user_session",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING
)

# --- Database Files ---
MOVIE_INDEX_DB_FILE = "movies.json"
TARGET_INDEX_DB_FILE = "target_movies.json"
CAPTION_LOCK_FILE = "caption_lock.txt" # "Locked" caption ko save karne ke liye

# --- Runtime State ---
is_forwarding = False
is_editing = False # Caption editing ke liye naya flag

# --- Helper Functions ---
def only_admin(_, __, m):
    # Check karta hai ki command ADMIN_ID se aa raha hai
    return m.from_user and m.from_user.id == ADMIN_ID

async def safe_reply(message, text):
    """
    Error messages ko safely bhejta hai bina Markdown crash ke.
    """
    try:
        # FIX: Markdown ko disable kar diya taaki 'EntityBoundsInvalid' error na aaye
        await message.reply(text, parse_mode=enums.ParseMode.DISABLED)
    except Exception as e:
        print(f"CRITICAL: Error message bhejte waqt bhi error aaya: {e}")

async def resolve_chat_id(client: Client, ref: str | int, client_name: str):
    """
    Chat ID, @username, ya invite link ko resolve karta hai.
    Client_name batata hai ki (Bot) fail hua ya (User).
    """    
    if isinstance(ref, int) or (isinstance(ref, str) and ref.lstrip("-").isdigit()):
        try:
            chat = await client.get_chat(int(ref))
            return chat
        except PeerIdInvalid:
             raise RuntimeError(f"❌ Peer not mila. Check karein ki aapka **{client_name}** uss chat mein hai.\nBot ke liye `/sync_bot_account` ya User ke liye `/sync_user_account` chalaayein.")
        except Exception as e:
            pass # Fallback
            
    if isinstance(ref, str) and (ref.startswith("t.me/+") or "joinchat" in ref):
        try:
            if client.me.is_bot:
                 raise RuntimeError(f"❌ Bot Account invite link join nahi kar sakta. Sirf User Account (String) hi join kar sakta hai.")
            chat = await client.join_chat(ref)
            return chat
        except UserAlreadyParticipant:
            chat = await client.get_chat(ref)
            return chat
        except (InviteHashExpired, InviteHashInvalid) as e:
            raise RuntimeError(f"❌ Invite link invalid/expired: {e}")
        except ChatAdminRequired as e:
            raise RuntimeError(f"❌ Invite link se join karne ke liye Admin permission chahiye: {e}")
            
    try:
        chat = await client.get_chat(ref) # @username ya public link
        return chat
    except PeerIdInvalid:
         raise RuntimeError(f"❌ Peer not mila. Check karein ki aapka **{client_name}** uss chat (`{ref}`) mein hai.")
    except RPCError as e:
        raise RuntimeError(f"❌ Chat resolve fail hua: {e}")

# --- Core Logic: Channel Scanning (User Client ka kaam) ---
async def scan_channel(client: Client, chat_ref: str, filename: str, status_message):
    """
    User Client ka istemaal karke channel scan karta hai aur JSON file banata hai.
    """
    try:
        chat = await resolve_chat_id(client, chat_ref, "User Account (String)")
        chat_id = chat.id
        chat_name = chat.title or chat.username
    except Exception as e:
        await status_message.edit(f"❌ Error (User Client): {e}", parse_mode=enums.ParseMode.DISABLED)
        return False # Fail

    movie_data = {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "movies": {} # { unique_id: { message_id, file_name, file_size } }
    }
    
    processed_s1 = 0
    processed_s2 = 0
    found_count = 0

    try:
        await status_message.edit(f"⏳ (User Client) Scanning {chat_name}...\n(Stage 1: Videos)")
        
        # Stage 1: Videos
        async for m in client.search_messages(chat_id, filter=enums.MessagesFilter.VIDEO, limit=0):
            processed_s1 += 1
            try:
                if not m.video or not m.video.file_unique_id:
                    continue
                
                unique_id = m.video.file_unique_id
                if unique_id not in movie_data["movies"]:
                    movie_data["movies"][unique_id] = {
                        "message_id": m.id,
                        "file_name": m.video.file_name,
                        "file_size": m.video.file_size
                    }
                    found_count += 1
            except Exception as e:
                print(f"[SCAN S1 ERR] Msg {m.id}: {e}")

            if processed_s1 % 500 == 0:
                try: await status_message.edit(f"⏳ (User Client) Scanning... (Stage 1)\nProcessed: {processed_s1} videos\nFound: {found_count} unique")
                except FloodWait: pass
        
        await status_message.edit(f"⏳ (User Client) Scanning... (Stage 2: Files)\nProcessed: {processed_s1} videos\nFound: {found_count} unique")

        # Stage 2: Documents (Files)
        async for m in client.search_messages(chat_id, filter=enums.MessagesFilter.DOCUMENT, limit=0):
            processed_s2 += 1
            try:
                if not (m.document and m.document.mime_type and m.document.mime_type.startswith("video/")):
                    continue
                
                unique_id = m.document.file_unique_id
                if unique_id not in movie_data["movies"]:
                    movie_data["movies"][unique_id] = {
                        "message_id": m.id,
                        "file_name": m.document.file_name,
                        "file_size": m.document.file_size
                    }
                    found_count += 1
            except Exception as e:
                print(f"[SCAN S2 ERR] Msg {m.id}: {e}")

            if processed_s2 % 500 == 0:
                try: await status_message.edit(f"⏳ (User Client) Scanning... (Stage 2)\nProcessed: {processed_s2} files\nFound: {found_count} unique")
                except FloodWait: pass
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(movie_data, f, indent=2, ensure_ascii=False)
        
        await status_message.edit(f"🎉 (User Client) Scan Complete!\nChannel: {chat_name}\nFound: {found_count} unique movies.\nSaved to {filename}.", parse_mode=enums.ParseMode.DISABLED)
        return True # Success
        
    except Exception as e:
        await status_message.edit(f"❌ (User Client) Scan Error: {e}", parse_mode=enums.ParseMode.DISABLED)
        return False # Fail

# --- Bot Commands (Bot Client ka kaam) ---

@app_bot.on_message(filters.command("start") & filters.create(only_admin))
async def start_cmd(_, message):
    await message.reply(
        f"**🚀 Hybrid Bot (Token) Active!**\nAdmin ID: `{ADMIN_ID}`\n\n"
        "**Pehla Kadam (Ek Baar):**\n"
        "1. Bot ko Source aur Target channels mein Admin banayein.\n"
        "2. `/sync_user_account` - User Account ki memory sync karein.\n"
        "3. `/sync_bot_account` - Bot Account ki memory sync karein.\n\n"
        "**Roz Ka Kaam:**\n"
        "1. `/sync_channels <source_id> <target_id>` - Dono channels ko scan karke JSON file banata hai.\n"
        "2. `/start_forward <target_id>` - Bot tezi se forwarding shuru karega.\n\n"
        "**Scan Commands (User Client):**\n"
        "* `/index <chat_id>` - Source ko scan karke `{MOVIE_INDEX_DB_FILE}` banata hai.\n"
        "* `/index_target <chat_id>` - Target ko scan karke `{TARGET_INDEX_DB_FILE}` banata hai.\n"
        "* `/sync_channels <src> <tgt>` - Dono ko ek saath scan karta hai. (Recommended)\n"
        "* `/clear_index` - Dono JSON files ko delete karta hai.\n\n"
        "**Forwarding Commands (Bot Client):**\n"
        "* `/start_forward <target_id> [limit]` - Full speed forwarding (Bot Token se).\n"
        "* `/stop_fwd` - Forwarding rokta hai.\n\n"
        "**Caption Editing (Bot Client):**\n"
        "* `/clean_captions <chat_id>` - Target channel se links/usernames hataata hai (Batch 100).\n"
        "* `/add_caption <chat_id> [text]` - Caption ke neeche text add/lock karta hai (Batch 100).\n"
        "* `/replace_caption <chat_id> [new text]` - Puraane 'locked' text ko naye text se badalta hai (Batch 100).\n"
        "* `/stop_edit` - Editing rokta hai."
    )

@app_bot.on_message(filters.command("sync_user_account") & filters.create(only_admin))
async def sync_user_account_cmd(client, message):
    status = await message.reply("✅ Command received. User-client ko start kar raha hoon cache sync ke liye...")
    try:
        await app_user.start()
        count = 0
        async for _ in app_user.get_dialogs():
            count += 1
            if count % 100 == 0:
                try: await status.edit(f"⏳ (User Client) Syncing... Found {count} chats...")
                except: pass
        await status.edit(f"🎉 **User Account Sync Complete!**\nFound {count} chats. Ab User Account numeric IDs ko pehchaan sakta hai.\nUser-client stop ho gaya hai.")
    except Exception as e:
        await status.edit(f"❌ Main Bot Error: {e}", parse_mode=enums.ParseMode.DISABLED)
    finally:
        # FIX: Ensure user client always stops
        if app_user.is_initialized:
            await app_user.stop()

@app_bot.on_message(filters.command("sync_bot_account") & filters.create(only_admin))
async def sync_bot_account_cmd(client, message):
    status = await message.reply("⏳ **Bot Account** ko sync kar raha hoon...")
    try:
        count = 0
        async for _ in client.get_dialogs(): # 'client' ya 'app_bot' ek hi baat hai yahaan
            count += 1
            if count % 100 == 0:
                try: await status.edit(f"⏳ (Bot Account) Syncing... Found {count} chats...")
                except: pass
        await status.edit(f"🎉 **Bot Account Sync Complete!**\nFound {count} chats. Ab Bot Account numeric IDs ko pehchaan sakta hai.")
    except Exception as e:
        await status.edit(f"❌ Bot Sync Error: {e}", parse_mode=enums.ParseMode.DISABLED)

@app_bot.on_message(filters.command("index") & filters.create(only_admin))
async def index_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
    except IndexError:
        # FIX: Crash-proof error message
        await safe_reply(message, f"❌ Usage:\n/index -100123... or /index @channel")
        return

    status = await message.reply("✅ Command received. User-client ko start kar raha hoon...")
    try:
        await app_user.start()
        await scan_channel(app_user, chat_ref, MOVIE_INDEX_DB_FILE, status)
    except Exception as e:
        await status.edit(f"❌ Main Bot Error: {e}", parse_mode=enums.ParseMode.DISABLED)
    finally:
        if app_user.is_initialized:
            await app_user.stop()

@app_bot.on_message(filters.command("index_target") & filters.create(only_admin))
async def index_target_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
    except IndexError:
        await safe_reply(message, f"❌ Usage:\n/index_target -100123... or /index_target @channel")
        return

    status = await message.reply("✅ Command received. User-client ko start kar raha hoon...")
    try:
        await app_user.start()
        await scan_channel(app_user, chat_ref, TARGET_INDEX_DB_FILE, status)
    except Exception as e:
        await status.edit(f"❌ Main Bot Error: {e}", parse_mode=enums.ParseMode.DISABLED)
    finally:
        if app_user.is_initialized:
            await app_user.stop()
        
@app_bot.on_message(filters.command("sync_channels") & filters.create(only_admin))
async def sync_channels_cmd(client, message):
    try:
        source_ref = message.text.split(" ", 2)[1].strip()
        target_ref = message.text.split(" ", 2)[2].strip()
    except IndexError:
        # FIX: Crash-proof error message (This was your error)
        await safe_reply(message, f"❌ Usage:\n/sync_channels <source_id> <target_id>")
        return

    status = await message.reply("✅ Command received. Full Sync (Source+Target) ke liye User-client ko start kar raha hoon...")
    try:
        await app_user.start()
        
        # Sync Source
        await status.edit("⏳ **(1/2)** Source Channel ko scan kar raha hoon...")
        success = await scan_channel(app_user, source_ref, MOVIE_INDEX_DB_FILE, status)
        
        # Sync Target
        if success: # Agar source fail ho jaaye toh target mat karo
            await status.edit("⏳ **(2/2)** Target Channel ko scan kar raha hoon...")
            await scan_channel(app_user, target_ref, TARGET_INDEX_DB_FILE, status)
        
        await status.edit("🎉 **Full Sync Complete!** Dono JSON files ban gayi hain.\nUser-client stop ho gaya hai.")
    except Exception as e:
        await status.edit(f"❌ Main Bot Error: {e}", parse_mode=enums.ParseMode.DISABLED)
    finally:
        if app_user.is_initialized:
            await app_user.stop()

@app_bot.on_message(filters.command("clear_index") & filters.create(only_admin))
async def clear_index_cmd(_, message):
    count = 0
    if os.path.exists(MOVIE_INDEX_DB_FILE):
        os.remove(MOVIE_INDEX_DB_FILE)
        count += 1
    if os.path.exists(TARGET_INDEX_DB_FILE):
        os.remove(TARGET_INDEX_DB_FILE)
        count += 1
    await safe_reply(message, f"✅ {count} JSON index files delete kar di hain.")

# --- Stop Buttons ---
STOP_BUTTON = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop", callback_data="stop_fwd")]])
STOP_EDIT_BUTTON = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 Stop Editing", callback_data="stop_edit")]])

@app_bot.on_callback_query(filters.regex("^stop_fwd$"))
async def cb_stop_forward(client, query):
    if query.from_user.id != ADMIN_ID:
        return await query.answer("❌ Allowed nahi hai!", show_alert=True)
    global is_forwarding
    is_forwarding = False
    await query.answer("🛑 Stop request received.", show_alert=False)
    try: await query.message.edit_text("🛑 Forwarding ruk gayi hai.")
    except: pass

@app_bot.on_callback_query(filters.regex("^stop_edit$"))
async def cb_stop_edit(client, query):
    if query.from_user.id != ADMIN_ID:
        return await query.answer("❌ Allowed nahi hai!", show_alert=True)
    global is_editing
    is_editing = False
    await query.answer("🛑 Stop request received.", show_alert=False)
    try: await query.message.edit_text("🛑 Caption editing ruk gayi hai.")
    except: pass
# --------------------

@app_bot.on_message(filters.command("start_forward") & filters.create(only_admin))
async def start_forward_cmd(client, message):
    global is_forwarding
    
    args = message.text.split(" ", 2)
    if len(args) < 2:
        return await safe_reply(message, "❌ Usage:\n/start_forward <target_id> [limit]\nTarget ID -100... ya @username ho sakta hai.")
        
    target_ref = args[1].strip()
    fwd_limit = None
    
    if len(args) == 3:
        try: 
            fwd_limit = int(args[2].strip())
        except ValueError: 
            return await safe_reply(message, "❌ Limit number hona chahiye.")

    # Files check
    if not os.path.exists(MOVIE_INDEX_DB_FILE):
        return await safe_reply(message, f"❌ {MOVIE_INDEX_DB_FILE} nahi mili. Pehle /index ya /sync_channels chalao.")
    if not os.path.exists(TARGET_INDEX_DB_FILE):
        return await safe_reply(message, f"❌ {TARGET_INDEX_DB_FILE} nahi mili. Pehle /index_target ya /sync_channels chalao.")

    status = await message.reply("✅ Command received. Databases load kar raha hoon...")

    source_chat_id = None
    try:
        # Load source movies
        with open(MOVIE_INDEX_DB_FILE, "r") as f:
            source_data = json.load(f)
            source_movies = source_data["movies"]
            source_chat_id = source_data["chat_id"] # Yeh ID -100... hai
        
        # Load target movies (sirf duplicates ke liye)
        with open(TARGET_INDEX_DB_FILE, "r") as f:
            target_data = json.load(f)
            target_movies_set = set(target_data["movies"].keys())
        
        # Bot ko target chat resolve karna hai
        target_chat = await resolve_chat_id(client, target_ref, "Bot Account")
        target_chat_id = target_chat.id
        
        # --- FIX: Check karo ki Bot Account source channel ko padh sakta hai ya nahi ---
        # Yeh aapka "Peer id invalid" error ko pakdega
        try:
            await client.get_chat(source_chat_id)
        except Exception as e:
            raise RuntimeError(f"❌ Bot Account (`{client.me.username}`) Source Channel ({source_chat_id}) ko access nahi kar paa raha hai.\nError: {e}\n\n**Solution:** Bot ko Source Channel mein (Admin/Member) add karein.")
        # -------------------------------------------------------------------------
        
    except Exception as e:
        return await status.edit(f"❌ Error loading DBs ya resolving chat: {e}", parse_mode=enums.ParseMode.DISABLED)

    is_forwarding = True
    forwarded_count = 0
    duplicate_count = 0
    processed_count = 0
    
    total_in_index = len(source_movies)
    total_to_forward_str = fwd_limit or "sab"

    await status.edit(
        f"⏳ **Fast Forwarding (Bot Mode)**\n"
        f"Source (Cache): `{source_data['chat_name']}` ({source_chat_id})\n"
        f"Target: `{target_chat.title}` ({target_chat_id})\n"
        f"Limit: `{total_to_forward_str}`\n\n"
        f"Total Movies: `{total_in_index}`\n"
        f"Target Duplicates (Loaded): `{len(target_movies_set)}`",
        reply_markup=STOP_BUTTON,
        parse_mode=enums.ParseMode.DISABLED
    )
    
    for unique_id, data in source_movies.items():
        if not is_forwarding: break
        
        processed_count += 1
        
        try:
            if unique_id in target_movies_set:
                duplicate_count += 1
                continue
                
            message_id = data["message_id"]
            
            # --- YEH HAI BOT TOKEN KI SPEED ---
            await client.copy_message(
                chat_id=target_chat_id,
                from_chat_id=source_chat_id,
                message_id=message_id
            )
            forwarded_count += 1
            
            # Bot token ke liye chhota delay kaafi hai
            await asyncio.sleep(0.05) # 50ms delay
            
            if (forwarded_count % 50 == 0) or (processed_count % 500 == 0):
                try: await status.edit(
                    f"✅ Fwd: {forwarded_count} / {total_to_forward_str}, 🔍 Dup: {duplicate_count}\n"
                    f"⏳ Processed: {processed_count} / {total_in_index}",
                    reply_markup=STOP_BUTTON
                )
                except FloodWait: pass
            
            if fwd_limit and forwarded_count >= fwd_limit:
                break
                
        except FloodWait as e:
            await status.edit(f"⏳ FloodWait: Bot {e.value}s ke liye so raha hai...", reply_markup=STOP_BUTTON)
            await asyncio.sleep(e.value)
        except (MessageIdInvalid, MessageAuthorRequired):
            print(f"[FWD ERR] Skipping deleted msg {message_id}")
        except RPCError as e:
            print(f"[FWD RPC ERR] Skipping msg {message_id}: {e}")
            if "USER_IS_BLOCKED" in str(e): 
                is_forwarding = False
                await status.edit("❌ Error: Bot ko target chat mein block kar diya gaya hai. Forwarding ruki.")
                break
        except Exception as e:
            print(f"[FWD ERR] Skipping msg {message_id}: {e}")

    is_forwarding = False
    await status.edit(
        f"🎉 **Fast Forwarding Complete!**\n"
        f"✅ Total Forwarded: `{forwarded_count}`\n"
        f"🔍 Duplicates Skipped: `{duplicate_count}`",
        reply_markup=None,
        parse_mode=enums.ParseMode.DISABLED
    )


# --- Naye Caption Editing Commands ---

LINK_USER_REGEX = re.compile(r"t\.me/[a-zA-Z0-9_]+|@[a-zA-Z0-9_]+|https?://[^\s]+")

async def batch_editor(client, message, chat_ref, edit_function):
    """
    Ek helper function jo 100 ke batch mein messages ko edit karta hai.
    """
    global is_editing
    try:
        # Bot ko member hona chahiye
        chat = await resolve_chat_id(client, chat_ref, "Bot Account")
        chat_id = chat.id
    except Exception as e:
        return await safe_reply(message, f"❌ Error (Bot Client): {e}\n\nHint: Kya aapka Bot Account (`{client.me.username}`) channel ka member hai?")

    status = await message.reply(f"⏳ Batch editing {chat.title} mein shuru kar raha hoon... (Batch 100)", parse_mode=enums.ParseMode.DISABLED)
    is_editing = True
    processed_count = 0
    edited_count = 0

    try:
        # Bot channel ko scan karega
        async for m in client.get_chat_history(chat_id):
            if not is_editing:
                break
            
            # Sirf videos/documents ko edit karo
            if not m.video and not m.document:
                continue
                
            processed_count += 1
            
            try:
                new_caption = await edit_function(m.caption or "")
                
                # Agar caption badla hai, tabhi edit karo
                if new_caption != m.caption:
                    await client.edit_message_caption(
                        chat_id=chat_id,
                        message_id=m.id,
                        caption=new_caption
                    )
                    edited_count += 1
                    await asyncio.sleep(0.2) # Batch edit mein pause zaroori hai

            except FloodWait as e:
                await status.edit(f"⏳ FloodWait: Editing {e.value}s ke liye ruki hai...", reply_markup=STOP_EDIT_BUTTON)
                await asyncio.sleep(e.value)
            except Exception as e:
                print(f"[EDIT ERR] Msg {m.id}: {e}") # Skip bad message

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

@app_bot.on_message(filters.command("clean_captions") & filters.create(only_admin))
async def clean_captions_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 1)[1].strip()
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/clean_captions <target_chat_id>")

    async def _clean(caption):
        # Links aur usernames ko hata deta hai
        return LINK_USER_REGEX.sub("", caption).strip()

    await batch_editor(client, message, chat_ref, _clean)

@app_bot.on_message(filters.command("add_caption") & filters.create(only_admin))
async def add_caption_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 2)[1].strip()
        text_to_add = message.text.split(" ", 2)[2].strip()
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/add_caption <target_chat_id> [text to add]")
        
    # Naye text ko "lock" kar do
    with open(CAPTION_LOCK_FILE, "w", encoding="utf-8") as f:
        f.write(text_to_add)
    
    await message.reply(f"✅ Caption text '{text_to_add}' ko 'lock' kar diya hai. Ab add kar raha hoon...", parse_mode=enums.ParseMode.DISABLED)

    async def _add(caption):
        # Pehle se hai ya nahi check karo
        if text_to_add in caption:
            return caption
        return f"{caption.strip()}\n\n{text_to_add}"

    await batch_editor(client, message, chat_ref, _add)

@app_bot.on_message(filters.command("replace_caption") & filters.create(only_admin))
async def replace_caption_cmd(client, message):
    try:
        chat_ref = message.text.split(" ", 2)[1].strip()
        new_text = message.text.split(" ", 2)[2].strip()
    except IndexError:
        return await safe_reply(message, "❌ Usage:\n/replace_caption <target_chat_id> [new text]")
        
    old_text = ""
    if os.path.exists(CAPTION_LOCK_FILE):
        with open(CAPTION_LOCK_FILE, "r", encoding="utf-8") as f:
            old_text = f.read().strip()
            
    if not old_text:
        return await safe_reply(message, f"❌ Koi 'locked' text nahi mila. Pehle /add_caption chalao.")

    # Naye text ko "lock" kar do
    with open(CAPTION_LOCK_FILE, "w", encoding="utf-8") as f:
        f.write(new_text)
        
    await message.reply(f"✅ Caption '{old_text}' ko '{new_text}' se replace kar raha hoon...", parse_mode=enums.ParseMode.DISABLED)

    async def _replace(caption):
        if old_text and old_text in caption:
            # Replace karo
            return caption.replace(old_text, new_text)
        elif new_text not in caption:
            # Agar purana nahi mila, toh naya add kar do
            return f"{caption.strip()}\n\n{new_text}"
        return caption # Koi change nahi

    await batch_editor(client, message, chat_ref, _replace)

@app_bot.on_message(filters.command("stop_edit") & filters.create(only_admin))
async def stop_edit_cmd(client, message):
    global is_editing
    is_editing = False
    await message.reply("🛑 Caption editing ruk gayi hai.")

# --- Bot ko start karo ---
print("Hybrid Bot (Bot Token) start ho raha hai...")
print("User Client (Session String) tayyar hai scan karne ke liye...")
print(f"Bot ko control karne ke liye Admin ({ADMIN_ID}) ko commands bhejein.")
app_bot.run()
