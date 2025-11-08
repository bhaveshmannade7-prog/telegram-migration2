import os
import json
import re
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from http import HTTPStatus
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, RetryAfter, NetworkError
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ConversationHandler
from telegram.ext._contexttypes import ContextTypes
from fastapi import FastAPI, Request, Response
import hashlib
import uvicorn

# === NAYE IMPORTS ===
try:
    from pyrogram import Client
    from pyrogram.enums import MessageMediaType
    PYROGRAM_AVAILABLE = True
except ImportError:
    PYROGRAM_AVAILABLE = False
    print("WARNING: Pyrogram not installed. /sync command will not work.")

# Configuration
BOT_TOKEN = os.environ.get('BOT_TOKEN')
WEBHOOK_URL = os.environ.get('WEBHOOK_URL')
ADMIN_IDS = [int(id.strip()) for id in os.environ.get('ADMIN_IDS', '').split(',') if id.strip()]
CHANNEL_ID = -1002417767287
CHANNEL_USERNAME = "@MAZABACKUP01"
JSON_DB_FILE = "movies_database.json"

# === SYNC KE LIYE ENV VARS ===
API_ID = os.environ.get('API_ID')
API_HASH = os.environ.get('API_HASH')
SESSION_STRING = os.environ.get('SESSION_STRING')

# Batch & Flood Control Config
BATCH_SIZE = 100
ACTION_DELAY = 0.5
BATCH_SLEEP = 5

# Conversation states
(WAITING_FOR_WATERMARK, WAITING_FOR_UPLOAD_FILE) = range(2)

# === CRITICAL FIX: GLOBAL TASK LOCKS ===
TASK_LOCKS = {
    'sync': False,
    'clean': False,
    'forward': False,
    'addwatermark': False,
    'duplicates': False
}

# Database Handler
class MovieDatabase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.data = self.load_database()

    def load_database(self):
        try:
            if os.path.exists(self.db_file):
                if os.path.getsize(self.db_file) > 0:
                    with open(self.db_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
                else:
                    print("Database file is empty, loading default structure.")
                    return self._get_default_structure()
        except Exception as e:
            print(f"Error loading database: {e}")
        return self._get_default_structure()

    def _get_default_structure(self):
        return {
            "movies": [],
            "watermark": {
                "enabled": False,
                "text": "",
                "usernames": [],
                "links": []
            },
            "stats": {
                "total_movies": 0,
                "total_cleaned": 0,
                "total_forwarded": 0,
                "duplicates_removed": 0,
                "watermarks_added": 0,
                "last_updated": None
            }
        }

    def save_database(self):
        try:
            if "stats" not in self.data:
                self.data["stats"] = self._get_default_structure()["stats"]
            self.data["stats"]["total_movies"] = len(self.data["movies"])
            
            with open(self.db_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error saving database: {e}")
            return False

    def add_movie(self, message_id, file_id, caption, file_hash):
        movie_entry = {
            "message_id": message_id,
            "file_id": file_id,
            "caption": caption,
            "file_hash": file_hash,
            "added_date": datetime.now().isoformat()
        }
        self.data["movies"].append(movie_entry)
        self.data["stats"]["last_updated"] = datetime.now().isoformat()

    def update_movie_entry(self, old_message_id, new_message_id, new_caption):
        try:
            for movie in self.data["movies"]:
                if movie["message_id"] == old_message_id:
                    movie["message_id"] = new_message_id
                    movie["caption"] = new_caption
                    movie["file_hash"] = calculate_file_hash(movie.get("file_id", ""), new_caption)
                    return True
            return False
        except Exception as e:
            print(f"Error updating DB entry for {old_message_id}: {e}")
            return False

    def get_all_movies(self):
        return self.data.get("movies", [])

    def get_all_hashes(self):
        return {movie.get("file_hash") for movie in self.data.get("movies", []) if movie.get("file_hash")}

    def find_duplicates(self):
        seen = {}
        duplicates = []
        for idx, movie in enumerate(self.data.get("movies", [])):
            hash_key = movie.get("file_hash")
            if hash_key and hash_key in seen:
                duplicates.append((idx, movie))
            else:
                seen[hash_key] = idx
        return duplicates

    def remove_movie_by_index(self, index):
        if 0 <= index < len(self.data["movies"]):
            self.data["movies"].pop(index)
            self.data["stats"]["duplicates_removed"] += 1
            return True
        return False

    def remove_movie_by_message_id(self, message_id):
        initial_len = len(self.data["movies"])
        self.data["movies"] = [m for m in self.data["movies"] if m.get("message_id") != message_id]
        final_len = len(self.data["movies"])
        if final_len < initial_len:
            self.data["stats"]["total_movies"] = final_len
            self.save_database()
            return True
        return False

    def set_watermark(self, watermark_text):
        if "watermark" not in self.data:
            self.data["watermark"] = self._get_default_structure()["watermark"]
        self.data["watermark"]["text"] = watermark_text
        self.data["watermark"]["enabled"] = True
        usernames = re.findall(r'@w+', watermark_text)
        links = re.findall(r'(?:https?://)?(?:www.)?(?:t.me/|telegram.me/)[w.-]+(?:/[w-]+)*', watermark_text)
        self.data["watermark"]["usernames"] = list(set(usernames))
        self.data["watermark"]["links"] = list(set(links))
        self.save_database()

    def get_watermark(self):
        return self.data.get("watermark", self._get_default_structure()["watermark"])

    def disable_watermark(self):
        if "watermark" in self.data:
            self.data["watermark"]["enabled"] = False
            self.save_database()

    def get_stats(self):
        if "stats" not in self.data:
            self.data["stats"] = self._get_default_structure()["stats"]
        self.data["stats"]["total_movies"] = len(self.data.get("movies", []))
        return self.data["stats"]

    def update_stats(self, stat_type, increment=1):
        if "stats" not in self.data:
            self.data["stats"] = self._get_default_structure()["stats"]
        if stat_type in self.data["stats"]:
            self.data["stats"][stat_type] += increment
        self.data["stats"]["last_updated"] = datetime.now().isoformat()

# Initialize Database
db = MovieDatabase(JSON_DB_FILE)

# Utility Functions
def calculate_file_hash(file_id, caption):
    content = f"{file_id}_{caption}"
    return hashlib.md5(content.encode()).hexdigest()

def clean_caption(caption, preserve_watermark=True):
    if not caption:
        return ""
    watermark_config = db.get_watermark()
    protected_items = []
    if preserve_watermark and watermark_config.get("enabled"):
        protected_items = watermark_config.get("usernames", []) + watermark_config.get("links", [])
    
    placeholder_map = {}
    for idx, item in enumerate(protected_items):
        placeholder = f"__PROTECTED{idx}__"
        caption = caption.replace(item, placeholder)
        placeholder_map[placeholder] = item
    
    caption = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*,]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', caption)
    caption = re.sub(r'www.(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*,])+', '', caption)
    caption = re.sub(r'@w+', '', caption)
    caption = re.sub(r't.me/S+', '', caption)
    
    for placeholder, original in placeholder_map.items():
        caption = caption.replace(placeholder, original)
    
    caption = re.sub(r's+', ' ', caption).strip()
    return caption

def add_watermark_to_caption(caption):
    watermark_config = db.get_watermark()
    if not watermark_config.get("enabled") or not watermark_config.get("text"):
        return caption
    caption = caption or ""
    watermark = watermark_config["text"]
    if watermark in caption:
        return caption
    if caption:
        return f"""{caption}

{watermark}"""
    else:
        return watermark

async def is_admin(user_id):
    return user_id in ADMIN_IDS

async def handle_edit_error(e, update_or_status_msg):
    error_text = str(e)
    print(f"Bounced edit error: {error_text}")
    if "Message can't be edited" in error_text:
        msg = """ℹ️ Note: 'Message can't be edited'

Ye message ya to forwarded tha ya kisi admin ne post kiya tha.
Bot ab copy-mode istemal kar raha hai..."""
        try:
            if isinstance(update_or_status_msg, Update):
                await update_or_status_msg.message.reply_text(msg)
            else:
                await update_or_status_msg.edit_text(msg)
        except Exception as e_inner:
            print(f"Error sending edit error note: {e_inner}")
    else:
        msg = f"❌ Error: {error_text}"
        try:
            if isinstance(update_or_status_msg, Update):
                await update_or_status_msg.message.reply_text(msg)
            else:
                await update_or_status_msg.edit_text(msg)
        except Exception:
            pass

# === CRITICAL FIX: TASK LOCK HELPER FUNCTIONS ===
def acquire_lock(task_name):
    if TASK_LOCKS.get(task_name, False):
        return False
    TASK_LOCKS[task_name] = True
    return True

def release_lock(task_name):
    TASK_LOCKS[task_name] = False

# Command Handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    welcome_text = f"""
🎬 Welcome {user.first_name}!

Ye bot aapke Telegram channel {CHANNEL_USERNAME} ko manage karta hai.

/help - Sabhi commands ki list dekho.
"""
    await update.message.reply_text(welcome_text)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
📚 *Bot Commands Guide:*

*1. Indexing* 🔄
/sync - Channel se sabhi movies ko scan karke database banata hai

*2. Watermark Management* 🏷️
/setwatermark - Apna channel username/link set karo
/addwatermark - Existing movies me watermark add karo
/removewatermark - Watermark disable karo
/viewwatermark - Current watermark dekho

*3. Caption Cleaning* ✨
/clean - Sabhi movies ke captions se unwanted links hatao

*4. Forwarding* 📤
/forward <channel\\_id> - Movies forward karo

*5. Management* 🛠️
/find <movie\\_name> - Database mein movie search karo
/delete <message\\_id> - Channel aur DB se movie delete karo
/duplicates - Duplicate movies detect aur delete karo

*6. Database & Stats* 📊
/stats - Complete bot statistics dekho
/backup - JSON backup download karo
/upload_db - JSON backup upload karke merge karo
/checkperms - Bot ki channel permissions check karo

*7. Control* ⏹️
/stop - Chalta hua task rok do

_Note: Lambe tasks ko /stop ya 'STOP ❌' button se rok sakte hain._
"""
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def check_permissions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    try:
        bot_id = context.bot.id
        chat_member = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=bot_id)
        status = chat_member.status
        can_edit = getattr(chat_member, 'can_edit_messages', False)
        can_delete = getattr(chat_member, 'can_delete_messages', False)
        can_post = getattr(chat_member, 'can_post_messages', False)

        response = f"""📋 *Bot Permission Check for {CHANNEL_USERNAME}*

"""
        response += f"""Bot Status: *{status.upper()}*

"""
        
        if status == "administrator":
            response += f"""• `can_post_messages`: *{can_post}* (Zaroori hai)
"""
            response += f"""• `can_edit_messages`: *{can_edit}* (Zaroori hai)
"""
            response += f"""• `can_delete_messages`: *{can_delete}* (Zaroori hai)

"""
            
            if can_edit and can_delete and can_post:
                response += """✅ *Sabhi zaroori permissions sahi hain!*
"""
            else:
                response += """❌ *ERROR!* Bot ko 'Post', 'Edit' aur 'Delete' messages ki permission dein."""
        else:
            response += """❌ *CRITICAL ERROR!* Bot channel mein Admin hi nahi hai."""
        
        await update.message.reply_text(response, parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"""❌ Error checking permissions:
`{e}`""")

# === SYNC COMMAND WITH LOCK ===
async def sync_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return

    # === FIX: CHECK LOCK ===
    if not acquire_lock('sync'):
        await update.message.reply_text("⚠️ Sync process pehle se hi chal raha hai! Kripya poora hone ka intezar karein.")
        return

    if not PYROGRAM_AVAILABLE:
        await update.message.reply_text("❌ `Pyrogram` library install nahi hai. Sync nahi ho sakta.")
        release_lock('sync')
        return

    if not all([API_ID, API_HASH, SESSION_STRING]):
        await update.message.reply_text("❌ `API_ID`, `API_HASH`, ya `SESSION_STRING` set nahi hai.")
        release_lock('sync')
        return

    context.user_data['stop_task'] = False
    keyboard = [[InlineKeyboardButton("STOP ❌", callback_data="stop_task")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    status_msg = await update.message.reply_text("🔄 Channel indexing shuru ho rahi hai...", reply_markup=reply_markup)

    pyro_client = None
    try:
        pyro_client = Client("bot_session", api_id=API_ID, api_hash=API_HASH, session_string=SESSION_STRING)
        await pyro_client.start()
        
        await status_msg.edit_text("✅ Connection successful! Messages scan ho rahe hain...", reply_markup=reply_markup)
        
        existing_hashes = db.get_all_hashes()
        added_count = 0
        skipped_count = 0
        total_processed = 0
        
        async for message in pyro_client.get_chat_history(CHANNEL_USERNAME):
            if context.user_data.get('stop_task'):
                await status_msg.edit_text("⚠️ Operation user ne rok diya.", reply_markup=None)
                break
            
            total_processed += 1
            file_id = None
            caption = message.caption or ""
            
            if message.media and (message.media == MessageMediaType.VIDEO or message.media == MessageMediaType.DOCUMENT):
                file_id = message.video.file_id if message.video else message.document.file_id
            
            if not file_id:
                continue

            file_hash = calculate_file_hash(file_id, caption)
            
            if file_hash not in existing_hashes:
                db.add_movie(message.id, file_id, caption, file_hash)
                existing_hashes.add(file_hash)
                added_count += 1
            else:
                skipped_count += 1

            if total_processed % 200 == 0:
                try:
                    await status_msg.edit_text(
                        f"""🔄 Progress...

Processed: {total_processed}
New Added: {added_count}
Duplicates Skipped: {skipped_count}""",
                        reply_markup=reply_markup
                    )
                except RetryAfter as e:
                    await asyncio.sleep(e.retry_after)
                except Exception as edit_e:
                    print(f"Error editing sync status: {edit_e}")
                await asyncio.sleep(ACTION_DELAY)
        
        if pyro_client:
            await pyro_client.stop()
        
    except Exception as e:
        print(f"Error during sync: {e}")
        await status_msg.edit_text(f"❌ Sync Error: {e}", reply_markup=None)
    finally:
        release_lock('sync')
        
        if pyro_client and pyro_client.is_connected:
            await pyro_client.stop()
        
        db.save_database()
        if not context.user_data.get('stop_task'):
            await status_msg.edit_text(
                f"""✅ *Sync Complete!*

Total Messages Scanned: {total_processed}
Naye Movies Add Kiye: {added_count}
Duplicates Skip Kiye: {skipped_count}
Total Movies ab DB mein: {len(db.get_all_movies())}""",
                reply_markup=None,
                parse_mode='Markdown'
            )
        
        if context.user_data.get('stop_task'):
            context.user_data['stop_task'] = False

# === WATERMARK COMMANDS ===
async def set_watermark_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return ConversationHandler.END
    await update.message.reply_text("""🏷️ Apna watermark text bhejo:

Cancel karne ke liye /cancel type karo.""")
    return WAITING_FOR_WATERMARK

async def set_watermark_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    watermark_text = update.message.text.strip()
    db.set_watermark(watermark_text)
    watermark_config = db.get_watermark()
    await update.message.reply_text(
        f"""✅ Watermark Successfully Set!

Watermark Text:
{watermark_config['text']}

Protected Usernames: {', '.join(watermark_config['usernames']) or 'None'}
Protected Links: {', '.join(watermark_config['links']) or 'None'}"""
    )
    return ConversationHandler.END

async def cancel_conv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Operation cancelled.")
    return ConversationHandler.END

async def view_watermark(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    watermark_config = db.get_watermark()
    if not watermark_config.get("enabled"):
        await update.message.reply_text("❌ Koi watermark set nahi hai.")
        return
    status_text = f"""
🏷️ *Current Watermark Settings*

Status: {'✅ Enabled' if watermark_config.get('enabled') else '❌ Disabled'}

*Watermark Text:*
{watermark_config.get('text', 'N/A')}

*Protected Usernames:* {', '.join(watermark_config.get('usernames', [])) or 'None'}
*Protected Links:* {', '.join(watermark_config.get('links', [])) or 'None'}
"""
    await update.message.reply_text(status_text, parse_mode='Markdown')

async def remove_watermark(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    db.disable_watermark()
    await update.message.reply_text("✅ Watermark disabled ho gaya hai!")

async def stop_task_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        return
    context.user_data['stop_task'] = True
    await update.message.reply_text("⚠️ Stop signal bhej diya gaya hai. Task agle item ke baad ruk jayega.")

# === ADD WATERMARK WITH LOCK ===
async def add_watermark_to_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return

    if not acquire_lock('addwatermark'):
        await update.message.reply_text("⚠️ Watermark addition pehle se chal raha hai!")
        return

    context.user_data['stop_task'] = False
    watermark_config = db.get_watermark()
    if not watermark_config.get("enabled"):
        await update.message.reply_text("❌ Pehle watermark set karo using /setwatermark")
        release_lock('addwatermark')
        return

    keyboard = [[InlineKeyboardButton("STOP ❌", callback_data="stop_task")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    status_msg = await update.message.reply_text("🏷️ Watermark add ho raha hai...", reply_markup=reply_markup)

    movies = db.get_all_movies()
    total = len(movies)
    added = 0

    try:
        for i, movie in enumerate(movies[:]):
            if context.user_data.get('stop_task'):
                await status_msg.edit_text("⚠️ Operation user ne rok diya.", reply_markup=None)
                break

            current_caption = movie.get("caption", "")
            if watermark_config.get("text") in current_caption:
                continue
            new_caption = add_watermark_to_caption(current_caption)

            if new_caption != current_caption:
                try:
                    await context.bot.edit_message_caption(
                        chat_id=CHANNEL_ID, message_id=movie["message_id"], caption=new_caption
                    )
                    movie["caption"] = new_caption
                except BadRequest as e:
                    if "Message can't be edited" in str(e):
                        print(f"Edit failed for {movie['message_id']}, trying copy...")
                        new_msg_id_obj = await context.bot.copy_message(
                            chat_id=CHANNEL_ID, from_chat_id=CHANNEL_ID, message_id=movie["message_id"], caption=new_caption
                        )
                        await context.bot.delete_message(
                            chat_id=CHANNEL_ID, message_id=movie["message_id"]
                        )
                        db.update_movie_entry(movie["message_id"], new_msg_id_obj.message_id, new_caption)
                    else:
                        raise e
                added += 1
                db.update_stats("watermarks_added")
                await asyncio.sleep(ACTION_DELAY)

            if (i + 1) % BATCH_SIZE == 0 and (i + 1) < total:
                await status_msg.edit_text(
                    f"""🏷️ Batch {i // BATCH_SIZE + 1} complete. Added: {added}
Processed: {i + 1}/{total}
Sleeping for {BATCH_SLEEP}s...""",
                    reply_markup=reply_markup
                )
                await asyncio.sleep(BATCH_SLEEP)

    except RetryAfter as e:
        print(f"Flood control hit: {e}")
        await status_msg.edit_text(f"❌ FLOOD ERROR! Retry in {e.retry_after}s.", reply_markup=None)
    except Exception as e:
        print(f"Error adding watermark: {e}")
        await handle_edit_error(e, status_msg)
    finally:
        release_lock('addwatermark')
        db.save_database()
        if not context.user_data.get('stop_task'):
            await status_msg.edit_text(
                f"""✅ *Watermark Addition Complete!*

*Total Movies:* {total}
*Watermark Added:* {added}
*Already Had:* {total - added}""",
                reply_markup=None,
                parse_mode='Markdown'
            )
        if context.user_data.get('stop_task'):
            context.user_data['stop_task'] = False

# === CLEAN CAPTIONS WITH LOCK ===
async def clean_captions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return

    if not acquire_lock('clean'):
        await update.message.reply_text("⚠️ Cleaning process pehle se chal raha hai!")
        return

    context.user_data['stop_task'] = False
    keyboard = [[InlineKeyboardButton("STOP ❌", callback_data="stop_task")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    status_msg = await update.message.reply_text("🔄 Caption cleaning shuru ho rahi hai...", reply_markup=reply_markup)

    movies = db.get_all_movies()
    total = len(movies)
    cleaned = 0

    try:
        for i, movie in enumerate(movies[:]):
            if context.user_data.get('stop_task'):
                await status_msg.edit_text("⚠️ Operation user ne rok diya.", reply_markup=None)
                break

            original_caption = movie.get("caption", "")
            new_caption = clean_caption(original_caption, preserve_watermark=True)

            if new_caption != original_caption:
                try:
                    await context.bot.edit_message_caption(
                        chat_id=CHANNEL_ID, message_id=movie["message_id"], caption=new_caption
                    )
                    movie["caption"] = new_caption
                except BadRequest as e:
                    if "Message can't be edited" in str(e):
                        print(f"Edit failed for {movie['message_id']}, trying copy...")
                        new_msg_id_obj = await context.bot.copy_message(
                            chat_id=CHANNEL_ID, from_chat_id=CHANNEL_ID, message_id=movie["message_id"], caption=new_caption
                        )
                        await context.bot.delete_message(
                            chat_id=CHANNEL_ID, message_id=movie["message_id"]
                        )
                        db.update_movie_entry(movie["message_id"], new_msg_id_obj.message_id, new_caption)
                    else:
                        raise e
                cleaned += 1
                db.update_stats("total_cleaned")
                await asyncio.sleep(ACTION_DELAY)

            if (i + 1) % BATCH_SIZE == 0 and (i + 1) < total:
                await status_msg.edit_text(
                    f"""🔄 Batch {i // BATCH_SIZE + 1} complete. Cleaned: {cleaned}
Processed: {i + 1}/{total}
Sleeping for {BATCH_SLEEP}s...""",
                    reply_markup=reply_markup
                )
                await asyncio.sleep(BATCH_SLEEP)

    except RetryAfter as e:
        print(f"Flood control hit: {e}")
        await status_msg.edit_text(f"❌ FLOOD ERROR! Retry in {e.retry_after}s.", reply_markup=None)
    except Exception as e:
        print(f"Error cleaning captions: {e}")
        await handle_edit_error(e, status_msg)
    finally:
        release_lock('clean')
        db.save_database()
        if not context.user_data.get('stop_task'):
            await status_msg.edit_text(
                f"""✅ *Cleaning Complete!*

Total Movies: {total}
Cleaned: {cleaned}
Unchanged: {total - cleaned}""",
                reply_markup=None,
                parse_mode='Markdown'
            )
        if context.user_data.get('stop_task'):
            context.user_data['stop_task'] = False

# === FORWARD WITH LOCK ===
async def forward_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    if not context.args:
        await update.message.reply_text("⚠️ Usage: `/forward <channel_id>`", parse_mode='Markdown')
        return

    if not acquire_lock('forward'):
        await update.message.reply_text("⚠️ Forwarding pehle se chal raha hai!")
        return

    context.user_data['stop_task'] = False
    target_channel = context.args[0]
    keyboard = [[InlineKeyboardButton("STOP ❌", callback_data="stop_task")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    status_msg = await update.message.reply_text(f"🔄 Forwarding shuru ho rahi hai... -> {target_channel}", reply_markup=reply_markup)

    movies = db.get_all_movies()
    total = len(movies)
    forwarded = 0

    try:
        for i, movie in enumerate(movies):
            if context.user_data.get('stop_task'):
                await status_msg.edit_text("⚠️ Operation user ne rok diya.", reply_markup=None)
                break

            try:
                await context.bot.forward_message(
                    chat_id=target_channel,
                    from_chat_id=CHANNEL_ID,
                    message_id=movie["message_id"]
                )
                forwarded += 1
                db.update_stats("total_forwarded")
                await asyncio.sleep(ACTION_DELAY)
            except RetryAfter as e:
                print(f"Flood control hit during forward: {e}")
                await status_msg.edit_text(f"❌ FLOOD ERROR! {e.retry_after}s tak rukna padega...", reply_markup=reply_markup)
                await asyncio.sleep(e.retry_after)
                continue
            except Exception as e:
                print(f"Error forwarding message {movie['message_id']}: {e}")
                continue

            if (i + 1) % BATCH_SIZE == 0 and (i + 1) < total:
                await status_msg.edit_text(
                    f"""📤 Batch {i // BATCH_SIZE + 1} complete. Forwarded: {forwarded}
Processed: {i + 1}/{total}
Sleeping for {BATCH_SLEEP}s...""",
                    reply_markup=reply_markup
                )
                await asyncio.sleep(BATCH_SLEEP)

    except RetryAfter as e:
        print(f"Flood control hit: {e}")
        await status_msg.edit_text(f"❌ FLOOD ERROR! Retry in {e.retry_after}s.", reply_markup=None)
    except Exception as e:
        print(f"Error forwarding movies: {e}")
        await status_msg.edit_text(f"❌ Error: {str(e)}", reply_markup=None)
    finally:
        release_lock('forward')
        db.save_database()
        if not context.user_data.get('stop_task'):
            await status_msg.edit_text(
                f"""✅ *Forwarding Complete!*

Total Movies: {total}
Forwarded: {forwarded}
Failed: {total - forwarded}""",
                reply_markup=None,
                parse_mode='Markdown'
            )
        if context.user_data.get('stop_task'):
            context.user_data['stop_task'] = False

# === DUPLICATES HANDLER ===
async def handle_duplicates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    status_msg = await update.message.reply_text("🔍 Duplicates detect ho rahe hain...")
    duplicates = db.find_duplicates()
    if not duplicates:
        await status_msg.edit_text("✅ Koi duplicate movies nahi mili!")
        return
    keyboard = [
        [InlineKeyboardButton("🗑️ Delete Duplicates", callback_data="delete_duplicates")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_duplicates")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await status_msg.edit_text(f"""⚠️ {len(duplicates)} Duplicate Movies Found!

Kya aap inhe delete karna chahte hain?""", reply_markup=reply_markup)
    context.user_data['duplicates'] = duplicates

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "stop_task":
        context.user_data['stop_task'] = True
        try:
            await query.edit_message_text("⚠️ Stop signal received. Finishing current item...")
        except Exception as e:
            print(f"Error editing stop message: {e}")
        return

    if query.data == "delete_duplicates":
        if not acquire_lock('duplicates'):
            await query.edit_message_text("⚠️ Duplicate deletion pehle se chal raha hai!")
            return

        duplicates = context.user_data.get('duplicates', [])
        deleted = 0
        status_msg = await query.edit_message_text("🗑️ Duplicates delete ho rahe hain...")

        try:
            for idx, movie in reversed(duplicates):
                try:
                    await context.bot.delete_message(
                        chat_id=CHANNEL_ID,
                        message_id=movie["message_id"]
                    )
                    db.remove_movie_by_index(idx)
                    deleted += 1
                    await asyncio.sleep(ACTION_DELAY)
                except Exception as e:
                    print(f"Error deleting duplicate: {e}")
                    if "Message to delete not found" in str(e):
                        db.remove_movie_by_index(idx)
                    elif "message can't be deleted" in str(e):
                        await status_msg.edit_text(f"""❌ *PERMISSION ERROR!*
Bot ko *'Delete messages'* ki permission dein.""", parse_mode='Markdown')
                        db.save_database()
                        release_lock('duplicates')
                        return
            db.save_database()
            await status_msg.edit_text(f"✅ *{deleted} Duplicate Movies Deleted!*", parse_mode='Markdown')
        finally:
            release_lock('duplicates')

    elif query.data == "cancel_duplicates":
        await query.edit_message_text("❌ Duplicate deletion cancelled.")

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    stats = db.get_stats()
    watermark_config = db.get_watermark()
    stats_text = f"""
📊 *Bot Statistics*

🎬 Total Movies: {stats['total_movies']}
✨ Captions Cleaned: {stats['total_cleaned']}
📤 Movies Forwarded: {stats['total_forwarded']}
🗑️ Duplicates Removed: {stats['duplicates_removed']}
🏷️ Watermarks Added: {stats['watermarks_added']}

Watermark Status: {'✅ Enabled' if watermark_config.get('enabled') else '❌ Disabled'}
🕒 Last Updated: {stats.get('last_updated', 'Never')}

Channel: {CHANNEL_USERNAME}
Channel ID: `{CHANNEL_ID}`
"""
    await update.message.reply_text(stats_text, parse_mode='Markdown')

async def refresh_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    db.data = db.load_database()
    await update.message.reply_text(f"""✅ Database file re-loaded! Total Movies: {len(db.get_all_movies())}

Note: Agar bot restart hua tha, to /sync ka istemal karein.""")

async def backup_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    if db.save_database():
        try:
            with open(JSON_DB_FILE, 'rb') as f:
                await update.message.reply_document(
                    document=f,
                    filename=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    caption="📦 Database Backup"
                )
        except Exception as e:
            await update.message.reply_text(f"❌ Error creating backup: {str(e)}")
    else:
        await update.message.reply_text("❌ Database save karne mein error aaya.")

# === JSON Database Upload Feature ===
async def upload_db_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return ConversationHandler.END
    await update.message.reply_text("""Kripya movies_database.json file upload karein.

Cancel karne ke liye /cancel type karein.""")
    return WAITING_FOR_UPLOAD_FILE

async def upload_db_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message.document:
            await update.message.reply_text("❌ Ye file nahi hai. Kripya .json file bhejein.")
            return WAITING_FOR_UPLOAD_FILE

        status_msg = await update.message.reply_text("🔄 Database file download ho rahi hai...")

        json_file = await update.message.document.get_file()
        await json_file.download_to_drive(f"temp_{JSON_DB_FILE}")

        await status_msg.edit_text("🔄 Database merge ho raha hai (duplicates skip kiye jayenge)...")

        uploaded_data = {}
        with open(f"temp_{JSON_DB_FILE}", 'r', encoding='utf-8') as f:
            uploaded_data = json.load(f)

        if "movies" not in uploaded_data:
            await status_msg.edit_text("❌ Invalid JSON file. 'movies' key nahi mili.")
            os.remove(f"temp_{JSON_DB_FILE}")
            return ConversationHandler.END

        current_hashes = db.get_all_hashes()
        added_count = 0
        skipped_count = 0

        for movie in uploaded_data.get("movies", []):
            file_hash = movie.get("file_hash")
            if not file_hash:
                skipped_count += 1
                continue

            if file_hash not in current_hashes:
                db.data["movies"].append(movie)
                current_hashes.add(file_hash)
                added_count += 1
            else:
                skipped_count += 1

        db.save_database()
        os.remove(f"temp_{JSON_DB_FILE}")

        await status_msg.edit_text(
            f"""✅ *Database Merge Complete!*

Naye movies add kiye: {added_count}
Duplicates skip kiye: {skipped_count}
Total movies ab: {len(db.data['movies'])}""",
            parse_mode='Markdown'
        )

    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

    return ConversationHandler.END

# === FIND MOVIE ===
async def find_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return

    query = " ".join(context.args).lower()
    if not query:
        await update.message.reply_text("⚠️ Usage: `/find <movie_name>`", parse_mode='Markdown')
        return

    movies = db.get_all_movies()
    results = []
    for movie in movies:
        if query in movie.get("caption", "").lower():
            results.append(movie)

    if not results:
        await update.message.reply_text("❌ Is naam se koi movie database mein nahi mili.")
        return

    response = f"""🔍 *Search Results for '{query}' ({len(results)} found):*

"""
    channel_link_base = f"https://t.me/c/{str(CHANNEL_ID).replace('-100', '')}/"

    for i, movie in enumerate(results[:20]):
        # === THE FINAL FIX ===
        # Changed split('
') to split('\n')
        caption_preview = movie.get("caption", "No Caption").split('\n')[0]
        response += f"""{i+1}. [Link to Movie]({channel_link_base}{movie['message_id']})
"""
        response += f"""   `{caption_preview[:70]}...`
"""

    if len(results) > 20:
        response += f"""
...aur {len(results) - 20} results."""

    await update.message.reply_text(response, parse_mode='Markdown', disable_web_page_preview=True)

# === DELETE MOVIE ===
async def delete_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return

    try:
        message_id_to_delete = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("⚠️ Usage: `/delete <message_id>`", parse_mode='Markdown')
        return

    try:
        await context.bot.delete_message(chat_id=CHANNEL_ID, message_id=message_id_to_delete)

        if db.remove_movie_by_message_id(message_id_to_delete):
            await update.message.reply_text(f"✅ Movie (ID: {message_id_to_delete}) channel aur database dono se delete ho gayi hai.")
        else:
            await update.message.reply_text(f"⚠️ Movie channel se delete ho gayi, lekin database mein nahi mili.")

    except BadRequest as e:
        if "Message to delete not found" in str(e):
            await update.message.reply_text("❌ Ye message channel mein nahi mila. Shayad pehle hi delete ho chuka hai.")
            if db.remove_movie_by_message_id(message_id_to_delete):
                await update.message.reply_text(f"ℹ️ Message ko database se bhi remove kar diya gaya hai.")
        elif "message can't be deleted" in str(e):
            await update.message.reply_text("""❌ *PERMISSION ERROR!*
Bot ko *'Delete messages'* ki permission dein.""", parse_mode='Markdown')
        else:
            await update.message.reply_text(f"❌ Error: {e}")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

# === FASTAPI SETUP ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await application.initialize()
    await application.start()
    
    # === CRITICAL FIX: WEBHOOK CLEANUP ===
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
        print("✅ Old webhook deleted successfully")
    except Exception as e:
        print(f"Warning: Could not delete old webhook: {e}")
    
    await application.bot.set_webhook(url=f"{WEBHOOK_URL}/webhook")
    print(f"✅ Webhook set to: {WEBHOOK_URL}/webhook")
    
    yield
    
    # Shutdown
    await application.stop()
    await application.shutdown()

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(request: Request):
    try:
        json_data = await request.json()
        update = Update.de_json(json_data, application.bot)
        await application.update_queue.put(update)
        return Response(status_code=HTTPStatus.OK)
    except Exception as e:
        print(f"Webhook error: {e}")
        return Response(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)

@app.get("/")
async def root():
    return {"status": "Bot is running!", "channel": CHANNEL_USERNAME}

# === APPLICATION SETUP ===
application = Application.builder().token(BOT_TOKEN).build()

# Conversation Handlers
watermark_conv = ConversationHandler(
    entry_points=[CommandHandler("setwatermark", set_watermark_start)],
    states={
        WAITING_FOR_WATERMARK: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_watermark_receive)]
    },
    fallbacks=[CommandHandler("cancel", cancel_conv)]
)

upload_db_conv = ConversationHandler(
    entry_points=[CommandHandler("upload_db", upload_db_start)],
    states={
        WAITING_FOR_UPLOAD_FILE: [MessageHandler(filters.Document.ALL, upload_db_receive)]
    },
    fallbacks=[CommandHandler("cancel", cancel_conv)]
)

# Add handlers
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("help", help_command))
application.add_handler(CommandHandler("sync", sync_channel))
application.add_handler(CommandHandler("clean", clean_captions))
application.add_handler(CommandHandler("forward", forward_movies))
application.add_handler(CommandHandler("addwatermark", add_watermark_to_movies))
application.add_handler(CommandHandler("removewatermark", remove_watermark))
application.add_handler(CommandHandler("viewwatermark", view_watermark))
application.add_handler(CommandHandler("duplicates", handle_duplicates))
application.add_handler(CommandHandler("stats", show_stats))
application.add_handler(CommandHandler("refresh", refresh_database))
application.add_handler(CommandHandler("backup", backup_database))
application.add_handler(CommandHandler("checkperms", check_permissions))
application.add_handler(CommandHandler("find", find_movie))
application.add_handler(CommandHandler("delete", delete_movie))
application.add_handler(CommandHandler("stop", stop_task_command))
application.add_handler(watermark_conv)
application.add_handler(upload_db_conv)
application.add_handler(CallbackQueryHandler(button_callback))

if __name__ == "__main__":
    print("🚀 Bot starting...")
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
