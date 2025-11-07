import os
import json
import re
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from http import HTTPStatus
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Document
# === FIX: FloodControl ka naam RetryAfter hai ===
from telegram.error import BadRequest, RetryAfter
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ConversationHandler
from telegram.ext._contexttypes import ContextTypes
from fastapi import FastAPI, Request, Response
import hashlib
import uvicorn

# Configuration
BOT_TOKEN = os.environ.get('BOT_TOKEN')
WEBHOOK_URL = os.environ.get('WEBHOOK_URL')
ADMIN_IDS = [int(id.strip()) for id in os.environ.get('ADMIN_IDS', '').split(',') if id.strip()]
CHANNEL_ID = -1002417767287
CHANNEL_USERNAME = "@MAZABACKUP01"
JSON_DB_FILE = "movies_database.json"

# Batch & Flood Control Config
BATCH_SIZE = 100
ACTION_DELAY = 0.5  # Har action ke beech 500ms ka delay
BATCH_SLEEP = 5     # Har 100 ke batch ke baad 5 second ka delay

# Conversation states
(WAITING_FOR_WATERMARK, WAITING_FOR_UPLOAD_FILE) = range(2)


# Database Handler
class MovieDatabase:
    def __init__(self, db_file):
        self.db_file = db_file
        self.data = self.load_database()
    
    def load_database(self):
        try:
            if os.path.exists(self.db_file):
                with open(self.db_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading database: {e}")
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
            # Update total movies stat before saving
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
        self.data["stats"]["total_movies"] += 1
        self.data["stats"]["last_updated"] = datetime.now().isoformat()
        # Save database handled by caller
        
    def update_movie_entry(self, old_message_id, new_message_id, new_caption):
        """Database mein entry update karta hai jab message copy hota hai."""
        try:
            for movie in self.data["movies"]:
                if movie["message_id"] == old_message_id:
                    movie["message_id"] = new_message_id
                    movie["caption"] = new_caption
                    # File ID remains the same, but hash might change due to caption
                    movie["file_hash"] = calculate_file_hash(movie["file_id"], new_caption)
                    # Save database handled by caller
                    return True
            return False
        except Exception as e:
            print(f"Error updating DB entry for {old_message_id}: {e}")
            return False

    def get_all_movies(self):
        return self.data["movies"]
    
    def get_all_hashes(self):
        """Duplicate check ke liye sabhi file hashes ka ek set return karta hai."""
        return {movie.get("file_hash") for movie in self.data["movies"] if movie.get("file_hash")}

    def find_duplicates(self):
        seen = {}
        duplicates = []
        for idx, movie in enumerate(self.data["movies"]):
            hash_key = movie.get("file_hash")
            if hash_key in seen:
                duplicates.append((idx, movie))
            else:
                seen[hash_key] = idx
        return duplicates
    
    def remove_movie_by_index(self, index):
        if 0 <= index < len(self.data["movies"]):
            self.data["movies"].pop(index)
            self.data["stats"]["total_movies"] -= 1
            self.data["stats"]["duplicates_removed"] += 1
            return True
        return False
    
    def set_watermark(self, watermark_text):
        self.data["watermark"]["text"] = watermark_text
        self.data["watermark"]["enabled"] = True
        usernames = re.findall(r'@\w+', watermark_text)
        links = re.findall(r'(?:https?://)?(?:www\.)?(?:t\.me/|telegram\.me/)[\w\.-]+(?:/[\w-]+)*', watermark_text)
        self.data["watermark"]["usernames"] = list(set(usernames))
        self.data["watermark"]["links"] = list(set(links))
        self.save_database()
    
    def get_watermark(self):
        return self.data["watermark"]
    
    def disable_watermark(self):
        self.data["watermark"]["enabled"] = False
        self.save_database()
    
    def get_stats(self):
        return self.data["stats"]
    
    def update_stats(self, stat_type, increment=1):
        if stat_type in self.data["stats"]:
            self.data["stats"][stat_type] += increment
        self.data["stats"]["last_updated"] = datetime.now().isoformat()
        # Save handled by the caller function

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
    if preserve_watermark and watermark_config["enabled"]:
        protected_items = watermark_config["usernames"] + watermark_config["links"]
    placeholder_map = {}
    for idx, item in enumerate(protected_items):
        placeholder = f"__PROTECTED_{idx}__"
        caption = caption.replace(item, placeholder)
        placeholder_map[placeholder] = item
    caption = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', caption)
    caption = re.sub(r'www\.(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),])+', '', caption)
    caption = re.sub(r'@\w+', '', caption)
    caption = re.sub(r't\.me/\S+', '', caption)
    for placeholder, original in placeholder_map.items():
        caption = caption.replace(placeholder, original)
    caption = re.sub(r'\s+', ' ', caption).strip()
    return caption

def add_watermark_to_caption(caption):
    watermark_config = db.get_watermark()
    if not watermark_config["enabled"] or not watermark_config["text"]:
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
        msg = (
            "ℹ️ **Note: 'Message can't be edited'**\n\n"
            "Ye message ya to forwarded tha ya kisi admin ne post kiya tha.\n"
            "**Bot ab copy-mode istemal kar raha hai...**"
        )
        try:
            if isinstance(update_or_status_msg, Update):
                await update_or_status_msg.message.reply_text(msg, parse_mode='Markdown')
            else:
                await update_or_status_msg.edit_text(msg, parse_mode='Markdown')
        except Exception as e_inner:
            print(f"Error sending edit error note: {e_inner}")
    else:
        msg = f"❌ Error: {error_text}"
        if isinstance(update_or_status_msg, Update):
            await update_or_status_msg.message.reply_text(msg)
        else:
            await update_or_status_msg.edit_text(msg)


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
📚 **Bot Commands Guide:**

**1. Watermark Management** 🏷️
   `/setwatermark` - Apna channel username/link set karo
   `/addwatermark` - Existing sabhi movies me watermark add karo (100 ke batch mein)
   `/removewatermark` - Watermark feature disable karo
   `/viewwatermark` - Current watermark settings dekho

**2. Caption Cleaning** ✨
   `/clean` - Sabhi movies ke captions se unwanted links/usernames hatao (100 ke batch mein)

**3. Forwarding** 📤
   `/forward <channel_id>` - Movies forward karo (100 ke batch mein)

**4. Duplicate Management** 🗑️
   `/duplicates` - Duplicate movies detect aur delete karo

**5. Statistics** 📊
   `/stats` - Complete bot statistics dekho

**6. Database** 💾
   `/backup` - JSON backup download karo.
   `/upload_db` - JSON backup upload karke merge karo (duplicates ignore honge).
   `/refresh` - Database refresh karo.

**7. Diagnostics** 🛠️
   `/checkperms` - Bot ki channel permissions check karo

**Note:** Lambe tasks (clean, forward, addwatermark) ke dauran /stop command ya 'STOP ❌' button ka istemal kar sakte hain.
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
        
        response = f"📋 **Bot Permission Check for {CHANNEL_USERNAME}**\n\n"
        response += f"Bot Status: **{status.upper()}**\n\n"
        
        if status == "administrator":
            response += f"• `can_post_messages`: **{can_post}** (Zaroori hai)\n"
            response += f"• `can_edit_messages`: **{can_edit}** (Zaroori hai)\n"
            response += f"• `can_delete_messages`: **{can_delete}** (Zaroori hai)\n\n"
            
            if can_edit and can_delete and can_post:
                response += "✅ **Sabhi zaroori permissions sahi hain!**\n"
            else:
                response += "❌ **ERROR!** Bot ko 'Post', 'Edit' aur 'Delete' messages ki permission dein."
        else:
            response += f"❌ **CRITICAL ERROR!** Bot channel mein Admin hi nahi hai."
        await update.message.reply_text(response, parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"❌ Error checking permissions:\n`{e}`")

async def set_watermark_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return ConversationHandler.END
    await update.message.reply_text("🏷️ Apna watermark text bhejo:\n\nCancel karne ke liye /cancel type karo.", parse_mode='Markdown')
    return WAITING_FOR_WATERMARK

async def set_watermark_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    watermark_text = update.message.text.strip()
    db.set_watermark(watermark_text)
    # === FIX: User ko feedback dena ki kya set hua hai ===
    watermark_config = db.get_watermark()
    await update.message.reply_text(
        f"✅ **Watermark Successfully Set!**\n\n"
        f"**Watermark Text:**\n{watermark_config['text']}\n\n"
        f"**Protected Usernames:** {', '.join(watermark_config['usernames']) or 'None'}\n"
        f"**Protected Links:** {', '.join(watermark_config['links']) or 'None'}",
        parse_mode='Markdown'
    )
    return ConversationHandler.END

async def cancel_conv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Universal cancel handler for conversations."""
    await update.message.reply_text("❌ Operation cancelled.")
    return ConversationHandler.END

async def view_watermark(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    watermark_config = db.get_watermark()
    if not watermark_config["enabled"]:
        await update.message.reply_text("❌ Koi watermark set nahi hai.")
        return
    # === FIX: User ko poori details dena ===
    status_text = f"""
🏷️ **Current Watermark Settings**

**Status:** {'✅ Enabled' if watermark_config['enabled'] else '❌ Disabled'}
**Watermark Text:**
{watermark_config['text']}

**Protected Usernames:** {', '.join(watermark_config['usernames']) or 'None'}
**Protected Links:** {', '.join(watermark_config['links']) or 'None'}
    """
    await update.message.reply_text(status_text, parse_mode='Markdown')

async def remove_watermark(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    db.disable_watermark()
    await update.message.reply_text("✅ Watermark disabled ho gaya hai!")

async def stop_task_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Command to stop an ongoing task."""
    if not await is_admin(update.effective_user.id):
        return
    context.user_data['stop_task'] = True
    await update.message.reply_text("⚠️ Stop signal bhej diya gaya hai. Task agle item ke baad ruk jayega.")

async def add_watermark_to_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    context.user_data['stop_task'] = False # Flag reset karo
    watermark_config = db.get_watermark()
    if not watermark_config["enabled"]:
        await update.message.reply_text("❌ Pehle watermark set karo using /setwatermark")
        return
    
    keyboard = [[InlineKeyboardButton("STOP ❌", callback_data="stop_task")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    status_msg = await update.message.reply_text("🏷️ Watermark add ho raha hai... (Batch 1)", reply_markup=reply_markup)
    
    movies = db.get_all_movies()
    total = len(movies)
    added = 0
    
    try:
        for i, movie in enumerate(movies[:]): # Iterate over a copy
            if context.user_data.get('stop_task'):
                await status_msg.edit_text("⚠️ Operation user ne rok diya.", reply_markup=None)
                break
            
            current_caption = movie.get("caption", "")
            if watermark_config["text"] in current_caption:
                continue
            new_caption = add_watermark_to_caption(current_caption)
            
            if new_caption != current_caption:
                try:
                    await context.bot.edit_message_caption(
                        chat_id=CHANNEL_ID, message_id=movie["message_id"], caption=new_caption
                    )
                    movie["caption"] = new_caption # Update in-memory
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
                    f"🏷️ Batch {i // BATCH_SIZE + 1} complete. Added: {added}\n"
                    f"Processed: {i + 1}/{total}\n"
                    f"Sleeping for {BATCH_SLEEP}s...",
                    reply_markup=reply_markup
                )
                await asyncio.sleep(BATCH_SLEEP)

    except RetryAfter as e:
        print(f"Flood control hit: {e}")
        await status_msg.edit_text(f"❌ FLOOD ERROR! Retry in {e.retry_after}s. Stopping task.", reply_markup=None)
    except Exception as e:
        print(f"Error adding watermark: {e}")
        await handle_edit_error(e, status_msg)
    finally:
        db.save_database()
        if not context.user_data.get('stop_task'):
            await status_msg.edit_text(
                f"✅ **Watermark Addition Complete!**\n\n"
                f"**Total Movies:** {total}\n"
                f"**Watermark Added:** {added}\n"
                f"**Already Had:** {total - added}",
                reply_markup=None
            )

async def clean_captions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    context.user_data['stop_task'] = False # Flag reset karo
    keyboard = [[InlineKeyboardButton("STOP ❌", callback_data="stop_task")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    status_msg = await update.message.reply_text("🔄 Caption cleaning shuru ho rahi hai... (Batch 1)", reply_markup=reply_markup)
    
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
                    f"🔄 Batch {i // BATCH_SIZE + 1} complete. Cleaned: {cleaned}\n"
                    f"Processed: {i + 1}/{total}\n"
                    f"Sleeping for {BATCH_SLEEP}s...",
                    reply_markup=reply_markup
                )
                await asyncio.sleep(BATCH_SLEEP)

    except RetryAfter as e:
        print(f"Flood control hit: {e}")
        await status_msg.edit_text(f"❌ FLOOD ERROR! Retry in {e.retry_after}s. Stopping task.", reply_markup=None)
    except Exception as e:
        print(f"Error cleaning captions: {e}")
        await handle_edit_error(e, status_msg)
    finally:
        db.save_database()
        if not context.user_data.get('stop_task'):
            await status_msg.edit_text(
                f"✅ **Cleaning Complete!**\n\n"
                f"Total Movies: {total}\n"
                f"Cleaned: {cleaned}\n"
                f"Unchanged: {total - cleaned}",
                reply_markup=None
            )

async def forward_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    if not context.args:
        await update.message.reply_text("⚠️ Usage: `/forward <channel_id>`", parse_mode='Markdown')
        return
    
    context.user_data['stop_task'] = False # Flag reset karo
    target_channel = context.args[0]
    keyboard = [[InlineKeyboardButton("STOP ❌", callback_data="stop_task")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    status_msg = await update.message.reply_text(f"🔄 Forwarding shuru ho rahi hai... -> {target_channel}", reply_markup=reply_markup)
    
    movies = db.get_all_movies()
    total = len(movies); forwarded = 0
    
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
                continue # Isi movie ko dobara try karo
            except Exception as e:
                print(f"Error forwarding message {movie['message_id']}: {e}")
                continue
            
            if (i + 1) % BATCH_SIZE == 0 and (i + 1) < total:
                await status_msg.edit_text(
                    f"📤 Batch {i // BATCH_SIZE + 1} complete. Forwarded: {forwarded}\n"
                    f"Processed: {i + 1}/{total}\n"
                    f"Sleeping for {BATCH_SLEEP}s...",
                    reply_markup=reply_markup
                )
                await asyncio.sleep(BATCH_SLEEP)

    except RetryAfter as e:
        print(f"Flood control hit: {e}")
        await status_msg.edit_text(f"❌ FLOOD ERROR! Retry in {e.retry_after}s. Stopping task.", reply_markup=None)
    except Exception as e:
        print(f"Error forwarding movies: {e}")
        await status_msg.edit_text(f"❌ Error: {str(e)}", reply_markup=None)
    finally:
        db.save_database()
        if not context.user_data.get('stop_task'):
            await status_msg.edit_text(
                f"✅ **Forwarding Complete!**\n\n"
                f"Total Movies: {total}\n"
                f"Forwarded: {forwarded}\n"
                f"Failed: {total - forwarded}",
                reply_markup=None
            )

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
    await status_msg.edit_text(f"⚠️ **{len(duplicates)} Duplicate Movies Found!**\n\nKya aap inhe delete karna chahte hain?", reply_markup=reply_markup)
    context.user_data['duplicates'] = duplicates

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == "stop_task":
        context.user_data['stop_task'] = True
        await query.edit_message_text("⚠️ Stop signal received. Finishing current item...")
        return
        
    if query.data == "delete_duplicates":
        duplicates = context.user_data.get('duplicates', [])
        deleted = 0
        status_msg = await query.edit_message_text("🗑️ Duplicates delete ho rahe hain...")
        
        for idx, movie in reversed(duplicates):
            try:
                await context.bot.delete_message(
                    chat_id=CHANNEL_ID,
                    message_id=movie["message_id"]
                )
                db.remove_movie_by_index(idx)
                deleted += 1
                await asyncio.sleep(ACTION_DELAY) # Flood control
            except Exception as e:
                print(f"Error deleting duplicate: {e}")
                if "Message to delete not found" in str(e):
                    db.remove_movie_by_index(idx)
                elif "message can't be deleted" in str(e):
                     await status_msg.edit_text(f"❌ **PERMISSION ERROR!**\nBot ko **'Delete messages'** ki permission dein.")
                     db.save_database()
                     return
        db.save_database()
        await status_msg.edit_text(f"✅ **{deleted} Duplicate Movies Deleted!**")
    
    elif query.data == "cancel_duplicates":
        await query.edit_message_text("❌ Duplicate deletion cancelled.")

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    stats = db.get_stats()
    watermark_config = db.get_watermark()
    stats_text = f"""
📊 **Bot Statistics**
🎬 Total Movies: {stats['total_movies']}
✨ Captions Cleaned: {stats['total_cleaned']}
📤 Movies Forwarded: {stats['total_forwarded']}
🗑️ Duplicates Removed: {stats['duplicates_removed']}
🏷️ Watermarks Added: {stats['watermarks_added']}
**Watermark Status:** {'✅ Enabled' if watermark_config['enabled'] else '❌ Disabled'}
🕒 Last Updated: {stats.get('last_updated', 'Never')}
**Channel:** {CHANNEL_USERNAME}
**Channel ID:** `{CHANNEL_ID}`
    """
    await update.message.reply_text(stats_text, parse_mode='Markdown')

async def refresh_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    db.data = db.load_database()
    await update.message.reply_text(f"✅ Database refreshed! Total Movies: {len(db.get_all_movies())}")

async def backup_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    db.save_database()
    try:
        with open(JSON_DB_FILE, 'rb') as f:
            await update.message.reply_document(
                document=f,
                filename=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                caption="📦 Database Backup"
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Error creating backup: {str(e)}")

# === NEW: JSON Database Upload Feature ===
async def upload_db_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start DB upload process."""
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return ConversationHandler.END
    await update.message.reply_text("Kripya `movies_database.json` file upload karein.\n\nCancel karne ke liye /cancel type karein.")
    return WAITING_FOR_UPLOAD_FILE

async def upload_db_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive and merge uploaded JSON file."""
    try:
        if not update.message.document:
            await update.message.reply_text("❌ Ye file nahi hai. Kripya `.json` file bhejein.")
            return WAITING_FOR_UPLOAD_FILE
        
        status_msg = await update.message.reply_text("🔄 Database file download ho rahi hai...")
        
        json_file = await update.message.document.get_file()
        await json_file.download_to_drive(f"temp_{JSON_DB_FILE}")
        
        await status_msg.edit_text("🔄 Database merge ho raha hai (duplicates skip kiye jayenge)...")
        
        # Load uploaded data
        uploaded_data = {}
        with open(f"temp_{JSON_DB_FILE}", 'r', encoding='utf-8') as f:
            uploaded_data = json.load(f)
        
        if "movies" not in uploaded_data:
             await status_msg.edit_text("❌ Invalid JSON file. 'movies' key nahi mili.")
             os.remove(f"temp_{JSON_DB_FILE}")
             return ConversationHandler.END

        # Get current hashes for duplicate check
        current_hashes = db.get_all_hashes()
        added_count = 0
        skipped_count = 0
        
        for movie in uploaded_data["movies"]:
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
        os.remove(f"temp_{JSON_DB_FILE}") # Clean up temp file
        
        await status_msg.edit_text(
            f"✅ **Database Merge Complete!**\n\n"
            f"Naye movies add kiye: {added_count}\n"
            f"Duplicates skip kiye: {skipped_count}\n"
            f"Total movies ab: {len(db.data['movies'])}"
        )
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
    
    return ConversationHandler.END
# === End of new feature ===

async def handle_new_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.channel_post and update.channel_post.chat.id == CHANNEL_ID:
        message = update.channel_post
        
        if message.video or message.document:
            file_id = message.video.file_id if message.video else message.document.file_id
            caption = message.caption or ""
            original_message_id = message.message_id
            message_id_to_save = original_message_id
            caption_updated = False

            watermark_config = db.get_watermark()
            if watermark_config["enabled"]:
                new_caption = add_watermark_to_caption(caption)
                if new_caption != caption:
                    try:
                        await context.bot.edit_message_caption(
                            chat_id=CHANNEL_ID, message_id=message.message_id, caption=new_caption
                        )
                        caption = new_caption
                        caption_updated = True
                    except BadRequest as e:
                        if "Message can't be edited" in str(e):
                            print(f"Edit failed for new movie {message.message_id}, trying copy...")
                            try:
                                new_msg = await context.bot.copy_message(
                                    chat_id=CHANNEL_ID, from_chat_id=CHANNEL_ID, message_id=message.message_id, caption=new_caption
                                )
                                await context.bot.delete_message(
                                    chat_id=CHANNEL_ID, message_id=message.message_id
                                )
                                message_id_to_save = new_msg.message_id
                                caption = new_caption
                                caption_updated = True
                            except Exception as copy_e:
                                print(f"CRITICAL: Copy-repost failed for {message.message_id}: {copy_e}")
                                # Failed to copy, revert to original
                                caption = message.caption or "" # Original caption
                        else:
                            print(f"Error adding watermark to new movie {message.message_id}: {e}")
                    
                    if caption_updated:
                        db.update_stats("watermarks_added")

            file_hash = calculate_file_hash(file_id, caption)
            
            # Check for duplicates *before* adding
            if file_hash not in db.get_all_hashes():
                db.add_movie(
                    message_id=message_id_to_save,
                    file_id=file_id,
                    caption=caption,
                    file_hash=file_hash
                )
                print(f"New movie processed and indexed: {message_id_to_save}")
                db.save_database()
            else:
                print(f"Duplicate movie ignored: {message_id_to_save}")

# Initialize Bot
ptb = (
    Application.builder()
    .updater(None)
    .token(BOT_TOKEN)
    .read_timeout(50)
    .write_timeout(50)
    .build()
)

# Conversation handler for watermark
watermark_conv = ConversationHandler(
    entry_points=[CommandHandler("setwatermark", set_watermark_start)],
    states={
        WAITING_FOR_WATERMARK: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_watermark_receive)]
    },
    fallbacks=[CommandHandler("cancel", cancel_conv)]
)

# Conversation handler for DB Upload
upload_db_conv = ConversationHandler(
    entry_points=[CommandHandler("upload_db", upload_db_start)],
    states={
        # === FIX: .JSON ko .MimeType("application/json") se badla ===
        WAITING_FOR_UPLOAD_FILE: [MessageHandler(filters.Document.MimeType("application/json"), upload_db_receive)]
    },
    fallbacks=[CommandHandler("cancel", cancel_conv)]
)

# Add handlers
ptb.add_handler(CommandHandler("start", start))
ptb.add_handler(CommandHandler("help", help_command))
ptb.add_handler(watermark_conv)
ptb.add_handler(upload_db_conv) # NEW
ptb.add_handler(CommandHandler("viewwatermark", view_watermark))
ptb.add_handler(CommandHandler("removewatermark", remove_watermark))
ptb.add_handler(CommandHandler("addwatermark", add_watermark_to_movies))
ptb.add_handler(CommandHandler("clean", clean_captions))
ptb.add_handler(CommandHandler("forward", forward_movies))
ptb.add_handler(CommandHandler("duplicates", handle_duplicates))
ptb.add_handler(CommandHandler("stats", show_stats))
ptb.add_handler(CommandHandler("refresh", refresh_database))
ptb.add_handler(CommandHandler("backup", backup_database)) # Already exists
ptb.add_handler(CommandHandler("checkperms", check_permissions))
ptb.add_handler(CommandHandler("stop", stop_task_command))
ptb.add_handler(MessageHandler(filters.ChatType.CHANNEL, handle_new_movies))
ptb.add_handler(CallbackQueryHandler(button_callback)) # Ye sabhi callbacks handle karega

@asynccontextmanager
async def lifespan(_: FastAPI):
    if not BOT_TOKEN:
        print("Error: BOT_TOKEN not set.")
        yield
        return
    if not WEBHOOK_URL:
        print("Error: WEBHOOK_URL not set.")
        yield
        return
    
    try:
        await ptb.bot.setWebhook(f"{WEBHOOK_URL}/webhook")
    except Exception as e:
        print(f"Error setting webhook: {e}")
        
    async with ptb:
        await ptb.start()
        yield
        await ptb.stop()

# FastAPI app
app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def process_update(request: Request):
    req = await request.json()
    update = Update.de_json(req, ptb.bot)
    await ptb.process_update(update)
    return Response(status_code=HTTPStatus.OK)

@app.get("/")
async def health_check():
    return {"status": "Bot is running!", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    if not BOT_TOKEN:
        print("CRITICAL: BOT_TOKEN not set. Exiting.")
        exit(1)
    if not WEBHOOK_URL:
        print("CRITICAL: WEBHOOK_URL not set. Exiting.")
        exit(1)
    if not ADMIN_IDS:
         print("WARNING: ADMIN_IDS not set. Admin commands will not work.")

    port = int(os.environ.get('PORT', 10000))
    print(f"--- Starting Uvicorn server on 0.0.0.0:{port} ---")
    uvicorn.run(app, host="0.0.0.0", port=port)
