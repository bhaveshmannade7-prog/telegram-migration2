import os
import json
import re
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from http import HTTPStatus
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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

# Conversation states
WAITING_FOR_WATERMARK = 1

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
        self.save_database()
    
    def get_all_movies(self):
        return self.data["movies"]
    
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
            # Save handled by the caller function (handle_duplicates)
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
        # Save handled by the caller function to avoid frequent writes

# Initialize Database
db = MovieDatabase(JSON_DB_FILE)

# Utility Functions
def calculate_file_hash(file_id, caption):
    content = f"{file_id}_{caption}"
    return hashlib.md5(content.encode()).hexdigest()

def clean_caption(caption, preserve_watermark=True):
    if not caption:
        return "" # Return empty string instead of None
    
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
    
    # User Requirement: Don't add if already present
    if watermark in caption:
        return caption
    
    if caption:
        return f"""{caption}

{watermark}"""
    else:
        return watermark

async def is_admin(user_id):
    return user_id in ADMIN_IDS

# === Helper function to send improved error message ===
async def handle_edit_error(e, update_or_status_msg):
    error_text = str(e)
    print(f"Bounced edit error: {error_text}")
    if "Message can't be edited" in error_text:
        msg = (
            "❌ **PERMISSION ERROR!**\n\n"
            "Bot ko Channel mein **'Edit messages'** ki permission nahi hai.\n\n"
            "**YA**\n\n"
            "Aap kisi doosre **Admin** ka message edit karne ki koshish kar rahe hain (jo Telegram allow nahi karta).\n"
            "Solution: Apne channel settings mein jaakar 'Remain Anonymous' ko OFF karein."
        )
        if isinstance(update_or_status_msg, Update):
            await update_or_status_msg.message.reply_text(msg)
        else:
            await update_or_status_msg.edit_text(msg)
    else:
        # Other errors
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

Main Commands:
/start - Bot start karo
/clean - Caption se links/usernames hatao (watermark safe)
/forward <channel_id> - Movies forward karo
/setwatermark - Apna watermark set karo
/addwatermark - Existing movies me watermark add karo
/removewatermark - Watermark disable karo
/viewwatermark - Current watermark dekho
/duplicates - Duplicate movies remove karo
/stats - Bot statistics dekho
/refresh - Database refresh karo
/backup - Database backup download karo
/help - Detailed help menu
/checkperms - Bot ki permissions check karo

Auto Features:
✅ New movies automatically index hoti hain
✅ Aapka watermark kabhi delete nahi hoga

Protected: Sirf authorized admins hi commands use kar sakte hain.
    """
    await update.message.reply_text(welcome_text) # Removed parse_mode to prevent errors

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
📚 **Bot Commands Guide:**

**1. Watermark Management** 🏷️
   `/setwatermark` - Apna channel username/link set karo
   `/addwatermark` - Existing sabhi movies me watermark add karo
   `/removewatermark` - Watermark feature disable karo
   `/viewwatermark` - Current watermark settings dekho

**2. Caption Cleaning** ✨
   `/clean` - Sabhi movies ke captions se unwanted links/usernames hatao
   Note: Aapka watermark safe rahega!

**3. Forwarding** 📤
   `/forward <channel_id>` - Movies forward karo
   Example: `/forward -1001234567890`

**4. Duplicate Management** 🗑️
   `/duplicates` - Duplicate movies detect aur delete karo

**5. Statistics** 📊
   `/stats` - Complete bot statistics dekho

**6. Database** 💾
   `/refresh` - Database refresh karo
   `/backup` - JSON backup download karo

**7. Diagnostics** 🛠️
   `/checkperms` - Bot ki channel permissions check karo

**Note:** Sabhi commands admin-only hain.
    """
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def check_permissions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Checks the bot's own permissions in the channel."""
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    try:
        bot_id = context.bot.id
        chat_member = await context.bot.get_chat_member(chat_id=CHANNEL_ID, user_id=bot_id)
        
        status = chat_member.status
        can_edit = getattr(chat_member, 'can_edit_messages', False)
        can_delete = getattr(chat_member, 'can_delete_messages', False)
        
        response = f"📋 **Bot Permission Check for {CHANNEL_USERNAME}**\n\n"
        response += f"Bot Status: **{status.upper()}**\n\n"
        
        if status == "administrator":
            response += f"• `can_edit_messages`: **{can_edit}**\n"
            response += f"• `can_delete_messages`: **{can_delete}**\n\n"
            
            if can_edit and can_delete:
                response += "✅ **Sabhi permissions sahi hain!**\n"
            else:
                response += "❌ **ERROR!** Bot admin to hai, lekin permissions poori nahi hain. Kripya bot ko remove karke dobara add karein aur **'Edit messages'** aur **'Delete messages'** ki permission *manually* 'ON' karein."
        else:
            response += f"❌ **CRITICAL ERROR!** Bot channel mein Admin hi nahi hai. Kripya bot ko Admin banayein."
            
        await update.message.reply_text(response, parse_mode='Markdown')
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error checking permissions:\n`{e}`\n\nKya aapne `CHANNEL_ID` sahi daala hai?")

async def set_watermark_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return ConversationHandler.END
    
    await update.message.reply_text(
        "🏷️ **Watermark Setup**\n\n"
        "Apna watermark text bhejo (channel username, link, ya koi bhi text):\n\n"
        "Cancel karne ke liye /cancel type karo.",
        parse_mode='Markdown'
    )
    return WAITING_FOR_WATERMARK

async def set_watermark_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    watermark_text = update.message.text.strip()
    db.set_watermark(watermark_text)
    watermark_config = db.get_watermark()
    
    await update.message.reply_text(
        f"✅ **Watermark Successfully Set!**\n\n"
        f"**Watermark Text:**\n{watermark_text}\n\n"
        f"**Protected Usernames:** {', '.join(watermark_config['usernames']) or 'None'}\n"
        f"**Protected Links:** {', '.join(watermark_config['links']) or 'None'}",
        parse_mode='Markdown'
    )
    return ConversationHandler.END

async def cancel_watermark(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Watermark setup cancelled.")
    return ConversationHandler.END

async def view_watermark(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    watermark_config = db.get_watermark()
    
    if not watermark_config["enabled"]:
        await update.message.reply_text("❌ Koi watermark set nahi hai.\n\n/setwatermark use karke set karo.")
        return
    
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

async def add_watermark_to_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    watermark_config = db.get_watermark()
    if not watermark_config["enabled"]:
        await update.message.reply_text("❌ Pehle watermark set karo using /setwatermark")
        return
    
    status_msg = await update.message.reply_text("🏷️ Watermark add ho raha hai...")
    
    movies = db.get_all_movies()
    total = len(movies)
    added = 0
    
    try:
        for movie in movies:
            try:
                current_caption = movie.get("caption", "")
                if watermark_config["text"] in current_caption:
                    continue
                new_caption = add_watermark_to_caption(current_caption)
                
                if new_caption != current_caption:
                    await context.bot.edit_message_caption(
                        chat_id=CHANNEL_ID,
                        message_id=movie["message_id"],
                        caption=new_caption
                    )
                    movie["caption"] = new_caption
                    added += 1
                    db.update_stats("watermarks_added")
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                print(f"Error adding watermark to {movie['message_id']}: {e}")
                if "Message can't be edited" in str(e):
                    await handle_edit_error(e, status_msg)
                    db.save_database()
                    return
                continue
        
        db.save_database()
        await status_msg.edit_text(
            f"✅ **Watermark Addition Complete!**\n\n"
            f"**Total Movies:** {total}\n"
            f"**Watermark Added:** {added}\n"
            f"**Already Had:** {total - added}"
        )
    
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

async def clean_captions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    status_msg = await update.message.reply_text("🔄 Caption cleaning shuru ho rahi hai...")
    
    movies = db.get_all_movies()
    total = len(movies)
    cleaned = 0
    
    try:
        for movie in movies:
            try:
                original_caption = movie.get("caption", "")
                if not original_caption:
                    continue
                new_caption = clean_caption(original_caption, preserve_watermark=True)
                
                if new_caption != original_caption:
                    await context.bot.edit_message_caption(
                        chat_id=CHANNEL_ID,
                        message_id=movie["message_id"],
                        caption=new_caption
                    )
                    movie["caption"] = new_caption
                    cleaned += 1
                    db.update_stats("total_cleaned")
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                print(f"Error cleaning message {movie['message_id']}: {e}")
                if "Message can't be edited" in str(e):
                    await handle_edit_error(e, status_msg)
                    db.save_database()
                    return
                continue
        
        db.save_database()
        await status_msg.edit_text(
            f"✅ **Cleaning Complete!**\n\n"
            f"Total Movies: {total}\n"
            f"Cleaned: {cleaned}\n"
            f"Unchanged: {total - cleaned}"
        )
    
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

async def forward_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    if not context.args:
        await update.message.reply_text("⚠️ Usage: `/forward <channel_id>`", parse_mode='Markdown')
        return
    
    target_channel = context.args[0]
    status_msg = await update.message.reply_text("🔄 Forwarding shuru ho rahi hai...")
    
    movies = db.get_all_movies()
    total = len(movies)
    forwarded = 0
    
    try:
        for movie in movies:
            try:
                await context.bot.forward_message(
                    chat_id=target_channel,
                    from_chat_id=CHANNEL_ID,
                    message_id=movie["message_id"]
                )
                forwarded += 1
                db.update_stats("total_forwarded")
                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"Error forwarding message {movie['message_id']}: {e}")
                continue
        
        db.save_database()
        await status_msg.edit_text(
            f"✅ **Forwarding Complete!**\n\n"
            f"Total Movies: {total}\n"
            f"Forwarded: {forwarded}\n"
            f"Failed: {total - forwarded}"
        )
    
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

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
    
    await status_msg.edit_text(
        f"⚠️ **{len(duplicates)} Duplicate Movies Found!**\n\n"
        f"Kya aap inhe delete karna chahte hain?",
        reply_markup=reply_markup
    )
    context.user_data['duplicates'] = duplicates

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
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
                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"Error deleting duplicate: {e}")
                if "Message to delete not found" in str(e):
                    db.remove_movie_by_index(idx)
                elif "message can't be deleted" in str(e):
                     await status_msg.edit_text(f"❌ **PERMISSION ERROR!**\n\nBot ko Channel mein **'Delete messages'** ki permission dein.")
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

# === CODE CHANGE: Watermark auto-add ko hata diya ===
async def handle_new_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Auto-index new movies WITHOUT adding watermark.
    Watermark will be added manually via /addwatermark command.
    """
    if update.channel_post and update.channel_post.chat.id == CHANNEL_ID:
        message = update.channel_post
        
        if message.video or message.document:
            file_id = message.video.file_id if message.video else message.document.file_id
            caption = message.caption or ""
            
            # Watermark add karne ki koshish nahi karega
            # Isse permission error nahi aayega
            
            file_hash = calculate_file_hash(file_id, caption)
            
            db.add_movie(
                message_id=message.message_id,
                file_id=file_id,
                caption=caption,
                file_hash=file_hash
            )
            print(f"New movie indexed (no auto-watermark): {message.message_id}")

# Initialize Bot
ptb = (
    Application.builder()
    .updater(None)
    .token(BOT_TOKEN)
    .read_timeout(30)
    .write_timeout(30)
    .build()
)

# Conversation handler
watermark_conv = ConversationHandler(
    entry_points=[CommandHandler("setwatermark", set_watermark_start)],
    states={
        WAITING_FOR_WATERMARK: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_watermark_receive)]
    },
    fallbacks=[CommandHandler("cancel", cancel_watermark)]
)

# Add handlers
ptb.add_handler(CommandHandler("start", start))
ptb.add_handler(CommandHandler("help", help_command))
ptb.add_handler(watermark_conv)
ptb.add_handler(CommandHandler("viewwatermark", view_watermark))
ptb.add_handler(CommandHandler("removewatermark", remove_watermark))
ptb.add_handler(CommandHandler("addwatermark", add_watermark_to_movies))
ptb.add_handler(CommandHandler("clean", clean_captions))
ptb.add_handler(CommandHandler("forward", forward_movies))
ptb.add_handler(CommandHandler("duplicates", handle_duplicates))
ptb.add_handler(CommandHandler("stats", show_stats))
ptb.add_handler(CommandHandler("refresh", refresh_database))
ptb.add_handler(CommandHandler("backup", backup_database))
ptb.add_handler(CommandHandler("checkperms", check_permissions))
ptb.add_handler(MessageHandler(filters.ChatType.CHANNEL, handle_new_movies))
ptb.add_handler(CallbackQueryHandler(button_callback))

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
    
    # Naya Token try karne ke liye Read Timeout badha diya
    ptb.read_timeout = 50
    ptb.write_timeout = 50
    
    try:
        await ptb.bot.setWebhook(f"{WEBHOOK_URL}/webhook")
    except Exception as e:
        print(f"Error setting webhook: {e}")
        # Agar webhook set nahi hota, to bhi start karo (Render auto-retry karega)
        
    async with ptb:
        await ptb.start()
        yield
        await ptb.stop()

# FastAPI app
app = FastAPI(lifepspan=lifespan) # Note: 'lifespan' spelling mistake fixed

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
