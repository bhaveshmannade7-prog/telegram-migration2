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

# Configuration
BOT_TOKEN = os.environ.get('BOT_TOKEN')
WEBHOOK_URL = os.environ.get('WEBHOOK_URL')
ADMIN_IDS = [int(id.strip()) for id in os.environ.get('ADMIN_IDS', '').split(',')]
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
            self.save_database()
            return True
        return False
    
    def set_watermark(self, watermark_text):
        """Set custom watermark for captions"""
        self.data["watermark"]["text"] = watermark_text
        self.data["watermark"]["enabled"] = True
        
        # Extract usernames and links from watermark
        usernames = re.findall(r'@w+', watermark_text)
        links = re.findall(r'(?:http[s]?://)?(?:www.)?(?:t.me/|telegram.me/)?[w-.]+(?:/[w-]+)*', watermark_text)
        
        self.data["watermark"]["usernames"] = list(set(usernames))
        self.data["watermark"]["links"] = list(set(links))
        self.save_database()
    
    def get_watermark(self):
        """Get current watermark settings"""
        return self.data["watermark"]
    
    def disable_watermark(self):
        """Disable watermark"""
        self.data["watermark"]["enabled"] = False
        self.save_database()
    
    def get_stats(self):
        return self.data["stats"]
    
    def update_stats(self, stat_type, increment=1):
        if stat_type in self.data["stats"]:
            self.data["stats"][stat_type] += increment
        self.data["stats"]["last_updated"] = datetime.now().isoformat()
        self.save_database()

# Initialize Database
db = MovieDatabase(JSON_DB_FILE)

# Utility Functions
def calculate_file_hash(file_id, caption):
    """Calculate unique hash for duplicate detection"""
    content = f"{file_id}_{caption}"
    return hashlib.md5(content.encode()).hexdigest()

def clean_caption(caption, preserve_watermark=True):
    """Remove usernames and links from caption while preserving watermark"""
    if not caption:
        return None
    
    watermark_config = db.get_watermark()
    protected_items = []
    
    if preserve_watermark and watermark_config["enabled"]:
        # Store protected usernames and links temporarily
        protected_items = watermark_config["usernames"] + watermark_config["links"]
    
    # Create temporary placeholders for protected items
    placeholder_map = {}
    for idx, item in enumerate(protected_items):
        placeholder = f"__PROTECTED_{idx}__"
        caption = caption.replace(item, placeholder)
        placeholder_map[placeholder] = item
    
    # Remove URLs
    caption = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', caption)
    caption = re.sub(r'www.(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),])+', '', caption)
    
    # Remove @usernames
    caption = re.sub(r'@w+', '', caption)
    
    # Remove telegram links
    caption = re.sub(r't.me/S+', '', caption)
    
    # Restore protected items
    for placeholder, original in placeholder_map.items():
        caption = caption.replace(placeholder, original)
    
    # Clean extra spaces
    caption = re.sub(r's+', ' ', caption).strip()
    
    return caption if caption else None

def add_watermark_to_caption(caption):
    """Add watermark to caption"""
    watermark_config = db.get_watermark()
    
    if not watermark_config["enabled"] or not watermark_config["text"]:
        return caption
    
    caption = caption or ""
    watermark = watermark_config["text"]
    
    # Check if watermark already exists
    if watermark in caption:
        return caption
    
    # Add watermark at the end
    if caption:
        return f"{caption}

{watermark}"
    else:
        return watermark

async def is_admin(user_id):
    """Check if user is admin"""
    return user_id in ADMIN_IDS

# Command Handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    user = update.effective_user
    welcome_text = f"""
🎬 **Welcome {user.first_name}!**

Ye bot aapke Telegram channel **{CHANNEL_USERNAME}** ko manage karta hai.

**Main Commands:**
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

**Auto Features:**
✅ New movies automatically index hoti hain
✅ Watermark automatically naye movies me add hota hai
✅ Aapka watermark kabhi delete nahi hoga

**Protected:** Sirf authorized admins hi commands use kar sakte hain.
    """
    await update.message.reply_text(welcome_text, parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command handler"""
    help_text = """
📚 **Bot Commands Guide:**

**1. Watermark Management** 🏷️
   `/setwatermark` - Apna channel username/link set karo
   Example: Watermark text bhejo jaise "@YourChannel"
   
   `/addwatermark` - Existing sabhi movies me watermark add karo (100 batch)
   
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

**Auto Features:**
• New movies automatically watermark ke saath save hoti hain
• Cleaning karte waqt aapka watermark protected rahta hai
• Duplicate detection automatic hai

**Note:** Sabhi commands admin-only hain.
    """
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def set_watermark_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start watermark setting process"""
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return ConversationHandler.END
    
    await update.message.reply_text(
        "🏷️ **Watermark Setup**

"
        "Apna watermark text bhejo (channel username, link, ya koi bhi text):

"
        "**Examples:**
"
        "• @YourChannel
"
        "• Join: @YourChannel
"
        "• t.me/YourChannel
"
        "• Follow us: @YourChannel | t.me/YourBackup

"
        "Cancel karne ke liye /cancel type karo.",
        parse_mode='Markdown'
    )
    return WAITING_FOR_WATERMARK

async def set_watermark_receive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Receive and save watermark"""
    watermark_text = update.message.text.strip()
    
    if len(watermark_text) > 500:
        await update.message.reply_text("❌ Watermark bahut lamba hai! Maximum 500 characters allowed.")
        return WAITING_FOR_WATERMARK
    
    db.set_watermark(watermark_text)
    
    watermark_config = db.get_watermark()
    
    await update.message.reply_text(
        f"✅ **Watermark Successfully Set!**

"
        f"**Watermark Text:**
{watermark_text}

"
        f"**Protected Usernames:** {', '.join(watermark_config['usernames']) or 'None'}
"
        f"**Protected Links:** {', '.join(watermark_config['links']) or 'None'}

"
        f"Ab ye watermark:
"
        f"• Cleaning ke dauran safe rahega
"
        f"• Naye movies me automatically add hoga
"
        f"• `/addwatermark` se existing movies me add kar sakte ho",
        parse_mode='Markdown'
    )
    return ConversationHandler.END

async def cancel_watermark(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel watermark setting"""
    await update.message.reply_text("❌ Watermark setup cancelled.")
    return ConversationHandler.END

async def view_watermark(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View current watermark settings"""
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    watermark_config = db.get_watermark()
    
    if not watermark_config["enabled"]:
        await update.message.reply_text("❌ Koi watermark set nahi hai.

/setwatermark use karke set karo.")
        return
    
    status_text = f"""
🏷️ **Current Watermark Settings**

**Status:** {'✅ Enabled' if watermark_config['enabled'] else '❌ Disabled'}

**Watermark Text:**
{watermark_config['text']}

**Protected Usernames:** {', '.join(watermark_config['usernames']) or 'None'}

**Protected Links:** {', '.join(watermark_config['links']) or 'None'}

**Features:**
• Auto-add to new movies: ✅
• Protected during cleaning: ✅
    """
    
    await update.message.reply_text(status_text, parse_mode='Markdown')

async def remove_watermark(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove/disable watermark"""
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    db.disable_watermark()
    await update.message.reply_text("✅ Watermark disabled ho gaya hai!")

async def add_watermark_to_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add watermark to all existing movies in batches"""
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
    batch_size = 100
    
    try:
        for i in range(0, total, batch_size):
            batch = movies[i:i+batch_size]
            for movie in batch:
                try:
                    # Get current message
                    message = await context.bot.forward_message(
                        chat_id=update.effective_chat.id,
                        from_chat_id=CHANNEL_ID,
                        message_id=movie["message_id"]
                    )
                    
                    # Delete forwarded message
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id,
                        message_id=message.message_id
                    )
                    
                    if message.caption or message.video or message.document:
                        current_caption = message.caption or ""
                        
                        # Check if watermark already exists
                        if watermark_config["text"] not in current_caption:
                            new_caption = add_watermark_to_caption(current_caption)
                            
                            await context.bot.edit_message_caption(
                                chat_id=CHANNEL_ID,
                                message_id=movie["message_id"],
                                caption=new_caption
                            )
                            added += 1
                            db.update_stats("watermarks_added")
                    
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    print(f"Error adding watermark to {movie['message_id']}: {e}")
                    continue
            
            await status_msg.edit_text(
                f"🏷️ Progress: {min(i+batch_size, total)}/{total} processed...
"
                f"Added: {added}"
            )
        
        await status_msg.edit_text(
            f"✅ **Watermark Addition Complete!**

"
            f"**Total Movies:** {total}
"
            f"**Watermark Added:** {added}
"
            f"**Already Had:** {total - added}

"
            f"**Watermark:** {watermark_config['text']}"
        )
    
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

async def clean_captions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clean captions from all movies in batches while preserving watermark"""
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    watermark_config = db.get_watermark()
    status_msg = await update.message.reply_text(
        f"🔄 Caption cleaning shuru ho rahi hai...
"
        f"{'🏷️ Watermark protected rahega!' if watermark_config['enabled'] else ''}"
    )
    
    movies = db.get_all_movies()
    total = len(movies)
    cleaned = 0
    batch_size = 100
    
    try:
        for i in range(0, total, batch_size):
            batch = movies[i:i+batch_size]
            for movie in batch:
                try:
                    message = await context.bot.forward_message(
                        chat_id=update.effective_chat.id,
                        from_chat_id=CHANNEL_ID,
                        message_id=movie["message_id"]
                    )
                    
                    # Delete forwarded message
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id,
                        message_id=message.message_id
                    )
                    
                    if message.caption:
                        new_caption = clean_caption(message.caption, preserve_watermark=True)
                        if new_caption != message.caption:
                            await context.bot.edit_message_caption(
                                chat_id=CHANNEL_ID,
                                message_id=movie["message_id"],
                                caption=new_caption
                            )
                            cleaned += 1
                            db.update_stats("total_cleaned")
                    
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    print(f"Error cleaning message {movie['message_id']}: {e}")
                    continue
            
            await status_msg.edit_text(f"🔄 Progress: {min(i+batch_size, total)}/{total} processed...")
        
        await status_msg.edit_text(
            f"✅ **Cleaning Complete!**

"
            f"Total Movies: {total}
"
            f"Cleaned: {cleaned}
"
            f"Unchanged: {total - cleaned}
"
            f"{'🏷️ Watermark protected!' if watermark_config['enabled'] else ''}"
        )
    
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

async def forward_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Forward all movies to another channel in batches"""
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    if not context.args:
        await update.message.reply_text(
            "⚠️ Usage: `/forward <channel_id>`
"
            "Example: `/forward -1001234567890` ya `/forward @channelname`",
            parse_mode='Markdown'
        )
        return
    
    target_channel = context.args[0]
    status_msg = await update.message.reply_text("🔄 Forwarding shuru ho rahi hai...")
    
    movies = db.get_all_movies()
    total = len(movies)
    forwarded = 0
    batch_size = 100
    
    try:
        for i in range(0, total, batch_size):
            batch = movies[i:i+batch_size]
            for movie in batch:
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
            
            await status_msg.edit_text(f"🔄 Progress: {min(i+batch_size, total)}/{total} forwarded...")
        
        await status_msg.edit_text(
            f"✅ **Forwarding Complete!**

"
            f"Total Movies: {total}
"
            f"Forwarded: {forwarded}
"
            f"Failed: {total - forwarded}"
        )
    
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {str(e)}")

async def handle_duplicates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Detect and remove duplicate movies"""
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
        f"⚠️ **{len(duplicates)} Duplicate Movies Found!**

"
        f"Kya aap inhe delete karna chahte hain?",
        reply_markup=reply_markup
    )
    
    context.user_data['duplicates'] = duplicates

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks"""
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
        
        await status_msg.edit_text(
            f"✅ **{deleted} Duplicate Movies Deleted!**"
        )
    
    elif query.data == "cancel_duplicates":
        await query.edit_message_text("❌ Duplicate deletion cancelled.")

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot statistics"""
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
    """Refresh database"""
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    status_msg = await update.message.reply_text("🔄 Database refresh ho raha hai...")
    
    db.data = db.load_database()
    
    await status_msg.edit_text(
        "✅ Database refreshed!
"
        f"Total Movies: {len(db.get_all_movies())}"
    )

async def backup_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send database backup file"""
    if not await is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Ye command sirf admins ke liye hai!")
        return
    
    try:
        with open(JSON_DB_FILE, 'rb') as f:
            await update.message.reply_document(
                document=f,
                filename=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                caption="📦 Database Backup"
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Error creating backup: {str(e)}")

async def handle_new_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Auto-index new movies and add watermark"""
    if update.channel_post and update.channel_post.chat.id == CHANNEL_ID:
        message = update.channel_post
        
        # Check if message has media
        if message.video or message.document:
            file_id = message.video.file_id if message.video else message.document.file_id
            caption = message.caption or ""
            
            # Add watermark if enabled
            watermark_config = db.get_watermark()
            if watermark_config["enabled"]:
                new_caption = add_watermark_to_caption(caption)
                if new_caption != caption:
                    try:
                        await context.bot.edit_message_caption(
                            chat_id=CHANNEL_ID,
                            message_id=message.message_id,
                            caption=new_caption
                        )
                        caption = new_caption
                        db.update_stats("watermarks_added")
                    except Exception as e:
                        print(f"Error adding watermark to new movie: {e}")
            
            file_hash = calculate_file_hash(file_id, caption)
            
            # Add to database
            db.add_movie(
                message_id=message.message_id,
                file_id=file_id,
                caption=caption,
                file_hash=file_hash
            )
            
            print(f"New movie indexed: {message.message_id}")

# Initialize Bot
ptb = (
    Application.builder()
    .updater(None)
    .token(BOT_TOKEN)
    .read_timeout(30)
    .write_timeout(30)
    .build()
)

# Conversation handler for watermark setting
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
ptb.add_handler(MessageHandler(filters.ChatType.CHANNEL, handle_new_movies))
ptb.add_handler(CallbackQueryHandler(button_callback))

@asynccontextmanager
async def lifespan(_: FastAPI):
    await ptb.bot.setWebhook(f"{WEBHOOK_URL}/webhook")
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
