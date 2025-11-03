import asyncio
import re
import sqlite3
import os
from pyrogram import Client, filters, enums
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, Message
)
from pyrogram.errors import FloodWait

# --- ⚠️ अपनी डिटेल्स यहाँ भरें (Render के Environment Variables में) ⚠️ ---
# Render या किसी भी होस्टिंग के लिए इन्हें Environment Variables में सेट करें
API_ID = int(os.environ.get("API_ID", "123456"))
API_HASH = os.environ.get("API_HASH", "your_api_hash")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "your_bot_token")
SESSION_STRING = os.environ.get("SESSION_STRING", "") # यह सबसे ज़रूरी है

# --- चैनल IDs (आपके दिए हुए) ---
SOURCE_CHANNEL_ID = -1003138949015  # @MOVIEMAZA19
BACKUP_CHANNEL_ID = -1002010174094  # @MAZABACKUP01 (ID को @RawDataBot से चेक कर लें)
# ज़रूरी: मैंने @MAZABACKUP01 की ID (-1002010174094) निकाली है, 
# अगर यह गलत है, तो कृपया @RawDataBot से खुद चेक करके बदल लें।

# --- सेटिंग्स ---
CAPTION_FOOTER = "\n\n@THEGREATMOVIESL9\n@MOVIEMAZASU"
USERNAME_WHITELIST = ["@THEGREATMOVIESL9", "@MOVIEMAZASU"]
BATCH_SIZE = 100
FORWARD_GAP = 7  # 7 सेकंड का गैप

# --- डेटाबेस सेटअप ---
db = sqlite3.connect("movie_bot.db")
db.execute("""
CREATE TABLE IF NOT EXISTS forwarded_movies (
    source_message_id INTEGER PRIMARY KEY,
    file_unique_id TEXT
)""")
db.commit()
db.close()

# --- Pyrogram Client ---
app = Client(
    "movie_manager",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    session_string=SESSION_STRING,
    in_memory=True # Render के लिए ज़रूरी
)

# --- ग्लोबल स्टेट (यह ट्रैक करने के लिए कि कोई काम चल रहा है या नहीं) ---
task_status = {
    "forwarding": False,
    "deleting_dupes": False,
    "appending_footer": False,
    "cleaning_old": False  # <-- आपका नया फीचर
}

# --- 1. ऑटोमैटिक कैप्शन क्लीनर (आपका फीचर 1 और 2) ---
def clean_caption(caption_text):
    """कैप्शन से लिंक और यूजरनेम हटाता है, व्हाइटलिस्ट को छोड़कर"""
    if not caption_text:
        return caption_text

    cleaned_text = caption_text
    
    # सभी लिंक्स ढूँढें और हटाएँ
    links = re.findall(r'(https?://\S+|www\.\S+|t\.me/\S+)', cleaned_text)
    for link in links:
        cleaned_text = cleaned_text.replace(link, "")

    # सभी यूजरनेम ढूँढें
    usernames = re.findall(r'@\S+', cleaned_text)
    for username in usernames:
        if username not in USERNAME_WHITELIST:
            cleaned_text = cleaned_text.replace(username, "")
            
    # फालतू खाली लाइनें हटाएँ
    cleaned_text = "\n".join([line.strip() for line in cleaned_text.split("\n") if line.strip()])
    return cleaned_text

@app.on_message(filters.chat(SOURCE_CHANNEL_ID) & filters.caption & filters.channel)
async def auto_clean_new_post(client: Client, message: Message):
    """नया मैसेज आते ही उसका कैप्शन ऑटो-क्लीन करता है"""
    original_caption = message.caption
    cleaned = clean_caption(original_caption)
    
    if original_caption != cleaned:
        try:
            await message.edit_caption(cleaned)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await message.edit_caption(cleaned)
        except Exception as e:
            print(f"Auto-clean Error: {e}")

# --- 2. मेन मेन्यू और बटन्स ---
def get_main_menu():
    buttons = [
        [InlineKeyboardButton("📤 मूवी बैकअप फॉरवर्ड करें", callback_data="start_forward")],
        [InlineKeyboardButton("🗑️ डुप्लीकेट मूवी डिलीट करें", callback_data="start_delete_dupes")],
        [InlineKeyboardButton("✍️ सबमें Footer ऐड करें", callback_data="start_append_footer")],
        [InlineKeyboardButton("🧹 पुराने कैप्शन क्लीन करें", callback_data="start_clean_old")], # <-- आपका नया बटन
        [InlineKeyboardButton("📊 चैनल स्टैट्स देखें (मेरा फीचर)", callback_data="show_stats")],
        [InlineKeyboardButton("⛔ काम रोकें (Stop Task)", callback_data="stop_task")],
    ]
    return InlineKeyboardMarkup(buttons)

@app.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    await message.reply(
        "👋 नमस्ते! मैं आपका मूवी चैनल मैनेजर हूँ।\n"
        "मैं @MOVIEMAZA19 चैनल को मैनेज कर सकता हूँ।\n\n"
        "**मेरे फीचर्स:**\n"
        "1.  **ऑटो-क्लीन:** नए पोस्ट के कैप्शन से फालतू लिंक/यूजरनेम खुद हटा दूँगा।\n"
        "2.  **बटन्स:** नीचे दिए गए बटन से आप बड़े काम करवा सकते हैं।\n\n"
        "**चेतावनी:** कोई भी बड़ा काम (जैसे फॉरवर्ड, डिलीट) शुरू करने से पहले, यह पक्का कर लें कि पिछला काम पूरा हो गया हो।",
        reply_markup=get_main_menu()
    )

# --- 3. बटन के काम (Callback Query Handler) ---
@app.on_callback_query()
async def handle_callbacks(client: Client, query: CallbackQuery):
    global task_status
    data = query.data

    if data == "stop_task":
        task_status["forwarding"] = False
        task_status["deleting_dupes"] = False
        task_status["appending_footer"] = False
        task_status["cleaning_old"] = False # <-- नया
        await query.answer("⛔ सभी कामों को रोकने का सिग्नल भेज दिया गया है। अगला बैच शुरू नहीं होगा।", show_alert=True)
        return

    # चेक करें कि कोई और काम तो नहीं चल रहा
    if any(task_status.values()):
        await query.answer("⚠️ पहले से एक काम चल रहा है! कृपया उसके खत्म होने का इंतज़ार करें या 'काम रोकें' बटन दबाएँ।", show_alert=True)
        return

    if data == "start_forward":
        await query.answer("📤 मूवी फॉरवर्डिंग शुरू की जा रही है...")
        task_status["forwarding"] = True
        await query.message.reply("✅ **बैकअप शुरू!**")
        asyncio.create_task(run_forward_job(query.message))
    
    elif data == "start_delete_dupes":
        await query.answer("🗑️ डुप्लीकेट ढूंढे जा रहे हैं...")
        task_status["deleting_dupes"] = True
        await query.message.reply("✅ **डुप्लीकेट डिलीशन शुरू!**")
        asyncio.create_task(run_delete_dupes_job(query.message))

    elif data == "start_append_footer":
        await query.answer("✍️ कैप्शन अपडेट किए जा रहे हैं...")
        task_status["appending_footer"] = True
        await query.message.reply("✅ **Footer ऐड करना शुरू!**")
        asyncio.create_task(run_append_footer_job(query.message))

    elif data == "start_clean_old": # <-- आपका नया बटन हैंडलर
        await query.answer("🧹 पुराने कैप्शन क्लीन किए जा रहे हैं...")
        task_status["cleaning_old"] = True
        await query.message.reply("✅ **पुराने कैप्शन की सफाई शुरू!**\nमैं @MOVIEMAZA19 के सभी पुराने पोस्ट के कैप्शन से लिंक/यूजरनेम हटा रहा हूँ।")
        asyncio.create_task(run_clean_old_posts_job(query.message))
        
    elif data == "show_stats":
        await query.answer("📊 स्टैट्स लोड हो रहे हैं...")
        asyncio.create_task(run_stats_job(query.message))


# --- 4. बड़े काम (Jobs) ---

# आपका फीचर 3: मूवी फॉरवर्ड करना
async def run_forward_job(message: Message):
    global task_status
    try:
        db = sqlite3.connect("movie_bot.db")
        cursor = db.cursor()
        cursor.execute("SELECT source_message_id FROM forwarded_movies")
        forwarded_ids = set(row[0] for row in cursor.fetchall())
        total_forwarded = 0
        total_skipped = 0
        
        async for post in app.get_chat_history(SOURCE_CHANNEL_ID):
            if not task_status["forwarding"]:
                await message.reply("⛔ फॉरवर्डिंग को यूज़र ने रोक दिया।")
                break
            if post.media and (post.video or post.document):
                if post.message_id in forwarded_ids:
                    total_skipped += 1
                    continue
                try:
                    await post.forward(BACKUP_CHANNEL_ID)
                    file_uid = post.video.file_unique_id if post.video else post.document.file_unique_id
                    cursor.execute("INSERT OR IGNORE INTO forwarded_movies (source_message_id, file_unique_id) VALUES (?, ?)",
                                   (post.message_id, file_uid))
                    db.commit()
                    forwarded_ids.add(post.message_id)
                    total_forwarded += 1
                    if total_forwarded % BATCH_SIZE == 0:
                        await message.reply(f"✅ {total_forwarded} मूवी फॉरवर्ड हो गई हैं...")
                    await asyncio.sleep(FORWARD_GAP)
                except FloodWait as e:
                    await message.reply(f"⏳ Flood Wait... {e.value} सेकंड के लिए रुक रहा हूँ।")
                    await asyncio.sleep(e.value)
                except Exception as e:
                    print(f"फॉरवर्ड एरर: {e} (ID: {post.message_id})")
        await message.reply(f"✅ **फॉरवर्डिंग पूरी हुई!**\n- नई मूवी: {total_forwarded}\n- स्किप: {total_skipped}")
    except Exception as e:
        await message.reply(f"❌ फॉरवर्डिंग में समस्या: {e}")
    finally:
        task_status["forwarding"] = False
        db.close()


# आपका फीचर 4: डुप्लीकेट डिलीट करना
async def run_delete_dupes_job(message: Message):
    global task_status
    try:
        seen_files = {} 
        deleted_count = 0
        batch_count = 0
        async for post in app.get_chat_history(SOURCE_CHANNEL_ID):
            if not task_status["deleting_dupes"]:
                await message.reply("⛔ डुप्लीकेट डिलीशन को रोक दिया।")
                break
            file_id = None
            if post.video: file_id = post.video.file_unique_id
            elif post.document: file_id = post.document.file_unique_id
            if file_id:
                if file_id in seen_files:
                    try:
                        await post.delete()
                        deleted_count += 1
                        batch_count += 1
                        if batch_count >= BATCH_SIZE:
                            await message.reply(f"🗑️ 100 डुप्लीकेट डिलीट हो गए... (कुल {deleted_count})")
                            await asyncio.sleep(10) 
                            batch_count = 0
                    except FloodWait as e:
                        await message.reply(f"⏳ Flood Wait... {e.value} सेकंड के लिए रुक रहा हूँ।")
                        await asyncio.sleep(e.value)
                    except Exception as e:
                        print(f"डिलीट एरर: {e} (ID: {post.message_id})")
                else:
                    seen_files[file_id] = post.message_id
        await message.reply(f"✅ **डुप्लीकेट डिलीशन पूरा हुआ!**\n- कुल {deleted_count} डुप्लीकेट पोस्ट डिलीट किए गए।")
    except Exception as e:
        await message.reply(f"❌ डुप्लीकेट डिलीशन में समस्या: {e}")
    finally:
        task_status["deleting_dupes"] = False


# आपका फीचर 5: कैप्शन में Footer ऐड करना
async def run_append_footer_job(message: Message):
    global task_status
    try:
        updated_count = 0
        batch_count = 0
        async for post in app.get_chat_history(SOURCE_CHANNEL_ID):
            if not task_status["appending_footer"]:
                await message.reply("⛔ Footer ऐड करने को रोक दिया।")
                break
            if post.caption:
                if not post.caption.endswith(CAPTION_FOOTER):
                    try:
                        new_caption = post.caption + CAPTION_FOOTER
                        await post.edit_caption(new_caption)
                        updated_count += 1
                        batch_count += 1
                        if batch_count >= BATCH_SIZE:
                            await message.reply(f"✍️ 100 कैप्शन अपडेट हो गए... (कुल {updated_count})")
                            await asyncio.sleep(10)
                    except FloodWait as e:
                        await message.reply(f"⏳ Flood Wait... {e.value} सेकंड के लिए रुक रहा हूँ।")
                        await asyncio.sleep(e.value)
                    except Exception as e:
                        print(f"कैप्शन एडिट एरर: {e} (ID: {post.message_id})")
        await message.reply(f"✅ **Footer ऐड करना पूरा हुआ!**\n- कुल {updated_count} पोस्ट अपडेट किए गए।")
    except Exception as e:
        await message.reply(f"❌ कैप्शन अपडेट करने में समस्या: {e}")
    finally:
        task_status["appending_footer"] = False


# --- (यह आपका नया फंक्शन है) ---
async def run_clean_old_posts_job(message: Message):
    """पुराने सभी पोस्ट के कैप्शन को क्लीन करता है"""
    global task_status
    try:
        cleaned_count = 0
        batch_count = 0
        async for post in app.get_chat_history(SOURCE_CHANNEL_ID):
            if not task_status["cleaning_old"]:
                await message.reply("⛔ पुराने कैप्शन की सफाई को यूज़र ने रोक दिया।")
                break
            if post.caption:
                original_caption = post.caption
                cleaned = clean_caption(original_caption) 
                if original_caption != cleaned:
                    try:
                        await post.edit_caption(cleaned)
                        cleaned_count += 1
                        batch_count += 1
                        if batch_count >= BATCH_SIZE:
                            await message.reply(f"🧹 100 कैप्शन क्लीन हो गए... (कुल {cleaned_count})...")
                            await asyncio.sleep(10) 
                    except FloodWait as e:
                        await message.reply(f"⏳ Flood Wait... {e.value} सेकंड के लिए रुक रहा हूँ।")
                        await asyncio.sleep(e.value)
                    except Exception as e:
                        print(f"पुराना कैप्शन एडिट एरर: {e} (ID: {post.message_id})")
        await message.reply(f"✅ **पुराने कैप्शन की सफाई पूरी हुई!**\n- कुल {cleaned_count} पोस्ट क्लीन किए गए।")
    except Exception as e:
        await message.reply(f"❌ पुराने कैप्शन क्लीन करने में समस्या: {e}")
    finally:
        task_status["cleaning_old"] = False


# --- 5. मेरे 3 एक्स्ट्रा फीचर्स ---

# 1. चैनल स्टैट्स (बटन में जोड़ा गया)
async def run_stats_job(message: Message):
    try:
        await message.reply("📊 स्टैट्स गिने जा रहे हैं... इसमें 1 मिनट लग सकता है...")
        total_posts = await app.get_chat_history_count(SOURCE_CHANNEL_ID)
        db = sqlite3.connect("movie_bot.db")
        cursor = db.cursor()
        cursor.execute("SELECT COUNT(DISTINCT file_unique_id) FROM forwarded_movies")
        unique_movies = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(source_message_id) FROM forwarded_movies")
        total_forwarded = cursor.fetchone()[0]
        db.close()
        await message.reply(
            "📊 **@MOVIEMAZA19 चैनल स्टैट्स**\n\n"
            f"- चैनल में कुल पोस्ट: `{total_posts}`\n"
            f"- बैकअप में फॉरवर्ड हुई मूवी: `{total_forwarded}`\n"
            f"- (लगभग) यूनिक मूवी फाइलें: `{unique_movies}`"
        )
    except Exception as e:
        await message.reply(f"❌ स्टैट्स लाने में एरर: {e}")

# 2. ऑटो-डिलीट ब्लैकलिस्ट (मेरा फीचर)
BLACKLIST_WORDS = ["18+", "adult", "hot"] # यहाँ और शब्द जोड़ें

@app.on_message(filters.chat(SOURCE_CHANNEL_ID) & filters.caption & filters.channel, group=2)
async def auto_delete_blacklist(client: Client, message: Message):
    if message.caption:
        if any(word in message.caption.lower() for word in BLACKLIST_WORDS):
            try:
                await message.delete()
                print(f"ब्लैकलिस्टेड पोस्ट {message.message_id} डिलीट किया।")
            except Exception as e:
                print(f"ब्लैकलिस्ट डिलीट एरर: {e}")

# 3. टेस्ट मोड / फोर्स क्लीन (मेरा फीचर)
@app.on_message(filters.command("clean") & filters.private)
async def force_clean_caption(client: Client, message: Message):
    if message.reply_to_message and message.reply_to_message.text:
        cleaned = clean_caption(message.reply_to_message.text)
        await message.reply(f"**क्लीन किया गया कैप्शन:**\n\n{cleaned}")
    else:
        await message.reply("क्लीन करने के लिए कृपया किसी मैसेज को रिप्लाई करें।")


# --- बॉट को शुरू करना ---
async def main():
    print("बॉट शुरू हो रहा है...")
    await app.start()
    print("बॉट सफलतापूर्वक शुरू हो गया है! (Started successfully!)")
    await asyncio.Event().wait() # बॉट को हमेशा चलता रखने के लिए

if __name__ == "__main__":
    if not SESSION_STRING:
        print("!! ज़रूरी: SESSION_STRING नहीं मिली !!")
        print("कृपया Render में Environment Variable में 'SESSION_STRING' को सेट करें।")
    else:
        asyncio.run(main())
