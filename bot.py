import asyncio
import re
import sqlite3
import os
import threading # <-- यह 'Render Fix' के लिए जोड़ा गया है
from flask import Flask # <-- यह 'Render Fix' के लिए जोड़ा गया है
from pyrogram import Client, filters, enums
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, Message
)
from pyrogram.errors import FloodWait

# --- ⚠️ अपनी डिटेल्स यहाँ भरें (Render के Environment Variables में) ⚠️ ---
API_ID = int(os.environ.get("API_ID", "123456"))
API_HASH = os.environ.get("API_HASH", "your_api_hash")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "your_bot_token")
SESSION_STRING = os.environ.get("SESSION_STRING", "") 

# --- चैनल IDs (आपके दिए हुए) ---
SOURCE_CHANNEL_ID = -1003138949015  # @MOVIEMAZA19
BACKUP_CHANNEL_ID = -1002010174094  # @MAZABACKUP01 

# --- सेटिंग्स ---
CAPTION_FOOTER = "\n\n@THEGREATMOVIESL9\n@MOVIEMAZASU"
USERNAME_WHITELIST = ["@THEGREATMOVIESL9", "@MOVIEMAZASU"]
BATCH_SIZE = 100
FORWARD_GAP = 7  

# --- डेटाबेस सेटअप ---
# Render Web Service हर रीस्टार्ट पर इसे डिलीट कर सकती है, 
# लेकिन यह हर बार शुरू होने पर अपने आप बन जाएगी।
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
    in_memory=True 
)

# --- ग्लोबल स्टेट ---
task_status = {
    "forwarding": False,
    "deleting_dupes": False,
    "appending_footer": False,
    "cleaning_old": False  
}

# --- 1. ऑटोमैटिक कैप्शन क्लीनर ---
def clean_caption(caption_text):
    if not caption_text:
        return caption_text
    cleaned_text = caption_text
    links = re.findall(r'(https?://\S+|www\.\S+|t\.me/\S+)', cleaned_text)
    for link in links:
        cleaned_text = cleaned_text.replace(link, "")
    usernames = re.findall(r'@\S+', cleaned_text)
    for username in usernames:
        if username not in USERNAME_WHITELIST:
            cleaned_text = cleaned_text.replace(username, "")
    cleaned_text = "\n".join([line.strip() for line in cleaned_text.split("\n") if line.strip()])
    return cleaned_text

@app.on_message(filters.chat(SOURCE_CHANNEL_ID) & filters.caption & filters.channel)
async def auto_clean_new_post(client: Client, message: Message):
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
        [InlineKeyboardButton("🧹 पुराने कैप्शन क्लीन करें", callback_data="start_clean_old")], 
        [InlineKeyboardButton("📊 चैनल स्टैट्स देखें (मेरा फीचर)", callback_data="show_stats")],
        [InlineKeyboardButton("⛔ काम रोकें (Stop Task)", callback_data="stop_task")],
    ]
    return InlineKeyboardMarkup(buttons)

@app.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    await message.reply(
        "👋 नमस्ते! मैं आपका मूवी चैनल मैनेजर हूँ।",
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
        task_status["cleaning_old"] = False 
        await query.answer("⛔ सभी कामों को रोकने का सिग्नल भेज दिया गया है।", show_alert=True)
        return

    if any(task_status.values()):
        await query.answer("⚠️ पहले से एक काम चल रहा है!", show_alert=True)
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

    elif data == "start_clean_old":
        await query.answer("🧹 पुराने कैप्शन क्लीन किए जा रहे हैं...")
        task_status["cleaning_old"] = True
        await query.message.reply("✅ **पुराने कैप्शन की सफाई शुरू!**")
        asyncio.create_task(run_clean_old_posts_job(query.message))
        
    elif data == "show_stats":
        await query.answer("📊 स्टैट्स लोड हो रहे हैं...")
        asyncio.create_task(run_stats_job(query.message))


# --- 4. बड़े काम (Jobs) ---
# ... (यहाँ आपके सभी 'run_..._job' वाले फंक्शन हैं, जैसे 'run_forward_job', 'run_delete_dupes_job', आदि) ...
# ... (उन्हें वैसे ही रहने दें, यहाँ जगह बचाने के लिए उन्हें दोबारा नहीं लिख रहा हूँ) ...

# (यहाँ आपके पिछले कोड के सभी run_..._job फंक्शन मान लिए गए हैं)

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


# आपका नया फीचर: पुराने कैप्शन क्लीन करना
async def run_clean_old_posts_job(message: Message):
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
async def run_stats_job(message: Message):
    try:
        await message.reply("📊 स्टैट्स गिने जा रहे हैं...")
        total_posts = await app.get_chat_history_count(SOURCE_CHANNEL_ID)
        db = sqlite3.connect("movie_bot.db")
        cursor = db.cursor()
        cursor.execute("SELECT COUNT(DISTINCT file_unique_id) FROM forwarded_movies")
        unique_movies = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(source_message_id) FROM forwarded_movies")
        total_forwarded = cursor.fetchone()[0]
        db.close()
        await message.reply(
            f"📊 **@MOVIEMAZA19 स्टैट्स**\n- कुल पोस्ट: `{total_posts}`\n- बैकअप में: `{total_forwarded}`"
        )
    except Exception as e:
        await message.reply(f"❌ स्टैट्स एरर: {e}")

BLACKLIST_WORDS = ["18+", "adult", "hot"] 
@app.on_message(filters.chat(SOURCE_CHANNEL_ID) & filters.caption & filters.channel, group=2)
async def auto_delete_blacklist(client: Client, message: Message):
    if message.caption:
        if any(word in message.caption.lower() for word in BLACKWORDS):
            await message.delete()

@app.on_message(filters.command("clean") & filters.private)
async def force_clean_caption(client: Client, message: Message):
    if message.reply_to_message and message.reply_to_message.text:
        cleaned = clean_caption(message.reply_to_message.text)
        await message.reply(f"**क्लीन कैप्शन:**\n\n{cleaned}")
    else:
        await message.reply("क्लीन करने के लिए किसी मैसेज को रिप्लाई करें।")


# --- (बॉट को शुरू करने वाला मुख्य फंक्शन) ---
async def main():
    print("बॉट शुरू हो रहा है...")
    await app.start()
    print("बॉट सफलतापूर्वक शुरू हो गया है!")
    await asyncio.Event().wait() 

# --- Render FIX: वेब सर्वर को जोड़ने वाला नया कोड ---

# 1. Flask ऐप बनाएँ
web_app = Flask(__name__)

@web_app.route('/')
def home():
    # यह Render के हेल्थ-चेक को बताएगा कि ऐप ज़िंदा है
    return "मैं ज़िंदा हूँ! (बॉट चल रहा है)"

# 2. इस फंक्शन को एक अलग थ्रेड (thread) में चलाएँगे
def run_web_server():
    # Render $PORT नाम का वेरिएबल खुद देता है
    port = int(os.environ.get('PORT', 8080))
    web_app.run(host='0.0.0.0', port=port)

# --- बॉट को शुरू करना (नया तरीका) ---
if __name__ == "__main__":
    if not SESSION_STRING:
        print("!! ज़रूरी: SESSION_STRING नहीं मिली !!")
        print("कृपया Render में Environment Variable में 'SESSION_STRING' को सेट करें।")
    else:
        # 1. वेब सर्वर को बैकग्राउंड में शुरू करें
        print("Render के लिए वेब सर्वर शुरू किया जा रहा है...")
        web_thread = threading.Thread(target=run_web_server)
        web_thread.daemon = True # यह सुनिश्चित करता है कि मुख्य ऐप बंद होने पर थ्रेड बंद हो जाए
        web_thread.start()
        
        # 2. बॉट को मुख्य थ्रेड में शुरू करें
        print("टेलीग्राम बॉट शुरू किया जा रहा है...")
        asyncio.run(main())

