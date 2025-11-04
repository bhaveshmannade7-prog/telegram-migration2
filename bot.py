import asyncio
import re
import os
import asyncpg
from aiohttp import web
from pyrogram import Client, filters, enums
from pyrogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, Message
)
from pyrogram.errors import FloodWait

# --- ⚠️ अपनी डिटेल्स यहाँ भरें (Render के Environment Variables में) ⚠️ ---
API_ID = int(os.environ.get("API_ID", "123456"))
API_HASH = os.environ.get("API_HASH", "your_api_hash")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "your_bot_token")
DATABASE_URL = os.environ.get("DATABASE_URL")
ADMIN_ID = 7263519581  # <-- [ADMIN FIX] आपकी एडमिन आईडी

# --- चैनल IDs ---
SOURCE_CHANNEL_ID = -1003138949015  # @MOVIEMAZA19
BACKUP_CHANNEL_ID = -1002010174094  # @MAZABACKUP01

# --- सेटिंग्स ---
CAPTION_FOOTER = "\n\n@THEGREATMOVIESL9\n@MOVIEMAZASU"
USERNAME_WHITELIST = ["@THEGREATMOVIESL9", "@MOVIEMAZASU"]
BATCH_SIZE = 100
FORWARD_GAP = 7
BLACKLIST_WORDS = ["18+", "adult", "hot"]

# --- डेटाबेस सेटअप (PostgreSQL) ---
db_pool = None

async def init_db():
    global db_pool
    if not DATABASE_URL:
        print("!! ज़रूरी: DATABASE_URL नहीं मिला! डेटाबेस काम नहीं करेगा। !!")
        return
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        async with db_pool.acquire() as conn:
            await conn.execute("""
            CREATE TABLE IF NOT EXISTS forwarded_movies (
                source_message_id BIGINT PRIMARY KEY,
                file_unique_id TEXT
            )""")
        print("✅ PostgreSQL डेटाबेस सफलतापूर्वक कनेक्ट और सेटअप हो गया है।")
    except Exception as e:
        print(f"❌ डेटाबेस कनेक्शन एरर: {e}")

# --- Pyrogram Client ---
app = Client(
    "movie_manager_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
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
        [InlineKeyboardButton("📊 चैनल स्टैट्स देखें", callback_data="show_stats")],
        [InlineKeyboardButton("⛔ काम रोकें (Stop Task)", callback_data="stop_task")],
    ]
    return InlineKeyboardMarkup(buttons)

# --- [ADMIN FIX] सिर्फ एडमिन ही /start कमांड इस्तेमाल कर सकता है ---
@app.on_message(filters.command("start") & filters.private & filters.user(ADMIN_ID))
async def start_command_admin(client: Client, message: Message):
    await message.reply(
        "👋 नमस्ते एडमिन! मैं आपका मूवी चैनल मैनेजर हूँ।\n\n"
        "** ज़रूरी:** काम करने के लिए मुझे दोनों चैनलों में एडमिन बनाएँ।",
        reply_markup=get_main_menu()
    )

# --- [ADMIN FIX] बाकी यूज़र्स के लिए मैसेज ---
@app.on_message(filters.command("start") & filters.private & ~filters.user(ADMIN_ID))
async def start_command_non_admin(client: Client, message: Message):
    await message.reply(
        "⛔ माफ कीजिए, यह बॉट सिर्फ एडमिन द्वारा इस्तेमाल किया जा सकता है।"
    )

# --- 3. बटन के काम (Callback Query Handler) ---
# --- [ADMIN FIX] सिर्फ एडमिन ही बटन दबा सकता है ---
@app.on_callback_query(filters.user(ADMIN_ID))
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
        await query.answer("⚠️ पहले से एक काम चल रहा है! कृपया इंतज़ार करें।", show_alert=True)
        return
        
    if not db_pool and data != "show_stats":
        await query.answer("❌ डेटाबेस कनेक्ट नहीं है! कृपया एडमिन से जाँच करने को कहें।", show_alert=True)
        return

    try:
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
    
    except Exception as e:
        await query.message.reply(f"❌ टास्क शुरू करने में एरर: {e}")

# --- [ADMIN FIX] अगर कोई नॉन-एडमिन बटन दबाता है ---
@app.on_callback_query(~filters.user(ADMIN_ID))
async def handle_callback_non_admin(client: Client, query: CallbackQuery):
    await query.answer("⛔ आप इस बॉट को इस्तेमाल करने के लिए अधिकृत नहीं हैं।", show_alert=True)


# --- 4. बड़े काम (Jobs) ---

# (यहाँ आपके सभी 'run_..._job' वाले फंक्शन हैं, जैसे 'run_forward_job', 'run_delete_dupes_job', आदि)
# (इन फंक्शन्स में कोई बदलाव नहीं किया गया है, इसलिए उन्हें दोबारा यहाँ नहीं लिख रहा हूँ)

# मूवी फॉरवर्ड करना (PostgreSQL के साथ अपडेटेड)
async def run_forward_job(message: Message):
    global task_status
    if not db_pool:
        await message.reply("❌ डेटाबेस एरर: कनेक्शन उपलब्ध नहीं है।")
        task_status["forwarding"] = False
        return

    total_forwarded = 0
    total_skipped = 0
    
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT source_message_id FROM forwarded_movies")
            forwarded_ids = set(row['source_message_id'] for row in rows)
            
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
                        
                        await conn.execute(
                            "INSERT INTO forwarded_movies (source_message_id, file_unique_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                            post.message_id, file_uid
                        )
                        
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
                        
        await message.reply(f"✅ **फॉरवर्डिंग पूरी हुई!**\n- नई मूवी: {total_forwarded}\n- स्किप: {total_skipped} (पहले से बैकअप में थीं)")
        
    except Exception as e:
        await message.reply(f"❌ फॉरवर्डिंग में बड़ी समस्या: {e}")
    finally:
        task_status["forwarding"] = False


# डुप्लीकेट डिलीट करना
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


# कैप्शन में Footer ऐड करना
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
                cleaned_caption = post.caption.strip()
                if not cleaned_caption.endswith(CAPTION_FOOTER.strip()):
                    try:
                        new_caption = cleaned_caption + CAPTION_FOOTER
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


# पुराने कैप्शन क्लीन करना
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


# --- 5. एक्स्ट्रा फीचर्स ---

# स्टैट्स (PostgreSQL के साथ अपडेटेड)
async def run_stats_job(message: Message):
    try:
        await message.reply("📊 स्टैट्स गिने जा रहे हैं...")
        total_posts = await app.get_chat_history_count(SOURCE_CHANNEL_ID)
        
        total_forwarded = 0
        if db_pool:
            async with db_pool.acquire() as conn:
                total_forwarded = await conn.fetchval("SELECT COUNT(source_message_id) FROM forwarded_movies")
        else:
            total_forwarded = "(DB कनेक्ट नहीं है)"

        await message.reply(
            f"📊 **@MOVIEMAZA19 स्टैट्स**\n"
            f"• कुल पोस्ट: `{total_posts}`\n"
            f"• बैकअप में (DB के अनुसार): `{total_forwarded}`"
        )
    except Exception as e:
        await message.reply(f"❌ स्टैट्स एरर: {e}")

# ब्लैकलिस्ट
@app.on_message(filters.chat(SOURCE_CHANNEL_ID) & filters.caption & filters.channel, group=2)
async def auto_delete_blacklist(client: Client, message: Message):
    if message.caption:
        if any(word in message.caption.lower() for word in BLACKLIST_WORDS):
            try:
                await message.delete()
                print(f"ब्लैकलिस्टेड पोस्ट डिलीट किया: {message.message_id}")
            except Exception as e:
                print(f"ब्लैकलिस्ट डिलीट एरर: {e}")

# /clean कमांड (एडमिन के लिए)
# --- [ADMIN FIX] सिर्फ एडमिन ही /clean कमांड इस्तेमाल कर सकता है ---
@app.on_message(filters.command("clean") & filters.private & filters.user(ADMIN_ID))
async def force_clean_caption(client: Client, message: Message):
    if message.reply_to_message and (message.reply_to_message.text or message.reply_to_message.caption):
        text_to_clean = message.reply_to_message.text or message.reply_to_message.caption
        cleaned = clean_caption(text_to_clean)
        await message.reply(f"**क्लीन कैप्शन:**\n\n{cleaned if cleaned else '*(कैप्शन खाली है)*'}")
    else:
        await message.reply("क्लीन करने के लिए किसी मैसेज को रिप्लाई करें।")


# --- Render FIX: Async वेब सर्वर ---
async def web_server():
    web_app = web.Application()
    web_app.router.add_get("/", lambda r: web.Response(text="मैं ज़िंदा हूँ! (बॉट चल रहा है)"))
    runner = web.AppRunner(web_app)
    await runner.setup()
    port = int(os.environ.get('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    try:
        await site.start()
        print(f"✅ Render के लिए वेब सर्वर 0.0.0.0:{port} पर शुरू हो गया है।")
    except Exception as e:
        print(f"❌ वेब सर्वर स्टार्ट करने में एरर: {e}")

# --- (बॉट को शुरू करने वाला मुख्य फंक्शन) ---
async def main():
    await init_db()
    
    print("बॉट शुरू हो रहा है...")
    await asyncio.gather(
        app.start(),
        web_server()
    )
    
    print("✅ बॉट और वेब सर्वर दोनों चल रहे हैं।")
    await asyncio.Event().wait()

# --- बॉट को शुरू करना ---
if __name__ == "__main__":
    if not BOT_TOKEN:
        print("!! ज़रूरी: BOT_TOKEN नहीं मिला !!")
        print("कृपया Render में Environment Variable में 'BOT_TOKEN' को सेट करें।")
    elif not API_ID or not API_HASH:
        print("!! ज़रूरी: API_ID या API_HASH नहीं मिला !!")
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("बॉट को बंद किया जा रहा है...")
