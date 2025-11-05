import os
import re
import asyncio
import asyncpg
from aiohttp import web

# --- Telebot (Bot) Imports ---
import telebot
from telebot.async_telebot import AsyncTeleBot
from telebot import types

# --- Pyrogram (User-Bot) Imports ---
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, MessageNotModified

# --- कॉन्फ़िगरेशन ---
# Render.com Environment Variables

# (Bot के लिए)
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0)) # अपना एडमिन ID डालें

# (User-Bot/Indexer के लिए)
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "") # आपकी स्क्रिप्ट से निकली स्ट्रिंग

# (डेटाबेस)
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# (चैनल)
SOURCE_CHANNEL_ID = int(os.environ.get("SOURCE_CHANNEL_ID", 0))

# (सेटिंग्स)
CAPTION_FOOTER = "\n\n@THEGREATMOVIESL9\n@MOVIEMAZASU"
USERNAME_WHITELIST = ["@THEGREATMOVIESL9", "@MOVIEMAZASU"]
BLACKLIST_WORDS = ["18+", "adult", "hot", "sexy"]
BATCH_SLEEP_TIME = 2 # बड़े कामों (index/clean) के बीच में 2 सेकंड का गैप (FloodWait से बचने के लिए)

# --- ग्लोबल वैरियेबल्स ---
db_pool = None # Async database pool
batch_job_lock = asyncio.Lock() # एक समय में एक ही बड़ा काम (index/clean) करने के लिए

# 1. Telebot (बॉट) को इनिशियलाइज़ करना
bot = AsyncTeleBot(BOT_TOKEN, parse_mode='Markdown')

# 2. Pyrogram (यूज़र-बॉट / Indexer) को इनिशियलाइज़ करना
app = Client(
    "movie_indexer_client",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING
)

print("="*60)
print("🤖 UPGRADED HYBRID TELEGRAM BOT STARTING...")
print("="*60)

# --- डेटाबेस ---
async def init_database():
    """डेटाबेस कनेक्शन पूल को इनिशियलाइज़ करता है।"""
    global db_pool
    if not DATABASE_URL:
        print("❌ एरर: DATABASE_URL नहीं मिला!")
        return None
    
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, ssl='require')
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS indexed_movies (
                    source_message_id BIGINT PRIMARY KEY,
                    file_unique_id TEXT NOT NULL
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_unique_id ON indexed_movies (file_unique_id);
            """)
        print("✅ डेटाबेस सफलतापूर्वक कनेक्ट हो गया (SSL: require)!")
        return db_pool
    except Exception as e:
        print(f"❌ डेटाबेस से कनेक्ट करने में गंभीर त्रुटि: {e}")
        db_pool = None
        return None

# --- हेल्पर फ़ंक्शंस ---
def clean_caption(caption_text):
    """कैप्शन से अनचाहे लिंक और यूज़रनेम हटाता है।"""
    if not caption_text:
        return ""
    cleaned = caption_text
    url_patterns = [r'https?://[^\s]+', r'www\.[^\s]+', r't\.me/[^\s]+']
    for pattern in url_patterns:
        urls = re.findall(pattern, cleaned, flags=re.IGNORECASE)
        for url in urls: cleaned = cleaned.replace(url, "")
    usernames = re.findall(r'@\S+', cleaned)
    for username in usernames:
        if username not in USERNAME_WHITELIST:
            cleaned = cleaned.replace(username, "")
    cleaned = "\n".join([line.strip() for line in cleaned.split("\n") if line.strip()])
    return cleaned

def get_file_unique_id(message):
    """Pyrogram मैसेज से file_unique_id निकालता है।"""
    if message.video:
        return message.video.file_unique_id
    if message.document:
        return message.document.file_unique_id
    return None

def get_main_menu():
    """मुख्य मेन्यू कीबोर्ड जेनरेट करता है।"""
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("📊 चैनल स्टैट्स देखें", callback_data="show_stats"),
        types.InlineKeyboardButton("⏳ फुल इंडेक्स (Run /index)", callback_data="info_index"),
        types.InlineKeyboardButton("🧹 क्लीन ऑल (Run /cleanall)", callback_data="info_clean"),
        types.InlineKeyboardButton("🔄 रिफ्रेश (Reply /refresh)", callback_data="info_refresh")
    )
    return markup

# --- वेब सर्वर (Render के लिए) ---
async def start_web_server():
    """Render.com के लिए एक बेसिक aiohttp वेब सर्वर शुरू करता है।"""
    try:
        app_web = web.Application()
        app_web.router.add_get("/", lambda r: web.Response(text="Bot & Indexer are alive! 🤖"))
        runner = web.AppRunner(app_web)
        await runner.setup()
        port = int(os.environ.get('PORT', 8080))
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        print(f"✅ वेब सर्वर पोर्ट {port} पर शुरू हो गया है।")
        await asyncio.Event().wait() # हमेशा चलता रहे
    except Exception as e:
        print(f"❌ वेब सर्वर शुरू करने में त्रुटि: {e}")

# --- 1. Telebot (बॉट) हैंडलर्स (यूज़र कमांड के लिए) ---

@bot.message_handler(commands=['start', 'help'])
async def start_command(message):
    """/start और /help कमांड को हैंडल करता है।"""
    if message.from_user.id != ADMIN_ID:
        return await bot.reply_to(message, "⛔ आप अधिकृत नहीं हैं।")

    print(f"✅ [BOT] एडमिन {ADMIN_ID} ने /start दबाया।")
    await bot.reply_to(
        message,
        "👋 *नमस्ते एडमिन!* यह हाइब्रिड बॉट है।\n\n"
        "**ऑटोमैटिक काम (Indexer):**\n"
        "1.  नई मूवी को इंडेक्स करना।\n"
        "2.  डुप्लीकेट को डिलीट करना।\n"
        "3.  ब्लैकलिस्टेड को डिलीट करना।\n\n"
        "**मैनुअल काम (कमांड्स):**\n"
        "•   `/stats`: (बटन) आँकड़े देखें।\n"
        "•   `/index`: (Saved Messages में) सभी पुरानी मूवीज़ को इंडेक्स करें।\n"
        "•   `/cleanall`: (Saved Messages में) सभी मूवीज़ के कैप्शन साफ़ करें और फुटर जोड़ें।\n"
        "•   `/refresh`: (चैनल में रिप्लाई) किसी एक मूवी का कैप्शन साफ़ करें।",
        reply_markup=get_main_menu()
    )

@bot.callback_query_handler(func=lambda call: True)
async def handle_callback(call):
    """सभी इनलाइन बटन को हैंडल करता है।"""
    if call.from_user.id != ADMIN_ID:
        return await bot.answer_callback_query(call.id, "⛔ आप अधिकृत नहीं हैं!", show_alert=True)

    if call.data == "show_stats":
        await bot.answer_callback_query(call.id, "📊 स्टैट्स लोड हो रहे हैं...")
        total_movies = 0
        db_status = "Not Connected"
        
        if db_pool:
            db_status = "Connected"
            try:
                async with db_pool.acquire() as conn:
                    total_movies = await conn.fetchval("SELECT COUNT(*) FROM indexed_movies")
            except Exception as e:
                print(f"❌ [BOT] स्टैट्स दिखाते समय DB त्रुटि: {e}")
                db_status = f"Error: {e}"
        
        await bot.send_message(
            call.message.chat.id,
            "📊 **चैनल स्टैटिस्टिक्स**\n\n"
            f"• *डेटाबेस स्थिति:* `{db_status}`\n"
            f"• *डेटाबेस में कुल मूवीज़:* `{total_movies}`\n"
            f"• *एडमिन ID:* `{ADMIN_ID}`\n"
            f"• *सोर्स चैनल:* `{SOURCE_CHANNEL_ID}`"
        )
    
    elif call.data == "info_index":
        await bot.answer_callback_query(call.id)
        await bot.send_message(call.message.chat.id, "ℹ️ *'फुल इंडेक्स'* चलाने के लिए:\n\n1. अपने 'Saved Messages' (या खुद को) में जाएँ।\n2. वहाँ `/index` टाइप करके भेजें।")
    
    elif call.data == "info_clean":
        await bot.answer_callback_query(call.id)
        await bot.send_message(call.message.chat.id, "ℹ️ *'क्लीन ऑल'* चलाने के लिए:\n\n1. अपने 'Saved Messages' में जाएँ।\n2. वहाँ `/cleanall` टाइप करके भेजें।\n3. बॉट DB में मौजूद सभी पोस्ट के कैप्शन साफ़ करके फुटर जोड़ देगा।")

    elif call.data == "info_refresh":
        await bot.answer_callback_query(call.id)
        await bot.send_message(call.message.chat.id, "ℹ️ *'रिफ्रेश'* करने के लिए:\n\n1. अपने *सोर्स चैनल* में जाएँ।\n2. जिस मूवी का कैप्शन साफ़ करना है, उसे *रिप्लाई* करें।\n3. रिप्लाई में `/refresh` टाइप करके भेजें।")


# --- 2. Pyrogram (यूज़र-बॉट) हैंडलर्स (चैनल को मैनेज करने के लिए) ---

async def process_new_message(client, message):
    """नई या एडिट की गई मूवी पोस्ट को प्रोसेस करता है (सिर्फ इंडेक्सिंग)।"""
    if not db_pool:
        print("⚠️ [Indexer] DB कनेक्ट नहीं है। प्रोसेसिंग स्किप की जा रही है।")
        return

    try:
        file_unique_id = get_file_unique_id(message)
        if not file_unique_id:
            return

        original_caption = message.caption if message.caption else ""
        caption_lower = original_caption.lower()

        # 1. ब्लैकलिस्ट चेक
        for word in BLACKLIST_WORDS:
            if word in caption_lower:
                print(f"🚫 [Indexer] ब्लैकलिस्टेड शब्द '{word}' मिला। मैसेज {message.id} डिलीट किया जा रहा है।")
                await message.delete()
                return

        # 2. डुप्लीकेट चेक
        async with db_pool.acquire() as conn:
            is_duplicate = await conn.fetchval(
                "SELECT 1 FROM indexed_movies WHERE file_unique_id = $1 LIMIT 1",
                file_unique_id
            )
            if is_duplicate:
                print(f"🚫 [Indexer] डुप्लीकेट मूवी मिली! मैसेज {message.id} डिलीट किया जा रहा है।")
                await message.delete()
                return

        # 3. डेटाबेस में नई मूवी जोड़ें (क्लीनिंग के बिना)
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO indexed_movies (source_message_id, file_unique_id) VALUES ($1, $2) ON CONFLICT (source_message_id) DO NOTHING",
                message.id,
                file_unique_id
            )
        print(f"💾 [Indexer] नई मूवी (Msg ID: {message.id}) डेटाबेस में सहेजी गई (बिना क्लीन किए)।")

    except FloodWait as e:
        print(f"⚠️ [Indexer] FloodWait: {e.value} सेकंड के लिए रुक रहा हूँ।")
        await asyncio.sleep(e.value)
    except Exception as e:
        print(f"❌ [Indexer] नई पोस्ट प्रोसेस करने में एरर: {e}")


@app.on_message(
    filters.chat(SOURCE_CHANNEL_ID) & 
    (filters.video | filters.document) & 
    filters.group
)
async def auto_index_new_post(client, message):
    """नई मूवी पोस्ट को ऑटो-इंडेक्स करता है।"""
    print(f"📥 [Indexer] नई पोस्ट मिली: {message.id}")
    await process_new_message(client, message)


@app.on_edited_message(
    filters.chat(SOURCE_CHANNEL_ID) & 
    (filters.video | filters.document) & 
    filters.group
)
async def auto_index_edited_post(client, message):
    """एडिट की गई मूवी पोस्ट को भी हैंडल करता है।"""
    print(f"🔄 [Indexer] एडिटेड पोस्ट मिली: {message.id}")
    await process_new_message(client, message)


# --- Pyrogram (यूज़र-बॉट) - मैनुअल कमांड्स ---

@app.on_message(
    filters.command("index", prefixes="/") & 
    filters.user(ADMIN_ID) & 
    (filters.private | filters.user("self")) 
)
async def manual_index_command(client, message):
    """/index कमांड (Saved Messages में) सुनकर पुरानी मूवीज़ को स्कैन करता है।"""
    
    if not db_pool:
        return await message.reply("⛔️ DB कनेक्ट नहीं है। इंडेक्सिंग विफल।")

    if batch_job_lock.locked():
        return await message.reply("⏳ एक और काम (जैसे /cleanall) पहले से चल रहा है। कृपया उसके पूरा होने के बाद प्रयास करें।")

    async with batch_job_lock:
        print(f"⏳ [Indexer] फुल चैनल इंडेक्स शुरू किया... (यूज़र: {message.from_user.id})")
        status_msg = await message.reply("⏳ **फुल चैनल इंडेक्स शुरू हो रहा है...**\n\nमैं सोर्स चैनल के सभी पुराने मैसेज को स्कैन कर रहा हूँ। इसमें समय लग सकता है।")
        
        total_scanned = 0
        total_added = 0
        
        try:
            async for msg in app.get_chat_history(SOURCE_CHANNEL_ID):
                total_scanned += 1
                
                file_uid = get_file_unique_id(msg)
                if file_uid:
                    try:
                        async with db_pool.acquire() as conn:
                            is_duplicate = await conn.fetchval(
                                "SELECT 1 FROM indexed_movies WHERE file_unique_id = $1 LIMIT 1",
                                file_uid
                            )
                            if not is_duplicate:
                                await conn.execute(
                                    "INSERT INTO indexed_movies (source_message_id, file_unique_id) VALUES ($1, $2) ON CONFLICT (source_message_id) DO NOTHING",
                                    msg.id,
                                    file_uid
                                )
                                total_added += 1
                    except Exception as e:
                        print(f"❌ [Indexer] DB इंसर्ट विफल (Msg ID: {msg.id}): {e}")

                if total_scanned % 500 == 0:
                    print(f"[Indexer] {total_scanned} मैसेज स्कैन किए...")
                    await status_msg.edit(f"⏳ **इंडेक्स जारी है...**\n\n"
                                          f"• मैसेज स्कैन किए: `{total_scanned}`\n"
                                          f"• नई मूवीज़ जोड़ी गईं: `{total_added}`")
                
                await asyncio.sleep(0.1) # सर्वर को थोड़ा आराम दें

        except FloodWait as e:
            print(f"❌ [Indexer] FloodWait: {e.value} सेकंड के लिए रुक रहा हूँ।")
            await status_msg.edit(f"❌ FloodWait: Telegram ने हमें {e.value} सेकंड के लिए रुकने को कहा है। इंडेक्सिंग रुक गई है।")
            return
        except Exception as e:
            print(f"❌ [Indexer] फुल इंडेक्स में गंभीर त्रुटि: {e}")
            await status_msg.edit(f"❌ इंडेक्सिंग में एरर: {e}")
            return

    print("✅ [Indexer] फुल इंडेक्स पूरा हुआ।")
    await status_msg.edit(f"✅ **फुल इंडेक्स पूरा हुआ!**\n\n"
                        f"• कुल मैसेज स्कैन किए: `{total_scanned}`\n"
                        f"• नई मूवीज़ डेटाबेस में जोड़ी गईं: `{total_added}`")

@app.on_message(
    filters.command("cleanall", prefixes="/") & 
    filters.user(ADMIN_ID) & 
    (filters.private | filters.user("self")) 
)
async def manual_clean_command(client, message):
    """/cleanall कमांड (Saved Messages में) सुनकर सभी पोस्ट को साफ़ करता है।"""
    
    if not db_pool:
        return await message.reply("⛔️ DB कनेक्ट नहीं है। क्लीनिंग विफल।")

    if batch_job_lock.locked():
        return await message.reply("⏳ एक और काम (जैसे /index) पहले से चल रहा है। कृपया उसके पूरा होने के बाद प्रयास करें।")
    
    async with batch_job_lock:
        print(f"⏳ [CLEANER] फुल चैनल क्लीन शुरू किया...")
        status_msg = await message.reply("⏳ **फुल चैनल क्लीनिंग शुरू हो रही है...**\n\nमैं डेटाबेस में मौजूद सभी मूवीज़ के कैप्शन साफ़ कर रहा हूँ और फुटर जोड़ रहा हूँ।")

        total_cleaned = 0
        total_failed = 0
        
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT source_message_id FROM indexed_movies")
            
            total_messages = len(rows)
            print(f"[CLEANER] {total_messages} मूवीज़ को क्लीन करना है।")

            for i, row in enumerate(rows):
                msg_id = row['source_message_id']
                try:
                    # चैनल से मैसेज प्राप्त करें
                    msg = await app.get_messages(SOURCE_CHANNEL_ID, msg_id)
                    
                    original_caption = msg.caption if msg.caption else ""
                    cleaned_caption = clean_caption(original_caption)
                    final_caption = cleaned_caption + CAPTION_FOOTER
                    
                    # अगर कैप्शन अलग है, तभी एडिट करें
                    if final_caption.strip() != original_caption.strip():
                        await msg.edit_caption(final_caption)
                        total_cleaned += 1
                    
                    # FloodWait से बचने के लिए हर मैसेज के बाद रुकें
                    await asyncio.sleep(BATCH_SLEEP_TIME)

                    if (i + 1) % 100 == 0:
                        print(f"[CLEANER] {i+1}/{total_messages} क्लीन किए...")
                        await status_msg.edit(f"⏳ **क्लीनिंग जारी है...**\n\n"
                                              f"• `{i+1}` / `{total_messages}` मैसेज चेक किए।\n"
                                              f"• `{total_cleaned}` कैप्शन एडिट किए।")

                except MessageNotModified:
                    # कैप्शन पहले से ही सही था
                    pass
                except FloodWait as e:
                    print(f"⚠️ [CLEANER] FloodWait: {e.value} सेकंड के लिए रुक रहा हूँ।")
                    await status_msg.edit(f"⏳ FloodWait... {e.value} सेकंड के लिए रुक रहा हूँ।")
                    await asyncio.sleep(e.value)
                except Exception as e:
                    print(f"❌ [CLEANER] Msg ID {msg_id} को क्लीन करने में विफल: {e}")
                    total_failed += 1

        except Exception as e:
            print(f"❌ [CLEANER] फुल क्लीन में गंभीर त्रुटि: {e}")
            await status_msg.edit(f"❌ क्लीनिंग में एरर: {e}")
            return

    print("✅ [CLEANER] फुल क्लीन पूरा हुआ।")
    await status_msg.edit(f"✅ **फुल क्लीनिंग पूरी हुई!**\n\n"
                        f"• कुल कैप्शन एडिट किए: `{total_cleaned}`\n"
                        f"• कुल विफल: `{total_failed}`")


@app.on_message(
    filters.command("refresh", prefixes="/") & 
    filters.user(ADMIN_ID) & 
    filters.chat(SOURCE_CHANNEL_ID)
)
async def refresh_command(client, message):
    """/refresh कमांड (चैनल में रिप्लाई) सुनकर एक पोस्ट को साफ़ करता है।"""
    
    if not message.reply_to_message:
        await message.reply("ℹ️ इस कमांड का इस्तेमाल करने के लिए किसी मैसेज को *रिप्लाई* करें।", quote=True, delete_in=10)
        return

    target_message = message.reply_to_message
    
    try:
        print(f"🔄 [REFRESH] मैसेज {target_message.id} को रिफ्रेश किया जा रहा है...")
        original_caption = target_message.caption if target_message.caption else ""
        cleaned_caption = clean_caption(original_caption)
        final_caption = cleaned_caption + CAPTION_FOOTER
        
        await target_message.edit_caption(final_caption)
        
        # कन्फर्मेशन मैसेज भेजें और फिर उसे डिलीट करें
        confirm_msg = await message.reply("✅ कैप्शन रिफ्रेश हो गया!", quote=True)
        await asyncio.sleep(10)
        await message.delete() # /refresh कमांड को डिलीट करें
        await confirm_msg.delete() # कन्फर्मेशन को डिलीट करें

    except MessageNotModified:
        confirm_msg = await message.reply("ℹ️ कैप्शन पहले से ही साफ़ है।", quote=True)
        await asyncio.sleep(10)
        await message.delete()
        await confirm_msg.delete()
    except FloodWait as e:
        print(f"⚠️ [REFRESH] FloodWait: {e.value} सेकंड के लिए रुक रहा हूँ।")
        await asyncio.sleep(e.value)
    except Exception as e:
        print(f"❌ [REFRESH] रिफ्रेश करने में एरर: {e}")
        await message.reply(f"❌ एरर: {e}", quote=True, delete_in=10)


# --- मुख्य फ़ंक्शन ---
async def main():
    """बॉट, वेब सर्वर और इंडेक्सर को शुरू करता है।"""
    # ज़रूरी वेरिएबल्स की जाँच
    if not all([BOT_TOKEN, API_ID, API_HASH, SESSION_STRING, DATABASE_URL, ADMIN_ID, SOURCE_CHANNEL_ID]):
        print("❌ एरर: सभी Environment Variables (BOT_TOKEN, API_ID, API_HASH, SESSION_STRING, DATABASE_URL, ADMIN_ID, SOURCE_CHANNEL_ID) ज़रूरी हैं!")
        exit(1)

    # 1. डेटाबेस शुरू करें
    if not await init_database():
        print("❌ डेटाबेस शुरू करने में विफल। बॉट बंद हो रहा है।")
        exit(1)
    
    # 2. Pyrogram (यूज़र-बॉट) को बैकग्राउंड में शुरू करें
    await app.start()
    me = await app.get_me()
    print(f"✅ [Pyrogram] Client (User-Bot) @{me.username} (ID: {me.id}) के तौर पर शुरू हो गया है।")
    
    # 3. Telebot (बॉट) को बैकग्राउंड में शुरू करें
    bot_info = await bot.get_me()
    print(f"✅ [Telebot] Bot (@{bot_info.username}) (ID: {bot_info.id}) शुरू हो गया है।")
    bot_polling_task = asyncio.create_task(bot.polling(non_stop=True, timeout=60))
    
    # 4. वेब सर्वर (Render के लिए) को शुरू करें (यह मुख्य टास्क होगा)
    print("\n🚀 बॉट और इंडेक्सर अब चल रहे हैं!")
    print(f"📱 अपने बॉट (@{bot_info.username}) को /start भेजें।")
    print("="*60 + "\n")
    
    web_server_task = asyncio.create_task(start_web_server())
    
    # सभी टास्क को एक साथ चलाएँ
    await asyncio.gather(bot_polling_task, web_server_task)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n✋ बॉट यूज़र द्वारा रोका गया।")
    except Exception as e:
        print(f"\n❌ मुख्य लूप में गंभीर त्रुटि: {e}")
        import traceback
        traceback.print_exc()
