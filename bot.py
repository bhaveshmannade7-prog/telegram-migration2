import os
import re
import asyncio
import asyncpg
import telebot # आम बॉट फ़ंक्शंस के लिए
from telebot.async_telebot import AsyncTeleBot # Async Bot के लिए
from telebot import types
from telebot.asyncio_helper import ApiTelegramException # Error handling
from aiohttp import web

# --- कॉन्फ़ि... (बाकी सब पहले जैसा) ---
# कृपया Render.com में ये सभी Environment Variables सेट करें
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "") # Render Postgres DB URL
ADMIN_ID = int(os.environ.get("ADMIN_ID", 7263519581)) # अपना एडमिन ID डालें

# चैनल IDs (सुनिश्चित करें कि बॉट इन दोनों में एडमिन है)
SOURCE_CHANNEL_ID = int(os.environ.get("SOURCE_CHANNEL_ID", -1003138949015))
BACKUP_CHANNEL_ID = int(os.environ.get("BACKUP_CHANNEL_ID", -1002010174094))

# कैप्शन सेटिंग्स
CAPTION_FOOTER = "\n\n@THEGREATMOVIESL9\n@MOVIEMAZASU"
USERNAME_WHITELIST = ["@THEGREATMOVIESL9", "@MOVIEMAZASU"]
BLACKLIST_WORDS = ["18+", "adult", "hot", "sexy"] # ब्लैकलिस्टेड शब्द (लोवरकेस में)

# --- ग्लोबल वैरियेबल्स ---
bot = AsyncTeleBot(BOT_TOKEN, parse_mode='Markdown')
db_pool = None # Async database pool

print("="*60)
print("🤖 TELEGRAM MOVIE BOT STARTING...")
print("="*60)

# --- डेटाबेस ---
async def init_database():
    """डेटाबेस कनेक्शन पूल को इनिशियलाइज़ करता है।"""
    global db_pool
    if not DATABASE_URL:
        print("⚠️  चेतावनी: DATABASE_URL नहीं मिला! डेटाबेस के बिना काम जारी है।")
        return None
    
    try:
        # Render के लिए SSL 'require' ज़रूरी हो सकता है
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, ssl='require')
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS forwarded_movies (
                    source_message_id BIGINT PRIMARY KEY,
                    file_unique_id TEXT NOT NULL
                )
            """)
            # file_unique_id पर इंडेक्स बनाने से डुप्लीकेट खोजना तेज़ हो जाएगा
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_unique_id ON forwarded_movies (file_unique_id);
            """)
        print("✅ डेटाबेस सफलतापूर्वक कनेक्ट हो गया (SSL: require)!")
        return db_pool
    except Exception as e:
        print(f"❌ SSL 'require' के साथ DB कनेक्शन विफल: {e}")
        try:
            # बिना SSL के प्रयास (लोकल टेस्टिंग के लिए)
            db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS forwarded_movies (
                        source_message_id BIGINT PRIMARY KEY,
                        file_unique_id TEXT NOT NULL
                    )
                """)
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_file_unique_id ON forwarded_movies (file_unique_id);
                """)
            print("✅ डेटाबेस सफलतापूर्वक कनेक्ट हो गया (SSL: No)!")
            return db_pool
        except Exception as e2:
            print(f"❌ डेटाबेस से कनेक्ट करने में गंभीर त्रुटि: {e2}")
            db_pool = None
            return None

# --- हेल्पर फ़ंक्शंस ---
def clean_caption(caption_text):
    """कैप्शन से अनचाहे लिंक और यूज़रनेम हटाता है।"""
    if not caption_text:
        return ""
    
    cleaned = caption_text
    
    # URL पैटर्न
    url_patterns = [r'https?://[^\s]+', r'www\.[^\s]+', r't\.me/[^\s]+']
    for pattern in url_patterns:
        urls = re.findall(pattern, cleaned, flags=re.IGNORECASE)
        for url in urls:
            cleaned = cleaned.replace(url, "")
    
    # यूज़रनेम पैटर्न
    usernames = re.findall(r'@\S+', cleaned)
    for username in usernames:
        if username not in USERNAME_WHITELIST:
            cleaned = cleaned.replace(username, "")
    
    # खाली लाइनों को साफ करें
    cleaned = "\n".join([line.strip() for line in cleaned.split("\n") if line.strip()])
    return cleaned

def get_main_menu():
    """मुख्य मेन्यू कीबोर्ड जेनरेट करता है।"""
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📤 मूवी बैकअप (Auto)", callback_data="info_forward"),
        types.InlineKeyboardButton("🗑️ डुप्लीकेट (Auto)", callback_data="info_delete_dupes"),
        types.InlineKeyboardButton("✍️ Footer (Auto)", callback_data="info_append_footer"),
        types.InlineKeyboardButton("🧹 कैप्शन क्लीन (Auto)", callback_data="info_clean_old"),
        types.InlineKeyboardButton("📊 चैनल स्टैट्स", callback_data="show_stats"),
        types.InlineKeyboardButton("⛔ काम रोकें (N/A)", callback_data="stop_task")
    )
    return markup

def get_file_unique_id(message):
    """मैसेज से file_unique_id निकालता है।"""
    if message.video:
        return message.video.file_unique_id
    if message.document:
        return message.document.file_unique_id
    return None

# --- वेब सर्वर (Render के लिए) ---
async def start_web_server():
    """Render.com के लिए एक बेसिक aiohttp वेब सर्वर शुरू करता है।"""
    try:
        app = web.Application()
        app.router.add_get("/", lambda r: web.Response(text="Bot is alive! 🤖"))
        runner = web.AppRunner(app)
        await runner.setup()
        port = int(os.environ.get('PORT', 8080))
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        print(f"✅ वेब सर्वर पोर्ट {port} पर शुरू हो गया है।")
        # लूप को हमेशा चलता रखने के लिए
        await asyncio.Event().wait()
    except Exception as e:
        print(f"❌ वेब सर्वर शुरू करने में त्रुटि: {e}")

# --- बॉट हैंडलर्स (Async) ---

@bot.message_handler(commands=['start', 'help'])
async def start_command(message):
    """/start और /help कमांड को हैंडल करता है।"""
    user_id = message.from_user.id
    print(f"📨 /start या /help कमांड यूज़र {user_id} से प्राप्त हुआ।")
    
    if user_id == ADMIN_ID:
        print(f"✅ एडमिन वेरिफ़ाइड: {user_id}")
        await bot.reply_to(
            message,
            "👋 *नमस्ते एडमिन!* मैं आपका मूवी चैनल मैनेजर हूँ।\n\n"
            "**बॉट अब फुली ऑटोमैटिक है:**\n"
            "1.  **ऑटो-क्लीन/फुटर:** नई पोस्ट के कैप्शन अपने आप साफ़ होकर फुटर जुड़ जाएगा।\n"
            "2.  **ऑटो-डिलीट डुप्लीकेट:** डुप्लीकेट मूवी पोस्ट होने पर अपने आप डिलीट हो जाएगी।\n"
            "3.  **ऑटो-ब्लैकलिस्ट:** ब्लैकलिस्टेड शब्द (18+) वाली पोस्ट डिलीट हो जाएगी।\n\n"
            "--- \n"
            "**नए फीचर्स:**\n"
            "•   **पुरानी मूवीज़ के लिए:** अपनी पुरानी मूवीज़ को चैनल से *इसी चैट में फॉरवर्ड* करें। मैं उन्हें डेटाबेस में सेव कर लूँगा।\n"
            "•   **ID चेक करने के लिए:** `/id` कमांड का इस्तेमाल करें।",
            reply_markup=get_main_menu()
        )
    else:
        print(f"❌ गैर-एडमिन यूज़र: {user_id}")
        await bot.reply_to(
            message,
            f"⛔ माफ कीजिए, यह बॉट सिर्फ एडमिन द्वारा इस्तेमाल किया जा सकता है।\n\n"
            f"आपकी User ID: `{user_id}`"
        )

@bot.message_handler(commands=['id'])
async def get_id_command(message):
    """चैट ID और फॉरवर्डेड ID दिखाता है।"""
    if message.from_user.id != ADMIN_ID:
        return
    
    reply = f"ℹ️ आपकी User ID: `{message.from_user.id}`\n"
    reply += f"ℹ️ इस चैट की (Private) Chat ID: `{message.chat.id}`\n\n"
    
    if message.forward_from_chat:
        reply += f"⭐ *फॉरवर्डेड चैनल का नाम:* {message.forward_from_chat.title}\n"
        reply += f"🔑 *फॉरवर्डेड चैनल की ID:* `{message.forward_from_chat.id}`\n\n"
        reply += "Render के `SOURCE_CHANNEL_ID` में इस ID का इस्तेमाल करें।"
    else:
        reply += "अपने सोर्स चैनल से एक मैसेज मुझे फॉरवर्ड करें ताकि मैं उसकी ID बता सकूँ।"
        
    await bot.reply_to(message, reply)

@bot.callback_query_handler(func=lambda call: True)
async def handle_callback(call):
    """सभी इनलाइन बटन क्लिक को हैंडल करता है।"""
    user_id = call.from_user.id
    print(f"🔘 बटन दबाया: {call.data} यूज़र {user_id} द्वारा")
    
    if user_id != ADMIN_ID:
        await bot.answer_callback_query(call.id, "⛔ आप अधिकृत नहीं हैं!", show_alert=True)
        return

    # जानकारी वाले बटन्स
    info_messages = {
        "info_forward": "✅ **ऑटो-फॉरवर्डिंग** अभी लागू नहीं है।\nयह सुविधा भविष्य में जोड़ी जा सकती है। अभी बॉट केवल डुप्लीकेट और कैप्शन क्लीन करता है।",
        "info_delete_dupes": "✅ **ऑटो-डुप्लीकेट डिलीशन** चालू है!\nजब भी आप सोर्स चैनल में कोई नई मूवी पोस्ट करेंगे, बॉट चेक करेगा कि वह पहले से डेटाबेस में है या नहीं। अगर है, तो उसे तुरंत डिलीट कर दिया जाएगा।",
        "info_append_footer": "✅ **ऑटो-फुटर** चालू है!\nसोर्स चैनल में हर नई पोस्ट के कैप्शन को साफ़ करने के बाद, यह फुटर अपने आप जोड़ दिया जाएगा:\n\n" + CAPTION_FOOTER,
        "info_clean_old": "✅ **ऑटो-कैप्शन क्लीनिंग** चालू है!\nसोर्स चैनल में हर नई पोस्ट से अनचाहे लिंक्स और यूज़रनेम (@) अपने आप हटा दिए जाएँगे।"
    }

    if call.data in info_messages:
        await bot.answer_callback_query(call.id)
        await bot.send_message(call.message.chat.id, info_messages[call.data])

    elif call.data == "stop_task":
        await bot.answer_callback_query(call.id, "ℹ️ यह बॉट अब ऑटोमैटिक है।", show_alert=True)
        await bot.send_message(call.message.chat.id, "⛔ *टास्क रोकने की ज़रूरत नहीं।*\n\nबॉट अब 'इवेंट-बेस्ड' है। यह तभी काम करता है जब आप चैनल में कोई नई पोस्ट डालते हैं। यह कोई बैकग्राउंड टास्क नहीं चला रहा है।")
    
    elif call.data == "show_stats":
        await bot.answer_callback_query(call.id, "📊 स्टैट्स लोड हो रहे हैं...")
        
        total_movies = 0
        db_status = "Not Connected"
        
        if db_pool:
            db_status = "Connected"
            try:
                async with db_pool.acquire() as conn:
                    total_movies = await conn.fetchval("SELECT COUNT(*) FROM forwarded_movies")
            except Exception as e:
                print(f"❌ स्टैट्स दिखाते समय DB त्रुटि: {e}")
                db_status = f"Error: {e}"
        
        await bot.send_message(
            call.message.chat.id,
            "📊 **चैनल स्टैटिस्टिक्स**\n\n"
            f"• *डेटाबेस स्थिति:* `{db_status}`\n"
            f"• *डेटाबेस में कुल मूवीज़:* `{total_movies}`\n"
            f"• *एडमिन ID:* `{ADMIN_ID}`\n"
            f"• *सोर्स चैनल:* `{SOURCE_CHANNEL_ID}`\n"
            f"• *बैकअप चैनल:* `{BACKUP_CHANNEL_ID}`"
        )

# --- मुख्य ऑटोमैटिक हैंडलर ---

@bot.channel_post_handler(
    func=lambda message: message.chat.id == SOURCE_CHANNEL_ID,
    content_types=['video', 'document'] # केवल वीडियो या डॉक्यूमेंट वाली पोस्ट पर काम करें
)
async def handle_new_movie_post(message):
    """सोर्स चैनल में नई मूवी पोस्ट को हैंडल करता है।"""
    print(f"📥 [AUTO] सोर्स चैनल में नया मैसेज मिला: {message.message_id}")
    
    if not db_pool:
        print("⚠️ [AUTO] नया पोस्ट मिला, लेकिन DB कनेक्टेड नहीं है। क्लीनिंग और डुप्लीकेट चेक स्किप किया जा रहा है।")
        return

    try:
        file_unique_id = get_file_unique_id(message)
        if not file_unique_id:
            print("ℹ️ [AUTO] पोस्ट में कोई वीडियो/डॉक्यूमेंट नहीं है। स्किप किया जा रहा है।")
            return
            
        original_caption = message.caption if message.caption else ""
        caption_lower = original_caption.lower()

        # 1. ब्लैकलिस्ट चेक
        for word in BLACKLIST_WORDS:
            if word in caption_lower:
                print(f"🚫 [AUTO] ब्लैकलिस्टेड शब्द '{word}' मिला। मैसेज {message.message_id} डिलीट किया जा रहा है।")
                try:
                    await bot.delete_message(message.chat.id, message.message_id)
                except ApiTelegramException as e:
                    print(f"❌ [AUTO] मैसेज डिलीट करने में विफल (ब्लैकलिस्ट): {e}")
                return # आगे कुछ न करें

        # 2. डुप्लीकेट चेक
        async with db_pool.acquire() as conn:
            is_duplicate = await conn.fetchval(
                "SELECT 1 FROM forwarded_movies WHERE file_unique_id = $1 LIMIT 1",
                file_unique_id
            )
            
            if is_duplicate:
                print(f"🚫 [AUTO] डुप्लीकेट मूवी मिली! मैसेज {message.message_id} डिलीट किया जा रहा है।")
                try:
                    await bot.delete_message(message.chat.id, message.message_id)
                except ApiTelegramException as e:
                    print(f"❌ [AUTO] मैसेज डिलीट करने में विफल (डुप्लीकेट): {e}")
                return # आगे कुछ न करें

        # 3. कैप्शन क्लीन और फुटर ऐड
        cleaned_caption = clean_caption(original_caption)
        final_caption = cleaned_caption + CAPTION_FOOTER
        
        caption_changed = True
        if final_caption.strip() == original_caption.strip():
            caption_changed = False
            print(f"ℹ️ [AUTO] मैसेज {message.message_id} का कैप्शन पहले से ही साफ़ है।")
        
        if caption_changed:
            try:
                await bot.edit_message_caption(
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    caption=final_caption
                )
                print(f"✅ [AUTO] मैसेज {message.message_id} का कैप्शन साफ़ किया गया और फुटर जोड़ा गया।")
            except ApiTelegramException as e:
                if "message is not modified" in str(e):
                    print(f"ℹ️ [AUTO] मैसेज {message.message_id} का कैप्शन पहले से ही साफ़ है (API Error)।")
                else:
                    print(f"❌ [AUTO] कैप्शन एडिट करने में विफल: {e}")
                    pass

        # 4. डेटाबेस में नई मूवी जोड़ें
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO forwarded_movies (source_message_id, file_unique_id) VALUES ($1, $2) ON CONFLICT (source_message_id) DO NOTHING",
                message.message_id,
                file_unique_id
            )
        print(f"💾 [AUTO] नई मूवी (Msg ID: {message.message_id}) डेटाबेस में सहेजी गई।")

    except Exception as e:
        print(f"‼️ [AUTO] चैनल पोस्ट हैंडलर में गंभीर त्रुटि: {e}")
        import traceback
        traceback.print_exc()

@bot.edited_channel_post_handler(
    func=lambda message: message.chat.id == SOURCE_CHANNEL_ID,
    content_types=['video', 'document']
)
async def handle_edited_movie_post(message):
    """एडिट की गई पोस्ट को भी हैंडल करता है (डुप्लीकेट/ब्लैकलिस्ट के लिए)।"""
    print(f"🔄 [AUTO] मैसेज {message.message_id} एडिट हुआ। पुनः जाँच की जा रही है...")
    await handle_new_movie_post(message)


# --- नया: मैनुअल इंडेक्स हैंडलर ---

@bot.message_handler(
    func=lambda message: 
        message.chat.id == ADMIN_ID and 
        message.forward_from_chat and 
        message.forward_from_chat.id == SOURCE_CHANNEL_ID,
    content_types=['video', 'document']
)
async def handle_manual_index(message):
    """एडमिन द्वारा प्राइवेट चैट में फॉरवर्ड की गई पुरानी मूवीज़ को इंडेक्स करता है।"""
    
    print(f"📥 [MANUAL] फॉरवर्डेड मैसेज मिला: {message.forward_from_message_id}")

    if not db_pool:
        await bot.reply_to(message, "⛔️ DB कनेक्ट नहीं है। इंडेक्सिंग विफल।")
        print("❌ [MANUAL] DB कनेक्ट नहीं है।")
        return

    file_unique_id = get_file_unique_id(message)
    if not file_unique_id:
        print("❌ [MANUAL] कोई file_unique_id नहीं मिला।")
        return

    try:
        async with db_pool.acquire() as conn:
            # हम ओरिजिनल मैसेज ID को सेव कर रहे हैं
            source_msg_id = message.forward_from_message_id
            
            is_duplicate = await conn.fetchval(
                "SELECT 1 FROM forwarded_movies WHERE file_unique_id = $1 LIMIT 1",
                file_unique_id
            )
            
            if is_duplicate:
                await bot.reply_to(message, f"ℹ️ (Msg ID: {source_msg_id})\nयह मूवी पहले से ही डेटाबेस में है।")
                print(f"ℹ️ [MANUAL] डुप्लीकेट (Msg ID: {source_msg_id})")
                return

            await conn.execute(
                "INSERT INTO forwarded_movies (source_message_id, file_unique_id) VALUES ($1, $2) ON CONFLICT (source_message_id) DO NOTHING",
                source_msg_id,
                file_unique_id
            )
        
        print(f"💾 [MANUAL] मूवी (Msg ID: {source_msg_id}) डेटाबेस में सहेजी गई।")
        await bot.reply_to(message, f"✅ (Msg ID: {source_msg_id})\nयह मूवी सफलतापूर्वक इंडेक्स हो गई है!")

    except Exception as e:
        print(f"‼️ [MANUAL] इंडेक्स हैंडलर में त्रुटि: {e}")
        await bot.reply_to(message, f"❌ (Msg ID: {source_msg_id})\nइंडेक्स करने में त्रुटि: {e}")


# --- मुख्य फ़ंक्शन ---
async def main():
    """बॉट, वेब सर्वर और डेटाबेस को शुरू करता है।"""
    if not BOT_TOKEN:
        print("❌ एरर: BOT_TOKEN नहीं मिला! बॉट बंद हो रहा है।")
        exit(1)
        
    if not ADMIN_ID:
        print("❌ एरर: ADMIN_ID नहीं मिला! बॉट बंद हो रहा है।")
        exit(1)

    try:
        bot_info = await bot.get_me()
        print(f"✅ बॉट कनेक्टेड: @{bot_info.username} (ID: {bot_info.id})")
        print(f"✅ एडमिन ID: {ADMIN_ID}")
        print("="*60)
    except Exception as e:
        print(f"❌ बॉट टोकन अमान्य है: {e}")
        exit(1)

    # 1. डेटाबेस शुरू करें
    await init_database()
    
    # 2. वेब सर्वर (Render के लिए) को एक बैकग्राउंड टास्क के रूप में शुरू करें
    web_server_task = asyncio.create_task(start_web_server())
    
    # 3. बॉट पोलिंग को एक बैकग्राउंड टास्क के रूप में शुरू करें
    polling_task = asyncio.create_task(bot.polling(non_stop=True, timeout=60))
    
    print("\n🚀 बॉट अब चल रहा है!")
    print("📱 बॉट को /start भेजकर शुरू करें।")
    print("="*60 + "\n")
    
    # दोनों टास्क को हमेशा चलाते रहें
    await asyncio.gather(web_server_task, polling_task)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n✋ बॉट यूज़र द्वारा रोका गया।")
    except Exception as e:
        print(f"\n❌ मुख्य लूप में गंभीर त्रुटि: {e}")
        import traceback
        traceback.print_exc()
