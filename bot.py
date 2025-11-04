import os
import asyncio
import threading
from flask import Flask
from pyrogram import Client, filters
from pyrogram.types import Message

# --- इस बार हम 3 चीज़ों का इस्तेमाल करेंगे ---
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# --- Pyrogram Client (सिर्फ बॉट, बिना सेशन स्ट्रिंग) ---
app = Client(
    "final_test_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

# --- सिंपल /start कमांड ---
@app.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    print("✅ /start कमांड मिली, जवाब भेजा जा रहा है...")
    await message.reply("नमस्ते! आपका API_ID, API_HASH और BOT_TOKEN सही हैं।")

# --- बॉट को शुरू करने वाला मुख्य फंक्शन ---
async def main():
    print("बॉट (फाइनल टेस्ट) शुरू हो रहा है...")
    await app.start()
    print("✅✅✅ बॉट (फाइनल टेस्ट) सफलतापूर्वक शुरू हो गया है!")
    await asyncio.Event().wait()

# --- Render FIX: वेब सर्वर ---
web_app = Flask(__name__)
@web_app.route('/')
def home():
    return "मैं ज़िंदा हूँ! (बॉट फाइनल टेस्ट मोड में चल रहा है)"

def run_web_server():
    port = int(os.environ.get('PORT', 8080))
    web_app.run(host='0.0.0.0', port=port)

# --- बॉट को शुरू करना ---
if __name__ == "__main__":
    if not BOT_TOKEN or not API_ID or not API_HASH:
        print("!! ज़रूरी: API_ID, API_HASH, या BOT_TOKEN नहीं मिला !!")
    else:
        print("Render के लिए वेब सर्वर शुरू किया जा रहा है...")
        web_thread = threading.Thread(target=run_web_server)
        web_thread.daemon = True
        web_thread.start()
        
        print("टेलीग्राम बॉट (फाइनल टेस्ट) शुरू किया जा रहा है...")
        asyncio.run(main())

