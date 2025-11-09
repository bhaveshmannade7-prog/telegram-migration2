import os
import re
import json
import time
import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Any

from pyrogram import Client, filters, idle
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError, UserNotParticipant, ChatAdminRequired

from dotenv import load_dotenv
import aiofiles
from aiohttp import web, ClientSession # Webhook हटाने के लिए ClientSession

# .env फ़ाइल से एनवायरनमेंट वेरिएबल्स लोड करें
load_dotenv()

# ===========================
# Configuration
# ===========================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# एनवायरनमेंट वेरिएबल्स से कॉन्फ़िगरेशन
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
OWNER_ID_ENV = os.getenv("OWNER_ID")

# DATA_DIR (Render Disk के लिए)
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(exist_ok=True)
METADATA_FILE = DATA_DIR / "metadata.json"
EXCEPTIONS_FILE = DATA_DIR / "exceptions.json"

log.info(f"डेटा डायरेक्टरी इस पर सेट है: {DATA_DIR}")

# डिफ़ॉल्ट बैच आकार
DEFAULT_FORWARD_BATCH = 100
DEFAULT_CLEAN_BATCH = 100
BATCH_SLEEP_SECONDS = 5

# Regex
URL_REGEX = re.compile(
    r"(https?:\/\/[^\s]+|www\.[^\s]+)", re.IGNORECASE
)
USERNAME_REGEX = re.compile(r"@([A-Za-z0-9_]{5,32})")

# ज़रूरी वेरिएबल्स की जाँच करें
if not all([BOT_TOKEN, API_ID, API_HASH]):
    log.error("ज़रूरी एनवायरनमेंट वेरिएबल्स (BOT_TOKEN, API_ID, API_HASH) सेट नहीं हैं।")
    raise SystemExit("ज़रूरी एनवायरनमेंट वेरिएबल्स सेट नहीं हैं।")

# ===========================
# Async Data Persistence (स्थिरता के लिए)
# ===========================

async def read_metadata() -> Dict[str, Any]:
    """मेटाडेटा को JSON फ़ाइल से एसिंक्रोनस रूप से पढ़ता है।"""
    if not METADATA_FILE.exists():
        # .env से OWNER_ID को प्राथमिकता दें
        owner_id = int(OWNER_ID_ENV) if OWNER_ID_ENV and OWNER_ID_ENV.isdigit() else None
        meta = {
            "owner_id": owner_id,
            "managed_channel": None,
            "last_indexed": None,
            "forward_batch": DEFAULT_FORWARD_BATCH,
            "clean_batch": DEFAULT_CLEAN_BATCH,
            "replacement_text": "",
            "admin_session": None,
        }
        await write_metadata(meta)
        if owner_id:
            log.info(f"पर्यावरण से OWNER_ID लोड किया गया: {owner_id}")
        return meta
    try:
        async with aiofiles.open(METADATA_FILE, "r", encoding="utf-8") as f:
            content = await f.read()
            # .env से OWNER_ID को हमेशा प्राथमिकता दें यदि यह मौजूद है
            data = json.loads(content)
            if OWNER_ID_ENV and OWNER_ID_ENV.isdigit():
                data["owner_id"] = int(OWNER_ID_ENV)
            return data
    except (json.JSONDecodeError, IOError) as e:
        log.error(f"मेटाडेटा पढ़ने में विफल: {e}. डिफ़ॉल्ट पर वापस जा रहा हूँ।")
        owner_id = int(OWNER_ID_ENV) if OWNER_ID_ENV and OWNER_ID_ENV.isdigit() else None
        return { "owner_id": owner_id }


async def write_metadata(meta: Dict[str, Any]):
    """मेटाडेटा को JSON फ़ाइल में एसिंक्रोनस रूप से लिखता है।"""
    try:
        async with aiofiles.open(METADATA_FILE, "w", encoding="utf-8") as f:
            await f.write(json.dumps(meta, indent=2))
    except IOError as e:
        log.error(f"मेटाडेटा लिखने में विफल: {e}")


async def read_exceptions() -> List[str]:
    """अपवाद (exceptions) सूची को एसिंक्रोनस रूप से पढ़ता है।"""
    if not EXCEPTIONS_FILE.exists():
        await write_exceptions([])
        return []
    try:
        async with aiofiles.open(EXCEPTIONS_FILE, "r", encoding="utf-8") as f:
            content = await f.read()
            return json.loads(content)
    except (json.JSONDecodeError, IOError) as e:
        log.error(f"अपवाद पढ़ने में विफल: {e}. खाली सूची पर वापस जा रहा हूँ।")
        return []


async def write_exceptions(lst: List[str]):
    """अपवाद (exceptions) सूची को एसिंक्रोनस रूप से लिखता है।"""
    try:
        async with aiofiles.open(EXCEPTIONS_FILE, "w", encoding="utf-8") as f:
            await f.write(json.dumps(lst, indent=2))
    except IOError as e:
        log.error(f"अपवाद लिखने में विफल: {e}")

# save_channel_index (पिछले संस्करण जैसा ही)
async def save_channel_index(channel_id: int, messages: List[Dict[str, Any]]):
    if not messages:
        return 0
    fn = DATA_DIR / f"channel_{str(channel_id)}.json"
    existing = []
    if fn.exists():
        try:
            async with aiofiles.open(fn, "r", encoding="utf-8") as f:
                content = await f.read()
                existing = json.loads(content)
        except Exception as e:
            log.warning(f"चैनल इंडेक्स फ़ाइल {fn} पढ़ने में विफल: {e}")
            existing = []
    existing_ids = {m.get("message_id") for m in existing}
    new_items = [m for m in messages if m.get("message_id") not in existing_ids]
    if new_items:
        combined = existing + new_items
        try:
            async with aiofiles.open(fn, "w", encoding="utf-8") as f:
                await f.write(json.dumps(combined, indent=2, default=str))
            log.info(f"{len(new_items)} नए संदेश चैनल {channel_id} में सहेजे गए।")
        except IOError as e:
            log.error(f"चैनल इंडेक्स फ़ाइल {fn} लिखने में विफल: {e}")
    else:
        log.info(f"चैनल {channel_id} के लिए कोई नया संदेश नहीं मिला।")
    return len(new_items)

# ===========================
# Bot & Filters
# ===========================

bot = Client(
    "channel_manager_bot",
    bot_token=BOT_TOKEN,
    api_id=int(API_ID),
    api_hash=API_HASH,
    # session_string एक बॉट के लिए नहीं, बल्कि यूज़रबॉट के लिए होता है
    # bot_token का उपयोग करने से यह स्वचालित रूप से .session फ़ाइल को संभालता है
)

# --- ओनर (Owner) फ़िल्टर ---
async def owner_filter(_, __, message: Message):
    try:
        meta = await read_metadata()
        owner_id = meta.get("owner_id")
        
        if not owner_id:
            # यदि कोई मालिक सेट नहीं है, तो फ़िल्टर विफल हो जाता है (सिवाय /start के)
            log.warning(f"मालिक फ़िल्टर विफल: उपयोगकर्ता {message.from_user.id} से आया अनुरोध, लेकिन कोई मालिक सेट नहीं है।")
            return False
            
        is_owner = message.from_user.id == owner_id
        if not is_owner:
            log.warning(f"मालिक फ़िल्टर विफल: उपयोगकर्ता {message.from_user.id} मालिक नहीं है (मालिक ID: {owner_id})।")
        
        return is_owner
    except Exception as e:
        log.error(f"मालिक फ़िल्टर में त्रुटि: {e}")
        return False

filters.owner = filters.create(owner_filter)
log.info("मालिक (owner) फ़िल्टर बनाया गया।")

# ===========================
# Helper Functions
# ===========================

async def resolve_chat_id(client: Client, chat_identifier: str):
    """चैट आइडेंटिफ़ायर को एक इंट ID में बदलता है।"""
    try:
        if chat_identifier.lstrip('-').isdigit():
            return int(chat_identifier)
        chat = await client.get_chat(chat_identifier)
        return chat.id
    except Exception as e:
        log.error(f"चैट {chat_identifier} का समाधान करने में विफल: {e}")
        raise ValueError(f"चैट '{chat_identifier}' का समाधान नहीं किया जा सका।")


async def with_admin_client(fn):
    """एडमिन सेशन (StringSession) से एक अस्थायी क्लाइंट बनाता है और चलाता है।"""
    meta = await read_metadata()
    sess = meta.get("admin_session")
    if not sess or not all(k in sess for k in ("api_id", "api_hash", "session")):
        raise RuntimeError("एडमिन सेशन कॉन्फ़िगर नहीं है। /setsession का उपयोग करें।")
    
    admin_client = Client(
        name="admin_session_temp", # सेशन फ़ाइल के लिए एक अलग नाम
        api_id=int(sess["api_id"]),
        api_hash=sess["api_hash"],
        session_string=sess["session"],
    )
    
    try:
        await admin_client.start()
        log.info("अस्थायी एडमिन क्लाइंट शुरू किया गया।")
        result = await fn(admin_client, meta)
    except Exception as e:
        log.error(f"एडमिन क्लाइंट ऑपरेशन विफल: {e}")
        raise
    finally:
        await admin_client.stop()
        log.info("अस्थायी एडमिन क्लाइंट बंद किया गया।")
    return result


def msg_to_indexable(m: Message) -> Dict[str, Any]:
    """Message ऑब्जेक्ट को एक सरल डिक्शनरी में बदलता है।"""
    data = {
        "message_id": m.message_id,
        "date": int(m.date.timestamp()) if m.date else int(time.time()),
        "text": m.text or m.caption or "",
        "media_type": str(m.media).split(".")[1].lower() if m.media else None,
        "has_media": bool(m.media),
        "from_user": (m.from_user.username if m.from_user else "Unknown"),
    }
    return data

# ===========================
# Caption Cleaning Logic
# ===========================

def caption_cleaner(text: str, exceptions: List[str], replacement: str) -> str:
    """टेक्स्ट से URLs और @usernames को हटाता है।"""
    if not text:
        return text

    clean_exceptions = [ex.lower().strip('@') for ex in exceptions]

    def url_repl(match):
        s = match.group(0).lower()
        for ex in clean_exceptions:
            if ex and ex in s:
                return match.group(0)
        return replacement

    text = URL_REGEX.sub(url_repl, text)

    def user_repl(match):
        username_part = match.group(1).lower() 
        if username_part in clean_exceptions:
            return match.group(0)
        return replacement

    text = USERNAME_REGEX.sub(user_repl, text)
    return text.strip()


# ===========================
# Bot Commands
# ===========================

@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, message: Message):
    """बॉट शुरू करता है और मालिक की जाँच करता है। (सुधार किया गया)"""
    meta = await read_metadata()
    owner_id = meta.get("owner_id")
    
    # 1. यदि कोई मालिक (owner) सेट नहीं है, तो इस उपयोगकर्ता को मालिक बनाएं
    if owner_id is None:
        meta["owner_id"] = message.from_user.id
        await write_metadata(meta)
        log.info(f"नया मालिक सेट किया गया: {message.from_user.id}")
        await message.reply_text("बधाई हो, आप इस बॉट के मालिक बन गए हैं!\nकमांड देखने के लिए फिर से /start टाइप करें।")
        return

    # 2. यदि उपयोगकर्ता मालिक है
    if message.from_user.id == owner_id:
        text = (
            "चैनल मैनेजर बॉट तैयार है।\n\n"
            "कमांड (केवल एडमिन):\n"
            "/setsession <api_id>|<api_hash>|<string_session> - एडमिन सेशन सेट करें\n"
            "/index_channel <channel_id_or_username> - चैनल को इंडेक्स करें\n"
            "/sync - प्रबंधित चैनल को सिंक करें\n"
            "/forward <source> <target> - संदेशों को फॉरवर्ड करें\n"
            "/clean_channel <channel> - कैप्शन साफ़ करें\n"
            "/set_replacement [text] - प्रतिस्थापन टेक्स्ट सेट करें\n"
            "/add_exception <text> - अपवाद जोड़ें\n"
            "/remove_exception <text> - अपवाद हटाएं\n"
            "/exceptions - सभी अपवादों की सूची देखें\n"
            "/status - वर्तमान स्थिति दिखाएं"
        )
        await message.reply_text(text)
    
    # 3. यदि उपयोगकर्ता मालिक नहीं है
    else:
        log.warning(f"अनाधिकृत /start प्रयास: उपयोगकर्ता {message.from_user.id}")
        await message.reply_text(f"⛔️ एक्सेस अस्वीकृत।\nयह बॉट केवल मालिक (ID: `{owner_id}`) द्वारा उपयोग के लिए है।")


@bot.on_message(filters.command("setsession") & filters.private & filters.owner)
async def cmd_setsession(client: Client, message: Message):
    """एडमिन (यूज़रबॉट) का स्ट्रिंग सेशन सहेजता है।"""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("उपयोग: /setsession <api_id>|<api_hash>|<string_session>")
    
    payload = args[1].strip()
    try:
        api_id, api_hash, session = payload.split("|", 2)
        if not api_id.isdigit() or not api_hash or not session:
            raise ValueError("अमान्य प्रारूप")
    except ValueError:
        return await message.reply_text("प्रारूप त्रुटि। उपयोग: api_id|api_hash|string_session")
    
    meta = await read_metadata()
    meta["admin_session"] = {"api_id": int(api_id), "api_hash": api_hash, "session": session}
    await write_metadata(meta)
    await message.reply_text("✅ एडमिन सेशन सहेजा गया।")


@bot.on_message(filters.command("index_channel") & filters.private & filters.owner)
async def cmd_index_channel(client: Client, message: Message):
    """एडमिन सेशन का उपयोग करके चैनल के सभी संदेशों को इंडेक्स करता है।"""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("उपयोग: /index_channel <channel_id_or_username>")
    chat_ident = args[1].strip()

    status_msg = await message.reply_text(f"⏳ `{chat_ident}` को इंडेक्स किया जा रहा है... यह धीमा हो सकता है।")

    async def _do_index(admin_client: Client, meta_local):
        try:
            chat_id = await resolve_chat_id(admin_client, chat_ident)
        except Exception as e:
            return f"❌ चैट का समाधान करने में विफल: {e}"
        
        channel_id = chat_id
        count = 0
        added_total = 0
        messages_to_save = []
        
        try:
            async for m in admin_client.get_chat_history(channel_id, limit=0):
                messages_to_save.append(msg_to_indexable(m))
                count += 1
                
                if len(messages_to_save) >= 500:
                    added = await save_channel_index(channel_id, messages_to_save)
                    added_total += added
                    messages_to_save = []
                    # बहुत ज़्यादा अपडेट न भेजें
                    if count % 2000 == 0:
                        await status_msg.edit_text(f"⏳ `{chat_ident}` को इंडेक्स किया जा रहा है...\n{count} संदेश स्कैन किए गए।\n{added_total} नए सहेजे गए।")
            
            if messages_to_save:
                added = await save_channel_index(channel_id, messages_to_save)
                added_total += added

            meta_local["managed_channel"] = channel_id
            meta_local["last_indexed"] = int(time.time())
            await write_metadata(meta_local)
            
            return f"✅ इंडेक्सिंग पूर्ण।\nकुल स्कैन किए गए संदेश: {count}\nकुल नए जोड़े गए: {added_total}\nचैनल को प्रबंधित के रूप में सेट किया गया: {channel_id}"

        except (UserNotParticipant, ChatAdminRequired):
            return "❌ त्रुटि: एडमिन (यूज़रबॉट) इस चैनल का सदस्य नहीं है या उसके पास संदेश पढ़ने की अनुमति नहीं है।"
        except Exception as e:
            log.exception(f"इंडेक्सिंग में त्रुटि: {e}")
            return f"❌ इंडेक्सिंग विफल: {type(e).__name__}: {e}"

    try:
        result = await with_admin_client(_do_index)
        await status_msg.edit_text(str(result))
    except Exception as e:
        log.error(f"इंडेक्स विफल: {e}")
        await status_msg.edit_text(f"❌ इंडेक्स विफल: {e}")

# ... (cmd_sync, cmd_forward, cmd_clean_channel और अन्य कमांड पिछले संस्करण जैसे ही रहेंगे, 
# क्योंकि वे `filters.owner` द्वारा पहले ही सुरक्षित हैं और उनका तर्क ठीक था) ...

# (यहाँ cmd_sync डालें - पिछले कोड से कॉपी करें)
@bot.on_message(filters.command("sync") & filters.private & filters.owner)
async def cmd_sync(client: Client, message: Message):
    """प्रबंधित चैनल को सिंक करता है (केवल नए संदेशों को इंडेक्स करता है)।"""
    meta = await read_metadata()
    managed = meta.get("managed_channel")
    if not managed:
        return await message.reply_text("❌ कोई प्रबंधित चैनल सेट नहीं है। पहले /index_channel का उपयोग करें।")

    status_msg = await message.reply_text(f"⏳ प्रबंधित चैनल `{managed}` को सिंक किया जा रहा है...")
    
    async def _do_sync(admin_client: Client, meta_local):
        try:
            chat = await admin_client.get_chat(managed)
        except Exception as e:
            return f"❌ प्रबंधित चैट का समाधान करने में विफल: {e}"
        
        count = 0
        added_total = 0
        messages_to_save = []
        try:
            async for m in admin_client.get_chat_history(chat.id, limit=0):
                messages_to_save.append(msg_to_indexable(m))
                count += 1
                if len(messages_to_save) >= 500:
                    added = await save_channel_index(chat.id, messages_to_save)
                    added_total += added
                    messages_to_save = []
                    if count % 2000 == 0:
                        await status_msg.edit_text(f"⏳ सिंक हो रहा है...\n{count} संदेश स्कैन किए गए।\n{added_total} नए सहेजे गए।")
            
            if messages_to_save:
                added = await save_channel_index(chat.id, messages_to_save)
                added_total += added
            
            meta_local["last_indexed"] = int(time.time())
            await write_metadata(meta_local)
            return f"✅ सिंक पूर्ण।\nकुल स्कैन किए गए संदेश: {count}\nकुल नए जोड़े गए: {added_total}।"
        except (UserNotParticipant, ChatAdminRequired):
            return "❌ त्रुटि: एडमिन (यूज़रबॉट) इस चैनल का सदस्य नहीं है।"
        except Exception as e:
            log.exception(f"सिंक में त्रुटि: {e}")
            return f"❌ सिंक विफल: {type(e).__name__}: {e}"

    try:
        result = await with_admin_client(_do_sync)
        await status_msg.edit_text(str(result))
    except Exception as e:
        await status_msg.edit_text(f"❌ सिंक विफल: {e}")


# (यहाँ forward_messages_in_batches और cmd_forward डालें)
async def forward_messages_in_batches(src_id: int, tgt_id: int, batch_size: int, status_msg: Message):
    success = 0
    failed = 0
    total_scanned = 0
    message_ids_batch = []
    try:
        async for msg in bot.get_chat_history(src_id, limit=0):
            total_scanned += 1
            message_ids_batch.append(msg.message_id)
            if len(message_ids_batch) >= batch_size:
                try:
                    await bot.forward_messages(chat_id=tgt_id, from_chat_id=src_id, message_ids=message_ids_batch)
                    success += len(message_ids_batch)
                except FloodWait as e:
                    log.warning(f"फॉरवर्ड करते समय FloodWait {e.x} सेकंड")
                    await status_msg.edit_text(f"⏳ FloodWait: {e.x} सेकंड के लिए सो रहा हूँ...")
                    await asyncio.sleep(e.x + 2)
                    try:
                        await bot.forward_messages(chat_id=tgt_id, from_chat_id=src_id, message_ids=message_ids_batch)
                        success += len(message_ids_batch)
                    except Exception as retry_e:
                        log.error(f"बैच को फॉरवर्ड करने में पुनः प्रयास विफल: {retry_e}")
                        failed += len(message_ids_batch)
                except Exception as e:
                    log.exception(f"बैच को फॉरवर्ड करने में विफल: {e}")
                    failed += len(message_ids_batch)
                message_ids_batch = []
                await status_msg.edit_text(f"⏳ फॉरवर्ड हो रहा है...\nसफलता: {success}, विफल: {failed}, कुल स्कैन: {total_scanned}")
                await asyncio.sleep(BATCH_SLEEP_SECONDS)
        if message_ids_batch:
            try:
                await bot.forward_messages(chat_id=tgt_id, from_chat_id=src_id, message_ids=message_ids_batch)
                success += len(message_ids_batch)
            except Exception as e:
                log.exception(f"अंतिम बैच को फॉरवर्ड करने में विफल: {e}")
                failed += len(message_ids_batch)
    except (UserNotParticipant, ChatAdminRequired):
        return {"error": "❌ त्रुटि: बॉट स्रोत चैनल का सदस्य नहीं है।"}
    except Exception as e:
        log.exception(f"फॉरवर्ड लूप में त्रुटि: {e}")
        return {"error": f"❌ एक अप्रत्याशित त्रुटि हुई: {e}"}
    return {"success": success, "failed": failed, "total": total_scanned}

@bot.on_message(filters.command("forward") & filters.private & filters.owner)
async def cmd_forward(client: Client, message: Message):
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.reply_text("उपयोग: /forward <source_channel> <target_channel>")
    src, tgt = args[1].strip(), args[2].strip()
    try:
        src_id = await resolve_chat_id(bot, src)
        tgt_id = await resolve_chat_id(bot, tgt)
    except Exception as e:
        return await message.reply_text(f"❌ चैट का समाधान करने में विफल: {e}")
    meta = await read_metadata()
    batch = meta.get("forward_batch", DEFAULT_FORWARD_BATCH)
    status_msg = await message.reply_text(f"⏳ {src_id} -> {tgt_id} से फॉरवर्ड करना शुरू कर रहा हूँ (बैच: {batch})।")
    res = await forward_messages_in_batches(src_id, tgt_id, batch, status_msg)
    if "error" in res:
        await status_msg.edit_text(res["error"])
    else:
        await status_msg.edit_text(f"✅ फॉरवर्ड समाप्त।\nसफलता: {res['success']}\nविफल: {res['failed']}\nकुल स्कैन किए गए: {res['total']}")

# (यहाँ clean_channel_captions और cmd_clean_channel डालें)
async def clean_channel_captions(chat_id: int, batch_size: int, replacement: str, exceptions: List[str], status_msg: Message):
    edited, skipped, failed, counter = 0, 0, 0, 0
    try:
        async for m in bot.get_chat_history(chat_id, limit=0):
            counter += 1
            original = m.text or m.caption
            if not original:
                skipped += 1
                continue
            new = caption_cleaner(original, exceptions, replacement)
            if new == original:
                skipped += 1
                continue
            try:
                if m.media and m.caption is not None:
                    await bot.edit_message_caption(chat_id=chat_id, message_id=m.message_id, caption=new)
                else:
                    await bot.edit_message_text(chat_id=chat_id, message_id=m.message_id, text=new)
                edited += 1
            except FloodWait as e:
                log.warning(f"एडिट करते समय FloodWait {e.x}")
                await status_msg.edit_text(f"⏳ FloodWait: {e.x} सेकंड के लिए सो रहा हूँ...")
                await asyncio.sleep(e.x + 2)
                try:
                    if m.media and m.caption is not None:
                        await bot.edit_message_caption(chat_id=chat_id, message_id=m.message_id, caption=new)
                    else:
                        await bot.edit_message_text(chat_id=chat_id, message_id=m.message_id, text=new)
                    edited += 1
                except Exception as retry_e:
                    log.error(f"एडिट पुनः प्रयास विफल: {retry_e}")
                    failed += 1
            except RPCError as e:
                log.error(f"RPCError एडिट करते समय: {e}")
                failed += 1
            except Exception as e:
                log.exception(f"एडिट करने में विफल: {e}")
                failed += 1
            if (edited + failed) % batch_size == 0:
                await status_msg.edit_text(f"⏳ सफ़ाई जारी है...\nस्कैन किए गए: {counter}\nएडिट किए गए: {edited}\nछोड़े गए: {skipped}\nविफल: {failed}")
                await asyncio.sleep(BATCH_SLEEP_SECONDS)
    except (UserNotParticipant, ChatAdminRequired):
        return {"error": "❌ त्रुटि: बॉट इस चैनल का सदस्य नहीं है या उसके पास संदेश एडिट करने की अनुमति नहीं है।"}
    except Exception as e:
        log.exception(f"सफ़ाई लूप में त्रुटि: {e}")
        return {"error": f"❌ एक अप्रत्याशित त्रुटि हुई: {e}"}
    return {"scanned": counter, "edited": edited, "skipped": skipped, "failed": failed}

@bot.on_message(filters.command("clean_channel") & filters.private & filters.owner)
async def cmd_clean_channel(client: Client, message: Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("उपयोग: /clean_channel <channel_id_or_username>")
    target = args[1].strip()
    try:
        tgt_id = await resolve_chat_id(bot, target)
    except Exception as e:
        return await message.reply_text(f"❌ चैट का समाधान करने में विफल: {e}")
    meta = await read_metadata()
    exceptions = await read_exceptions()
    batch = meta.get("clean_batch", DEFAULT_CLEAN_BATCH)
    replacement = meta.get("replacement_text", "")
    status_msg = await message.reply_text(f"⏳ `{tgt_id}` में कैप्शन की सफ़ाई शुरू हो रही है...")
    res = await clean_channel_captions(tgt_id, batch, replacement, exceptions, status_msg)
    if "error" in res:
        await status_msg.edit_text(res["error"])
    else:
        await status_msg.edit_text(f"✅ सफ़ाई समाप्त।\nस्कैन किए गए: {res['scanned']}\nएडिट किए गए: {res['edited']}\nछोड़े गए: {res['skipped']}\nविफल: {res['failed']}")

# (यहाँ cmd_set_replacement, add_exception, remove_exception, exceptions, status डालें)
@bot.on_message(filters.command("set_replacement") & filters.private & filters.owner)
async def cmd_set_replacement(client: Client, message: Message):
    meta = await read_metadata()
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        meta["replacement_text"] = ""
        await write_metadata(meta)
        return await message.reply_text("✅ प्रतिस्थापन टेक्स्ट खाली पर सेट किया गया।")
    meta["replacement_text"] = args[1]
    await write_metadata(meta)
    await message.reply_text(f"✅ प्रतिस्थापन टेक्स्ट अपडेट किया गया: `{args[1]}`")

@bot.on_message(filters.command("add_exception") & filters.private & filters.owner)
async def cmd_add_exception(client: Client, message: Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("उपयोग: /add_exception <text_to_ignore>")
    val = args[1].strip().lower()
    lst = await read_exceptions()
    if val in lst:
        return await message.reply_text(f"`{val}` पहले से ही अपवादों में है।")
    lst.append(val)
    await write_exceptions(lst)
    await message.reply_text(f"✅ अपवाद जोड़ा गया: `{val}`")

@bot.on_message(filters.command("remove_exception") & filters.private & filters.owner)
async def cmd_remove_exception(client: Client, message: Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("उपयोग: /remove_exception <text_to_remove>")
    val = args[1].strip().lower()
    lst = await read_exceptions()
    if val not in lst:
        return await message.reply_text(f"`{val}` अपवादों में नहीं मिला।")
    lst.remove(val)
    await write_exceptions(lst)
    await message.reply_text(f"✅ अपवाद हटाया गया: `{val}`")

@bot.on_message(filters.command("exceptions") & filters.private & filters.owner)
async def cmd_list_exceptions(client: Client, message: Message):
    lst = await read_exceptions()
    if not lst:
        return await message.reply_text("कोई अपवाद सेट नहीं है।")
    text = "सफ़ाई अपवाद:\n\n" + "\n".join([f"- `{item}`" for item in lst])
    await message.reply_text(text)

@bot.on_message(filters.command("status") & filters.private & filters.owner)
async def cmd_status(client: Client, message: Message):
    meta = await read_metadata()
    exceptions = await read_exceptions()
    managed = meta.get("managed_channel", "कोई नहीं")
    last_indexed_ts = meta.get("last_indexed")
    last_indexed = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_indexed_ts)) if last_indexed_ts else "कभी नहीं"
    forward_b = meta.get("forward_batch", DEFAULT_FORWARD_BATCH)
    clean_b = meta.get("clean_batch", DEFAULT_CLEAN_BATCH)
    rep = meta.get("replacement_text", "'' (खाली)")
    txt = (
        f"**ℹ️ स्थिति:**\n\n"
        f"**मालिक ID:** `{meta.get('owner_id')}`\n"
        f"**प्रबंधित चैनल:** `{managed}`\n"
        f"**अंतिम इंडेक्स:** `{last_indexed}`\n\n"
        f"**कॉन्फ़िगरेशन:**\n"
        f"फ़ॉरवर्ड बैच: `{forward_b}`\n"
        f"सफ़ाई बैच: `{clean_b}`\n"
        f"प्रतिस्थापन: `{rep}`\n"
        f"अपवाद आइटम: `{len(exceptions)}`"
    )
    await message.reply_text(txt)


# ===========================
# Render Web Service Support
# ===========================

async def health_check(request):
    """Render हेल्थ चेक के लिए सिंपल रिस्पांस।"""
    log.info("Render हेल्थ चेक पिंग प्राप्त हुआ।")
    return web.Response(text="Bot is running")

async def start_web_server():
    """Render के लिए वेब सर्वर शुरू करता है।"""
    app = web.Application()
    app.router.add_get("/", health_check) # '/' पर हेल्थ चेक
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    try:
        await site.start()
        log.info(f"Render के लिए हेल्थ चेक सर्वर पोर्ट {port} पर शुरू हो गया है।")
    except Exception as e:
        log.error(f"वेब सर्वर शुरू करने में विफल: {e}")

# ===========================
# Start the Bot (सुधार किया गया)
# ===========================

async def delete_webhook():
    """
    बॉट शुरू होने पर किसी भी मौजूदा Webhook को ज़बरदस्ती हटा देता है।
    यह सुनिश्चित करता है कि बॉट पोलिंग (polling) मोड में काम करे।
    """
    log.info("किसी भी मौजूदा Webhook को हटाने का प्रयास किया जा रहा है...")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
    try:
        async with ClientSession() as session:
            async with session.get(url) as response:
                result = await response.json()
                if result.get("ok"):
                    log.info("Webhook सफलतापूर्वक हटा दिया गया। पोलिंग मोड में शुरू हो रहा है।")
                else:
                    log.warning(f"Webhook हटाने में विफल: {result.get('description')}")
    except Exception as e:
        log.error(f"Webhook हटाने का अनुरोध विफल: {e}")


async def main():
    """बॉट और वेब सर्वर दोनों को एक साथ शुरू करता है।"""
    
    # 1. पहले, Webhook को हटाएँ
    await delete_webhook()
    
    # 2. फिर, बॉट क्लाइंट शुरू करें
    try:
        await bot.start()
        me = await bot.get_me()
        log.info(f"बॉट क्लाइंट @{me.username} के रूप में शुरू हो गया है।")
    except Exception as e:
        log.error(f"बॉट शुरू करने में विफल: {e}")
        return

    # 3. वेब सर्वर शुरू करें (Render के लिए)
    await start_web_server()
    
    # 4. बॉट को चलते रहने के लिए idle() का उपयोग करें
    log.info("बॉट अब चल रहा है। रोकने के लिए CTRL+C दबाएँ।")
    await idle()
    
    # 5. बंद होने पर
    await bot.stop()
    log.info("बॉट क्लाइंट बंद हो गया है।")


if __name__ == "__main__":
    asyncio.run(main())
