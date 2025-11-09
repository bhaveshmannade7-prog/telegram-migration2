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
from pyrogram.raw import functions

from dotenv import load_dotenv
import aiofiles
from aiohttp import web  # Render पर तैनाती के लिए

# .env फ़ाइल से एनवायरनमेंट वेरिएबल्स लोड करें
load_dotenv()

# ===========================
# Configuration
# ===========================

# बेसिक लॉगिंग
# लॉग लेवल को INFO पर सेट करें ताकि हमें पता चले कि क्या हो रहा है
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# एनवायरनमेंट वेरिएबल्स से कॉन्फ़िगरेशन
# ये Render के डैशबोर्ड में सेट किए जाएंगे
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
# OWNER_ID वैकल्पिक है, लेकिन अनुशंसित है। यदि सेट नहीं है, तो /start करने वाला पहला उपयोगकर्ता मालिक बन जाएगा।
OWNER_ID_ENV = os.getenv("OWNER_ID")

# फ़ाइल पाथ
# Render डिस्क के लिए इसे '/var/data' या किसी अन्य माउंट पथ पर सेट किया जा सकता है
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(exist_ok=True)
METADATA_FILE = DATA_DIR / "metadata.json"
EXCEPTIONS_FILE = DATA_DIR / "exceptions.json"

# डिफ़ॉल्ट बैच आकार (मेटाडेटा में बदला जा सकता है)
DEFAULT_FORWARD_BATCH = 100  # 100 का बैच ज़्यादा सुरक्षित है
DEFAULT_CLEAN_BATCH = 100
BATCH_SLEEP_SECONDS = 5      # बैचों के बीच ज़्यादा देर रुकें

# कैप्शन में लिंक और यूज़रनेम का पता लगाने के लिए Regex
URL_REGEX = re.compile(
    r"(https?:\/\/[^\s]+|www\.[^\s]+)", re.IGNORECASE
)
USERNAME_REGEX = re.compile(r"@([A-Za-z0-9_]{5,32})")

# यह सुनिश्चित करने के लिए जांचें कि ज़रूरी वेरिएबल्स सेट हैं
if not all([BOT_TOKEN, API_ID, API_HASH]):
    log.error("ज़रूरी एनवायरनमेंट वेरिएबल्स (BOT_TOKEN, API_ID, API_HASH) सेट नहीं हैं।")
    raise SystemExit("ज़रूरी एनवायरनमेंट वेरिएबल्स सेट नहीं हैं।")

# ===========================
# Async Data Persistence (स्थिरता के लिए महत्वपूर्ण)
# ===========================
# फ़ाइल I/O के लिए 'aiofiles' का उपयोग करना बॉट को ब्लॉक होने से रोकता है

async def read_metadata() -> Dict[str, Any]:
    """मेटाडेटा को JSON फ़ाइल से एसिंक्रोनस रूप से पढ़ता है।"""
    if not METADATA_FILE.exists():
        meta = {
            "owner_id": int(OWNER_ID_ENV) if OWNER_ID_ENV else None,
            "managed_channel": None,
            "last_indexed": None,
            "forward_batch": DEFAULT_FORWARD_BATCH,
            "clean_batch": DEFAULT_CLEAN_BATCH,
            "replacement_text": "",
            "admin_session": None,
        }
        await write_metadata(meta)
        return meta
    try:
        async with aiofiles.open(METADATA_FILE, "r", encoding="utf-8") as f:
            content = await f.read()
            return json.loads(content)
    except (json.JSONDecodeError, IOError) as e:
        log.error(f"मेटाडेटा पढ़ने में विफल: {e}. डिफ़ॉल्ट पर वापस जा रहा हूँ।")
        # यदि फ़ाइल ख़राब है, तो एक डिफ़ॉल्ट बनाएं
        return { "owner_id": int(OWNER_ID_ENV) if OWNER_ID_ENV else None }


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


async def save_channel_index(channel_id: int, messages: List[Dict[str, Any]]):
    """चैनल इंडेक्स को एसिंक्रोनस रूप से सहेजता है, डुप्लिकेट से बचता है।"""
    if not messages:
        return 0
    
    fn = DATA_DIR / f"channel_{str(channel_id)}.json"
    existing = []
    if fn.exists():
        try:
            async with aiofiles.open(fn, "r", encoding="utf-8") as f:
                content = await f.read()
                existing = json.loads(content)
        except (json.JSONDecodeError, IOError) as e:
            log.warning(f"चैनल इंडेक्स फ़ाइल {fn} पढ़ने में विफल: {e}")
            existing = []

    existing_ids = {m.get("message_id") for m in existing}
    new_items = [m for m in messages if m.get("message_id") not in existing_ids]

    if new_items:
        combined = existing + new_items
        try:
            async with aiofiles.open(fn, "w", encoding="utf-8") as f:
                # default=str दिनांक/समय ऑब्जेक्ट को हैंडल करने के लिए
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

# Pyrogram क्लाइंट शुरू करें
bot = Client(
    "channel_manager_bot",
    bot_token=BOT_TOKEN,
    api_id=int(API_ID),
    api_hash=API_HASH
)

# --- ओनर (Owner) फ़िल्टर ---
# यह कमांड को केवल बॉट के मालिक तक सीमित करने का एक साफ़ तरीका है
async def owner_filter(_, __, message: Message):
    meta = await read_metadata()
    owner_id = meta.get("owner_id")
    if owner_id:
        return message.from_user.id == owner_id
    # यदि OWNER_ID एनवायरनमेंट में सेट है, तो उसका उपयोग करें
    if OWNER_ID_ENV:
        return message.from_user.id == int(OWNER_ID_ENV)
    # यदि कोई मालिक सेट नहीं है, तो फ़िल्टर विफल हो जाता है (सिवाय /start के)
    return False

# फ़िल्टर को एक नाम दें
filters.owner = filters.create(owner_filter)
log.info("मालिक (owner) फ़िल्टर बनाया गया।")

# ===========================
# Helper Functions
# ===========================

async def resolve_chat_id(client: Client, chat_identifier: str):
    """
    चैट आइडेंटिफ़ायर (जैसे '@username' या '-100123..._') को एक इंट ID में बदलता है।
    """
    try:
        # यदि यह पहले से ही एक संख्यात्मक ID है
        if chat_identifier.lstrip('-').isdigit():
            return int(chat_identifier)
        chat = await client.get_chat(chat_identifier)
        return chat.id
    except Exception as e:
        log.error(f"चैट {chat_identifier} का समाधान करने में विफल: {e}")
        raise ValueError(f"चैट '{chat_identifier}' का समाधान नहीं किया जा सका। सुनिश्चित करें कि बॉट या एडमिन सदस्य है।")


async def with_admin_client(fn):
    """
    एडमिन सेशन (StringSession) से एक अस्थायी क्लाइंट बनाता है और चलाता है।
    यह सुनिश्चित करता है कि क्लाइंट ठीक से शुरू और बंद हो।
    """
    meta = await read_metadata()
    sess = meta.get("admin_session")
    if not sess or not all(k in sess for k in ("api_id", "api_hash", "session")):
        raise RuntimeError("एडमिन सेशन कॉन्फ़िगर नहीं है। /setsession का उपयोग करें।")
    
    admin_client = Client(
        name="admin_session_temp",
        api_id=int(sess["api_id"]),
        api_hash=sess["api_hash"],
        session_string=sess["session"],
    )
    
    try:
        await admin_client.start()
        log.info("अस्थायी एडमिन क्लाइंट शुरू किया गया।")
        # फ़ंक्शन को क्लाइंट और मेटा के साथ निष्पादित करें
        result = await fn(admin_client, meta)
    except Exception as e:
        log.error(f"एडमिन क्लाइंट ऑपरेशन विफल: {e}")
        raise  # त्रुटि को कॉलर तक फिर से बढ़ाएँ
    finally:
        await admin_client.stop()
        log.info("अस्थायी एडमिन क्लाइंट बंद किया गया।")
    return result


def msg_to_indexable(m: Message) -> Dict[str, Any]:
    """Pyrogram Message ऑब्जेक्ट को इंडेक्सिंग के लिए एक सरल डिक्शनरी में बदलता है।"""
    data = {
        "message_id": m.message_id,
        "date": int(m.date.timestamp()) if m.date else int(time.time()),
        "text": m.text or m.caption or "",
        "media_type": str(m.media).split(".")[1].lower() if m.media else None, # 'MediaType.PHOTO' -> 'photo'
        "has_media": bool(m.media),
        "from_user": (m.from_user.username if m.from_user else "Unknown"),
    }
    return data

# ===========================
# Caption Cleaning Logic
# ===========================

def caption_cleaner(text: str, exceptions: List[str], replacement: str) -> str:
    """
    टेक्स्ट से URLs और @usernames को हटाता है, सिवाय उनके जो अपवाद सूची में हैं।
    """
    if not text:
        return text

    # अपवादों को साफ़ करें (लोअरकेस और @ हटा दें)
    clean_exceptions = [ex.lower().strip('@') for ex in exceptions]

    # पहले URLs को बदलें
    def url_repl(match):
        s = match.group(0).lower()
        # जांचें कि क्या URL का कोई हिस्सा अपवाद में है
        for ex in clean_exceptions:
            if ex and ex in s: # 'ex' खाली नहीं होना चाहिए
                return match.group(0)  # इसे रखें
        return replacement

    text = URL_REGEX.sub(url_repl, text)

    # फिर यूज़रनेम को बदलें
    def user_repl(match):
        # group(0) है '@username', group(1) है 'username'
        username_part = match.group(1).lower() 
        if username_part in clean_exceptions:
            return match.group(0) # इसे रखें
        return replacement

    text = USERNAME_REGEX.sub(user_repl, text)
    return text.strip() # अतिरिक्त व्हाइटस्पेस हटा दें


# ===========================
# Bot Commands
# ===========================

@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, message: Message):
    """बॉट शुरू करता है और यदि कोई मालिक सेट नहीं है, तो पहले उपयोगकर्ता को मालिक के रूप में सेट करता है।"""
    meta = await read_metadata()
    owner_id = meta.get("owner_id")
    
    # यदि कोई मालिक (owner) सेट नहीं है, तो इस उपयोगकर्ता को मालिक बनाएं
    if owner_id is None:
        meta["owner_id"] = message.from_user.id
        await write_metadata(meta)
        log.info(f"नया मालिक सेट किया गया: {message.from_user.id}")
        await message.reply_text("बधाई हो, आप इस बॉट के मालिक बन गए हैं!")
    elif message.from_user.id != owner_id:
        return await message.reply_text("मैं चैनल मैनेजर बॉट हूँ। मेरे कमांड केवल मेरे मालिक द्वारा ही उपयोग किए जा सकते हैं।")

    text = (
        "चैनल मैनेजर बॉट तैयार है।\n\n"
        "कमांड (केवल एडमिन):\n"
        "/setsession <api_id>|<api_hash>|<string_session> - एडमिन सेशन सेट करें (इंडेक्सिंग के लिए)\n"
        "/index_channel <channel_id_or_username> - चैनल को इंडेक्स करें और इसे प्रबंधित (managed) के रूप में सेट करें\n"
        "/sync - प्रबंधित चैनल को फिर से स्कैन करें और JSON को अपडेट करें\n"
        "/forward <source> <target> - संदेशों को स्रोत से लक्ष्य तक बैचों में फॉरवर्ड करें\n"
        "/clean_channel <channel> - बैचों में कैप्शन साफ़ करें (लिंक/@यूज़रनेम हटाएं)\n"
        "/set_replacement [text] - हटाए गए आइटम के लिए प्रतिस्थापन टेक्स्ट सेट करें (खाली के लिए कोई टेक्स्ट न दें)\n"
        "/add_exception <text> - सफ़ाई करते समय एक लिंक/यूज़रनेम को न हटाने के लिए जोड़ें\n"
        "/remove_exception <text> - अपवाद (exception) हटाएं\n"
        "/exceptions - सभी अपवादों की सूची देखें\n"
        "/status - वर्तमान स्थिति दिखाएं"
    )
    await message.reply_text(text)


@bot.on_message(filters.command("setsession") & filters.private & filters.owner)
async def cmd_setsession(client: Client, message: Message):
    """एडमिन (यूज़रबॉट) का स्ट्रिंग सेशन सहेजता है।"""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("उपयोग: /setsession <api_id>|<api_hash>|<string_session>")
    
    payload = args[1].strip()
    try:
        api_id, api_hash, session = payload.split("|", 2)
        # मूल बातें जांचें
        if not api_id.isdigit() or not api_hash or not session:
            raise ValueError("अमान्य प्रारूप")
    except ValueError:
        return await message.reply_text("प्रारूप त्रुटि। उपयोग: api_id|api_hash|string_session")
    
    meta = await read_metadata()
    meta["admin_session"] = {"api_id": int(api_id), "api_hash": api_hash, "session": session}
    await write_metadata(meta)
    await message.reply_text("एडमिन सेशन सहेजा गया। अब आप /index_channel और /sync का उपयोग कर सकते हैं।")


@bot.on_message(filters.command("index_channel") & filters.private & filters.owner)
async def cmd_index_channel(client: Client, message: Message):
    """एडमिन सेशन का उपयोग करके चैनल के सभी संदेशों को इंडेक्स करता है।"""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("उपयोग: /index_channel <channel_id_or_username>")
    chat_ident = args[1].strip()

    status_msg = await message.reply_text(f"`{chat_ident}` को इंडेक्स किया जा रहा है... यह धीमा हो सकता है।")

    async def _do_index(admin_client: Client, meta_local):
        try:
            chat_id = await resolve_chat_id(admin_client, chat_ident)
        except Exception as e:
            return f"चैट का समाधान करने में विफल: {e}"
        
        channel_id = chat_id # यह सुनिश्चित करने के लिए कि यह संख्यात्मक ID है
        count = 0
        added_total = 0
        messages_to_save = []
        
        try:
            # BUG FIX: limit=0 का मतलब है सभी संदेश, 200 नहीं।
            async for m in admin_client.get_chat_history(channel_id, limit=0):
                messages_to_save.append(msg_to_indexable(m))
                count += 1
                
                # मेमोरी उपयोग को कम करने के लिए समय-समय पर चंक्स में लिखें
                if len(messages_to_save) >= 500:
                    added = await save_channel_index(channel_id, messages_to_save)
                    added_total += added
                    messages_to_save = []
                    await status_msg.edit_text(f"`{chat_ident}` को इंडेक्स किया जा रहा है...\n{count} संदेश स्कैन किए गए।\n{added_total} नए सहेजे गए।")
            
            if messages_to_save:
                added = await save_channel_index(channel_id, messages_to_save)
                added_total += added

            # प्रबंधित चैनल सेट करें
            meta_local["managed_channel"] = channel_id
            meta_local["last_indexed"] = int(time.time())
            await write_metadata(meta_local)
            
            return f"इंडेक्सिंग पूर्ण।\nकुल स्कैन किए गए संदेश: {count}\nकुल नए जोड़े गए: {added_total}\nचैनल को प्रबंधित के रूप में सेट किया गया: {channel_id}"

        except (UserNotParticipant, ChatAdminRequired):
            return "त्रुटि: एडमिन (यूज़रबॉट) इस चैनल का सदस्य नहीं है या उसके पास संदेश पढ़ने की अनुमति नहीं है।"
        except Exception as e:
            log.exception(f"इंडेक्सिंग में त्रुटि: {e}")
            return f"इंडेक्सिंग विफल: {type(e).__name__}: {e}"

    try:
        result = await with_admin_client(_do_index)
        await status_msg.edit_text(str(result))
    except Exception as e:
        log.error(f"इंडेक्स विफल: {e}")
        await status_msg.edit_text(f"इंडेक्स विफल: {e}")


@bot.on_message(filters.command("sync") & filters.private & filters.owner)
async def cmd_sync(client: Client, message: Message):
    """प्रबंधित चैनल को सिंक करता है (केवल नए संदेशों को इंडेक्स करता है)।"""
    meta = await read_metadata()
    managed = meta.get("managed_channel")
    if not managed:
        return await message.reply_text("कोई प्रबंधित चैनल सेट नहीं है। पहले /index_channel का उपयोग करें।")

    status_msg = await message.reply_text(f"प्रबंधित चैनल `{managed}` को सिंक किया जा रहा है...")

    # नोट: यह बग-फिक्स नहीं है, बल्कि आपके मूल तर्क का कार्यान्वयन है।
    # save_channel_index डुप्लिकेट को संभालता है, इसलिए पूरा इतिहास फिर से स्कैन करना
    # केवल नए संदेशों को जोड़ेगा।
    
    async def _do_sync(admin_client: Client, meta_local):
        try:
            chat = await admin_client.get_chat(managed)
        except Exception as e:
            return f"प्रबंधित चैट का समाधान करने में विफल: {e}"
        
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
                    await status_msg.edit_text(f"सिंक हो रहा है...\n{count} संदेश स्कैन किए गए।\n{added_total} नए सहेजे गए।")
            
            if messages_to_save:
                added = await save_channel_index(chat.id, messages_to_save)
                added_total += added
            
            meta_local["last_indexed"] = int(time.time())
            await write_metadata(meta_local)
            return f"सिंक पूर्ण।\nकुल स्कैन किए गए संदेश: {count}\nकुल नए जोड़े गए: {added_total}।"
        except (UserNotParticipant, ChatAdminRequired):
            return "त्रुटि: एडमिन (यूज़रबॉट) इस चैनल का सदस्य नहीं है या उसके पास संदेश पढ़ने की अनुमति नहीं है।"
        except Exception as e:
            log.exception(f"सिंक में त्रुटि: {e}")
            return f"सिंक विफल: {type(e).__name__}: {e}"

    try:
        result = await with_admin_client(_do_sync)
        await status_msg.edit_text(str(result))
    except Exception as e:
        await status_msg.edit_text(f"सिंक विफल: {e}")


async def forward_messages_in_batches(src_id: int, tgt_id: int, batch_size: int, status_msg: Message):
    """
    संदेशों को बैचों में फॉरवर्ड करता है। (BUG FIXED: अब सही में बैच का उपयोग करता है)
    नोट: get_chat_history सबसे नए से सबसे पुराने तक जाता है।
    """
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
                    await bot.forward_messages(
                        chat_id=tgt_id,
                        from_chat_id=src_id,
                        message_ids=message_ids_batch
                    )
                    success += len(message_ids_batch)
                except FloodWait as e:
                    log.warning(f"फॉरवर्ड करते समय FloodWait {e.x} सेकंड - सो रहा हूँ")
                    await status_msg.edit_text(f"FloodWait: {e.x} सेकंड के लिए सो रहा हूँ...")
                    await asyncio.sleep(e.x + 2)
                    try:
                        # एक बार फिर प्रयास करें
                        await bot.forward_messages(
                            chat_id=tgt_id,
                            from_chat_id=src_id,
                            message_ids=message_ids_batch
                        )
                        success += len(message_ids_batch)
                    except Exception as retry_e:
                        log.error(f"बैच {message_ids_batch} को फॉरवर्ड करने में पुनः प्रयास विफल: {retry_e}")
                        failed += len(message_ids_batch)
                except Exception as e:
                    log.exception(f"बैच {message_ids_batch} को फॉरवर्ड करने में विफल: {e}")
                    failed += len(message_ids_batch)
                
                # बैच को साफ़ करें और सोएं
                message_ids_batch = []
                await status_msg.edit_text(f"फॉरवर्ड हो रहा है...\nसफलता: {success}, विफल: {failed}, कुल स्कैन: {total_scanned}")
                await asyncio.sleep(BATCH_SLEEP_SECONDS)

        # किसी भी शेष संदेश को फॉरवर्ड करें
        if message_ids_batch:
            try:
                await bot.forward_messages(
                    chat_id=tgt_id,
                    from_chat_id=src_id,
                    message_ids=message_ids_batch
                )
                success += len(message_ids_batch)
            except Exception as e:
                log.exception(f"अंतिम बैच {message_ids_batch} को फॉरवर्ड करने में विफल: {e}")
                failed += len(message_ids_batch)

    except (UserNotParticipant, ChatAdminRequired):
        return {"error": "त्रुटि: बॉट स्रोत चैनल का सदस्य नहीं है या उसके पास संदेश पढ़ने की अनुमति नहीं है।"}
    except Exception as e:
        log.exception(f"फॉरवर्ड लूप में त्रुटि: {e}")
        return {"error": f"एक अप्रत्याशित त्रुटि हुई: {e}"}

    return {"success": success, "failed": failed, "total": total_scanned}


@bot.on_message(filters.command("forward") & filters.private & filters.owner)
async def cmd_forward(client: Client, message: Message):
    """एक चैनल से दूसरे चैनल में संदेश फॉरवर्ड करता है।"""
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.reply_text("उपयोग: /forward <source_channel> <target_channel>")
    
    src = args[1].strip()
    tgt = args[2].strip()
    
    try:
        # बॉट क्लाइंट का उपयोग करके दोनों को हल करें
        src_id = await resolve_chat_id(bot, src)
        tgt_id = await resolve_chat_id(bot, tgt)
    except Exception as e:
        return await message.reply_text(f"चैट का समाधान करने में विफल: {e}")
    
    meta = await read_metadata()
    batch = meta.get("forward_batch", DEFAULT_FORWARD_BATCH)
    
    status_msg = await message.reply_text(f"{src_id} -> {tgt_id} से फॉरवर्ड करना शुरू कर रहा हूँ (बैच: {batch})।\n(नोट: संदेश सबसे नए से सबसे पुराने क्रम में फॉरवर्ड किए जाएंगे)")
    
    # इस फ़ंक्शन को 'await' करना कमांड को तब तक ब्लॉक कर देगा जब तक यह पूरा नहीं हो जाता।
    # यह लंबे कार्यों के लिए ठीक है जब तक कि उपयोगकर्ता जानता है।
    res = await forward_messages_in_batches(src_id, tgt_id, batch, status_msg)
    
    if "error" in res:
        await status_msg.edit_text(res["error"])
    else:
        await status_msg.edit_text(f"फॉरवर्ड समाप्त।\nसफलता: {res['success']}\nविफल: {res['failed']}\nकुल स्कैन किए गए: {res['total']}")


async def clean_channel_captions(chat_id: int, batch_size: int, replacement: str, exceptions: List[str], status_msg: Message):
    """
    चैट_id में संदेशों के माध्यम से इटरेट करता है और कैप्शन/टेक्स्ट को साफ़ करता है।
    """
    edited = 0
    skipped = 0
    failed = 0
    counter = 0

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
                await status_msg.edit_text(f"FloodWait: {e.x} सेकंड के लिए सो रहा हूँ...")
                await asyncio.sleep(e.x + 2)
                try:
                    # पुनः प्रयास
                    if m.media and m.caption is not None:
                        await bot.edit_message_caption(chat_id=chat_id, message_id=m.message_id, caption=new)
                    else:
                        await bot.edit_message_text(chat_id=chat_id, message_id=m.message_id, text=new)
                    edited += 1
                except Exception as retry_e:
                    log.error(f"संदेश {m.message_id} को एडिट करने में पुनः प्रयास विफल: {retry_e}")
                    failed += 1
            except RPCError as e:
                log.error(f"RPCError संदेश {m.message_id} को एडिट करते समय: {e}")
                failed += 1
            except Exception as e:
                log.exception(f"संदेश {m.message_id} को एडिट करने में विफल: {e}")
                failed += 1
            
            # बैच नियंत्रण
            if (edited + failed) % batch_size == 0:
                await status_msg.edit_text(f"सफ़ाई जारी है...\nस्कैन किए गए: {counter}\nएडिट किए गए: {edited}\nछोड़े गए: {skipped}\nविफल: {failed}")
                await asyncio.sleep(BATCH_SLEEP_SECONDS)

    except (UserNotParticipant, ChatAdminRequired):
        return {"error": "त्रुटि: बॉट इस चैनल का सदस्य नहीं है या उसके पास संदेश एडिट करने की अनुमति नहीं है।"}
    except Exception as e:
        log.exception(f"सफ़ाई लूप में त्रुटि: {e}")
        return {"error": f"एक अप्रत्याशित त्रुटि हुई: {e}"}

    return {"scanned": counter, "edited": edited, "skipped": skipped, "failed": failed}


@bot.on_message(filters.command("clean_channel") & filters.private & filters.owner)
async def cmd_clean_channel(client: Client, message: Message):
    """चैनल में सभी संदेशों के कैप्शन/टेक्स्ट को साफ़ करता है।"""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("उपयोग: /clean_channel <channel_id_or_username>")
    
    target = args[1].strip()
    try:
        tgt_id = await resolve_chat_id(bot, target)
    except Exception as e:
        return await message.reply_text(f"चैट का समाधान करने में विफल: {e}")
    
    meta = await read_metadata()
    exceptions = await read_exceptions()
    batch = meta.get("clean_batch", DEFAULT_CLEAN_BATCH)
    replacement = meta.get("replacement_text", "")
    
    status_msg = await message.reply_text(
        f"`{tgt_id}` में कैप्शन की सफ़ाई शुरू हो रही है...\n"
        f"बैच: {batch}\n"
        f"प्रतिस्थापन: '{replacement}'\n"
        f"अपवाद: {len(exceptions)} आइटम"
    )
    
    res = await clean_channel_captions(tgt_id, batch, replacement, exceptions, status_msg)
    
    if "error" in res:
        await status_msg.edit_text(res["error"])
    else:
        await status_msg.edit_text(
            f"सफ़ाई समाप्त।\nस्कैन किए गए: {res['scanned']}\nएडिट किए गए: {res['edited']}\n"
            f"छोड़े गए (कोई बदलाव नहीं): {res['skipped']}\nविफल: {res['failed']}"
        )


@bot.on_message(filters.command("set_replacement") & filters.private & filters.owner)
async def cmd_set_replacement(client: Client, message: Message):
    """हटाए गए लिंक/यूज़रनेम के लिए प्रतिस्थापन टेक्स्ट सेट करता है।"""
    meta = await read_metadata()
    args = message.text.split(maxsplit=1)
    
    if len(args) < 2:
        # यदि कोई टेक्स्ट नहीं दिया गया है, तो इसे खाली स्ट्रिंग पर सेट करें
        meta["replacement_text"] = ""
        await write_metadata(meta)
        return await message.reply_text("प्रतिस्थापन टेक्स्ट खाली पर सेट किया गया।")
        
    meta["replacement_text"] = args[1]
    await write_metadata(meta)
    await message.reply_text(f"प्रतिस्थापन टेक्स्ट अपडेट किया गया: `{args[1]}`")


@bot.on_message(filters.command("add_exception") & filters.private & filters.owner)
async def cmd_add_exception(client: Client, message: Message):
    """सफ़ाई अपवाद सूची में एक आइटम (यूज़रनेम या लिंक का हिस्सा) जोड़ता है।"""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("उपयोग: /add_exception <text_to_ignore>")
    
    val = args[1].strip().lower() # स्थिरता के लिए लोअरकेस में सहेजें
    lst = await read_exceptions()
    
    if val in lst:
        return await message.reply_text(f"`{val}` पहले से ही अपवादों में है।")
    
    lst.append(val)
    await write_exceptions(lst)
    await message.reply_text(f"अपवाद जोड़ा गया: `{val}`")


@bot.on_message(filters.command("remove_exception") & filters.private & filters.owner)
async def cmd_remove_exception(client: Client, message: Message):
    """सफ़ाई अपवाद सूची से एक आइटम हटाता है।"""
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("उपयोग: /remove_exception <text_to_remove>")
    
    val = args[1].strip().lower()
    lst = await read_exceptions()
    
    if val not in lst:
        return await message.reply_text(f"`{val}` अपवादों में नहीं मिला।")
    
    lst.remove(val)
    await write_exceptions(lst)
    await message.reply_text(f"अपवाद हटाया गया: `{val}`")

@bot.on_message(filters.command("exceptions") & filters.private & filters.owner)
async def cmd_list_exceptions(client: Client, message: Message):
    """सभी सहेजे गए अपवादों की सूची बनाता है।"""
    lst = await read_exceptions()
    if not lst:
        return await message.reply_text("कोई अपवाद सेट नहीं है।")
    
    text = "सफ़ाई अपवाद (लिंक/यूज़रनेम जो नहीं हटाए जाएंगे):\n"
    text += "\n".join([f"- `{item}`" for item in lst])
    await message.reply_text(text)


@bot.on_message(filters.command("status") & filters.private & filters.owner)
async def cmd_status(client: Client, message: Message):
    """बॉट की वर्तमान कॉन्फ़िगरेशन स्थिति दिखाता है।"""
    meta = await read_metadata()
    exceptions = await read_exceptions()
    
    managed = meta.get("managed_channel", "कोई नहीं")
    last_indexed_ts = meta.get("last_indexed")
    last_indexed = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_indexed_ts)) if last_indexed_ts else "कभी नहीं"
    
    forward_b = meta.get("forward_batch", DEFAULT_FORWARD_BATCH)
    clean_b = meta.get("clean_batch", DEFAULT_CLEAN_BATCH)
    rep = meta.get("replacement_text", "'' (खाली)")
    
    txt = (
        f"**स्थिति:**\n"
        f"मालिक ID: `{meta.get('owner_id')}`\n"
        f"प्रबंधित चैनल: `{managed}`\n"
        f"अंतिम इंडेक्स: `{last_indexed}`\n"
        f"फ़ॉरवर्ड बैच आकार: `{forward_b}`\n"
        f"सफ़ाई बैच आकार: `{clean_b}`\n"
        f"प्रतिस्थापन टेक्स्ट: `{rep}`\n"
        f"अपवाद आइटम: `{len(exceptions)}`"
    )
    await message.reply_text(txt)


# ===========================
# Render Web Service Support
# ===========================
# Render.com वेब सेवाओं को $PORT पर एक वेब सर्वर की उम्मीद होती है।
# हम Render को यह दिखाने के लिए कि बॉट "स्वस्थ" है, एक न्यूनतम aiohttp सर्वर चलाएंगे।

async def health_check(request):
    """Render हेल्थ चेक के लिए एक सिंपल रिस्पांस।"""
    return web.Response(text="Bot is running")

async def start_web_server():
    """Render के लिए वेब सर्वर शुरू करता है।"""
    app = web.Application()
    app.router.add_get("/", health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Render $PORT एनवायरनमेंट वेरिएबल प्रदान करता है
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    try:
        await site.start()
        log.info(f"Render के लिए हेल्थ चेक सर्वर पोर्ट {port} पर शुरू हो गया है।")
    except Exception as e:
        log.error(f"वेब सर्वर शुरू करने में विफल: {e}")

# ===========================
# Start the Bot
# ===========================

async def main():
    """बॉट और वेब सर्वर दोनों को एक साथ शुरू करता है।"""
    try:
        await bot.start()
        log.info("बॉट क्लाइंट शुरू हो गया है।")
        
        # वेब सर्वर शुरू करें
        await start_web_server()
        
        # बॉट को चलते रहने के लिए idle() का उपयोग करें
        log.info("बॉट अब चल रहा है। रोकने के लिए CTRL+C दबाएँ।")
        await idle()
        
    except KeyboardInterrupt:
        log.info("बॉट बंद किया जा रहा है...")
    except Exception as e:
        log.exception(f"मुख्य लूप में एक घातक त्रुटि हुई: {e}")
    finally:
        await bot.stop()
        log.info("बॉट क्लाइंट बंद हो गया है।")

if __name__ == "__main__":
    # यह बॉट को एसिंक्रोनस रूप से चलाता है
    asyncio.run(main())
