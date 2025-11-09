import os
import re
import json
import time
import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Any

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError

from dotenv import load_dotenv
import aiofiles

load_dotenv()

# Basic logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Config via env
BOT_TOKEN = os.getenv("BOT_TOKEN")  # required
API_ID = os.getenv("API_ID")  # optional: for bot-level client if needed
API_HASH = os.getenv("API_HASH")

# file paths
DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)
METADATA_FILE = DATA_DIR / "metadata.json"
EXCEPTIONS_FILE = DATA_DIR / "exceptions.json"

# default batch sizes (can be adjusted in metadata)
DEFAULT_FORWARD_BATCH = 200
DEFAULT_CLEAN_BATCH = 150
BATCH_SLEEP_SECONDS = 2  # small pause between batches to avoid bursts

# regex to detect links and usernames in captions
URL_REGEX = re.compile(
    r"(https?:\/\/[^\s]+|www\.[^\s]+)", re.IGNORECASE
)
USERNAME_REGEX = re.compile(r"@([A-Za-z0-9_]{5,32})")  # telegram username pattern

# helper functions for data persistence


def read_metadata() -> Dict[str, Any]:
    if METADATA_FILE.exists():
        return json.loads(METADATA_FILE.read_text(encoding="utf-8"))
    meta = {
        "managed_channel": None,
        "last_indexed": None,
        "forward_batch": DEFAULT_FORWARD_BATCH,
        "clean_batch": DEFAULT_CLEAN_BATCH,
        "replacement_text": "",
        "admin_session": None,  # { "api_id":..., "api_hash":..., "session":... }
    }
    METADATA_FILE.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return meta


def write_metadata(meta: Dict[str, Any]):
    METADATA_FILE.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def read_exceptions() -> List[str]:
    if EXCEPTIONS_FILE.exists():
        return json.loads(EXCEPTIONS_FILE.read_text(encoding="utf-8"))
    EXCEPTIONS_FILE.write_text(json.dumps([], indent=2), encoding="utf-8")
    return []


def write_exceptions(lst: List[str]):
    EXCEPTIONS_FILE.write_text(json.dumps(lst, indent=2), encoding="utf-8")


# utility to normalize channel identifier (username or id)
async def resolve_chat_id(client: Client, chat_identifier: str):
    """
    Accepts '-10012345' or 'username' or numeric id. Returns int chat_id or raises.
    """
    try:
        chat = await client.get_chat(chat_identifier)
        return chat.id
    except Exception as e:
        raise e


# create bot client
if not BOT_TOKEN:
    log.error("BOT_TOKEN env var missing. Set BOT_TOKEN.")
    raise SystemExit("BOT_TOKEN required")

bot = Client("channel_manager_bot", bot_token=BOT_TOKEN, api_id=int(API_ID) if API_ID else None, api_hash=API_HASH)


# ===========================
# Indexing using admin session (StringSession)
# ===========================
# We'll create a helper that builds a temporary pyrogram client using saved admin session info (stored in metadata)
async def with_admin_client(fn):
    """Decorator-like helper to run `fn(admin_client, meta)` where admin_client is a Pyrogram Client
    built from metadata['admin_session']."""
    meta = read_metadata()
    sess = meta.get("admin_session")
    if not sess or not all(k in sess for k in ("api_id", "api_hash", "session")):
        raise RuntimeError("Admin session not configured. Use /setsession api_id|api_hash|string_session")
    admin_client = Client(
        name="admin_session_temp",
        api_id=int(sess["api_id"]),
        api_hash=sess["api_hash"],
        session_string=sess["session"],
    )
    await admin_client.__aenter__()  # start
    try:
        result = await fn(admin_client, meta)
    finally:
        await admin_client.__aexit__(None, None, None)
    return result


# save a channel index into JSON
async def save_channel_index(channel_id: int, messages: List[Dict[str, Any]]):
    fn = DATA_DIR / f"channel_{str(channel_id)}.json"
    # Load existing messages to prevent duplicates by message_id
    existing = []
    if fn.exists():
        existing = json.loads(fn.read_text(encoding="utf-8"))
    existing_ids = {m.get("message_id") for m in existing}
    new_items = [m for m in messages if m.get("message_id") not in existing_ids]
    if new_items:
        combined = existing + new_items
        fn.write_text(json.dumps(combined, indent=2, default=str), encoding="utf-8")
    else:
        # no new items
        pass
    return len(new_items)


# extract metadata from Message to store JSON-friendly info
def msg_to_indexable(m: Message) -> Dict[str, Any]:
    data = {
        "message_id": m.message_id,
        "date": int(m.date.timestamp()),
        "text": m.text or m.caption or "",
        "media_type": None,
        "has_media": bool(m.media),
        "from_user": (m.from_user.username if m.from_user else None),
    }
    if m.photo:
        data["media_type"] = "photo"
    elif m.video:
        data["media_type"] = "video"
    elif m.document:
        data["media_type"] = "document"
    elif m.audio:
        data["media_type"] = "audio"
    elif m.sticker:
        data["media_type"] = "sticker"
    return data


# ===========================
# Caption cleaning logic
# ===========================
def caption_cleaner(text: str, exceptions: List[str], replacement: str) -> str:
    """
    Remove URLs and @usernames from text except those matching exceptions list.
    exceptions: list of substrings - if any exception substring exists in the detected token, skip removal.
    replacement: text to put instead of removed items (can be empty).
    """
    if not text:
        return text

    # first find URLs
    def url_repl(match):
        s = match.group(0)
        for ex in exceptions:
            if ex.lower() in s.lower():
                return s  # keep it
        return replacement

    text = URL_REGEX.sub(url_repl, text)

    # then usernames
    def user_repl(match):
        matched = match.group(0)
        for ex in exceptions:
            if ex.lower().strip("@") == matched.lower().strip("@"):
                return matched
        return replacement

    text = USERNAME_REGEX.sub(user_repl, text)
    return text


# ===========================
# Bot commands
# ===========================

def is_admin_user(user_id: int) -> bool:
    # For simplicity: we consider bot owner = the user who started the bot first or we can allow a list
    # For now read metadata admin_id if present
    meta = read_metadata()
    owner = meta.get("owner_id")
    if owner:
        return int(owner) == int(user_id)
    # fallback: allow the developer to set a single owner via env var OWNER_ID
    env_owner = os.getenv("OWNER_ID")
    if env_owner:
        return int(env_owner) == int(user_id)
    # If neither present, allow whoever runs /start first to become owner (we will set on /start)
    return False


@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, message: Message):
    meta = read_metadata()
    if meta.get("owner_id") is None:
        meta["owner_id"] = message.from_user.id
        write_metadata(meta)
    text = (
        "Channel Manager Bot ready.\n\n"
        "Commands (admin only):\n"
        "/setsession <api_id>|<api_hash>|<string_session>  - setup admin session (used only for indexing)\n"
        "/index_channel <channel_id_or_username> - index channel into JSON and set it as managed\n"
        "/sync - rescan managed channel and update JSON (no duplicates)\n"
        "/forward <source> <target> - forward messages from source to target in batches\n"
        "/clean_recent <channel> - clean captions (remove links/@usernames) in batches\n"
        "/set_replacement <text> - set replacement text for removed items\n"
        "/add_exception <text> and /remove_exception <text>\n"
        "/status - show status\n\n"
        "Note: session string is stored locally. Use carefully."
    )
    await message.reply_text(text)


@bot.on_message(filters.command("setsession") & filters.private)
async def cmd_setsession(client: Client, message: Message):
    # only owner
    meta = read_metadata()
    owner = meta.get("owner_id")
    if owner and message.from_user.id != owner:
        return await message.reply_text("Only bot owner can set the admin session.")
    # expect payload: api_id|api_hash|string_session
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("Usage: /setsession <api_id>|<api_hash>|<string_session>")
    payload = args[1].strip()
    try:
        api_id, api_hash, session = payload.split("|", 2)
    except ValueError:
        return await message.reply_text("Format error. Use: api_id|api_hash|string_session")
    meta["admin_session"] = {"api_id": int(api_id), "api_hash": api_hash, "session": session}
    write_metadata(meta)
    await message.reply_text("Admin session saved. You can now use /index_channel and /sync.")


@bot.on_message(filters.command("index_channel") & filters.private)
async def cmd_index_channel(client: Client, message: Message):
    meta = read_metadata()
    owner = meta.get("owner_id")
    if owner and message.from_user.id != owner:
        return await message.reply_text("Only bot owner can index channels.")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("Usage: /index_channel <channel_id_or_username>")
    chat_ident = args[1].strip()

    async def _do_index(admin_client: Client, meta_local):
        try:
            chat = await admin_client.get_chat(chat_ident)
        except Exception as e:
            return f"Failed to resolve chat: {e}"
        channel_id = chat.id
        # iterate history and index
        count = 0
        page = admin_client.get_history(chat.id, limit=200)  # generator
        messages_to_save = []
        async for m in page:
            messages_to_save.append(msg_to_indexable(m))
            count += 1
            # periodically write in chunks to reduce memory usage
            if len(messages_to_save) >= 500:
                added = await save_channel_index(channel_id, messages_to_save)
                messages_to_save = []
        if messages_to_save:
            added = await save_channel_index(channel_id, messages_to_save)
        # set managed channel
        meta_local["managed_channel"] = channel_id
        meta_local["last_indexed"] = int(time.time())
        write_metadata(meta_local)
        return f"Indexing complete. Messages scanned: {count}. Channel set as managed: {channel_id}"

    try:
        result = await with_admin_client(_do_index)
        await message.reply_text(str(result))
    except Exception as e:
        await message.reply_text(f"Index failed: {e}")


@bot.on_message(filters.command("sync") & filters.private)
async def cmd_sync(client: Client, message: Message):
    meta = read_metadata()
    owner = meta.get("owner_id")
    if owner and message.from_user.id != owner:
        return await message.reply_text("Only bot owner can run sync.")
    managed = meta.get("managed_channel")
    if not managed:
        return await message.reply_text("No managed channel set. Use /index_channel first.")
    # reuse indexing but only new messages
    async def _do_sync(admin_client: Client, meta_local):
        try:
            chat = await admin_client.get_chat(managed)
        except Exception as e:
            return f"Failed to resolve managed chat: {e}"
        count = 0
        messages_to_save = []
        async for m in admin_client.get_history(chat.id, limit=0):  # 0 means all in pyrogram generator usage
            messages_to_save.append(msg_to_indexable(m))
            count += 1
            if len(messages_to_save) >= 500:
                await save_channel_index(chat.id, messages_to_save)
                messages_to_save = []
        if messages_to_save:
            await save_channel_index(chat.id, messages_to_save)
        meta_local["last_indexed"] = int(time.time())
        write_metadata(meta_local)
        return f"Sync complete. Messages scanned: {count}."
    try:
        result = await with_admin_client(_do_sync)
        await message.reply_text(str(result))
    except Exception as e:
        await message.reply_text(f"Sync failed: {e}")


# helper to forward in batches
async def forward_messages_in_batches(src_id: int, tgt_id: int, batch_size: int, invoking_user_id: int):
    # This function runs asynchronously and returns a summary
    success = 0
    failed = 0
    meta = read_metadata()
    exceptions = read_exceptions()
    # Use the bot client for forwarding (bot must be admin in target and have access to source)
    async for msg in bot.get_chat_history(src_id, limit=0):
        try:
            await bot.forward_messages(chat_id=tgt_id, from_chat_id=src_id, message_ids=msg.message_id)
            success += 1
        except FloodWait as e:
            log.warning(f"FloodWait {e.x} seconds - sleeping")
            await asyncio.sleep(e.x + 1)
            # retry once
            try:
                await bot.forward_messages(chat_id=tgt_id, from_chat_id=src_id, message_ids=msg.message_id)
                success += 1
            except Exception:
                failed += 1
        except Exception as e:
            log.exception("Forward error")
            failed += 1
        # batch control
        if success % batch_size == 0:
            await asyncio.sleep(BATCH_SLEEP_SECONDS)
    return {"success": success, "failed": failed}


@bot.on_message(filters.command("forward") & filters.private)
async def cmd_forward(client: Client, message: Message):
    meta = read_metadata()
    owner = meta.get("owner_id")
    if owner and message.from_user.id != owner:
        return await message.reply_text("Only bot owner can forward via this command.")
    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        return await message.reply_text("Usage: /forward <source_channel> <target_channel>")
    src = args[1].strip()
    tgt = args[2].strip()
    # resolve both via bot client
    try:
        src_id = await resolve_chat_id(bot, src)
        tgt_id = await resolve_chat_id(bot, tgt)
    except Exception as e:
        return await message.reply_text(f"Failed to resolve chat: {e}")
    batch = meta.get("forward_batch", DEFAULT_FORWARD_BATCH)
    await message.reply_text(f"Starting forward from {src_id} -> {tgt_id} in batches of {batch}. This may take time.")
    # run forwarding in background task but still within same response - we will await it (per system rule no background later)
    res = await forward_messages_in_batches(src_id, tgt_id, batch, message.from_user.id)
    await message.reply_text(f"Forward finished. Success: {res['success']}, Failed: {res['failed']}")


# cleaning captions in batches
async def clean_channel_captions(chat_id: int, batch_size: int, replacement: str, exceptions: List[str], invoking_user_id: int):
    """
    Iterate through messages in chat_id and edit captions to remove links/usernames.
    Returns summary dict.
    """
    success = 0
    edited = 0
    skipped = 0
    failed = 0
    counter = 0
    async for m in bot.get_chat_history(chat_id, limit=0):
        counter += 1
        # skip if no caption/text
        original = m.caption if m.caption is not None else m.text
        if not original:
            skipped += 1
            continue
        new = caption_cleaner(original, exceptions, replacement)
        if new == original:
            skipped += 1
            continue
        try:
            # When message owned by channel, editing requires bot to be admin in that channel and to have permission
            # Use edit_message_caption if media has caption else edit_message_text
            if m.media and m.caption is not None:
                await bot.edit_message_caption(chat_id=chat_id, message_id=m.message_id, caption=new)
            else:
                await bot.edit_message_text(chat_id=chat_id, message_id=m.message_id, text=new)
            edited += 1
        except FloodWait as e:
            log.warning(f"FloodWait during edit {e.x}")
            await asyncio.sleep(e.x + 1)
            try:
                if m.media and m.caption is not None:
                    await bot.edit_message_caption(chat_id=chat_id, message_id=m.message_id, caption=new)
                else:
                    await bot.edit_message_text(chat_id=chat_id, message_id=m.message_id, text=new)
                edited += 1
            except Exception:
                failed += 1
        except Exception as e:
            log.exception("Edit error")
            failed += 1
        if edited % batch_size == 0:
            await asyncio.sleep(BATCH_SLEEP_SECONDS)
    return {"scanned": counter, "edited": edited, "skipped": skipped, "failed": failed}


@bot.on_message(filters.command("clean_recent") & filters.private)
async def cmd_clean_recent(client: Client, message: Message):
    meta = read_metadata()
    owner = meta.get("owner_id")
    if owner and message.from_user.id != owner:
        return await message.reply_text("Only bot owner can run clean commands.")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("Usage: /clean_recent <channel_id_or_username>")
    target = args[1].strip()
    try:
        tgt_id = await resolve_chat_id(bot, target)
    except Exception as e:
        return await message.reply_text(f"Resolve failed: {e}")
    batch = meta.get("clean_batch", DEFAULT_CLEAN_BATCH)
    exceptions = read_exceptions()
    replacement = meta.get("replacement_text", "")
    await message.reply_text(f"Cleaning captions in {tgt_id} with batch {batch}. Exceptions: {exceptions}")
    res = await clean_channel_captions(tgt_id, batch, replacement, exceptions, message.from_user.id)
    await message.reply_text(f"Cleaning finished. Scanned: {res['scanned']}, Edited: {res['edited']}, Skipped: {res['skipped']}, Failed: {res['failed']}")


@bot.on_message(filters.command("set_replacement") & filters.private)
async def cmd_set_replacement(client: Client, message: Message):
    meta = read_metadata()
    owner = meta.get("owner_id")
    if owner and message.from_user.id != owner:
        return await message.reply_text("Only bot owner can set replacement text.")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("Usage: /set_replacement <text>")
    meta["replacement_text"] = args[1]
    write_metadata(meta)
    await message.reply_text("Replacement text updated.")


@bot.on_message(filters.command("add_exception") & filters.private)
async def cmd_add_exception(client: Client, message: Message):
    meta = read_metadata()
    owner = meta.get("owner_id")
    if owner and message.from_user.id != owner:
        return await message.reply_text("Only bot owner can manage exceptions.")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("Usage: /add_exception <text>")
    val = args[1].strip()
    lst = read_exceptions()
    if val in lst:
        return await message.reply_text("Already in exceptions.")
    lst.append(val)
    write_exceptions(lst)
    await message.reply_text(f"Added exception: {val}")


@bot.on_message(filters.command("remove_exception") & filters.private)
async def cmd_remove_exception(client: Client, message: Message):
    meta = read_metadata()
    owner = meta.get("owner_id")
    if owner and message.from_user.id != owner:
        return await message.reply_text("Only bot owner can manage exceptions.")
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await message.reply_text("Usage: /remove_exception <text>")
    val = args[1].strip()
    lst = read_exceptions()
    if val not in lst:
        return await message.reply_text("Not found in exceptions.")
    lst.remove(val)
    write_exceptions(lst)
    await message.reply_text(f"Removed exception: {val}")


@bot.on_message(filters.command("status") & filters.private)
async def cmd_status(client: Client, message: Message):
    meta = read_metadata()
    exceptions = read_exceptions()
    managed = meta.get("managed_channel")
    last_indexed = meta.get("last_indexed")
    forward_b = meta.get("forward_batch", DEFAULT_FORWARD_BATCH)
    clean_b = meta.get("clean_batch", DEFAULT_CLEAN_BATCH)
    rep = meta.get("replacement_text", "")
    txt = (
        f"Status:\nManaged channel: {managed}\nLast indexed: {last_indexed}\n"
        f"Forward batch: {forward_b}\nClean batch: {clean_b}\nReplacement: {rep}\n"
        f"Exceptions: {exceptions}\n"
    )
    await message.reply_text(txt)


# three extra commands as requested (I added setsession, add_exception, remove_exception above) - done.

# ===========================
# Start the bot
# ===========================
if __name__ == "__main__":
    log.info("Starting Channel Manager Bot...")
    bot.run()
