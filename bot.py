import os, json, asyncio, re, time
from pathlib import Path
from typing import Dict, Any, List, Optional
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram import Client as SClient

# ========= ENV =========
BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID") or 0)
API_ID = int(os.getenv("API_ID") or 0)
API_HASH = os.getenv("API_HASH")

if not BOT_TOKEN or not OWNER_ID or not API_ID or not API_HASH:
    raise SystemExit("Set BOT_TOKEN, OWNER_ID, API_ID, API_HASH env variables")

# ========= STORAGE =========
BASE = Path(".")
DATA = BASE / "data"
DATA.mkdir(exist_ok=True)

META_FILE = DATA / "meta.json"         # global settings (managed_channel, batches, replacement, session)
EXC_FILE  = DATA / "exceptions.json"   # list[str]
REG_FILE  = DATA / "registry.json"     # { "channels": {channel_id: {"title":..., "username":...}}, "managed_channel": id }

def load_json(p: Path, default):
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    p.write_text(json.dumps(default, indent=2), encoding="utf-8")
    return default

def save_json(p: Path, obj):
    p.write_text(json.dumps(obj, indent=2), encoding="utf-8")

# defaults
meta = load_json(META_FILE, {
    "admin_session": None,         # {"api_id": int, "api_hash": str, "session": str}
    "replacement": "",             # inserted where link/@ removed (can be empty = just remove)
    "forward_batch": 200,
    "clean_batch": 150
})
exceptions: List[str] = load_json(EXC_FILE, [])
registry = load_json(REG_FILE, {
    "channels": {},                # channel_id(str) -> {"title": str, "username": str|None}
    "managed_channel": None
})

# ========= CLIENT =========
bot = Client("CHANNEL_MANAGER_BOT",
             bot_token=BOT_TOKEN,
             api_id=API_ID,
             api_hash=API_HASH)

# ========= REGEX =========
URL = re.compile(r"(https?://[^\s]+|www\.[^\s]+)", re.IGNORECASE)
USER = re.compile(r"@([A-Za-z0-9_]{5,32})")

# ========= HELPERS =========
def owner(uid: int) -> bool:
    return uid == OWNER_ID

def channel_json_path(cid: int) -> Path:
    return DATA / f"channel_{cid}.json"

def ensure_list(obj) -> list:
    return obj if isinstance(obj, list) else []

def cleaner(text: Optional[str]) -> Optional[str]:
    """Remove only links & @usernames; preserve rest. Respect exceptions. Insert meta['replacement'] if set."""
    if not text:
        return text
    repl = meta.get("replacement") or ""

    def url_repl(m):
        token = m.group(0)
        for ex in exceptions:
            if ex.lower() in token.lower():
                return token
        return repl

    def user_repl(m):
        token = m.group(0)              # e.g., "@username"
        uname = token[1:].lower()
        for ex in exceptions:
            if ex.lower().lstrip("@") == uname:
                return token
        return repl

    text = URL.sub(url_repl, text)
    text = USER.sub(user_repl, text)
    text = re.sub(r"\s{2,}", " ", text).strip()  # tidy spaces
    return text

async def resolve_chat_id(c: Client, ident: str) -> int:
    chat = await c.get_chat(ident)
    return chat.id

def add_to_registry(cid: int, title: str, username: Optional[str]):
    registry["channels"][str(cid)] = {"title": title, "username": username}
    save_json(REG_FILE, registry)

def remove_from_registry(cid: int):
    registry["channels"].pop(str(cid), None)
    if registry.get("managed_channel") == cid:
        registry["managed_channel"] = None
    save_json(REG_FILE, registry)

def set_managed(cid: int):
    registry["managed_channel"] = cid
    save_json(REG_FILE, registry)

def list_registry_lines() -> List[str]:
    lines = []
    for k, v in registry["channels"].items():
        tag = " (managed)" if registry.get("managed_channel") == int(k) else ""
        uname = ("@" + v["username"]) if v.get("username") else "—"
        lines.append(f"`{k}` | {v.get('title','?')} | {uname}{tag}")
    if not lines:
        lines.append("_No DB channels added yet_")
    return lines

async def index_channel_with_session(target_ident: str) -> str:
    """Use admin session to index all messages of the channel to its JSON file (no duplicates)."""
    sess = meta.get("admin_session")
    if not sess:
        return "❌ First set session with /setsession API_ID|API_HASH|STRING_SESSION"

    async with SClient("ADMIN_SESSION",
                       api_id=int(sess["api_id"]),
                       api_hash=sess["api_hash"],
                       session_string=sess["session"]) as sc:
        chat = await sc.get_chat(target_ident)
        cid = chat.id
        fn = channel_json_path(cid)
        existing = load_json(fn, [])
        existing_ids = {int(m["id"]) for m in existing if "id" in m}
        new_count = 0

        async for msg in sc.get_chat_history(cid, limit=0):
            mid = msg.id
            if mid not in existing_ids:
                existing.append({"id": mid})
                new_count += 1
        save_json(fn, existing)
        add_to_registry(cid, chat.title or "", getattr(chat, "username", None))
        set_managed(cid)
        return f"✅ Indexed `{new_count}` new messages\nChannel: {chat.title or cid}\nID: `{cid}` (now managed)"

async def sync_managed_with_session() -> str:
    """Rescan managed channel & append only new message IDs."""
    cid = registry.get("managed_channel")
    if not cid:
        return "❌ No managed channel. Use /switchdb or /adddb first."

    sess = meta.get("admin_session")
    if not sess:
        return "❌ First set session with /setsession."

    fn = channel_json_path(cid)
    stored = load_json(fn, [])
    existing_ids = {int(m["id"]) for m in stored if "id" in m}
    new_count = 0

    async with SClient("ADMIN_SESSION",
                       api_id=int(sess["api_id"]),
                       api_hash=sess["api_hash"],
                       session_string=sess["session"]) as sc:
        async for msg in sc.get_chat_history(cid, limit=0):
            mid = msg.id
            if mid not in existing_ids:
                stored.append({"id": mid})
                new_count += 1
    save_json(fn, stored)
    return f"✅ Sync complete. New messages added: {new_count}"

# ========= COMMANDS =========
@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(_, m):
    await m.reply(
        "✅ **Channel Manager Bot**\n"
        "Only owner can use admin commands.\n\n"
        "**DB & Session**\n"
        "/setsession `API_ID|API_HASH|STRING_SESSION`\n"
        "/adddb `<channel>` – index & add to DB\n"
        "/removedb `<channel_or_id>` – remove from DB\n"
        "/switchdb `<channel_or_id>` – set active\n"
        "/listdb – list DB channels\n"
        "/index `<channel>` – index to JSON (also sets managed)\n"
        "/sync – rescan managed channel\n\n"
        "**Ops**\n"
        "/forward `<source>` `<target>` – batch 200\n"
        "/clean `<channel>` – remove links/@ at 150 batch\n\n"
        "**Config**\n"
        "/set_replacement `<text>`\n"
        "/add_exception `<text>`\n"
        "/remove_exception `<text>`\n"
        "/set_batches `<forward>` `<clean>`\n"
        "/status"
    )

@bot.on_message(filters.command("setsession") & filters.private)
async def cmd_setsession(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    try:
        payload = m.text.split(" ", 1)[1]
        api_id, api_hash, sess = payload.split("|", 2)
    except:
        return await m.reply("Format:\n`/setsession API_ID|API_HASH|STRING_SESSION`")
    meta["admin_session"] = {"api_id": int(api_id), "api_hash": api_hash, "session": sess}
    save_json(META_FILE, meta)
    await m.reply("✅ Admin session saved.")

@bot.on_message(filters.command("adddb") & filters.private)
async def cmd_adddb(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    args = m.text.split(maxsplit=1)
    if len(args) < 2:
        return await m.reply("Usage:\n`/adddb <channel_id_or_username>`")
    res = await index_channel_with_session(args[1].strip())
    await m.reply(res)

@bot.on_message(filters.command("removedb") & filters.private)
async def cmd_removedb(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    args = m.text.split(maxsplit=1)
    if len(args) < 2:
        return await m.reply("Usage:\n`/removedb <channel_id_or_username>`")
    ident = args[1].strip()
    # resolve via bot (best-effort); else if numeric, use directly
    try:
        cid = await resolve_chat_id(bot, ident)
    except:
        try:
            cid = int(ident)
        except:
            return await m.reply("❌ Could not resolve channel.")
    remove_from_registry(cid)
    # keep the JSON file by default (safety); comment next 3 lines if you want deletion
    # jf = channel_json_path(cid)
    # if jf.exists():
    #     jf.unlink()
    await m.reply(f"✅ Removed from DB: `{cid}`")

@bot.on_message(filters.command("switchdb") & filters.private)
async def cmd_switchdb(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    args = m.text.split(maxsplit=1)
    if len(args) < 2:
        return await m.reply("Usage:\n`/switchdb <channel_id_or_username>`")
    ident = args[1].strip()
    # resolve to id present in registry
    try:
        cid = await resolve_chat_id(bot, ident)
    except:
        try:
            cid = int(ident)
        except:
            return await m.reply("❌ Could not resolve channel.")
    if str(cid) not in registry["channels"]:
        return await m.reply("❌ Not in DB. Use /adddb first.")
    set_managed(cid)
    await m.reply(f"✅ Managed channel switched to: `{cid}`")

@bot.on_message(filters.command("listdb") & filters.private)
async def cmd_listdb(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    lines = list_registry_lines()
    await m.reply("**DB Channels:**\n" + "\n".join(lines))

@bot.on_message(filters.command("index") & filters.private)
async def cmd_index(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    args = m.text.split(maxsplit=1)
    if len(args) < 2:
        return await m.reply("Usage:\n`/index <channel_id_or_username>`")
    res = await index_channel_with_session(args[1].strip())
    await m.reply(res)

@bot.on_message(filters.command("sync") & filters.private)
async def cmd_sync(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    res = await sync_managed_with_session()
    await m.reply(res)

@bot.on_message(filters.command("forward") & filters.private)
async def cmd_forward(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    parts = m.text.split()
    if len(parts) != 3:
        return await m.reply("Usage:\n`/forward <source> <target>`")
    src_ident, tgt_ident = parts[1], parts[2]
    try:
        src = await bot.get_chat(src_ident)
        tgt = await bot.get_chat(tgt_ident)
    except:
        return await m.reply("❌ Could not resolve source/target.")
    batch = int(meta.get("forward_batch", 200)) or 200
    count = 0
    await m.reply(f"🚀 Forward started\nBatch: {batch}")
    async for msg in bot.get_chat_history(src.id, limit=0):
        try:
            await bot.forward_messages(tgt.id, src.id, msg.id)
            count += 1
        except FloodWait as e:
            await asyncio.sleep(e.x + 1)
        if count % batch == 0:
            await asyncio.sleep(2)
    await m.reply(f"✅ Forward finished. Total forwarded: {count}")

@bot.on_message(filters.command("clean") & filters.private)
async def cmd_clean(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return await m.reply("Usage:\n`/clean <channel_id_or_username>`")
    try:
        chat = await bot.get_chat(parts[1].strip())
    except:
        return await m.reply("❌ Could not resolve channel.")
    batch = int(meta.get("clean_batch", 150)) or 150
    scanned = edited = 0
    await m.reply(f"🧹 Cleaning started for {chat.id}\nBatch: {batch}")
    async for msg in bot.get_chat_history(chat.id, limit=0):
        txt = msg.caption or msg.text
        new_txt = cleaner(txt)
        if new_txt != txt:
            try:
                if msg.caption is not None:
                    await bot.edit_message_caption(chat.id, msg.id, new_txt)
                else:
                    await bot.edit_message_text(chat.id, msg.id, new_txt)
                edited += 1
            except FloodWait as e:
                await asyncio.sleep(e.x + 1)
            except:
                pass
        scanned += 1
        if scanned % batch == 0:
            await asyncio.sleep(2)
    await m.reply(f"✅ Cleaning finished.\nScanned: {scanned}\nEdited: {edited}")

@bot.on_message(filters.command("set_replacement") & filters.private)
async def cmd_set_replacement(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    args = m.text.split(maxsplit=1)
    if len(args) < 2:
        meta["replacement"] = ""
    else:
        meta["replacement"] = args[1]
    save_json(META_FILE, meta)
    await m.reply(f"✅ Replacement set to: `{meta['replacement']}`")

@bot.on_message(filters.command("add_exception") & filters.private)
async def cmd_add_exception(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    args = m.text.split(maxsplit=1)
    if len(args) < 2:
        return await m.reply("Usage:\n`/add_exception <text>`")
    val = args[1].strip()
    if val not in exceptions:
        exceptions.append(val)
        save_json(EXC_FILE, exceptions)
    await m.reply(f"✅ Exception added: `{val}`")

@bot.on_message(filters.command("remove_exception") & filters.private)
async def cmd_remove_exception(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    args = m.text.split(maxsplit=1)
    if len(args) < 2:
        return await m.reply("Usage:\n`/remove_exception <text>`")
    val = args[1].strip()
    if val in exceptions:
        exceptions.remove(val)
        save_json(EXC_FILE, exceptions)
    await m.reply(f"✅ Exception removed: `{val}`")

@bot.on_message(filters.command("set_batches") & filters.private)
async def cmd_set_batches(_, m):
    if not owner(m.from_user.id):
        return await m.reply("⛔ Only owner allowed.")
    parts = m.text.split()
    if len(parts) != 3:
        return await m.reply("Usage:\n`/set_batches <forward_batch> <clean_batch>`")
    try:
        fwd = int(parts[1]); cln = int(parts[2])
    except:
        return await m.reply("❌ Enter integers.")
    meta["forward_batch"] = max(1, fwd)
    meta["clean_batch"] = max(1, cln)
    save_json(META_FILE, meta)
    await m.reply(f"✅ Batches updated.\nForward: {meta['forward_batch']}\nClean: {meta['clean_batch']}")

@bot.on_message(filters.command("status") & filters.private)
async def cmd_status(_, m):
    man = registry.get("managed_channel")
    info = registry["channels"].get(str(man), {}) if man else {}
    await m.reply(
        "**Status**\n"
        f"Owner: `{OWNER_ID}`\n"
        f"Managed Channel: `{man}` | {info.get('title','-')} | @{info.get('username','-')}\n"
        f"DB Channels: {len(registry['channels'])}\n"
        f"Forward Batch: {meta.get('forward_batch')}\n"
        f"Clean Batch: {meta.get('clean_batch')}\n"
        f"Replacement: `{meta.get('replacement','')}`\n"
        f"Exceptions: {len(exceptions)}"
    )

# ===== RUN =====
if __name__ == "__main__":
    bot.run()
