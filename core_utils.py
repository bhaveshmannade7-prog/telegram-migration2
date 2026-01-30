# core_utils.py
import asyncio
import logging
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest

# Logger Setup
logger = logging.getLogger("bot.core_utils")

# ============ GLOBAL SEMAPHORES & CONSTANTS (STABLE LIMITS) ============
# NOTE: Ye limits kam hain (5-15) kyunki free server (Render/Atlas) 
# heavy parallel traffic handle nahi kar sakte. Ise badhana mat.

TG_OP_TIMEOUT = 10
DB_OP_TIMEOUT = 15

# Connection Limits (Old Working Values)
DB_SEMAPHORE = asyncio.Semaphore(5)  # Sirf 5 DB calls ek saath (Queue maintain karega)
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(10)
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(10)
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(15)
WEBHOOK_SEMAPHORE = asyncio.Semaphore(1) 

# Default fallback semaphore
DEFAULT_TG_SEMAPHORE = asyncio.Semaphore(5)

# --- 1. SAFE DATABASE CALL (Queue Based) ---
async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    """
    Stabilized DB Wrapper:
    - Queue system use karta hai (Semaphore=5).
    - Agar 5 requests chal rahi hain, to 6th wait karegi (Crash nahi karegi).
    """
    # Safety Check: Agar galti se function call karke bhej diya (await na kiya ho)
    if not asyncio.iscoroutine(coro):
        # logger.warning(f"Non-coroutine passed to DB call: {coro}")
        return coro # Value return kar do

    try:
        # Lock acquire karo (Wait forever until slot is free)
        async with DB_SEMAPHORE: 
            # Slot milne ke baad timeout shuru karo
            return await asyncio.wait_for(coro, timeout=timeout)
            
    except asyncio.TimeoutError:
        logger.error(f"⚠️ DB Timeout ({timeout}s) - Server slow hai.")
        return default
    except Exception as e:
        logger.error(f"❌ DB Error: {e}", exc_info=True)
        return default


# --- 2. SAFE TELEGRAM CALL (Paced & Mounted) ---
async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore | None = None, bot: Bot | None = None):
    """
    Stabilized TG Wrapper:
    - 0.1s sleep add kiya hai (Rate Limit bachane ke liye).
    - Bot instance mount karta hai (RuntimeError fix).
    """
    # Agar koi semaphore nahi diya, to default chhota semaphore use karo
    semaphore_to_use = semaphore or DEFAULT_TG_SEMAPHORE

    try:
        async with semaphore_to_use:
            # 1. Pacing (The Magic Logic): Thoda ruko taaki flood na ho
            if semaphore: 
                await asyncio.sleep(0.1) 
            
            # 2. Context Fix: Bot instance bind karo (Agar available hai)
            if bot and hasattr(coro, "as_"):
                coro = coro.as_(bot)
            
            # 3. Execute with Timeout
            return await asyncio.wait_for(coro, timeout=timeout)
            
    except asyncio.TimeoutError: 
        logger.warning(f"⚠️ TG Timeout: Request took >{timeout}s")
        return None
        
    except (TelegramAPIError, TelegramBadRequest) as e:
        err_msg = str(e).lower()
        
        # Ignored Errors (Logs clean rakhne ke liye)
        if "bot was blocked" in err_msg or "user is deactivated" in err_msg:
            return False
        elif "chat not found" in err_msg or "peer_id_invalid" in err_msg:
            return False
        elif "message is not modified" in err_msg:
            return None # Ye Dashboard refresh ke liye normal hai
        elif "message to delete not found" in err_msg:
            return None
        elif "too many requests" in err_msg:
            # FloodWait handling
            logger.warning(f"⏳ FloodWait detected: {e}")
            await asyncio.sleep(5) 
            return None
        else:
            logger.error(f"❌ TG API Error: {e}")
            return None
            
    except Exception as e:
        logger.error(f"❌ TG Unknown Error: {e}", exc_info=False)
        return None
