# core_utils.py
import asyncio
import logging
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest

# Logger Setup
logger = logging.getLogger("bot.core_utils")

# ============ GLOBAL OPTIMIZED CONSTANTS ============
# Timeout settings (Thoda badhaya taaki dashboard load ho sake)
TG_OP_TIMEOUT = 15  
DB_OP_TIMEOUT = 20 

# ============ SMART SEMAPHORES (High Concurrency Fix) ============
# OLD LIMIT: 10 (Yeh dashboard ko block kar raha tha)
# NEW LIMIT: 100 (Ab Admin Dashboard aur Search ek saath chalenge)

DB_SEMAPHORE = asyncio.Semaphore(100)               # Database bottlenecks hataye
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(30)   # Delete speed badhai
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(30)     # Copy/Forward speed badhai
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(50)# Broadcast fast hoga
WEBHOOK_SEMAPHORE = asyncio.Semaphore(5)
DEFAULT_TG_SEMAPHORE = asyncio.Semaphore(100)       # General messages ke liye high limit

# --- 1. HYBRID DATABASE CALL WRAPPER (DEADLOCK FIX) ---
async def safe_db_call(coro_or_value, timeout=DB_OP_TIMEOUT, default=None):
    """
    ULTIMATE DB WRAPPER (Freeze Proof):
    - Semaphore limit 100 kar di gayi hai.
    - Agar DB busy hai to timeout dega, par bot ko freeze nahi karega.
    """
    # Check: Kya ye async function (coroutine) hai?
    if asyncio.iscoroutine(coro_or_value):
        try:
            # Semaphore acquire karne me timeout lagaya taaki wait na kare
            async with DB_SEMAPHORE: 
                return await asyncio.wait_for(coro_or_value, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"⚠️ DB Slow: Operation timed out (> {timeout}s)")
            return default
        except Exception as e:
            logger.error(f"❌ DB Error: {e}", exc_info=False) # Full stack trace ki zarurat nahi
            return default
    
    # Sync value return
    return coro_or_value


# --- 2. SMART TELEGRAM CALL WRAPPER (CONTEXT FIX) ---
async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore | None = None, bot: Bot | None = None):
    """
    Smart Wrapper for Telegram API:
    - Fixes 'RuntimeError: Bot not found' by mounting bot instance.
    - Uses a HIGH limit semaphore by default to prevent self-blocking.
    """
    # FIX: Har call ke liye naya semaphore banane ki jagah Global High Limit use karein
    semaphore_to_use = semaphore or DEFAULT_TG_SEMAPHORE
    
    try:
        async with semaphore_to_use:
            # FIX: Bot instance mounting for ContextVar errors
            if bot is not None:
                # Aiogram 3.x trick to bind bot context
                # Yeh check karta hai ki kya method 'as_' support karta hai
                if hasattr(coro, "as_"):
                    coro = coro.as_(bot)
            
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
            # Dashboard refresh karte waqt ye error normal hai
            return True 
        elif "message to delete not found" in err_msg:
            return None 
        elif "too many requests" in err_msg:
            # FloodWait handling
            try:
                wait_time = int(str(e).split()[-1]) if str(e).split()[-1].isdigit() else 5
            except:
                wait_time = 5
            logger.warning(f"⚠️ TG FloodWait: Sleeping {wait_time}s...")
            await asyncio.sleep(wait_time) 
            return None
        else:
            logger.error(f"❌ TG API Error: {e}")
            return None
            
    except Exception as e:
        logger.error(f"❌ TG Critical Crash: {e}", exc_info=False)
        return None
