# core_utils.py
import asyncio
import logging
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest

# Logger Setup
logger = logging.getLogger("bot.core_utils")

# ============ GLOBAL OPTIMIZED CONSTANTS ============
# Timeout badha diya taaki slow network par "End of Results" na aaye
TG_OP_TIMEOUT = 12  
DB_OP_TIMEOUT = 15 

# ============ SMART SEMAPHORES (Render Friendly) ============
# Limit control taaki bot freeze na ho
DB_SEMAPHORE = asyncio.Semaphore(10)               # Database ke liye 10 parallel requests
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(10)  # Delete limit
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(10)    # Copy/Forward limit
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(20) # Broadcast fast karne ke liye badhaya
WEBHOOK_SEMAPHORE = asyncio.Semaphore(1) 

# --- 1. HYBRID DATABASE CALL WRAPPER (CRASH FIX) ---
async def safe_db_call(coro_or_value, timeout=DB_OP_TIMEOUT, default=None):
    """
    ULTIMATE DB WRAPPER (Sync + Async Compatible):
    - Agar input 'async function' hai, to usse await karega.
    - Agar input 'normal value' hai, to usse direct return karega.
    - Result: 'Non-coroutine object passed' wala error JAD SE KHATAM.
    """
    # Check: Kya ye async function (coroutine) hai?
    if asyncio.iscoroutine(coro_or_value):
        try:
            async with DB_SEMAPHORE: 
                return await asyncio.wait_for(coro_or_value, timeout=timeout)
        except asyncio.TimeoutError:
            # Sirf warning print karo, crash mat karo
            logger.warning(f"⚠️ DB Slow: Operation took >{timeout}s")
            return default
        except Exception as e:
            logger.error(f"❌ DB Error: {e}", exc_info=True)
            return default
    
    # Agar ye coroutine nahi hai (matlab Sync value hai), to seedha return karo
    # Error dene ki jagah hum value accept kar lenge.
    return coro_or_value


# --- 2. SMART TELEGRAM CALL WRAPPER (RunTime Error Fix) ---
async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore | None = None, bot: Bot | None = None):
    """
    Smart Wrapper for Telegram API:
    - Automatically mounts 'bot' instance to fix 'RuntimeError: Bot not found'.
    - Handles Blocked Users, FloodWaits, and Deleted Messages silently.
    """
    semaphore_to_use = semaphore or asyncio.Semaphore(1)
    
    try:
        async with semaphore_to_use:
            # FIX: Agar bot instance pass hua hai, to request ko uspar mount karo
            # Ye 'ContextVar' error ko fix karta hai
            if bot and hasattr(coro, "as_"):
                coro = coro.as_(bot)
                
            return await asyncio.wait_for(coro, timeout=timeout)
            
    except asyncio.TimeoutError: 
        logger.warning(f"⚠️ TG Timeout: Request took >{timeout}s (Skipped)")
        return None
        
    except (TelegramAPIError, TelegramBadRequest) as e:
        err_msg = str(e).lower()
        
        # Common Errors ko ignore karo (Log mat bharo)
        if "bot was blocked" in err_msg or "user is deactivated" in err_msg:
            return False # User ne block kiya hai, chill raho
        elif "chat not found" in err_msg or "peer_id_invalid" in err_msg:
            return False
        elif "message is not modified" in err_msg:
            return None # Same content edit karne ki koshish ki
        elif "message to delete not found" in err_msg:
            return None # Message pehle hi delete ho chuka hai
        elif "too many requests" in err_msg:
            logger.warning(f"⚠️ TG FloodWait: Sleeping 5s...")
            await asyncio.sleep(5) # Auto-cooldown
            return None
        else:
            logger.error(f"❌ TG API Error: {e}")
            return None
            
    except Exception as e:
        logger.exception(f"❌ TG Critical Crash: {e}")
        return None
