# core_utils.py
# -*- coding: utf-8 -*-
import asyncio
import logging
from typing import Any, Coroutine, Optional, Union
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramRetryAfter

# Logger Setup
logger = logging.getLogger("bot.core_utils")

# ============ GLOBAL STABILITY CONSTANTS ============
# NOTE: Ye limits FREE TIER servers (Render/Heroku/Atlas) ke liye optimized hain.
# Inhe badhane se bot "Freeze" ya "Crash" ho sakta hai.

TG_OP_TIMEOUT = 10   # Telegram API calls timeout
DB_OP_TIMEOUT = 15   # Database queries timeout

# --- TRAFFIC CONTROL (SEMAPHORES) ---
# "Old is Gold" Settings - Low concurrency prevents deadlocks
DB_SEMAPHORE = asyncio.Semaphore(5)                # Sirf 5 DB calls ek saath (Deadlock Killer)
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(10)  # Delete limits
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(10)    # Copy/Forward limits
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(15) # Broadcast throttling
WEBHOOK_SEMAPHORE = asyncio.Semaphore(1)           # Webhook setup limit

# Default fallback semaphore
DEFAULT_TG_SEMAPHORE = asyncio.Semaphore(5)

# --- 1. ULTIMATE DATABASE CALL WRAPPER ---
async def safe_db_call(coro_or_value: Any, timeout: int = DB_OP_TIMEOUT, default: Any = None) -> Any:
    """
    Stabilized DB Wrapper (The Anti-Freeze Version):
    - Handles Async functions AND Static values automatically.
    - Uses strict Semaphore (5) to prevent CPU overload.
    """
    # 1. Static Value Check: Agar ye function nahi hai, to seedha return karo
    if not asyncio.iscoroutine(coro_or_value):
        return coro_or_value

    try:
        # 2. Semaphore Lock: Line mein lago
        async with DB_SEMAPHORE: 
            # 3. Execution with Timeout: Slot milne ke baad run karo
            return await asyncio.wait_for(coro_or_value, timeout=timeout)
            
    except asyncio.TimeoutError:
        logger.warning(f"⚠️ DB Slow: Query took >{timeout}s (Skipped to prevent freeze)")
        # Coroutine close cleanup (Memory leak prevent)
        try:
            coro_or_value.close()
        except: 
            pass
        return default
        
    except Exception as e:
        logger.error(f"❌ DB Error: {e}", exc_info=False) # Full stack trace ki zarurat nahi
        return default


# --- 2. ULTIMATE TELEGRAM CALL WRAPPER ---
async def safe_tg_call(
    coro: Coroutine, 
    timeout: int = TG_OP_TIMEOUT, 
    semaphore: asyncio.Semaphore | None = None, 
    bot: Bot | None = None
) -> Any:
    """
    Smart TG Wrapper (The 'No-Crash' Version):
    - Includes 'Pacing' (Sleep 0.1s) to prevent FloodWait.
    - Auto-Fixes 'Bot Context' errors.
    """
    # Agar semaphore None hai, to default safe limit use karo
    semaphore_to_use = semaphore or DEFAULT_TG_SEMAPHORE

    try:
        async with semaphore_to_use:
            # --- THE MAGIC PACING FIX ---
            # Har request se pehle thoda sa pause. Ye bot ko block hone se bachata hai.
            if semaphore: 
                await asyncio.sleep(0.1) 
            
            # --- CONTEXT MOUNTING FIX ---
            # Aiogram 3.x mein bot instance bind karna zaroori hai
            if bot and hasattr(coro, "as_"):
                coro = coro.as_(bot)
                
            # Execute
            return await asyncio.wait_for(coro, timeout=timeout)
            
    except asyncio.TimeoutError: 
        logger.warning(f"⚠️ TG Timeout: API did not respond in {timeout}s")
        return None

    except TelegramRetryAfter as e:
        # Agar Telegram bole "Ruko", to hum rukenge (Crash nahi karenge)
        wait_time = e.retry_after
        logger.warning(f"⏳ FloodWait: Sleeping for {wait_time}s...")
        await asyncio.sleep(wait_time if wait_time < 10 else 5) # Max 5s sleep inside handler
        return None 
        
    except (TelegramAPIError, TelegramBadRequest) as e:
        err_msg = str(e).lower()
        
        # Ignored Errors (Log file clean rakhne ke liye)
        if any(x in err_msg for x in ["bot was blocked", "user is deactivated", "chat not found", "peer_id_invalid"]):
            return False 
        
        if "message is not modified" in err_msg:
            # Dashboard Refresh ke liye: Ye Success hai!
            return True 
            
        if "message to delete not found" in err_msg:
            return None 

        logger.error(f"❌ TG API Error: {e}")
        return None
            
    except Exception as e:
        logger.exception(f"❌ TG Critical Crash: {e}")
        return None
