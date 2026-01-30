# core_utils.py
# -*- coding: utf-8 -*-
import asyncio
import logging
from typing import Any, Coroutine
# Bot import zaroori hai mounting ke liye
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramRetryAfter

logger = logging.getLogger("bot.core_utils")

# ============ GLOBAL SEMAPHORES & CONSTANTS ============
# Wahi purani settings jo aapke server par perfectly kaam kar rahi hain.
# Inhe change mat karna.

TG_OP_TIMEOUT = 10  
DB_OP_TIMEOUT = 15  

# Old Limits (Tested & Working)
DB_SEMAPHORE = asyncio.Semaphore(5)                # 5 DB calls limit (Deadlock se bachata hai)
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(10)  
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(10)    
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(15) 
WEBHOOK_SEMAPHORE = asyncio.Semaphore(1)           # Ye variable wapas daal diya hai

# ============ SAFE API CALL WRAPPERS (Original Logic + Fixes) ============

async def safe_db_call(coro, timeout=DB_OP_TIMEOUT, default=None):
    """
    UPGRADED DB WRAPPER:
    - Logic: Old (Wait inside lock) -> Dashboard khulne ki guarantee.
    - Fix: Non-coroutine check added.
    """
    if not asyncio.iscoroutine(coro):
         return coro
         
    try:
        # Lock ke andar wait karna hi aapke dashboard ke liye sahi hai
        async with DB_SEMAPHORE: 
            return await asyncio.wait_for(coro, timeout=timeout)
            
    except asyncio.TimeoutError:
        logger.warning(f"⚠️ DB Timeout ({timeout}s) - Query slow thi par crash nahi hua.")
        return default
    except Exception as e:
         logger.error(f"❌ DB Error: {e}", exc_info=True)
         return default


async def safe_tg_call(coro, timeout=TG_OP_TIMEOUT, semaphore: asyncio.Semaphore | None = None, bot: Bot | None = None):
    """
    UPGRADED TG WRAPPER:
    - Logic: Old (Sleep 0.1s) -> FloodWait se bachata hai.
    - Fix: 'bot' instance mounting added (RuntimeError Fix).
    """
    semaphore_to_use = semaphore or asyncio.Semaphore(1)
    
    try:
        async with semaphore_to_use:
            # 1. THE MAGIC SLEEP (Ye aapke purane code ki jaan thi)
            # Isse telegram requests reject nahi karta
            if semaphore: 
                await asyncio.sleep(0.1) 
            
            # 2. RUNTIME ERROR FIX (Ye naye bot.py ke liye zaroori hai)
            if bot and hasattr(coro, "as_"):
                coro = coro.as_(bot)
                
            # 3. Execution
            return await asyncio.wait_for(coro, timeout=timeout)
            
    except asyncio.TimeoutError: 
        logger.warning(f"⚠️ TG Timeout: API busy thi, skip kiya.")
        return None
        
    except TelegramRetryAfter as e:
        # FloodWait Handling
        wait_time = e.retry_after
        logger.warning(f"⏳ FloodWait: {wait_time}s ka break liya.")
        await asyncio.sleep(wait_time if wait_time < 10 else 5)
        return None

    except (TelegramAPIError, TelegramBadRequest) as e:
        error_msg = str(e).lower()
        
        # Ignored Errors (Clean Logs)
        if "bot was blocked" in error_msg or "user is deactivated" in error_msg:
            return False
        elif "chat not found" in error_msg or "peer_id_invalid" in error_msg:
            return False
        elif "message is not modified" in error_msg:
            return True # Dashboard refresh success
        elif "message to delete not found" in error_msg:
            return None
        elif "too many requests" in error_msg:
            await asyncio.sleep(5)
            return None
        else:
            logger.error(f"❌ TG API Error: {e}")
            return None
            
    except Exception as e:
        logger.exception(f"❌ TG Critical Crash: {e}")
        return None
