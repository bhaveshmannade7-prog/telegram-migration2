# core_utils.py
# -*- coding: utf-8 -*-
import asyncio
import logging
from typing import Any, Callable, Union, Coroutine

from aiogram import Bot
from aiogram.exceptions import (
    TelegramAPIError, 
    TelegramBadRequest, 
    TelegramRetryAfter, 
    TelegramNetworkError
)

# Logger Setup
logger = logging.getLogger("bot.core_utils")

# ============ GLOBAL CONSTANTS (Optimized) ============
TG_OP_TIMEOUT = 25  # Telegram API timeout (slow server ke liye badhaya)
DB_OP_TIMEOUT = 20  # Database query timeout

# ============ SMART SEMAPHORES ============
# Hum limits ko kaafi high rakh rahe hain taaki bot kabhi stuck na ho
DB_SEMAPHORE = asyncio.Semaphore(200)               
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(50)   
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(50)     
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(50)
DEFAULT_TG_SEMAPHORE = asyncio.Semaphore(100)       

# --- 1. ROBUST DATABASE WRAPPER ---
async def safe_db_call(func_or_coro: Union[Callable, Coroutine], timeout: int = DB_OP_TIMEOUT, default: Any = None):
    """
    ULTIMATE DB CALL HANDLER:
    1. Checks if input is a Value, Function, or Coroutine.
    2. Handles Timeouts gracefully without freezing the bot.
    3. Returns 'default' value on failure so Dashboard doesn't crash.
    """
    try:
        # Case A: Input is already a computed value (Not a function/coroutine)
        if not asyncio.iscoroutine(func_or_coro) and not callable(func_or_coro):
            return func_or_coro

        # Case B: Input is a Coroutine (Async Function Call)
        if asyncio.iscoroutine(func_or_coro):
            async with DB_SEMAPHORE:
                return await asyncio.wait_for(func_or_coro, timeout=timeout)

        # Case C: Input is a callable function (Sync or Async wrapper)
        if callable(func_or_coro):
            res = func_or_coro()
            if asyncio.iscoroutine(res):
                async with DB_SEMAPHORE:
                    return await asyncio.wait_for(res, timeout=timeout)
            return res

    except asyncio.TimeoutError:
        logger.warning(f"⚠️ DB Timeout ({timeout}s) - Returning default: {default}")
        return default
        
    except Exception as e:
        logger.error(f"❌ DB Critical Error: {e}", exc_info=True)
        return default


# --- 2. INTELLIGENT TELEGRAM WRAPPER ---
async def safe_tg_call(
    coro: Coroutine, 
    timeout: int = TG_OP_TIMEOUT, 
    semaphore: asyncio.Semaphore = None, 
    bot: Bot = None
):
    """
    SMART TG WRAPPER:
    - Automatically handles 'Message Not Modified' (Stops Loading Circle).
    - Handles 'FloodWait' (Auto-Sleep).
    - Prevents 'Bot Context' errors.
    """
    sem = semaphore or DEFAULT_TG_SEMAPHORE
    
    try:
        async with sem:
            # FIX: Ensure Bot Context is valid
            if bot is not None and hasattr(coro, "as_"):
                coro = coro.as_(bot)
            
            return await asyncio.wait_for(coro, timeout=timeout)

    except asyncio.TimeoutError:
        logger.warning(f"⚠️ TG Request Timeout ({timeout}s) - Skipped.")
        return None

    except TelegramRetryAfter as e:
        # FloodWait: Ye sabse zaroori fix hai
        wait_time = e.retry_after
        logger.warning(f"⏳ FloodWait: Sleeping for {wait_time}s...")
        await asyncio.sleep(wait_time)
        return None # Retry logic caller par chhodte hain complex na karne ke liye

    except TelegramBadRequest as e:
        err = str(e).lower()
        if "message is not modified" in err:
            # Ye Error nahi, Success hai! (Dashboard refresh logic ke liye)
            return True 
        if "chat not found" in err or "message to delete not found" in err:
            return None
        
        logger.error(f"❌ TG Bad Request: {e}")
        return None

    except TelegramNetworkError:
        logger.warning("⚠️ TG Network Error: Connection Unstable.")
        return None

    except Exception as e:
        logger.error(f"❌ TG Unknown Error: {e}", exc_info=True)
        return None
