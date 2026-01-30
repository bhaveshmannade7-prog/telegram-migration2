# core_utils.py
# -*- coding: utf-8 -*-
import asyncio
import logging
import contextlib
from typing import Any, Callable, Coroutine
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramRetryAfter

# Logger Setup
logger = logging.getLogger("bot.core_utils")

# ============ GLOBAL OPTIMIZED CONSTANTS ============
# Timeout settings (Seconds)
TG_OP_TIMEOUT = 25  # Telegram API calls ke liye
DB_OP_TIMEOUT = 20  # Database queries ke liye

# ============ SMART SEMAPHORES (High Performance) ============
# Humne limit ko 500 kar diya hai taaki kabhi bhi bottleneck na ho.
# MongoDB Atlas 500 connections easily handle kar sakta hai.

# Database ke liye High Limit (Read/Write mixed)
DB_SEMAPHORE = asyncio.Semaphore(500) 

# Telegram limits (API guidelines ke hisaab se safe limits)
TELEGRAM_DELETE_SEMAPHORE = asyncio.Semaphore(30)
TELEGRAM_COPY_SEMAPHORE = asyncio.Semaphore(30)
TELEGRAM_BROADCAST_SEMAPHORE = asyncio.Semaphore(40)
DEFAULT_TG_SEMAPHORE = asyncio.Semaphore(100) # General messages

# ============ HELPER FUNCTIONS ============

async def _run_with_semaphore(semaphore: asyncio.Semaphore, coro: Coroutine):
    """Internal helper to acquire semaphore and run coroutine."""
    async with semaphore:
        return await coro

# --- 1. HYBRID DATABASE CALL WRAPPER (CRASH PROOF) ---
async def safe_db_call(coro_or_value: Any, timeout: int = DB_OP_TIMEOUT, default: Any = None) -> Any:
    """
    ULTIMATE DB WRAPPER v2:
    - Fixes 'Indefinite Wait': Timeout covers BOTH waiting for lock AND execution.
    - Handles Sync values automatically.
    """
    # 1. Agar value Async Coroutine nahi hai (e.g. static data), to direct return karo
    if not asyncio.iscoroutine(coro_or_value):
        return coro_or_value

    try:
        # 2. Timeout Wrapper (SABSE ZAROORI FIX)
        # Hum timeout ko bahar lagate hain. Agar Semaphore milne me time laga,
        # to bhi ye cancel ho jayega. Bot freeze nahi hoga.
        return await asyncio.wait_for(
            _run_with_semaphore(DB_SEMAPHORE, coro_or_value),
            timeout=timeout
        )
    except asyncio.TimeoutError:
        logger.warning(f"⚠️ DB Slow/Busy: Query cancelled after {timeout}s")
        # Coroutine ko close karna zaroori hai taaki 'was never awaited' warning na aaye
        try:
            coro_or_value.close()
        except: 
            pass
        return default
    except Exception as e:
        logger.error(f"❌ DB Error: {e}", exc_info=False)
        return default

# --- 2. SMART TELEGRAM CALL WRAPPER (CONTEXT AWARE) ---
async def safe_tg_call(
    coro: Coroutine, 
    timeout: int = TG_OP_TIMEOUT, 
    semaphore: asyncio.Semaphore | None = None, 
    bot: Bot | None = None
) -> Any:
    """
    Smart Wrapper for Telegram API:
    - Auto-mounts 'bot' instance to fix ContextVar/RuntimeError.
    - Handles FloodWait, Blocked User, and Deleted Message errors gracefully.
    """
    sem = semaphore or DEFAULT_TG_SEMAPHORE
    
    # FIX: Bot context mount logic
    if bot is not None:
        # Aiogram 3.x trick: Request ko bot instance ke saath bind karo
        if hasattr(coro, "as_"):
            coro = coro.as_(bot)

    try:
        # Timeout covers lock acquisition + execution
        return await asyncio.wait_for(
            _run_with_semaphore(sem, coro),
            timeout=timeout
        )

    except asyncio.TimeoutError:
        logger.warning(f"⚠️ TG Timeout: API call took >{timeout}s")
        return None

    except TelegramRetryAfter as e:
        # FloodWait handle karna zaroori hai
        wait_time = e.retry_after
        logger.warning(f"⏳ FloodWait: Sleeping for {wait_time}s...")
        await asyncio.sleep(wait_time)
        return None # Retry logic caller par chhodte hain complex na karne ke liye

    except (TelegramAPIError, TelegramBadRequest) as e:
        err_msg = str(e).lower()
        
        # Ignored Errors (Log spam kam karne ke liye)
        if any(x in err_msg for x in ["bot was blocked", "user is deactivated", "chat not found", "peer_id_invalid"]):
            return False 
        
        if "message is not modified" in err_msg:
            # Dashboard refresh ke liye ye success hai
            return True 
            
        if "message to delete not found" in err_msg:
            return None 

        logger.error(f"❌ TG API Error: {e}")
        return None
            
    except Exception as e:
        logger.error(f"❌ TG Critical Crash: {e}", exc_info=True)
        return None

# --- 3. SYNC TO ASYNC WRAPPER (For PSUTIL) ---
async def run_sync(func: Callable, *args, **kwargs) -> Any:
    """
    Runs blocking functions (like psutil, json.load) in a separate thread.
    Prevents Event Loop Lag.
    """
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
    except Exception as e:
        logger.error(f"Async Wrapper Error: {e}")
        return None
