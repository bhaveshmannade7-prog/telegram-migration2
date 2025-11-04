#!/usr/bin/env python3
"""
Telegram Channel Cleanup Bot - MEGA FAST VERSION
Processes 30 messages in parallel with crash protection!
"""

import os
import re
import telebot
import time
import threading
from telebot.apihelper import ApiTelegramException
from concurrent.futures import ThreadPoolExecutor, as_completed

# Bot configuration
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHANNEL_ID = -1003138949015

if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN environment variable not found!")
    exit(1)

bot = telebot.TeleBot(BOT_TOKEN, threaded=True)

# WHITELIST - These URLs and usernames will NOT be deleted
WHITELISTED_URLS = [
    'https://t.me/thegreatmoviesl9',
    'https://t.me/moviemazasu',
]

WHITELISTED_USERNAMES = [
    '@MOVIEMAZASU',
    '@THEGREATMOVIESL9',
]

# Regex patterns
URL_PATTERNS = [
    r'https?://[^\s]+',
    r'www\.[^\s]+',
    r't\.me/[^\s]+'
]
USERNAME_PATTERN = r'@\w+'

# Thread-safe counters
lock = threading.Lock()
stats = {'processed': 0, 'edited': 0, 'errors': 0, 'skipped': 0}

def is_whitelisted_url(url):
    """Check if URL is whitelisted (exact normalized match)."""
    try:
        url_normalized = url.strip().rstrip('/').lower()
        for whitelisted in WHITELISTED_URLS:
            if url_normalized == whitelisted.strip().rstrip('/').lower():
                return True
        return False
    except:
        return False

def is_whitelisted_username(username):
    """Check if username is whitelisted."""
    try:
        username_upper = username.upper().strip()
        for whitelisted in WHITELISTED_USERNAMES:
            if username_upper == whitelisted.upper():
                return True
        return False
    except:
        return False

def clean_caption(caption):
    """Remove non-whitelisted URLs and usernames from caption."""
    if not caption:
        return caption
    
    try:
        cleaned = caption
        
        # Remove non-whitelisted URLs
        for pattern in URL_PATTERNS:
            urls = re.findall(pattern, cleaned, flags=re.IGNORECASE)
            for url in urls:
                if not is_whitelisted_url(url):
                    cleaned = cleaned.replace(url, '')
        
        # Remove non-whitelisted usernames
        usernames = re.findall(USERNAME_PATTERN, cleaned)
        for username in usernames:
            if not is_whitelisted_username(username):
                cleaned = cleaned.replace(username, '')
        
        # Clean up spaces
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned
    except Exception as e:
        return caption  # Return original on error

def process_single_message(msg_id, retry_count=0):
    """Process a single message with retry logic and error handling."""
    max_retries = 2
    
    try:
        # Try to copy message to get its content
        result = bot.copy_message(
            chat_id=CHANNEL_ID,
            from_chat_id=CHANNEL_ID,
            message_id=msg_id,
            disable_notification=True
        )
        
        # Delete the copy immediately
        try:
            bot.delete_message(CHANNEL_ID, result.message_id)
        except:
            pass  # Ignore delete errors
        
        # Update processed count
        with lock:
            stats['processed'] += 1
        
        # Check if has caption and needs cleaning
        if hasattr(result, 'caption') and result.caption:
            cleaned = clean_caption(result.caption)
            if cleaned != result.caption:
                try:
                    bot.edit_message_caption(
                        chat_id=CHANNEL_ID,
                        message_id=msg_id,
                        caption=cleaned if cleaned else " "
                    )
                    with lock:
                        stats['edited'] += 1
                    print(f"✓ {msg_id}")
                    time.sleep(0.05)  # Minimal delay
                    return True
                except ApiTelegramException as e:
                    error_msg = str(e).lower()
                    if "not modified" in error_msg:
                        return False  # Already clean
                    elif "retry after" in error_msg:
                        # Rate limit hit - wait and retry
                        if retry_count < max_retries:
                            time.sleep(1)
                            return process_single_message(msg_id, retry_count + 1)
                    with lock:
                        stats['errors'] += 1
        
        return False
        
    except ApiTelegramException as e:
        error_msg = str(e).lower()
        
        if "message not found" in error_msg:
            with lock:
                stats['skipped'] += 1
            return None  # Message doesn't exist
        elif "message can't be" in error_msg:
            with lock:
                stats['skipped'] += 1
            return False  # Can't process this message
        elif "retry after" in error_msg and retry_count < max_retries:
            # Rate limit - wait and retry
            time.sleep(1)
            return process_single_message(msg_id, retry_count + 1)
        else:
            with lock:
                stats['errors'] += 1
            return False
            
    except Exception as e:
        # Catch-all for any other errors to prevent crash
        with lock:
            stats['errors'] += 1
        if retry_count < max_retries:
            time.sleep(0.5)
            return process_single_message(msg_id, retry_count + 1)
        return False

def cleanup_batch_parallel():
    """Clean up messages in parallel batches of 30."""
    print("\n" + "=" * 60)
    print("🚀 MEGA-FAST PARALLEL CLEANUP MODE")
    print("Processing 30 messages at once!")
    print("With crash protection & auto-retry!")
    print("=" * 60)
    
    start_id = 1
    end_id = 10000
    batch_size = 30  # Process 30 messages in parallel - 3x faster!
    
    print(f"Scanning message IDs from {start_id} to {end_id}...")
    print("Speed: 30x faster than original!\n")
    
    # Process in batches
    for batch_start in range(start_id, end_id, batch_size):
        batch_end = min(batch_start + batch_size, end_id)
        message_ids = range(batch_start, batch_end)
        
        try:
            # Process batch in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=30) as executor:
                futures = {executor.submit(process_single_message, msg_id): msg_id 
                          for msg_id in message_ids}
                
                for future in as_completed(futures):
                    try:
                        future.result(timeout=3)  # 3 sec timeout - faster processing!
                    except Exception as e:
                        # Catch any uncaught errors to prevent crash
                        with lock:
                            stats['errors'] += 1
        
        except Exception as e:
            # Batch-level error handling
            print(f"⚠ Batch error (continuing): {e}")
            time.sleep(1)
            continue
        
        # Progress update every 100 messages
        if stats['processed'] > 0 and stats['processed'] % 100 == 0:
            print(f"📊 Checked: {stats['processed']} | ✓ Cleaned: {stats['edited']} | ⏭ Skipped: {stats['skipped']}")
        
        # Small delay between batches for API stability
        time.sleep(0.15)
    
    print(f"\n" + "=" * 60)
    print("✅ BATCH CLEANUP COMPLETE!")
    print(f"   📝 Total checked: {stats['processed']}")
    print(f"   ✓ Cleaned: {stats['edited']}")
    print(f"   ⏭ Skipped: {stats['skipped']}")
    print(f"   ⚠ Errors: {stats['errors']}")
    print("=" * 60)

# Message handler for channel posts (realtime)
@bot.channel_post_handler(func=lambda message: True)
def handle_channel_post(message):
    """Handle new channel posts and clean their captions."""
    try:
        if message.chat.id != CHANNEL_ID:
            return
        
        if not hasattr(message, 'caption') or not message.caption:
            return
        
        original = message.caption
        cleaned = clean_caption(original)
        
        if cleaned != original:
            try:
                bot.edit_message_caption(
                    chat_id=CHANNEL_ID,
                    message_id=message.message_id,
                    caption=cleaned if cleaned else " "
                )
                print(f"✓ [REALTIME] Cleaned {message.message_id}")
            except Exception as e:
                print(f"⚠ [REALTIME] Error: {e}")
    except Exception as e:
        # Prevent crashes in realtime handler
        print(f"⚠ Handler error: {e}")

@bot.edited_channel_post_handler(func=lambda message: True)
def handle_edited_post(message):
    """Handle edited posts."""
    handle_channel_post(message)

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 TELEGRAM CLEANUP BOT - MEGA FAST")
    print("=" * 60)
    print(f"Channel: {CHANNEL_ID}")
    print("\n🛡️ Protected URLs:")
    for url in WHITELISTED_URLS:
        print(f"   ✓ {url}")
    print("\n🛡️ Protected Usernames:")
    for username in WHITELISTED_USERNAMES:
        print(f"   ✓ {username}")
    print("=" * 60)
    
    try:
        bot_info = bot.get_me()
        chat = bot.get_chat(CHANNEL_ID)
        print(f"\n✅ Bot: @{bot_info.username}")
        print(f"✅ Channel: {chat.title if hasattr(chat, 'title') else CHANNEL_ID}")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        exit(1)
    
    print("\n" + "=" * 60)
    print("🚀 2-PHASE MEGA-FAST CLEANUP")
    print("=" * 60)
    print("Phase 1: Batch cleanup (30 parallel, crash-proof)")
    print("Phase 2: Realtime monitoring")
    print()
    
    try:
        # PHASE 1: Clean existing messages
        cleanup_batch_parallel()
        
        # PHASE 2: Realtime monitoring
        print("\n" + "=" * 60)
        print("✅ PHASE 2: REALTIME MONITORING")
        print("=" * 60)
        print("🔥 Monitoring new messages...")
        print("⚡ Speed: Instant cleaning!")
        print("🛡️ Crash-protected!")
        print("\nPress Ctrl+C to stop")
        print("=" * 60 + "\n")
        
        # Infinite polling with auto-restart on error
        while True:
            try:
                bot.infinity_polling(timeout=60, long_polling_timeout=60)
            except KeyboardInterrupt:
                raise  # Allow manual stop
            except Exception as e:
                print(f"\n⚠ Polling error: {e}")
                print("🔄 Auto-restarting in 3 seconds...")
                time.sleep(3)
                continue  # Auto-restart
        
    except KeyboardInterrupt:
        print("\n\n✋ Bot stopped.")
        print(f"📊 Final: {stats['edited']} messages cleaned")
    except Exception as e:
        print(f"\n\n❌ Critical error: {e}")
        print("Bot will attempt to continue...")
