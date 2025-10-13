import os
import asyncio
import json
import time
from pyrogram import Client, errors
from pyrogram.types import Message
import firebase_admin
from firebase_admin import credentials, firestore
from algoliasearch.search_client import SearchClient

# --- CONFIGURATION (Loading from Replit Secrets) ---
try:
    API_ID = int(os.environ.get("TELEGRAM_API_ID"))
    API_HASH = os.environ.get("TELEGRAM_API_HASH") 
    BOT_TOKEN = os.environ.get("BOT_TOKEN")
    CHANNEL_ID = int(os.environ.get("CHANNEL_ID")) 
    
    ALGOLIA_APP_ID = os.environ.get("ALGOLIA_APP_ID")
    ALGOLIA_ADMIN_KEY = os.environ.get("ALGOLIA_ADMIN_KEY")
    ALGOLIA_INDEX_NAME = os.environ.get("ALGOLIA_INDEX_NAME")

    FIREBASE_PRIVATE_KEY_STR = os.environ.get("FIREBASE_PRIVATE_KEY")
    FIREBASE_PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID")

    if not all([API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID, ALGOLIA_ADMIN_KEY, FIREBASE_PRIVATE_KEY_STR, FIREBASE_PROJECT_ID]):
        raise ValueError("One or more essential environment variables are missing.")

except Exception as e:
    print(f"❌ CONFIG ERROR: Check Replit Secrets! Details: {e}")
    exit()

SESSION_NAME = "migration_session"
db = None
algolia_index = None
app = Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- INITIALIZATION ---
def initialize_services():
    global db, algolia_index
    try:
        # 1. Firebase Initialization
        with open("firebase_creds.json", 'r') as f:
            cred_dict = json.load(f)
        
        # Inject the Private Key from the secret, handling the newline characters
        cred_dict['private_key'] = FIREBASE_PRIVATE_KEY_STR.replace('\\n', '\n')
        
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred, {'projectId': FIREBASE_PROJECT_ID})
        db = firestore.client()
        print("✅ Firebase Initialized.")
        
        # 2. Algolia Initialization (Using Admin Key for Writing)
        algolia_client = SearchClient.create(ALGOLIA_APP_ID, ALGOLIA_ADMIN_KEY)
        algolia_index = algolia_client.init_index(ALGOLIA_INDEX_NAME)
        print("✅ Algolia Client Initialized.")
        
    except FileNotFoundError:
        print("❌ CRITICAL: 'firebase_creds.json' not found. Please create it and paste the service account data.")
        return
    except Exception as e:
        print(f"❌ ERROR: Service Initialization Failed: {e}")

def load_movie_data():
    """Loads the 1100+ movie data from the local JSON file."""
    try:
        with open('old_movies.json', 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        print(f"Loaded {len(json_data)} entries from old_movies.json.")
        return json_data
    except FileNotFoundError:
        print("❌ CRITICAL: 'old_movies.json' not found. Please upload your data file.")
        return []

# --- CORE MIGRATION LOGIC ---

async def find_message_id_and_sync():
    """
    Scans the channel, creates a file_id -> message_id map, and syncs data to DB/Algolia.
    """
    if not db or not algolia_index:
        return

    # Start Pyrogram client (triggers login on first run)
    await app.start()
    
    total_synced = 0
    total_failed = 0
    
    print("\n--- Starting Channel History Scan (Creating File ID Map)... ---")
    
    file_id_map = {}
    
    try:
        # Scan channel history up to 50,000 messages
        async for message in app.get_chat_history(CHANNEL_ID, limit=50000):
            if message.document or message.video:
                current_file_id = message.document.file_id if message.document else message.video.file_id
                if current_file_id:
                    file_id_map[current_file_id] = message.id
        
    except errors.UserNotParticipant:
        print(f"❌ ERROR: Bot/User is not a member of Channel ID {CHANNEL_ID}. Please add it as Admin!")
        await app.stop()
        return
    except Exception as e:
        print(f"Error during history scan: {e}")
        await app.stop()
        return

    print(f"\n✅ Found {len(file_id_map)} media posts in the channel history.")

    movie_list = load_movie_data()
    if not movie_list:
        await app.stop()
        return

    print("\n--- Starting Data Sync to Firebase/Algolia... ---")
    
    algolia_sync_data = []

    for idx, movie in enumerate(movie_list):
        if idx % 100 == 0:
            print(f"Processing... {idx}/{len(movie_list)} (Synced: {total_synced}, Failed: {total_failed})")

        file_id_to_check = movie.get('file_id')
        title = movie.get('title')
        
        if not file_id_to_check or not title:
            total_failed += 1
            continue

        message_id = file_id_map.get(file_id_to_check)

        if message_id:
            try:
                # 1. Check for duplicate post_id
                docs = await asyncio.to_thread(lambda: db.collection('movies').where('post_id', '==', message_id).limit(1).get())
                if docs:
                    total_synced += 1
                    continue
                
                # 2. Add to Firestore 
                doc_ref = await asyncio.to_thread(lambda: db.collection('movies').add({
                    "title": title.strip(),
                    "post_id": message_id,
                    "created_at": firestore.SERVER_TIMESTAMP
                }))
                
                doc_id = doc_ref[1].id

                # 3. Collect Algolia data for batch push
                algolia_sync_data.append({
                    "objectID": doc_id, 
                    "title": title.strip(),
                    "post_id": message_id
                })
                
                total_synced += 1
                
            except Exception as e:
                print(f"❌ DB/Algolia Sync Failed for {title}: {e}")
                total_failed += 1
        else:
            total_failed += 1

    # 4. Final Algolia Batch Push
    if algolia_sync_data:
        try:
            print(f"\nPushing {len(algolia_sync_data)} records to Algolia in batch...")
            await asyncio.to_thread(lambda: algolia_index.save_objects(algolia_sync_data))
            print(f"✅ Successfully pushed records to Algolia.")
        except Exception as e:
            print(f"❌ Final Algolia Batch Push Failed: {e}")

    await app.stop()
    print(f"\n--- Migration Complete ---")
    print(f"Total Entries Processed: {len(movie_list)}")
    print(f"Total Successfully Synced (Found message_id): {total_synced}")
    print(f"Total Failed/Skipped (File ID not found): {total_failed}")


if __name__ == "__main__":
    initialize_services()
    if db and algolia_index:
        asyncio.run(find_message_id_and_sync())
