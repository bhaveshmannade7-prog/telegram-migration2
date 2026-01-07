import time
import logging
from collections import defaultdict

logger = logging.getLogger("spam_guard")

class SpamGuard:
    def __init__(self):
        # Memory structure: {user_id: [timestamp1, timestamp2, ...]}
        self.requests = defaultdict(list)
        # Blocked users: {user_id: unblock_timestamp}
        self.blocked_users = {}
        
        # Settings
        self.MAX_REQUESTS = 6      # 6 requests allowed...
        self.TIME_WINDOW = 10      # ...in 10 seconds (Strict limit)
        self.BLOCK_DURATION = 3600 # 1 Hour block time

    def check_user(self, user_id: int) -> dict:
        """
        Returns: {'status': 'ok'|'blocked'|'ban_now', 'remaining': int}
        """
        current_time = time.time()

        # 1. Check if already blocked
        if user_id in self.blocked_users:
            if current_time < self.blocked_users[user_id]:
                return {'status': 'blocked', 'remaining': int(self.blocked_users[user_id] - current_time)}
            else:
                del self.blocked_users[user_id] # Unblock automatically

        # 2. Cleanup old timestamps
        self.requests[user_id] = [t for t in self.requests[user_id] if current_time - t < self.TIME_WINDOW]

        # 3. Add new timestamp
        self.requests[user_id].append(current_time)

        # 4. Check Limit
        if len(self.requests[user_id]) > self.MAX_REQUESTS:
            self.blocked_users[user_id] = current_time + self.BLOCK_DURATION
            logger.warning(f"🚫 SPAM GUARD: Blocked User {user_id} for 1 hour.")
            return {'status': 'ban_now', 'remaining': self.BLOCK_DURATION}

        return {'status': 'ok'}

# Global Instance
spam_guard = SpamGuard()
              
