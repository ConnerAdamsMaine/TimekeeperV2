import asyncio
import redis.asyncio as redis
from typing import Dict, Optional, List, Set, Any
import logging
from datetime import datetime
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TimeTrackerError(Exception):
    """Base exception for TimeTracker errors"""
    pass


class ConnectionError(TimeTrackerError):
    """Raised when Redis connection fails"""
    pass


class TimeTracker:
    """
    Async time tracker for Discord servers using Redis with table-like structure
    
    Data Structure:
        server:{server_id}:users = {
            "user_id": '{"category1": seconds, "category2": seconds, ...}',
            ...
        }
        
        server:{server_id}:metadata = {
            "categories": '["cat1", "cat2", ...]',
            "created_at": "ISO_timestamp",
            "updated_at": "ISO_timestamp"
        }
    """
    
    def __init__(self, redis_url: Optional[str] = None):
        """
        Initialize TimeTracker
        
        Args:
            redis_url: Redis connection URL. If None, loads from REDIS_URL env var
        """
        self.redis_url = redis_url or os.getenv('REDIS_URL')
        self.redis_password = os.getenv('REDIS_PASSWORD')
        if not self.redis_url:
            raise TimeTrackerError("Redis URL not provided. Set REDIS_URL environment variable or pass redis_url parameter")
        
        self.redis: Optional[redis.Redis] = None
        self._connected = False
    
    async def connect(self):
        """Connect to Redis"""
        try:
            login = f"redis://{self.redis_url}?password={self.redis_password}" if self.redis_password else f"redis://{self.redis_url}"
            self.redis = redis.from_url(login, decode_responses=True)
            # Test connection
            await self.redis.ping()
            self._connected = True
            logger.info("Connected to Redis successfully")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Redis: {e}")
    
    async def close(self):
        """Close Redis connection"""
        if self.redis:
            await self.redis.aclose()
            self._connected = False
            logger.info("Redis connection closed")
    
    def _ensure_connected(self):
        """Ensure Redis is connected"""
        if not self._connected or not self.redis:
            raise ConnectionError("Not connected to Redis. Call connect() first.")
    
    def _get_server_hash_key(self, server_id: int) -> str:
        """Get Redis hash key for a server's user data"""
        return f"server:{server_id}:users"
    
    def _get_server_metadata_key(self, server_id: int) -> str:
        """Get Redis key for server metadata (categories, etc.)"""
        return f"server:{server_id}:metadata"
    
    async def _get_user_data(self, server_id: int, user_id: int) -> Dict[str, int]:
        """Get user's category data as dict"""
        self._ensure_connected()
        server_key = self._get_server_hash_key(server_id)
        user_data_json = await self.redis.hget(server_key, str(user_id))
        
        if user_data_json:
            try:
                return json.loads(user_data_json)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON data for user {user_id} in server {server_id}")
                return {}
        return {}
    
    async def _set_user_data(self, server_id: int, user_id: int, data: Dict[str, int]):
        """Set user's category data"""
        self._ensure_connected()
        server_key = self._get_server_hash_key(server_id)
        await self.redis.hset(server_key, str(user_id), json.dumps(data))
    
    async def _ensure_server_metadata(self, server_id: int, categories: Set[str] = None):
        """Ensure server metadata exists and update categories"""
        self._ensure_connected()
        metadata_key = self._get_server_metadata_key(server_id)
        
        # Get existing categories
        existing_data = await self.redis.hgetall(metadata_key)
        if existing_data and 'categories' in existing_data:
            try:
                existing_categories = set(json.loads(existing_data['categories']))
            except json.JSONDecodeError:
                existing_categories = set()
        else:
            existing_categories = set()
        
        # Combine with new categories
        if categories:
            all_categories = existing_categories | categories
        else:
            all_categories = existing_categories or {'general'}  # Default category
        
        # Update metadata if categories changed
        if all_categories != existing_categories:
            metadata = {
                'categories': json.dumps(list(all_categories)),
                'created_at': existing_data.get('created_at', datetime.now().isoformat()),
                'updated_at': datetime.now().isoformat()
            }
            await self.redis.hset(metadata_key, mapping=metadata)
    
    # Public API Methods
    async def add_time(self, server_id: int, user_id: int, category: str, seconds: int) -> int:
        """
        Add time to a category for a user in a server
        
        Args:
            server_id: Discord server ID
            user_id: Discord user ID
            category: Time category (e.g., "work", "study")
            seconds: Time to add in seconds
            
        Returns:
            New total time for that category
        """
        await self._ensure_server_metadata(server_id, {category})
        
        # Get current user data
        user_data = await self._get_user_data(server_id, user_id)
        
        # Add time to category
        current_time = user_data.get(category, 0)
        new_time = current_time + seconds
        user_data[category] = new_time
        
        # Save updated data
        await self._set_user_data(server_id, user_id, user_data)
        
        return new_time
    
    async def get_user_times(self, server_id: int, user_id: int) -> Dict[str, int]:
        """
        Get all category times for a user in a server
        
        Returns:
            Dict mapping category names to seconds
        """
        user_data = await self._get_user_data(server_id, user_id)
        categories = await self.get_server_categories(server_id)
        
        # Ensure all categories are present (fill missing with 0)
        result = {}
        for category in categories:
            result[category] = user_data.get(category, 0)
        
        return result
    
    async def set_user_time(self, server_id: int, user_id: int, category: str, seconds: int) -> None:
        """Set a specific category time for a user (overwrite)"""
        await self._ensure_server_metadata(server_id, {category})
        
        # Get current user data
        user_data = await self._get_user_data(server_id, user_id)
        
        # Set category time
        user_data[category] = seconds
        
        # Save updated data
        await self._set_user_data(server_id, user_id, user_data)
    
    async def get_user_total(self, server_id: int, user_id: int) -> int:
        """Get total time across all categories for a user"""
        times = await self.get_user_times(server_id, user_id)
        return sum(times.values())
    
    async def delete_user(self, server_id: int, user_id: int) -> bool:
        """Delete all time data for a user in a server"""
        self._ensure_connected()
        server_key = self._get_server_hash_key(server_id)
        
        # Delete the user's field from the hash
        deleted_count = await self.redis.hdel(server_key, str(user_id))
        return deleted_count > 0
    
    async def reset_category(self, server_id: int, user_id: int, category: str) -> None:
        """Reset a specific category to 0 for a user"""
        # Get current user data
        user_data = await self._get_user_data(server_id, user_id)
        
        # Remove the category
        if category in user_data:
            del user_data[category]
            
        # Save updated data
        await self._set_user_data(server_id, user_id, user_data)
    
    async def get_server_categories(self, server_id: int) -> Set[str]:
        """Get all categories used in a server"""
        self._ensure_connected()
        metadata_key = self._get_server_metadata_key(server_id)
        categories_json = await self.redis.hget(metadata_key, 'categories')
        
        if categories_json:
            try:
                return set(json.loads(categories_json))
            except json.JSONDecodeError:
                pass
        return set()
    
    async def get_all_users_in_server(self, server_id: int) -> Dict[int, Dict[str, int]]:
        """Get time data for all users in a server"""
        self._ensure_connected()
        server_key = self._get_server_hash_key(server_id)
        categories = await self.get_server_categories(server_id)
        
        if not categories:
            return {}
        
        # Get all user data from server hash
        all_data = await self.redis.hgetall(server_key)
        
        # Parse user data
        server_data = {}
        for user_id_str, user_data_json in all_data.items():
            try:
                user_id = int(user_id_str)
                user_times = json.loads(user_data_json)
                
                # Ensure all categories are present (fill missing with 0)
                complete_user_times = {}
                for category in categories:
                    complete_user_times[category] = user_times.get(category, 0)
                
                server_data[user_id] = complete_user_times
            except (ValueError, json.JSONDecodeError):
                # Skip malformed data
                continue
        
        return server_data
    
    async def get_server_leaderboard(self, server_id: int, category: str = None, limit: int = 10) -> List[Dict]:
        """
        Get leaderboard for a server
        
        Args:
            server_id: Discord server ID
            category: Specific category to rank by (None for total time)
            limit: Maximum number of users to return
            
        Returns:
            List of dicts with user_id, time, and categories
        """
        all_users = await self.get_all_users_in_server(server_id)
        
        leaderboard = []
        for user_id, categories_data in all_users.items():
            if category:
                # Rank by specific category
                time_value = categories_data.get(category, 0)
            else:
                # Rank by total time
                time_value = sum(categories_data.values())
            
            if time_value > 0:  # Only include users with time logged
                leaderboard.append({
                    "user_id": user_id,
                    "time": time_value,
                    "categories": categories_data
                })
        
        leaderboard.sort(key=lambda x: x["time"], reverse=True)
        return leaderboard[:limit]
    
    async def delete_server_data(self, server_id: int) -> int:
        """Delete all data for a server (when bot leaves server)"""
        self._ensure_connected()
        server_key = self._get_server_hash_key(server_id)
        metadata_key = self._get_server_metadata_key(server_id)
        
        # Count existing users (each user is one field now)
        user_count = await self.redis.hlen(server_key)
        
        # Delete server data and metadata
        await self.redis.delete(server_key, metadata_key)
        
        logger.info(f"Deleted {user_count} user records for server {server_id}")
        return user_count
    
    async def get_server_stats(self, server_id: int) -> Dict[str, Any]:
        """Get comprehensive stats for a server"""
        categories = await self.get_server_categories(server_id)
        all_users = await self.get_all_users_in_server(server_id)
        
        if not all_users:
            return {
                "total_users": 0,
                "total_time": 0,
                "categories": list(categories),
                "category_totals": {},
                "active_users": 0
            }
        
        # Calculate stats
        total_time = 0
        category_totals = {cat: 0 for cat in categories}
        active_users = 0
        
        for user_times in all_users.values():
            user_total = sum(user_times.values())
            if user_total > 0:
                active_users += 1
            total_time += user_total
            
            for category, time in user_times.items():
                category_totals[category] += time
        
        return {
            "total_users": len(all_users),
            "total_time": total_time,
            "categories": list(categories),
            "category_totals": category_totals,
            "active_users": active_users
        }
    
    async def bulk_add_time(self, operations: List[Dict]) -> List[int]:
        """
        Bulk add time for multiple users/categories
        
        Args:
            operations: List of dicts with server_id, user_id, category, seconds
            
        Returns:
            List of new totals for each operation
        """
        results = []
        
        # Group operations by server for efficiency
        by_server = {}
        for op in operations:
            server_id = op["server_id"]
            if server_id not in by_server:
                by_server[server_id] = []
            by_server[server_id].append(op)
        
        # Process each server's operations
        for server_id, server_ops in by_server.items():
            # Ensure all categories exist
            categories = {op["category"] for op in server_ops}
            await self._ensure_server_metadata(server_id, categories)
            
            # Process operations for this server
            server_results = await asyncio.gather(*[
                self.add_time(op["server_id"], op["user_id"], op["category"], op["seconds"])
                for op in server_ops
            ])
            results.extend(server_results)
        
        return results


class ClockManager:
    """
    High-level async manager for Discord clock-in/clock-out functionality
    """
    
    def __init__(self, tracker: TimeTracker):
        self.tracker = tracker
        self.active_sessions: Dict[tuple, Dict] = {}  # (server_id, user_id) -> session_info
    
    @staticmethod
    def format_time(seconds: int) -> str:
        """Format seconds into human readable time"""
        if seconds < 0:
            return "0s"
        
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"
    
    async def clock_in(self, server_id: int, user_id: int, category: str) -> Dict:
        """
        Clock in a user to a category
        
        Returns:
            Dict with success status and message
        """
        session_key = (server_id, user_id)
        
        # Check if already clocked in
        if session_key in self.active_sessions:
            current_session = self.active_sessions[session_key]
            return {
                "success": False,
                "message": f"Already clocked into '{current_session['category']}' since {current_session['start_time'].strftime('%H:%M:%S')}"
            }
        
        # Start new session
        start_time = datetime.now()
        self.active_sessions[session_key] = {
            "category": category,
            "start_time": start_time,
            "start_timestamp": start_time.timestamp()
        }
        
        logger.info(f"User {user_id} clocked into '{category}' in server {server_id}")
        
        return {
            "success": True,
            "message": f"Clocked into '{category}' at {start_time.strftime('%H:%M:%S')}",
            "category": category,
            "start_time": start_time
        }
    
    async def clock_out(self, server_id: int, user_id: int) -> Dict:
        """
        Clock out a user and add the time to their total
        
        Returns:
            Dict with session summary
        """
        session_key = (server_id, user_id)
        
        # Check if clocked in
        if session_key not in self.active_sessions:
            return {
                "success": False,
                "message": "Not currently clocked in"
            }
        
        session = self.active_sessions.pop(session_key)
        end_time = datetime.now()
        
        # Calculate session duration
        duration_seconds = int(end_time.timestamp() - session["start_timestamp"])
        
        # Add time to database
        new_total = await self.tracker.add_time(
            server_id, user_id, session["category"], duration_seconds
        )
        
        logger.info(f"User {user_id} clocked out of '{session['category']}' after {duration_seconds}s in server {server_id}")
        
        return {
            "success": True,
            "category": session["category"],
            "session_duration": duration_seconds,
            "session_duration_formatted": self.format_time(duration_seconds),
            "category_total": new_total,
            "category_total_formatted": self.format_time(new_total),
            "start_time": session["start_time"],
            "end_time": end_time
        }
    
    async def get_status(self, server_id: int, user_id: int) -> Dict:
        """Get current status for a user"""
        session_key = (server_id, user_id)
        
        # Check if currently clocked in
        if session_key in self.active_sessions:
            session = self.active_sessions[session_key]
            current_duration = int(datetime.now().timestamp() - session["start_timestamp"])
            
            return {
                "clocked_in": True,
                "category": session["category"],
                "start_time": session["start_time"],
                "current_duration": current_duration,
                "current_duration_formatted": self.format_time(current_duration)
            }
        
        # Get stored time data
        times = await self.tracker.get_user_times(server_id, user_id)
        total_time = sum(times.values())
        
        return {
            "clocked_in": False,
            "total_time": total_time,
            "total_time_formatted": self.format_time(total_time),
            "categories": {cat: self.format_time(time) for cat, time in times.items()}
        }
    
    async def force_clock_out_all(self, server_id: int) -> List[Dict]:
        """Force clock out all users in a server (for bot restart, etc.)"""
        results = []
        sessions_to_remove = [(s_id, u_id) for (s_id, u_id) in self.active_sessions.keys() if s_id == server_id]
        
        for session_key in sessions_to_remove:
            server_id, user_id = session_key
            result = await self.clock_out(server_id, user_id)
            results.append({"user_id": user_id, **result})
        
        return results
    
    def get_active_sessions(self, server_id: int = None) -> Dict:
        """Get all active sessions, optionally filtered by server"""
        if server_id is None:
            return self.active_sessions.copy()
        
        return {
            key: session for key, session in self.active_sessions.items()
            if key[0] == server_id
        }


# Convenience functions
async def create_tracker(redis_url: Optional[str] = None) -> TimeTracker:
    """Create and connect a TimeTracker instance"""
    tracker = TimeTracker(redis_url)
    await tracker.connect()
    return tracker


async def create_clock_manager(redis_url: Optional[str] = None) -> tuple[TimeTracker, ClockManager]:
    """Create and connect TimeTracker and ClockManager instances"""
    tracker = await create_tracker(redis_url)
    clock = ClockManager(tracker)
    return tracker, clock


# Example usage function
async def example_usage():
    """Example showing how to use the module"""
    # Create tracker and clock manager
    tracker, clock = await create_clock_manager()
    
    # Simulate Discord server and users
    SERVER_ID = 123456789
    ALICE_ID = 111111
    BOB_ID = 222222
    
    try:
        print("=== Discord Time Tracker Module Example ===\n")
        
        # Clock in users
        result = await clock.clock_in(SERVER_ID, ALICE_ID, "work")
        print(f"Alice: {result['message']}")
        
        await asyncio.sleep(1)  # Simulate time passing
        
        result = await clock.clock_in(SERVER_ID, BOB_ID, "study")
        print(f"Bob: {result['message']}")
        
        # Wait a bit
        await asyncio.sleep(2)
        
        # Check status
        status = await clock.get_status(SERVER_ID, ALICE_ID)
        print(f"Alice status: Clocked into '{status['category']}' for {status['current_duration_formatted']}")
        
        # Clock out
        result = await clock.clock_out(SERVER_ID, ALICE_ID)
        print(f"Alice clocked out: {result['session_duration_formatted']} in {result['category']}")
        
        # Add some manual time
        await tracker.add_time(SERVER_ID, ALICE_ID, "work", 7200)  # 2 hours
        await tracker.add_time(SERVER_ID, BOB_ID, "study", 3600)  # 1 hour
        
        # Get leaderboard
        leaderboard = await tracker.get_server_leaderboard(SERVER_ID)
        print(f"\n=== Leaderboard ===")
        for i, entry in enumerate(leaderboard, 1):
            print(f"{i}. User {entry['user_id']}: {ClockManager.format_time(entry['time'])}")
        
        # Get server stats
        stats = await tracker.get_server_stats(SERVER_ID)
        print(f"\n=== Server Stats ===")
        print(f"Active users: {stats['active_users']}/{stats['total_users']}")
        print(f"Total time: {ClockManager.format_time(stats['total_time'])}")
        print(f"Categories: {', '.join(stats['categories'])}")
        
    finally:
        await tracker.close()


if __name__ == "__main__":
    asyncio.run(example_usage())