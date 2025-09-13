# ============================================================================
# PERMISSION SYSTEM EXTENSION FOR ULTIMATE TIME TRACKER
# ============================================================================

"""
Add this to your existing ultimate time tracker module, or create a separate
permissions.py file and import these methods into your main tracker class.

This extends the UltimateTimeTracker class with permission management capabilities.
"""

import json
from datetime import datetime
from typing import Dict, List, Any, Optional

class PermissionMixin:
    """Mixin class to add permission functionality to UltimateTimeTracker"""
    
    def _get_permissions_key(self, server_id: int) -> str:
        """Get Redis key for server permissions"""
        return f"permissions:{server_id}"
    
    async def get_server_permissions(self, server_id: int) -> Dict[str, Any]:
        """Get server permission settings"""
        self._ensure_connected()
        perms_key = self._get_permissions_key(server_id)
        perms_data = await self.redis.hgetall(perms_key)
        
        return {
            "required_roles": json.loads(perms_data.get("required_roles", "[]")),
            "suspended_users": json.loads(perms_data.get("suspended_users", "[]")),
            "admin_roles": json.loads(perms_data.get("admin_roles", "[]")),
            "enabled": perms_data.get("enabled", "true") == "true",
            "created_at": perms_data.get("created_at", datetime.now().isoformat()),
            "updated_at": perms_data.get("updated_at", datetime.now().isoformat())
        }
    
    async def save_server_permissions(self, server_id: int, permissions: Dict[str, Any]) -> None:
        """Save server permission settings atomically"""
        self._ensure_connected()
        perms_key = self._get_permissions_key(server_id)
        
        # Validate permission data
        if not isinstance(permissions.get("required_roles", []), list):
            raise ValidationError("required_roles must be a list")
        if not isinstance(permissions.get("suspended_users", []), list):
            raise ValidationError("suspended_users must be a list")
        if not isinstance(permissions.get("admin_roles", []), list):
            raise ValidationError("admin_roles must be a list")
        
        # Prepare data for storage
        perms_data = {
            "required_roles": json.dumps(permissions.get("required_roles", [])),
            "suspended_users": json.dumps(permissions.get("suspended_users", [])),
            "admin_roles": json.dumps(permissions.get("admin_roles", [])),
            "enabled": str(permissions.get("enabled", True)).lower(),
            "created_at": permissions.get("created_at", datetime.now().isoformat()),
            "updated_at": datetime.now().isoformat()
        }
        
        # Atomic save using pipeline
        async with self.redis.pipeline(transaction=True) as pipe:
            await pipe.hset(perms_key, mapping=perms_data)
            await pipe.expire(perms_key, 86400 * 365)  # 1 year expiry
            await pipe.execute()
        
        # Clear related caches
        cache_key = f"permissions:{server_id}"
        if hasattr(self, 'permission_cache'):
            self.permission_cache.pop(cache_key, None)
    
    async def is_user_suspended(self, server_id: int, user_id: int) -> bool:
        """Check if a user is suspended"""
        permissions = await self.get_server_permissions(server_id)
        return user_id in permissions["suspended_users"]
    
    async def suspend_user(self, server_id: int, user_id: int) -> bool:
        """
        Suspend a user from time tracking
        Returns True if user was suspended, False if already suspended
        """
        self._validate_ids(server_id, user_id)
        
        permissions = await self.get_server_permissions(server_id)
        
        if user_id in permissions["suspended_users"]:
            return False  # Already suspended
        
        permissions["suspended_users"].append(user_id)
        await self.save_server_permissions(server_id, permissions)
        
        # Force clock out the user if they're currently clocked in
        session_key = self._get_session_key(server_id, user_id)
        await self.redis.delete(session_key)
        
        logger.info(f"User {user_id} suspended in server {server_id}")
        return True
    
    async def unsuspend_user(self, server_id: int, user_id: int) -> bool:
        """
        Unsuspend a user from time tracking
        Returns True if user was unsuspended, False if not suspended
        """
        self._validate_ids(server_id, user_id)
        
        permissions = await self.get_server_permissions(server_id)
        
        if user_id not in permissions["suspended_users"]:
            return False  # Not suspended
        
        permissions["suspended_users"].remove(user_id)
        await self.save_server_permissions(server_id, permissions)
        
        logger.info(f"User {user_id} unsuspended in server {server_id}")
        return True
    
    async def add_required_role(self, server_id: int, role_id: int) -> bool:
        """
        Add a required role for time tracking
        Returns True if role was added, False if already required
        """
        if not isinstance(role_id, int) or role_id <= 0:
            raise ValidationError("Role ID must be a positive integer")
        
        permissions = await self.get_server_permissions(server_id)
        
        if role_id in permissions["required_roles"]:
            return False  # Already required
        
        permissions["required_roles"].append(role_id)
        await self.save_server_permissions(server_id, permissions)
        
        logger.info(f"Required role {role_id} added to server {server_id}")
        return True
    
    async def remove_required_role(self, server_id: int, role_id: int) -> bool:
        """
        Remove a required role for time tracking
        Returns True if role was removed, False if not required
        """
        if not isinstance(role_id, int) or role_id <= 0:
            raise ValidationError("Role ID must be a positive integer")
        
        permissions = await self.get_server_permissions(server_id)
        
        if role_id not in permissions["required_roles"]:
            return False  # Not required
        
        permissions["required_roles"].remove(role_id)
        await self.save_server_permissions(server_id, permissions)
        
        logger.info(f"Required role {role_id} removed from server {server_id}")
        return True
    
    async def set_system_enabled(self, server_id: int, enabled: bool) -> None:
        """Enable or disable the time tracking system for a server"""
        permissions = await self.get_server_permissions(server_id)
        permissions["enabled"] = enabled
        await self.save_server_permissions(server_id, permissions)
        
        if not enabled:
            # Force clock out all users when disabling
            session_pattern = f"session:{server_id}:*"
            session_keys = await self.redis.keys(session_pattern)
            if session_keys:
                await self.redis.delete(*session_keys)
        
        status = "enabled" if enabled else "disabled"
        logger.info(f"Time tracking {status} for server {server_id}")
    
    async def is_system_enabled(self, server_id: int) -> bool:
        """Check if time tracking is enabled for a server"""
        permissions = await self.get_server_permissions(server_id)
        return permissions["enabled"]
    
    async def check_user_access(self, server_id: int, user_id: int, user_role_ids: List[int]) -> tuple[bool, str]:
        """
        Comprehensive permission check for a user
        
        Args:
            server_id: Discord server ID
            user_id: Discord user ID  
            user_role_ids: List of role IDs the user has
            
        Returns:
            (can_access, reason_if_denied)
        """
        self._validate_ids(server_id, user_id)
        
        permissions = await self.get_server_permissions(server_id)
        
        # Check if system is enabled
        if not permissions["enabled"]:
            return False, "Time tracking is currently disabled on this server."
        
        # Check if user is suspended
        if user_id in permissions["suspended_users"]:
            return False, "You are suspended from using time tracking commands."
        
        # Check role requirements
        if permissions["required_roles"]:
            if not any(role_id in user_role_ids for role_id in permissions["required_roles"]):
                return False, "You don't have the required role to use time tracking."
        
        return True, ""
    
    async def get_permission_stats(self, server_id: int) -> Dict[str, Any]:
        """Get permission-related statistics for a server"""
        permissions = await self.get_server_permissions(server_id)
        
        return {
            "system_enabled": permissions["enabled"],
            "required_roles_count": len(permissions["required_roles"]),
            "suspended_users_count": len(permissions["suspended_users"]),
            "admin_roles_count": len(permissions["admin_roles"]),
            "required_roles": permissions["required_roles"],
            "suspended_users": permissions["suspended_users"],
            "created_at": permissions["created_at"],
            "updated_at": permissions["updated_at"]
        }
    
    async def bulk_suspend_users(self, server_id: int, user_ids: List[int]) -> Dict[str, int]:
        """
        Suspend multiple users at once
        
        Returns:
            Dict with counts of suspended, already_suspended, and errors
        """
        results = {"suspended": 0, "already_suspended": 0, "errors": 0}
        
        permissions = await self.get_server_permissions(server_id)
        
        for user_id in user_ids:
            try:
                if user_id in permissions["suspended_users"]:
                    results["already_suspended"] += 1
                else:
                    permissions["suspended_users"].append(user_id)
                    results["suspended"] += 1
                    
                    # Force clock out
                    session_key = self._get_session_key(server_id, user_id)
                    await self.redis.delete(session_key)
                    
            except Exception as e:
                logger.error(f"Error suspending user {user_id}: {e}")
                results["errors"] += 1
        
        if results["suspended"] > 0:
            await self.save_server_permissions(server_id, permissions)
        
        logger.info(f"Bulk suspend in server {server_id}: {results}")
        return results
    
    async def bulk_unsuspend_users(self, server_id: int, user_ids: List[int]) -> Dict[str, int]:
        """
        Unsuspend multiple users at once
        
        Returns:
            Dict with counts of unsuspended, not_suspended, and errors
        """
        results = {"unsuspended": 0, "not_suspended": 0, "errors": 0}
        
        permissions = await self.get_server_permissions(server_id)
        
        for user_id in user_ids:
            try:
                if user_id not in permissions["suspended_users"]:
                    results["not_suspended"] += 1
                else:
                    permissions["suspended_users"].remove(user_id)
                    results["unsuspended"] += 1
                    
            except Exception as e:
                logger.error(f"Error unsuspending user {user_id}: {e}")
                results["errors"] += 1
        
        if results["unsuspended"] > 0:
            await self.save_server_permissions(server_id, permissions)
        
        logger.info(f"Bulk unsuspend in server {server_id}: {results}")
        return results
    
    async def cleanup_invalid_permissions(self, server_id: int, valid_role_ids: List[int], valid_user_ids: List[int]) -> Dict[str, int]:
        """
        Clean up permissions for roles/users that no longer exist
        
        Args:
            server_id: Discord server ID
            valid_role_ids: List of role IDs that still exist
            valid_user_ids: List of user IDs that are still in the server
            
        Returns:
            Dict with cleanup statistics
        """
        permissions = await self.get_server_permissions(server_id)
        
        # Clean up roles
        old_required_roles = permissions["required_roles"].copy()
        permissions["required_roles"] = [
            role_id for role_id in permissions["required_roles"] 
            if role_id in valid_role_ids
        ]
        removed_roles = len(old_required_roles) - len(permissions["required_roles"])
        
        # Clean up suspended users (optional - you might want to keep suspensions)
        old_suspended_users = permissions["suspended_users"].copy()
        permissions["suspended_users"] = [
            user_id for user_id in permissions["suspended_users"] 
            if user_id in valid_user_ids
        ]
        removed_users = len(old_suspended_users) - len(permissions["suspended_users"])
        
        # Save if anything changed
        if removed_roles > 0 or removed_users > 0:
            await self.save_server_permissions(server_id, permissions)
        
        results = {
            "removed_roles": removed_roles,
            "removed_suspended_users": removed_users,
            "total_changes": removed_roles + removed_users
        }
        
        if results["total_changes"] > 0:
            logger.info(f"Cleaned up permissions for server {server_id}: {results}")
        
        return results


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

async def permission_examples():
    """Example usage of the permission system"""
    
    async with UltimateTimeTracker() as tracker:
        SERVER_ID = 123456789
        USER_ID = 111111
        ROLE_ID = 222222
        
        # Enable/disable system
        await tracker.set_system_enabled(SERVER_ID, True)
        print(f"System enabled: {await tracker.is_system_enabled(SERVER_ID)}")
        
        # Manage required roles
        await tracker.add_required_role(SERVER_ID, ROLE_ID)
        print("Added required role")
        
        # Suspend/unsuspend users
        await tracker.suspend_user(SERVER_ID, USER_ID)
        print(f"User suspended: {await tracker.is_user_suspended(SERVER_ID, USER_ID)}")
        
        await tracker.unsuspend_user(SERVER_ID, USER_ID)
        print(f"User suspended: {await tracker.is_user_suspended(SERVER_ID, USER_ID)}")
        
        # Check user access
        user_roles = [ROLE_ID, 333333]  # User's role IDs
        can_access, reason = await tracker.check_user_access(SERVER_ID, USER_ID, user_roles)
        print(f"Can access: {can_access}, Reason: {reason}")
        
        # Get permission stats
        stats = await tracker.get_permission_stats(SERVER_ID)
        print(f"Permission stats: {stats}")


if __name__ == "__main__":
    asyncio.run(permission_examples())