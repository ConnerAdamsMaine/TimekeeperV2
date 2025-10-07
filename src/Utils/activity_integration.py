"""
Activity logging integration module
This module provides functions to log clock in/out activities
"""

import logging
from typing import Optional, Dict, Any
import discord

logger = logging.getLogger(__name__)


async def log_clock_in_activity(bot, guild_id: int, user_id: int, category: str):
    """Log a clock in event to the activity channel"""
    try:
        # Get activity log cog
        activity_cog = bot.get_cog("ActivityLogCog")
        if not activity_cog:
            return
        
        # Get guild and user
        guild = bot.get_guild(guild_id)
        if not guild:
            return
        
        user = guild.get_member(user_id)
        if not user:
            return
        
        # Log the activity
        await activity_cog.log_activity(
            guild_id=guild_id,
            event_type="clock_in",
            user=user,
            details={'category': category}
        )
        
    except Exception as e:
        logger.error(f"Error logging clock in activity: {e}")


async def log_clock_out_activity(
    bot,
    guild_id: int,
    user_id: int,
    category: str,
    duration_seconds: int,
    duration_formatted: str,
    force: bool = False,
    admin_id: Optional[int] = None,
    reason: Optional[str] = None
):
    """Log a clock out event to the activity channel"""
    try:
        # Get activity log cog
        activity_cog = bot.get_cog("ActivityLogCog")
        if not activity_cog:
            return
        
        # Get guild and user
        guild = bot.get_guild(guild_id)
        if not guild:
            return
        
        user = guild.get_member(user_id)
        if not user:
            return
        
        # Prepare details
        details = {
            'category': category,
            'duration_seconds': duration_seconds,
            'duration_formatted': duration_formatted
        }
        
        # If force clockout, add admin info
        if force and admin_id:
            admin = guild.get_member(admin_id)
            if admin:
                details['admin'] = admin.mention
            
            if reason:
                details['reason'] = reason
        
        # Log the activity
        event_type = "force_clockout" if force else "clock_out"
        await activity_cog.log_activity(
            guild_id=guild_id,
            event_type=event_type,
            user=user,
            details=details
        )
        
    except Exception as e:
        logger.error(f"Error logging clock out activity: {e}")