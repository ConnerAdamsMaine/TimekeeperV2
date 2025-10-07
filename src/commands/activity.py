import discord
from discord.ext import commands
from discord import app_commands
import logging
from typing import Optional
import asyncio
from datetime import datetime
import json

from Utils.timekeeper import get_shared_role_tracker

logger = logging.getLogger(__name__)


class ActivityLogCog(commands.Cog):
    """Activity logging for time tracking events"""
    
    def __init__(self, bot):
        self.bot = bot
        self.tracker = None
        self.clock = None
        self.activity_channels = {}  # guild_id: channel_id
        logger.info("ActivityLogCog initialized")
    
    async def cog_load(self):
        try:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
            
            # Load saved activity channels
            await self._load_activity_channels()
            
            logger.info("ActivityLogCog connected to tracker system")
        except Exception as e:
            logger.error(f"Failed to initialize activity log system: {e}")
    
    async def _load_activity_channels(self):
        """Load activity channel settings from Redis"""
        try:
            pattern = "activity_channel:*"
            cursor = 0
            
            while True:
                cursor, keys = await self.tracker.redis.scan(cursor, match=pattern, count=100)
                
                for key in keys:
                    guild_id = int(key.decode('utf-8').split(':')[1])
                    channel_id_bytes = await self.tracker.redis.get(key)
                    if channel_id_bytes:
                        channel_id = int(channel_id_bytes)
                        self.activity_channels[guild_id] = channel_id
                
                if cursor == 0:
                    break
            
            logger.info(f"Loaded {len(self.activity_channels)} activity channels")
        except Exception as e:
            logger.error(f"Error loading activity channels: {e}")
    
    async def _save_activity_channel(self, guild_id: int, channel_id: int):
        """Save activity channel setting to Redis"""
        try:
            key = f"activity_channel:{guild_id}"
            await self.tracker.redis.set(key, str(channel_id))
            self.activity_channels[guild_id] = channel_id
        except Exception as e:
            logger.error(f"Error saving activity channel: {e}")
    
    async def _remove_activity_channel(self, guild_id: int):
        """Remove activity channel setting"""
        try:
            key = f"activity_channel:{guild_id}"
            await self.tracker.redis.delete(key)
            self.activity_channels.pop(guild_id, None)
        except Exception as e:
            logger.error(f"Error removing activity channel: {e}")
    
    async def log_activity(self, guild_id: int, event_type: str, user: discord.Member, details: dict):
        """Log activity to the designated channel"""
        try:
            channel_id = self.activity_channels.get(guild_id)
            if not channel_id:
                return
            
            channel = self.bot.get_channel(channel_id)
            if not channel:
                logger.warning(f"Activity channel {channel_id} not found for guild {guild_id}")
                return
            
            # Create embed based on event type
            if event_type == "clock_in":
                embed = discord.Embed(
                    title="⏰ Clocked In",
                    color=discord.Color.green(),
                    timestamp=datetime.now()
                )
                embed.set_author(name=user.display_name, icon_url=user.display_avatar.url)
                embed.add_field(
                    name="Category",
                    value=f"`{details.get('category', 'Unknown')}`",
                    inline=True
                )
                embed.add_field(
                    name="Started",
                    value=f"<t:{int(datetime.now().timestamp())}:t>",
                    inline=True
                )
                
            elif event_type == "clock_out":
                embed = discord.Embed(
                    title="🛑 Clocked Out",
                    color=discord.Color.orange(),
                    timestamp=datetime.now()
                )
                embed.set_author(name=user.display_name, icon_url=user.display_avatar.url)
                embed.add_field(
                    name="Category",
                    value=f"`{details.get('category', 'Unknown')}`",
                    inline=True
                )
                embed.add_field(
                    name="Duration",
                    value=details.get('duration_formatted', 'Unknown'),
                    inline=True
                )
                
                # Add productivity insight if duration is significant
                duration_seconds = details.get('duration_seconds', 0)
                if duration_seconds >= 3600:  # 1 hour or more
                    hours = duration_seconds / 3600
                    if hours >= 4:
                        embed.add_field(
                            name="💪 Achievement",
                            value="Excellent deep work session!",
                            inline=False
                        )
                    elif hours >= 2:
                        embed.add_field(
                            name="✅ Achievement",
                            value="Great focused session!",
                            inline=False
                        )
            
            elif event_type == "force_clockout":
                embed = discord.Embed(
                    title="⚠️ Force Clocked Out",
                    color=discord.Color.red(),
                    timestamp=datetime.now()
                )
                embed.set_author(name=user.display_name, icon_url=user.display_avatar.url)
                embed.add_field(
                    name="Category",
                    value=f"`{details.get('category', 'Unknown')}`",
                    inline=True
                )
                embed.add_field(
                    name="Duration",
                    value=details.get('duration_formatted', 'Unknown'),
                    inline=True
                )
                if details.get('admin'):
                    embed.add_field(
                        name="Admin",
                        value=details['admin'],
                        inline=True
                    )
                if details.get('reason'):
                    embed.add_field(
                        name="Reason",
                        value=details['reason'],
                        inline=False
                    )
            
            # Send to channel
            await channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error logging activity: {e}")
    
    @app_commands.command(name="activitylog", description="⚙️ Configure activity logging channel (Admin only)")
    @app_commands.describe(
        action="Set or remove activity logging",
        channel="Channel to log activities to (for 'set' action)"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="Set Channel", value="set"),
        app_commands.Choice(name="Remove Channel", value="remove"),
        app_commands.Choice(name="Show Current", value="show")
    ])
    async def activity_log(
        self,
        interaction: discord.Interaction,
        action: str,
        channel: Optional[discord.TextChannel] = None
    ):
        """Configure activity logging"""
        await interaction.response.defer()
        
        try:
            # Check admin permissions
            if not interaction.user.guild_permissions.administrator:
                embed = discord.Embed(
                    title="🔒 Admin Only",
                    description="Only administrators can configure activity logging.",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed)
                return
            
            if not self.tracker:
                await self.cog_load()
            
            if action == "set":
                if not channel:
                    embed = discord.Embed(
                        title="❌ Missing Channel",
                        description="Please specify a channel for activity logging.",
                        color=discord.Color.red()
                    )
                    embed.add_field(
                        name="💡 Usage",
                        value="`/activitylog action:Set Channel channel:#activity-log`",
                        inline=False
                    )
                    await interaction.followup.send(embed=embed)
                    return
                
                # Check bot permissions in channel
                bot_member = interaction.guild.get_member(self.bot.user.id)
                permissions = channel.permissions_for(bot_member)
                
                if not permissions.send_messages or not permissions.embed_links:
                    embed = discord.Embed(
                        title="❌ Insufficient Permissions",
                        description=f"I don't have permission to send messages in {channel.mention}",
                        color=discord.Color.red()
                    )
                    embed.add_field(
                        name="Required Permissions",
                        value="• Send Messages\n• Embed Links",
                        inline=False
                    )
                    await interaction.followup.send(embed=embed)
                    return
                
                # Save the channel
                await self._save_activity_channel(interaction.guild.id, channel.id)
                
                embed = discord.Embed(
                    title="✅ Activity Logging Enabled",
                    description=f"Clock in/out activities will be logged to {channel.mention}",
                    color=discord.Color.green()
                )
                
                # Send test message to the channel
                test_embed = discord.Embed(
                    title="📊 Activity Logging Started",
                    description="This channel will now receive time tracking activity notifications.",
                    color=discord.Color.blue(),
                    timestamp=datetime.now()
                )
                test_embed.set_footer(text=f"Configured by {interaction.user.name}")
                
                await channel.send(embed=test_embed)
                
                logger.info(f"Activity logging enabled for guild {interaction.guild.id} in channel {channel.id}")
                
            elif action == "remove":
                if interaction.guild.id not in self.activity_channels:
                    embed = discord.Embed(
                        title="ℹ️ No Activity Channel",
                        description="Activity logging is not currently enabled for this server.",
                        color=discord.Color.blue()
                    )
                    await interaction.followup.send(embed=embed)
                    return
                
                await self._remove_activity_channel(interaction.guild.id)
                
                embed = discord.Embed(
                    title="✅ Activity Logging Disabled",
                    description="Activity logging has been disabled for this server.",
                    color=discord.Color.green()
                )
                
                logger.info(f"Activity logging disabled for guild {interaction.guild.id}")
                
            elif action == "show":
                channel_id = self.activity_channels.get(interaction.guild.id)
                
                if channel_id:
                    channel = self.bot.get_channel(channel_id)
                    if channel:
                        embed = discord.Embed(
                            title="📊 Activity Logging Status",
                            description="Activity logging is currently **enabled**",
                            color=discord.Color.blue()
                        )
                        embed.add_field(
                            name="Channel",
                            value=channel.mention,
                            inline=True
                        )
                    else:
                        embed = discord.Embed(
                            title="⚠️ Activity Logging Status",
                            description="Activity logging is enabled but the channel no longer exists.",
                            color=discord.Color.orange()
                        )
                        embed.add_field(
                            name="Action Needed",
                            value="Please set a new channel using `/activitylog action:Set Channel`",
                            inline=False
                        )
                else:
                    embed = discord.Embed(
                        title="📊 Activity Logging Status",
                        description="Activity logging is currently **disabled**",
                        color=discord.Color.gray()
                    )
                    embed.add_field(
                        name="Enable Logging",
                        value="Use `/activitylog action:Set Channel channel:#your-channel`",
                        inline=False
                    )
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in activitylog command: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed)


async def setup(bot):
    await bot.add_cog(ActivityLogCog(bot))
    logger.info("ActivityLogCog loaded successfully")