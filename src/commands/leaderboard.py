import discord
from discord.ext import commands
from discord import app_commands
import logging
from typing import Optional, Dict, Any, List
import asyncio
from datetime import datetime, timedelta

# Import the enhanced shared tracker system
from Utils.timekeeper import (
    get_shared_role_tracker, 
    get_system_status,
    ValidationError,
    CategoryError,
    PermissionError,
    CircuitBreakerOpenError,
    TimeTrackerError
)

# Configure logging
logger = logging.getLogger(__name__)


class LeaderboardCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.tracker = None
        self.clock = None
        
        # Admin command metrics
        self.admin_metrics = {
            'categories_managed': 0,
            'leaderboard_requests': 0,
            'admin_commands': 0,
            'insights_generated': 0
        }
        
        logger.info("LeaderboardCog initialized")
    
    async def cog_load(self):
        """Initialize enterprise tracker when cog loads"""
        try:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
            logger.info("LeaderboardCog connected to enterprise tracker system")
        except Exception as e:
            logger.error(f"Failed to initialize timecard admin system: {e}")
    
    async def cog_unload(self):
        """Cleanup when cog unloads - shared tracker handled by main cog"""
        logger.info("LeaderboardCog unloaded")
    
    async def _ensure_initialized(self):
        """Ensure tracker is initialized"""
        if not self.tracker or not self.clock:
            try:
                self.tracker, self.clock = await get_shared_role_tracker(self.bot)
            except Exception as e:
                logger.error(f"Failed to reinitialize tracker: {e}")
                raise commands.CommandError("🔧 Admin system temporarily unavailable. Please try again in a moment.")
    
    def _create_error_embed(self, result: Dict[str, Any]) -> discord.Embed:
        """Create standardized error embed"""
        embed = discord.Embed(
            title="❌ Error",
            description=result['message'],
            color=discord.Color.red()
        )
        
        if result.get('error_code'):
            embed.add_field(
                name="Error Code",
                value=f"`{result['error_code']}`",
                inline=True
            )
        
        return embed
    
    def _create_generic_error_embed(self, error: Exception) -> discord.Embed:
        """Create generic error embed for unexpected errors"""
        embed = discord.Embed(
            title="❌ Unexpected Error",
            description="An unexpected error occurred. The team has been notified.",
            color=discord.Color.red()
        )
        embed.add_field(
            name="🆔 Error ID",
            value=f"`{hash(str(error)) % 100000:05d}`",
            inline=True
        )
        return embed
    
    # ========================================================================
    # LEADERBOARD COMMAND
    # ========================================================================
    
    @app_commands.command(name="leaderboard", description="🏆 Show time tracking leaderboard")
    @app_commands.describe(
        category="Specific category to show (optional)",
        timeframe="Time period to show (all, week, month)",
        limit="Number of top users to show (default: 10)"
    )
    @app_commands.choices(timeframe=[
        app_commands.Choice(name="All Time", value="all"),
        app_commands.Choice(name="This Week", value="week"),
        app_commands.Choice(name="This Month", value="month")
    ])
    async def leaderboard(self, interaction: discord.Interaction, 
                         category: Optional[str] = None, 
                         timeframe: Optional[str] = "all",
                         limit: Optional[int] = 10):
        """Enhanced leaderboard command"""
        await interaction.response.defer()
        
        try:
            await self._ensure_initialized()
            
            # Validate limit
            if limit < 1 or limit > 25:
                limit = 10
            
            # Validate category if provided
            if category:
                available_categories = await self.tracker.list_categories(interaction.guild.id)
                category = category.lower().strip()
                if category not in available_categories:
                    embed = discord.Embed(
                        title="❌ Invalid Category",
                        description=f"Category '{category}' not found.",
                        color=discord.Color.red()
                    )
                    embed.add_field(
                        name="📂 Available Categories",
                        value=", ".join(f"`{cat}`" for cat in sorted(available_categories)[:10]),
                        inline=False
                    )
                    await interaction.followup.send(embed=embed)
                    return
            
            # Get leaderboard data
            leaderboard = await self.tracker.get_server_leaderboard(
                interaction.guild.id,
                category=category,
                limit=limit,
                time_range=timeframe,
                include_stats=True
            )
            
            if not leaderboard:
                embed = discord.Embed(
                    title="📊 Leaderboard",
                    description="No time tracked yet! Be the first to start tracking.",
                    color=discord.Color.light_grey()
                )
                embed.add_field(
                    name="🚀 Get Started",
                    value="Use `/clockin <category>` to begin",
                    inline=False
                )
                await interaction.followup.send(embed=embed)
                return
            
            # Create leaderboard embed
            timeframe_display = {
                'all': 'All Time',
                'week': 'This Week', 
                'month': 'This Month'
            }.get(timeframe, 'All Time')
            
            title = f"🏆 {category.title() if category else 'Total'} Leaderboard ({timeframe_display})"
            embed = discord.Embed(title=title, color=discord.Color.gold())
            
            # Build leaderboard display
            leaderboard_text = []
            medal_emojis = ["🥇", "🥈", "🥉"]
            
            for i, entry in enumerate(leaderboard):
                rank = entry['rank']
                user_id = entry['user_id']
                time_formatted = entry['time_formatted']
                
                # Get user info
                user = self.bot.get_user(user_id)
                username = user.display_name if user else f"User {user_id}"
                
                # Truncate long usernames
                if len(username) > 20:
                    username = username[:17] + "..."
                
                # Medal or rank number
                if rank <= 3:
                    emoji = medal_emojis[rank - 1]
                else:
                    emoji = f"{rank}."
                
                # Basic entry
                entry_line = f"{emoji} **{username}** - {time_formatted}"
                
                # Add stats if available
                stats = entry.get('stats', {})
                if stats:
                    if stats.get('productivity_score'):
                        entry_line += f" (📈 {stats['productivity_score']}%)"
                    elif stats.get('days_since_activity', 0) <= 1:
                        entry_line += " (🔥 Active)"
                
                leaderboard_text.append(entry_line)
            
            embed.description = "\n".join(leaderboard_text)
            
            # Add additional info
            if category:
                embed.add_field(
                    name="📂 Category",
                    value=f"`{category}`",
                    inline=True
                )
            
            embed.add_field(
                name="📊 Showing",
                value=f"Top {len(leaderboard)} users",
                inline=True
            )
            
            # Server stats
            try:
                server_key = f"server_times:{interaction.guild.id}"
                server_totals = await self.tracker.redis.hgetall(server_key)
                if server_totals:
                    total_time = 0
                    if b'total' in server_totals:
                        total_time = int(server_totals[b'total'])
                    
                    if total_time > 0:
                        total_hours = total_time / 3600
                        embed.add_field(
                            name="🌐 Server Total",
                            value=f"{total_hours:.1f} hours",
                            inline=True
                        )
            except Exception as e:
                logger.warning(f"Could not get server totals: {e}")
            
            embed.set_footer(text=f"Use /clockin to start tracking • Updated: {datetime.now().strftime('%H:%M')}")
            
            await interaction.followup.send(embed=embed)
            self.admin_metrics['leaderboard_requests'] += 1
            
        except Exception as e:
            logger.error(f"Error in leaderboard command: {e}")
            embed = self._create_generic_error_embed(e)
            await interaction.followup.send(embed=embed)


# ========================================================================
# COG SETUP
# ========================================================================

async def setup(bot):
    """Setup function for the cog"""
    await bot.add_cog(LeaderboardCog(bot))
    logger.info("LeaderboardCog loaded successfully")

async def teardown(bot):
    """Teardown function for the cog"""
    cog = bot.get_cog("LeaderboardCog")
    if cog:
        await cog.cog_unload()
    logger.info("LeaderboardCog unloaded")