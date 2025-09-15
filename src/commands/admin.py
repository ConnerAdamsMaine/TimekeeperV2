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


class TimecardAdminCog(commands.Cog):
    """Administrative timecard system - categories, leaderboards, analytics, and admin tools"""
    
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
        
        logger.info("TimecardAdminCog initialized")
    
    async def cog_load(self):
        """Initialize enterprise tracker when cog loads"""
        try:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
            logger.info("TimecardAdminCog connected to enterprise tracker system")
        except Exception as e:
            logger.error(f"Failed to initialize timecard admin system: {e}")
    
    async def cog_unload(self):
        """Cleanup when cog unloads - shared tracker handled by main cog"""
        logger.info("TimecardAdminCog unloaded")
    
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
    # ADMIN COMMAND GROUP
    # ========================================================================
    
    admin_group = app_commands.Group(name="admin", description="👑 Administrative commands for timecard system")
    
    @admin_group.command(name="categories", description="👑 Manage server time tracking categories (Admin Only)")
    @app_commands.describe(
        action="Action to perform (list, add, remove)",
        name="Category name (for add/remove actions)",
        description="Description for the category (optional)",
        color="Hex color for the category (optional, e.g., #FF5733)"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="list", value="list"),
        app_commands.Choice(name="add", value="add"),
        app_commands.Choice(name="remove", value="remove")
    ])
    async def admin_categories(self, interaction: discord.Interaction, action: str, 
                              name: Optional[str] = None, description: Optional[str] = None, 
                              color: Optional[str] = None):
        """Server-controlled category management command"""
        await interaction.response.defer()
        
        try:
            await self._ensure_initialized()
            
            if action == "list":
                # Anyone can list categories
                categories = await self.tracker.list_categories(
                    interaction.guild.id, 
                    include_archived=False, 
                    include_metadata=True
                )
                
                embed = discord.Embed(
                    title="📋 Server Time Tracking Categories",
                    color=discord.Color.blue()
                )
                
                if not categories:
                    embed.description = "No categories have been set up for this server yet."
                    embed.add_field(
                        name="👑 Server Admins",
                        value="Use `/admin categories add <n>` to create your first category",
                        inline=False
                    )
                    embed.add_field(
                        name="💡 Common Categories",
                        value="• `work` • `meetings` • `development` • `support` • `training` • `break`",
                        inline=False
                    )
                else:
                    # Show active categories with metadata
                    active_cats = []
                    for cat, info in categories.items():
                        if info.get('active', True):
                            metadata = info.get('metadata', {})
                            usage = info.get('usage', {})
                            
                            # Build category display
                            cat_display = f"**{cat}**"
                            
                            # Add description if available
                            if metadata.get('description'):
                                cat_display += f"\n*{metadata['description']}*"
                            
                            # Add usage stats
                            if usage.get('total_time'):
                                cat_display += f"\n📊 {usage['total_time_formatted']} tracked"
                            
                            active_cats.append(cat_display)
                    
                    if active_cats:
                        # Split into chunks if too many categories
                        chunk_size = 5
                        for i in range(0, len(active_cats), chunk_size):
                            chunk = active_cats[i:i + chunk_size]
                            field_name = "📂 Available Categories" if i == 0 else "📂 More Categories"
                            embed.add_field(
                                name=field_name,
                                value="\n\n".join(chunk),
                                inline=False
                            )
                    
                    embed.add_field(
                        name="ℹ️ Usage",
                        value="Use `/clockin <category>` to start tracking time",
                        inline=False
                    )
                
                embed.set_footer(text="Only server admins can add/remove categories")
                
            elif action in ["add", "remove"]:
                # Check admin permissions
                if not interaction.user.guild_permissions.administrator:
                    embed = discord.Embed(
                        title="🔒 Admin Only",
                        description="Only server administrators can manage categories.",
                        color=discord.Color.red()
                    )
                    embed.add_field(
                        name="👑 Required Permission",
                        value="Administrator",
                        inline=True
                    )
                    await interaction.followup.send(embed=embed)
                    return
                
                if not name:
                    embed = discord.Embed(
                        title="❌ Missing Category Name",
                        description=f"Please provide a category name for the **{action}** action.",
                        color=discord.Color.red()
                    )
                    embed.add_field(
                        name="💡 Example",
                        value=f"`/admin categories {action} work`",
                        inline=False
                    )
                    await interaction.followup.send(embed=embed)
                    return
                
                if action == "add":
                    result = await self.tracker.add_category(
                        interaction.guild.id,
                        name,
                        user_id=interaction.user.id,
                        description=description,
                        color=color
                    )
                    
                    if result['success']:
                        embed = discord.Embed(
                            title="✅ Category Added",
                            description=f"Successfully added **{result['category']}** to your server!",
                            color=discord.Color.green()
                        )
                        
                        metadata = result.get('metadata', {})
                        if metadata:
                            embed.add_field(
                                name="📊 Category Details",
                                value=f"**Name:** `{metadata['name']}`\n"
                                      f"**Description:** {metadata.get('description', 'None')}\n"
                                      f"**Color:** {metadata.get('color', 'Auto-generated')}",
                                inline=False
                            )
                        
                        embed.add_field(
                            name="🚀 What's Next?",
                            value=f"Users can now track time with:\n`/clockin {result['category']}`",
                            inline=False
                        )
                        
                        self.admin_metrics['categories_managed'] += 1
                    else:
                        embed = self._create_error_embed(result)
                        
                        # Add helpful context for common errors
                        if result.get('error_code') == 'CATEGORY_EXISTS':
                            embed.add_field(
                                name="💡 Alternative",
                                value="You can view existing categories with `/admin categories list`",
                                inline=False
                            )
                
                elif action == "remove":
                    result = await self.tracker.remove_category(
                        interaction.guild.id,
                        name,
                        user_id=interaction.user.id
                    )
                    
                    if result['success']:
                        action_taken = result.get('action', 'removed')
                        embed = discord.Embed(
                            title=f"✅ Category {action_taken.title()}",
                            description=f"Successfully {action_taken} **{name}** from your server.",
                            color=discord.Color.green()
                        )
                        
                        usage_info = result.get('usage_info', {})
                        if usage_info and usage_info.get('total_time', 0) > 0:
                            embed.add_field(
                                name="📊 Historical Data",
                                value=f"**Preserved:** {usage_info['total_time_formatted']} from {usage_info['unique_users']} users\n"
                                      f"**Entries:** {usage_info['total_entries']} time entries archived",
                                inline=False
                            )
                        
                        if action_taken == "archived":
                            embed.add_field(
                                name="ℹ️ Note",
                                value="Category was archived (not deleted) because it contains time tracking data.",
                                inline=False
                            )
                        
                        self.admin_metrics['categories_managed'] += 1
                    else:
                        embed = self._create_error_embed(result)
                        
                        # Add context for category removal issues
                        usage_info = result.get('usage_info')
                        if usage_info and result.get('error_code') == 'CATEGORY_IN_USE':
                            embed.add_field(
                                name="📊 Usage Details",
                                value=f"**Time Tracked:** {usage_info['total_time_formatted']}\n"
                                      f"**Users:** {usage_info['unique_users']}\n"
                                      f"**Entries:** {usage_info['total_entries']}",
                                inline=False
                            )
                            embed.add_field(
                                name="🔧 Options",
                                value="• Archive instead of delete (preserves data)\n"
                                      "• Use force=True in admin commands (risky)",
                                inline=False
                            )
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in admin categories command: {e}")
            embed = self._create_generic_error_embed(e)
            await interaction.followup.send(embed=embed)
    
    @admin_group.command(name="system", description="🔧 View system status and health metrics")
    async def admin_system(self, interaction: discord.Interaction):
        """System status and health monitoring for admins"""
        await interaction.response.defer()
        
        try:
            # Check admin permissions
            if not interaction.user.guild_permissions.administrator:
                embed = discord.Embed(
                    title="🔒 Admin Only",
                    description="Only server administrators can view system status.",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed)
                return
            
            await self._ensure_initialized()
            
            # Get comprehensive system status
            system_status = await get_system_status()
            
            embed = discord.Embed(
                title="🔧 Timecard System Status",
                color=discord.Color.green() if system_status['status'] == 'operational' else discord.Color.red()
            )
            
            # Overall status
            embed.add_field(
                name="🌐 Overall Status",
                value=f"**{system_status['status'].title()}**",
                inline=True
            )
            
            # Tracker health
            tracker_health = system_status.get('tracker_health', {})
            if tracker_health:
                embed.add_field(
                    name="🏥 System Health",
                    value=f"**Score:** {tracker_health.get('health_score', 0):.1f}%\n"
                          f"**Status:** {tracker_health.get('status', 'unknown').title()}",
                    inline=True
                )
            
            # Clock metrics
            clock_metrics = system_status.get('clock_metrics', {})
            if clock_metrics:
                embed.add_field(
                    name="⏰ Session Stats",
                    value=f"**Created:** {clock_metrics.get('total_sessions_created', 0)}\n"
                          f"**Completed:** {clock_metrics.get('total_sessions_completed', 0)}\n"
                          f"**Avg Length:** {clock_metrics.get('average_session_length', 0)/3600:.1f}h",
                    inline=True
                )
            
            # Component details
            if tracker_health.get('components'):
                components = tracker_health['components']
                
                # Redis status
                redis_info = components.get('redis', {})
                embed.add_field(
                    name="💾 Redis Database",
                    value=f"**Status:** {redis_info.get('status', 'unknown').title()}\n"
                          f"**Response:** {redis_info.get('response_time_ms', 0):.1f}ms",
                    inline=True
                )
                
                # Performance metrics
                perf_info = components.get('performance', {})
                embed.add_field(
                    name="📊 Performance",
                    value=f"**Avg Response:** {perf_info.get('avg_response_time_ms', 0):.1f}ms\n"
                          f"**Success Rate:** {perf_info.get('success_rate', 0):.1f}%",
                    inline=True
                )
                
                # Cache performance
                cache_info = components.get('cache', {})
                embed.add_field(
                    name="🗂️ Cache System",
                    value=f"**Hit Rate:** {cache_info.get('hit_rate', 0):.1f}%\n"
                          f"**Operations:** {cache_info.get('total_operations', 0)}",
                    inline=True
                )
            
            # Admin metrics
            embed.add_field(
                name="👑 Admin Activity",
                value=f"**Categories Managed:** {self.admin_metrics['categories_managed']}\n"
                      f"**Leaderboards:** {self.admin_metrics['leaderboard_requests']}\n"
                      f"**Admin Commands:** {self.admin_metrics['admin_commands']}",
                inline=False
            )
            
            embed.set_footer(text="System monitoring • Updated in real-time")
            
            await interaction.followup.send(embed=embed)
            self.admin_metrics['admin_commands'] += 1
            
        except Exception as e:
            logger.error(f"Error in admin system command: {e}")
            embed = self._create_generic_error_embed(e)
            await interaction.followup.send(embed=embed)
    
    @admin_group.command(name="cleanup", description="🧹 Clean up orphaned roles and sessions")
    async def admin_cleanup(self, interaction: discord.Interaction):
        """Clean up orphaned timekeeper roles and sessions"""
        await interaction.response.defer()
        
        try:
            # Check admin permissions
            if not interaction.user.guild_permissions.administrator:
                embed = discord.Embed(
                    title="🔒 Admin Only",
                    description="Only server administrators can perform cleanup operations.",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed)
                return
            
            await self._ensure_initialized()
            
            # Cleanup orphaned roles
            result = await self.clock.cleanup_orphaned_roles(interaction.guild.id)
            
            if result['success']:
                embed = discord.Embed(
                    title="✅ Cleanup Complete",
                    description=result['message'],
                    color=discord.Color.green()
                )
                
                if result['removed_roles']:
                    embed.add_field(
                        name="🗑️ Removed Roles",
                        value="\n".join(f"• {role}" for role in result['removed_roles'][:10]),
                        inline=False
                    )
                
                if result['cleaned_members']:
                    embed.add_field(
                        name="🧹 Cleaned Members",
                        value="\n".join(f"• {member}" for member in result['cleaned_members'][:10]),
                        inline=False
                    )
                
                if not result['removed_roles'] and not result['cleaned_members']:
                    embed.add_field(
                        name="✨ All Clean",
                        value="No orphaned roles or assignments found!",
                        inline=False
                    )
            else:
                embed = self._create_error_embed(result)
            
            await interaction.followup.send(embed=embed)
            self.admin_metrics['admin_commands'] += 1
            
        except Exception as e:
            logger.error(f"Error in admin cleanup command: {e}")
            embed = self._create_generic_error_embed(e)
            await interaction.followup.send(embed=embed)


# ========================================================================
# COG SETUP
# ========================================================================

async def setup(bot):
    """Setup function for the cog"""
    await bot.add_cog(TimecardAdminCog(bot))
    logger.info("TimecardAdminCog loaded successfully")

async def teardown(bot):
    """Teardown function for the cog"""
    cog = bot.get_cog("TimecardAdminCog")
    if cog:
        await cog.cog_unload()
    logger.info("TimecardAdminCog unloaded")