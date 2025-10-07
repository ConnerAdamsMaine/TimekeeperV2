import discord
from discord.ext import commands
from discord import app_commands
import logging
from typing import Optional, Dict, Any
import asyncio
from datetime import datetime

# Import the enhanced shared tracker system
from Utils.timekeeper import (
    get_shared_role_tracker, 
    close_shared_role_tracker,
    ValidationError,
    CategoryError,
    PermissionError,
    CircuitBreakerOpenError,
    TimeTrackerError
)
# from Utils.activation import require_activation_slash

# Configure logging
logger = logging.getLogger(__name__)


class TimecardCog(commands.Cog):
    """Core timecard system - clock in, clock out, and status commands"""
    
    def __init__(self, bot):
        self.bot = bot
        self.tracker = None
        self.clock = None
        
        # Performance tracking for core commands only
        self.command_metrics = {
            'clockin_count': 0,
            'clockout_count': 0, 
            'status_count': 0,
            'total_response_time': 0.0,
            'error_count': 0
        }
        
        logger.info("TimecardCog (core) initialized")
    
    async def cog_load(self):
        """Initialize enterprise tracker when cog loads"""
        try:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
            logger.info("TimecardCog connected to enterprise tracker system")
            
            # Start background monitoring for core commands
            asyncio.create_task(self._performance_monitor())
            
        except Exception as e:
            logger.error(f"Failed to initialize timecard system: {e}")
    
    async def cog_unload(self):
        """Cleanup when cog unloads"""
        try:
            await close_shared_role_tracker()
            logger.info("TimecardCog cleaned up shared tracker")
        except Exception as e:
            logger.error(f"Error during timecard cleanup: {e}")
    
    async def _ensure_initialized(self):
        """Ensure tracker is initialized with retry logic"""
        if not self.tracker or not self.clock:
            try:
                self.tracker, self.clock = await get_shared_role_tracker(self.bot)
            except Exception as e:
                logger.error(f"Failed to reinitialize tracker: {e}")
                raise commands.CommandError("🔧 Timecard system temporarily unavailable. Please try again in a moment.")
    
    async def _track_command_performance(self, command_name: str, start_time: float, success: bool):
        """Track command performance metrics for core commands"""
        response_time = datetime.now().timestamp() - start_time
        self.command_metrics['total_response_time'] += response_time
        
        if success:
            self.command_metrics[f'{command_name}_count'] += 1
        else:
            self.command_metrics['error_count'] += 1
    
    async def _performance_monitor(self):
        """Background performance monitoring for core commands"""
        while True:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                total_commands = (self.command_metrics['clockin_count'] + 
                                self.command_metrics['clockout_count'] + 
                                self.command_metrics['status_count'])
                
                if total_commands > 0:
                    avg_response = self.command_metrics['total_response_time'] / total_commands
                    error_rate = (self.command_metrics['error_count'] / 
                                (total_commands + self.command_metrics['error_count']) * 100)
                    
                    logger.info(f"Core Timecard Performance - "
                              f"Commands: {total_commands}, "
                              f"Error Rate: {error_rate:.1f}%, "
                              f"Avg Response: {avg_response:.3f}s")
                
            except Exception as e:
                logger.error(f"Performance monitor error: {e}")
    
    def _get_category_suggestions(self, input_category: str, available_categories: list) -> list:
        """Get smart category suggestions using fuzzy matching"""
        if not available_categories:
            return []
            
        suggestions = []
        input_lower = input_category.lower()
        
        # Exact substring matches first
        for cat in available_categories:
            if input_lower in cat.lower() or cat.lower() in input_lower:
                suggestions.append(cat)
        
        # Similar sounding matches
        if not suggestions:
            for cat in available_categories:
                # Simple similarity check
                if len(set(input_lower) & set(cat.lower())) >= min(2, len(input_lower) // 2):
                    suggestions.append(cat)
        
        return suggestions[:3]
    
    def _get_session_tips(self, category: str) -> Optional[str]:
        """Get contextual tips based on category"""
        tips = {
            'work': "💪 Stay focused! Consider using the Pomodoro technique (25min work, 5min break)",
            'meeting': "🗣️ Remember to take notes and track action items",
            'development': "💻 Great for coding sessions! Don't forget to commit your code",
            'break': "☕ Enjoy your break! Even short breaks boost productivity",
            'training': "📚 Learning time! Take notes and practice what you learn",
            'support': "🆘 Helping others builds teamwork and knowledge sharing"
        }
        return tips.get(category)
    
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
    # CLOCK IN COMMAND
    # ========================================================================
    @app_commands.command(name="clockin", description="🕐 Clock in to start tracking time")
    @app_commands.describe(
        category="Category to track time for",
        description="Optional description for this session"
    )
    async def clockin(self, interaction: discord.Interaction, category: str = "main", description: Optional[str] = None):
        """Clock in command with enterprise features"""
        start_time = datetime.now().timestamp()
        
        # LOG: Function entry
        logger.info(f"[CLOCKIN] Function called - User: {interaction.user.id} ({interaction.user.name}), "
                    f"Guild: {interaction.guild.id} ({interaction.guild.name}), "
                    f"Category: '{category}', Description: '{description}'")
        
        await interaction.response.defer()
        logger.debug(f"[CLOCKIN] Interaction deferred for user {interaction.user.id}")
        
        try:
            # LOG: Initialization start
            logger.debug(f"[CLOCKIN] Starting initialization for user {interaction.user.id}")
            await self._ensure_initialized()
            logger.info(f"[CLOCKIN] Initialization complete for user {interaction.user.id}")
            
            # LOG: Category lookup start
            logger.debug(f"[CLOCKIN] Fetching categories for guild {interaction.guild.id}")
            categories_info = await self.tracker.list_categories(
                interaction.guild.id, 
                include_metadata=True
            )
            logger.info(f"[CLOCKIN] Categories fetched - Count: {len(categories_info) if categories_info else 0}, "
                        f"Categories: {list(categories_info.keys()) if categories_info else []}")
            
            # LOG: Role determination
            logger.debug(f"[CLOCKIN] Determining role for category '{category}'")
            match category.lower().strip():
                case 'break':
                    role = "Break"
                    logger.debug(f"[CLOCKIN] Role set to 'Break' for category '{category}'")
                case _:
                    role = "Clocked In"
                    logger.debug(f"[CLOCKIN] Role set to 'Clocked In' for category '{category}'")
            
            # LOG: Check if categories exist
            logger.debug(f"[CLOCKIN] Checking if server has categories configured")
            if not categories_info:
                logger.warning(f"[CLOCKIN] No categories configured for guild {interaction.guild.id}")
                embed = discord.Embed(
                    title="⚙️ Categories Not Configured",
                    description="This server hasn't set up any time tracking categories yet.",
                    color=discord.Color.orange()
                )
                embed.add_field(
                    name="👑 Server Admins",
                    value="Use `/admin categories add <n>` to set up your first category\n"
                        "Example: `/admin categories add work`",
                    inline=False
                )
                embed.add_field(
                    name="💡 Suggested Categories",
                    value="• `work` - General work time\n"
                        "• `meetings` - Team meetings\n" 
                        "• `development` - Coding/development\n"
                        "• `support` - Customer support\n"
                        "• `training` - Learning & training",
                    inline=False
                )
                logger.debug(f"[CLOCKIN] Sending 'no categories' embed to user {interaction.user.id}")
                await interaction.followup.send(embed=embed)
                await self._track_command_performance("clockin", start_time, False)
                logger.info(f"[CLOCKIN] Exiting early - no categories configured for guild {interaction.guild.id}")
                return
            
            # LOG: Category validation
            logger.debug(f"[CLOCKIN] Validating if category '{category}' exists in server categories")
            if category.lower().strip() not in categories_info:
                logger.warning(f"[CLOCKIN] Category '{category}' not found in guild {interaction.guild.id}. "
                            f"Available: {list(categories_info.keys())}")
                
                # Smart category suggestions from server's categories
                logger.debug(f"[CLOCKIN] Generating category suggestions for '{category}'")
                suggestions = self._get_category_suggestions(category, list(categories_info.keys()))
                logger.debug(f"[CLOCKIN] Suggestions generated: {suggestions}")
                
                embed = discord.Embed(
                    title="❌ Category Not Available",
                    description=f"**'{category}'** is not set up for this server.",
                    color=discord.Color.red()
                )
                
                if suggestions:
                    logger.debug(f"[CLOCKIN] Adding suggestions to embed: {suggestions[:3]}")
                    embed.add_field(
                        name="🎯 Did you mean?",
                        value=", ".join(f"`{cat}`" for cat in suggestions[:3]),
                        inline=False
                    )
                
                # Show server's configured categories
                active_categories = [cat for cat, info in categories_info.items() if info.get('active', True)]
                logger.debug(f"[CLOCKIN] Active categories: {active_categories}")
                
                if active_categories:
                    embed.add_field(
                        name="📋 Available Categories (Set by Server Admins)",
                        value=", ".join(f"`{cat}`" for cat in sorted(active_categories)[:10]),
                        inline=False
                    )
                
                embed.add_field(
                    name="💬 Need a New Category?",
                    value="Ask your server admins to add it using:\n`/admin categories add <n>`",
                    inline=False
                )
                
                logger.debug(f"[CLOCKIN] Sending 'category not found' embed to user {interaction.user.id}")
                await interaction.followup.send(embed=embed)
                await self._track_command_performance("clockin", start_time, False)
                logger.info(f"[CLOCKIN] Exiting early - invalid category '{category}' for guild {interaction.guild.id}")
                return
            
            # LOG: Prepare metadata
            logger.debug(f"[CLOCKIN] Preparing session metadata for user {interaction.user.id}")
            session_metadata = {
                'description': description,
                'guild_name': interaction.guild.name,
                'channel_id': interaction.channel_id,
                'user_agent': 'Discord Bot v1.0'
            }
            logger.debug(f"[CLOCKIN] Session metadata prepared: {session_metadata}")
            
            # LOG: Clock in attempt
            logger.info(f"[CLOCKIN] Attempting to clock in - User: {interaction.user.id}, "
                    f"Guild: {interaction.guild.id}, Category: '{category}', Role: '{role}'")
            
            result = await self.clock.clock_in(
                server_id=interaction.guild.id,
                user_id=interaction.user.id,
                category=category,
                interaction=interaction,
                role=role,
                metadata=session_metadata
            )
            
            logger.info(f"[CLOCKIN] Clock in result - Success: {result['success']}, "
                    f"Message: {result.get('message', 'N/A')}, "
                    f"Session ID: {result.get('session_id', 'N/A')}")
            logger.debug(f"[CLOCKIN] Full result object: {result}")
            
            if result['success']:
                logger.info(f"[CLOCKIN] Clock in successful for user {interaction.user.id}")
                
                # Get category metadata for enhanced display
                logger.debug(f"[CLOCKIN] Fetching category metadata for '{category}'")
                category_info = categories_info.get(category, {})
                category_metadata = category_info.get('metadata', {})
                logger.debug(f"[CLOCKIN] Category metadata: {category_metadata}")
                
                # Parse color
                color_hex = category_metadata.get('color', '#3498db')
                logger.debug(f"[CLOCKIN] Using color: {color_hex}")
                
                embed = discord.Embed(
                    title="⏰ Successfully Clocked In",
                    description=result['message'],
                    color=int(color_hex.replace('#', ''), 16)
                )
                
                # Main session info
                logger.debug(f"[CLOCKIN] Building session details embed section")
                embed.add_field(
                    name="📊 Session Details",
                    value=f"**Category:** `{result['category']}`\n"
                        f"**Started:** <t:{int(result['start_time'].timestamp())}:t>\n"
                        f"**Session ID:** `{result['session_id'][:8]}...`",
                    inline=False
                )
                
                # Role and system status
                logger.debug(f"[CLOCKIN] Checking role assignment status")
                status_items = []
                if result.get('role_assigned'):
                    logger.info(f"[CLOCKIN] Role assigned successfully for user {interaction.user.id}")
                    status_items.append("✅ Role assigned")
                elif result.get('role_warning'):
                    logger.warning(f"[CLOCKIN] Role warning for user {interaction.user.id}: {result['role_warning']}")
                    status_items.append(f"⚠️ {result['role_warning']}")
                
                if status_items:
                    logger.debug(f"[CLOCKIN] Adding system status: {status_items}")
                    embed.add_field(
                        name="🔧 System Status",
                        value="\n".join(status_items),
                        inline=True
                    )
                
                # Category insights
                if category_metadata:
                    productivity_weight = category_metadata.get('productivity_weight', 1.0)
                    logger.debug(f"[CLOCKIN] Productivity weight: {productivity_weight}")
                    
                    if productivity_weight != 1.0:
                        logger.debug(f"[CLOCKIN] Adding productivity weight to embed")
                        embed.add_field(
                            name="📈 Category Info",
                            value=f"Productivity Weight: {productivity_weight}x",
                            inline=True
                        )
                
                # Session tips based on category
                logger.debug(f"[CLOCKIN] Fetching session tips for category '{category}'")
                tips = self._get_session_tips(category)
                logger.debug(f"[CLOCKIN] Tips: {tips if tips else 'None'}")
                
                if tips:
                    embed.add_field(
                        name="💡 Session Tips",
                        value=tips,
                        inline=False
                    )
                
                embed.set_footer(text="Use /clockout when finished • /status for session details")
                logger.debug(f"[CLOCKIN] Success embed built completely")
                
            else:
                logger.warning(f"[CLOCKIN] Clock in failed for user {interaction.user.id}: {result.get('message', 'Unknown error')}")
                logger.debug(f"[CLOCKIN] Creating error embed from result")
                embed = self._create_error_embed(result)
            
            logger.debug(f"[CLOCKIN] Sending final embed to user {interaction.user.id}")
            await interaction.followup.send(embed=embed)
            logger.info(f"[CLOCKIN] Embed sent successfully to user {interaction.user.id}")
            
            await self._track_command_performance("clockin", start_time, result['success'])
            logger.info(f"[CLOCKIN] Command performance tracked - Success: {result['success']}, "
                    f"Duration: {datetime.now().timestamp() - start_time:.2f}s")
            
        except CircuitBreakerOpenError as e:
            logger.error(f"[CLOCKIN] Circuit breaker open for user {interaction.user.id}: {e}")
            logger.debug(f"[CLOCKIN] Circuit breaker context: {getattr(e, 'context', {})}")
            
            embed = discord.Embed(
                title="🔧 Service Temporarily Unavailable",
                description="The timecard system is experiencing high load. Please try again in a moment.",
                color=discord.Color.orange()
            )
            
            if hasattr(e, 'context') and e.context.get('retry_after'):
                retry_after = e.context['retry_after']
                logger.info(f"[CLOCKIN] Retry after: {retry_after} seconds")
                embed.add_field(
                    name="⏱️ Retry After",
                    value=f"{retry_after} seconds",
                    inline=True
                )
            
            logger.debug(f"[CLOCKIN] Sending circuit breaker embed to user {interaction.user.id}")
            await interaction.followup.send(embed=embed)
            await self._track_command_performance("clockin", start_time, False)
            logger.info(f"[CLOCKIN] Circuit breaker error handled for user {interaction.user.id}")
            
        except Exception as e:
            logger.error(f"[CLOCKIN] Unexpected error for user {interaction.user.id}: {type(e).__name__}: {e}")
            logger.exception(f"[CLOCKIN] Full exception traceback:")
            
            embed = self._create_generic_error_embed(e)
            logger.debug(f"[CLOCKIN] Sending generic error embed to user {interaction.user.id}")
            
            await interaction.followup.send(embed=embed)
            await self._track_command_performance("clockin", start_time, False)
            logger.error(f"[CLOCKIN] Error handling complete for user {interaction.user.id}")
        
        logger.info(f"[CLOCKIN] Function exit - User: {interaction.user.id}, Guild: {interaction.guild.id}")
    
    # ========================================================================
    # CLOCK OUT COMMAND
    # ========================================================================
    @app_commands.command(name="clockout", description="🕐 Clock out to stop tracking time")
    
    async def clockout(self, interaction: discord.Interaction):
        """Clock out command with session analytics"""
        start_time = datetime.now().timestamp()
        await interaction.response.defer()
        
        try:
            await self._ensure_initialized()
            
            # Attempt to clock out
            result = await self.clock.clock_out(
                interaction.guild.id,
                interaction.user.id
            )
            
            if result['success']:
                embed = discord.Embed(
                    title="✅ Successfully Clocked Out",
                    description=result['message'],
                    color=discord.Color.green()
                )
                
                # Session summary
                session_hours = result['session_duration'] / 3600
                embed.add_field(
                    name="📊 Session Summary",
                    value=f"**Category:** `{result['category']}`\n"
                          f"**Duration:** {result['session_duration_formatted']}\n"
                          f"**Session:** <t:{int(result['start_time'].timestamp())}:t> → <t:{int(result['end_time'].timestamp())}:t>",
                    inline=False
                )
                
                # Progress and totals
                if result.get('time_added'):
                    embed.add_field(
                        name="📈 Progress Update",
                        value=f"**Category Total:** {result['time_added']['category_total_formatted']}\n"
                              f"**Time Added:** {result['session_duration_formatted']}",
                        inline=True
                    )
                
                # Session quality analysis
                quality_analysis = self._analyze_session_quality(result['session_duration'], result['category'])
                if quality_analysis:
                    embed.add_field(
                        name="🎯 Session Analysis",
                        value=quality_analysis,
                        inline=True
                    )
                
                # System status
                status_items = []
                if result.get('role_removed'):
                    status_items.append("✅ Role removed")
                elif result.get('role_warning'):
                    status_items.append(f"⚠️ {result['role_warning']}")
                
                if status_items:
                    embed.add_field(
                        name="🔧 System Status",
                        value="\n".join(status_items),
                        inline=True
                    )
                
                # Productivity insights
                insights = self._generate_session_insights(result['session_duration'], result['category'])
                if insights:
                    embed.add_field(
                        name="💡 Insights",
                        value=insights,
                        inline=False
                    )
                
                embed.set_footer(text="Use /status for detailed analytics")
                
            else:
                embed = self._create_error_embed(result)
                
                # Add helpful suggestions for common errors
                if result.get('error_code') == 'NOT_CLOCKED_IN':
                    embed.add_field(
                        name="💡 Quick Actions",
                        value="• Use `/clockin` to start tracking time\n• Use `/status` to see your progress",
                        inline=False
                    )
            
            await interaction.followup.send(embed=embed)
            await self._track_command_performance("clockout", start_time, result['success'])
            
        except Exception as e:
            logger.error(f"Error in clockout command: {e}")
            embed = self._create_generic_error_embed(e)
            await interaction.followup.send(embed=embed)
            await self._track_command_performance("clockout", start_time, False)
    
    def _analyze_session_quality(self, duration_seconds: int, category: str) -> Optional[str]:
        """Analyze session quality and provide feedback"""
        hours = duration_seconds / 3600
        
        if category in ['work', 'development']:
            if hours >= 4:
                return "🌟 **Excellent** - Great deep work session!"
            elif hours >= 2:
                return "👍 **Good** - Solid focused session"
            elif hours >= 1:
                return "✅ **Fair** - Good progress made"
            elif hours >= 0.5:
                return "⚡ **Short** - Every bit counts!"
            else:
                return "⏱️ **Brief** - Consider longer sessions for deep work"
        
        elif category == 'meeting':
            if hours <= 0.5:
                return "⚡ **Efficient** - Concise and focused"
            elif hours <= 1.5:
                return "✅ **Standard** - Good meeting length"
            else:
                return "🕐 **Extended** - Consider breaking into smaller meetings"
        
        elif category == 'break':
            if hours <= 0.5:
                return "☕ **Perfect** - Refreshing break"
            else:
                return "🛋️ **Extended** - Long break taken"
        
        return None
    
    def _generate_session_insights(self, duration_seconds: int, category: str) -> Optional[str]:
        """Generate personalized insights for the session"""
        hours = duration_seconds / 3600
        insights = []
        
        if category in ['work', 'development'] and hours >= 2:
            insights.append("🧠 Deep work achieved - great for complex tasks")
        
        if category == 'break' and hours >= 0.25:
            insights.append("🔋 Mental battery recharged")
        
        if hours >= 1 and category != 'break':
            insights.append("📈 This session will boost your productivity score")
        
        if category == 'meeting' and hours <= 1:
            insights.append("⚡ Efficient meeting - time well spent")
        
        return " • ".join(insights) if insights else None
    
    # ========================================================================
    # STATUS COMMAND
    # ========================================================================
    @app_commands.command(name="status", description="📊 Check your time tracking status")
    
    async def status(self, interaction: discord.Interaction):
        """Status command with comprehensive analytics"""
        start_time = datetime.now().timestamp()
        await interaction.response.defer()
        
        try:
            await self._ensure_initialized()
            
            # Get comprehensive status
            status = await self.clock.get_status(interaction.guild.id, interaction.user.id)
            
            if status.get('error'):
                embed = discord.Embed(
                    title="❌ Error Getting Status",
                    description=f"Error: {status['error']}",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed)
                await self._track_command_performance("status", start_time, False)
                return
            
            if status['clocked_in']:
                # Currently clocked in - show active session details
                embed = discord.Embed(
                    title="⏰ Currently Clocked In",
                    color=discord.Color.blue()
                )
                
                # Main session info
                embed.add_field(
                    name="📊 Active Session",
                    value=f"**Category:** `{status['category']}`\n"
                          f"**Duration:** {status['current_duration_formatted']}\n"
                          f"**Started:** <t:{int(status['start_time'].timestamp())}:t>",
                    inline=False
                )
                
                # Session analytics
                analytics = status.get('analytics', {})
                if analytics:
                    quality = analytics.get('session_quality', 'good')
                    quality_emoji = {
                        'excellent': '🌟', 'good': '👍', 'fair': '✅', 
                        'starting': '🚀', 'long': '🕐', 'extended': '⏰'
                    }.get(quality, '📊')
                    
                    analytics_text = f"{quality_emoji} **Quality:** {quality.title()}"
                    
                    if analytics.get('productivity_estimate'):
                        productivity = analytics['productivity_estimate'] * 100
                        analytics_text += f"\n📈 **Productivity:** {productivity:.0f}%"
                    
                    if analytics.get('recommended_break_in'):
                        analytics_text += f"\n☕ **Break recommended:** {analytics['recommended_break_in']}"
                    
                    embed.add_field(
                        name="🧠 Session Analytics",
                        value=analytics_text,
                        inline=True
                    )
                
                # System status
                system_items = []
                if status.get('role_assigned'):
                    system_items.append("✅ Role active")
                
                if system_items:
                    embed.add_field(
                        name="🔧 System Status",
                        value="\n".join(system_items),
                        inline=True
                    )
                
                # Progress visualization
                hours = status['current_duration'] / 3600
                if hours >= 1:
                    progress_bars = min(int(hours), 8)
                    progress = "█" * progress_bars + "░" * (8 - progress_bars)
                    embed.add_field(
                        name="⏱️ Session Progress",
                        value=f"`{progress}` {hours:.1f}h",
                        inline=False
                    )
                
                embed.set_footer(text="Use /clockout when finished")
                
            else:
                # Not clocked in - show summary
                if status['total_time'] > 0:
                    embed = discord.Embed(
                        title="📊 Time Tracking Summary",
                        color=discord.Color.green()
                    )
                    
                    # Total time
                    total_hours = status['total_time'] / 3600
                    embed.add_field(
                        name="⏱️ Total Time Tracked",
                        value=f"**{status['total_time_formatted']}** ({total_hours:.1f} hours)",
                        inline=False
                    )
                    
                    # Category breakdown
                    if status['categories']:
                        category_text = []
                        sorted_categories = sorted(
                            status['categories'].items(), 
                            key=lambda x: x[1]['percentage'], 
                            reverse=True
                        )
                        
                        for category, data in sorted_categories[:8]:  # Top 8 categories
                            percentage = data['percentage']
                            time_str = data['time']
                            
                            # Progress bar for visual appeal
                            bar_length = min(int(percentage / 10), 10)
                            bar = "█" * bar_length + "░" * (10 - bar_length)
                            
                            category_text.append(f"`{bar}` **{category}**: {time_str} ({percentage:.1f}%)")
                        
                        if category_text:
                            embed.add_field(
                                name="📈 Category Breakdown",
                                value="\n".join(category_text),
                                inline=False
                            )
                    
                    # Recent activity
                    recent_session = status.get('recent_session')
                    if recent_session:
                        embed.add_field(
                            name="🕐 Last Session",
                            value=f"**{recent_session['category']}** for {recent_session['duration']}\n"
                                  f"*{recent_session['hours_since']:.1f} hours ago*",
                            inline=True
                        )
                    
                    # Quick stats
                    summary = status.get('summary', {})
                    if summary:
                        summary_text = []
                        if summary.get('total_sessions'):
                            summary_text.append(f"📅 **{summary['total_sessions']}** sessions")
                        if summary.get('estimated_daily_avg'):
                            summary_text.append(f"📊 **{summary['estimated_daily_avg']}h** daily average")
                        
                        if summary_text:
                            embed.add_field(
                                name="📋 Quick Stats",
                                value="\n".join(summary_text),
                                inline=True
                            )
                        
                        # Achievements
                        insights = summary.get('insights', [])
                        if insights:
                            embed.add_field(
                                name="🌟 Achievements",
                                value="\n".join(insights),
                                inline=False
                            )
                    
                    embed.set_footer(text="Use /clockin <category> to start tracking")
                    
                else:
                    # No time tracked yet
                    embed = discord.Embed(
                        title="📋 No Time Tracked Yet",
                        description="Ready to start tracking your time?",
                        color=discord.Color.light_grey()
                    )
                    
                    # Show available categories
                    try:
                        categories = await self.tracker.list_categories(interaction.guild.id)
                        if categories:
                            embed.add_field(
                                name="📂 Available Categories",
                                value=", ".join(f"`{cat}`" for cat in sorted(categories)[:8]),
                                inline=False
                            )
                        else:
                            embed.add_field(
                                name="⚙️ Setup Required",
                                value="Ask your server admins to set up categories using `/admin categories add <n>`",
                                inline=False
                            )
                    except Exception as e:
                        logger.warning(f"Could not get categories: {e}")
                    
                    embed.add_field(
                        name="🚀 Get Started",
                        value="Use `/clockin <category>` to begin tracking time",
                        inline=False
                    )
            
            await interaction.followup.send(embed=embed)
            await self._track_command_performance("status", start_time, True)
            
        except Exception as e:
            logger.error(f"Error in status command: {e}")
            embed = self._create_generic_error_embed(e)
            await interaction.followup.send(embed=embed)
            await self._track_command_performance("status", start_time, False)


# ========================================================================
# COG SETUP
# ========================================================================

async def setup(bot):
    """Setup function for the cog"""
    await bot.add_cog(TimecardCog(bot))
    logger.info("TimecardCog (core) loaded successfully")

async def teardown(bot):
    """Teardown function for the cog"""
    cog = bot.get_cog("TimecardCog")
    if cog:
        await cog.cog_unload()
    logger.info("TimecardCog (core) unloaded")