import discord
from discord.ext import commands, tasks
from discord import app_commands
import logging
from typing import Optional, Dict, Any, List
import asyncio
from datetime import datetime
import json
import os
from pathlib import Path

from Utils.timekeeper import get_shared_role_tracker

logger = logging.getLogger(__name__)

# Path to store dashboard data
DASHBOARD_DATA_FILE = Path("data/dashboards.json")


class SharedDashboardView(discord.ui.View):
    """Shared dashboard for all users - no timeout"""
    
    def __init__(self, bot, guild_id: int):
        super().__init__(timeout=None)  # No timeout - persistent
        self.bot = bot
        self.guild_id = guild_id
        self.tracker = None
        self.clock = None
        
        # Set custom IDs for persistence across bot restarts - no user_id since it's shared
        self.children[0].custom_id = f"shared_clockin_{guild_id}"
        self.children[1].custom_id = f"shared_clockout_{guild_id}"
        self.children[2].custom_id = f"shared_stats_{guild_id}"
        self.children[3].custom_id = f"shared_server_{guild_id}"
        self.children[4].custom_id = f"shared_whoclocked_{guild_id}"
    
    async def _ensure_initialized(self):
        if not self.tracker or not self.clock:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
    
    @discord.ui.button(label="⏰ Clock In", style=discord.ButtonStyle.green, custom_id="clockin_btn")
    async def clock_in_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clock in button handler - works for any user"""
        await interaction.response.send_modal(ClockInModal(self.bot, self.guild_id))
    
    @discord.ui.button(label="🛑 Clock Out", style=discord.ButtonStyle.red, custom_id="clockout_btn")
    async def clock_out_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clock out button handler - works for any user"""
        await interaction.response.defer(ephemeral=True)
        await self._ensure_initialized()
        
        result = await self.clock.clock_out(self.guild_id, interaction.user.id)
        
        if result['success']:
            embed = discord.Embed(
                title="✅ Clocked Out",
                description=f"Session: {result['session_duration_formatted']}\n"
                           f"Category: `{result['category']}`",
                color=discord.Color.green()
            )
        else:
            embed = discord.Embed(
                title="❌ Error",
                description=result['message'],
                color=discord.Color.red()
            )
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="📊 My Stats", style=discord.ButtonStyle.blurple, custom_id="stats_btn")
    async def stats_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """View stats button handler - shows stats for whoever clicks"""
        await interaction.response.defer(ephemeral=True)
        await self._ensure_initialized()
        
        status = await self.clock.get_status(self.guild_id, interaction.user.id)
        
        embed = discord.Embed(
            title=f"📊 Your Stats",
            color=discord.Color.blue()
        )
        
        if status['clocked_in']:
            embed.add_field(
                name="⏰ Currently Clocked In",
                value=f"Category: `{status['category']}`\n"
                      f"Duration: {status['current_duration_formatted']}",
                inline=False
            )
        else:
            embed.add_field(
                name="Status",
                value="Not currently clocked in",
                inline=False
            )
        
        if status.get('total_time', 0) > 0:
            embed.add_field(
                name="⏱️ Total Time",
                value=status['total_time_formatted'],
                inline=True
            )
            
            if status.get('categories'):
                top_cats = sorted(
                    status['categories'].items(),
                    key=lambda x: x[1]['percentage'],
                    reverse=True
                )[:3]
                
                cat_text = "\n".join([f"`{cat}`: {data['time']}" for cat, data in top_cats])
                embed.add_field(
                    name="📈 Top Categories",
                    value=cat_text,
                    inline=True
                )
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="🌐 Server Total", style=discord.ButtonStyle.gray, custom_id="server_btn")
    async def server_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """View server total button handler - works for any user"""
        await interaction.response.defer(ephemeral=True)
        await self._ensure_initialized()
        
        # Get server totals
        server_key = f"server_times:{self.guild_id}"
        server_data = await self.tracker.redis.hgetall(server_key)
        
        embed = discord.Embed(
            title=f"🌐 {interaction.guild.name} - Server Statistics",
            color=discord.Color.gold()
        )
        
        if server_data:
            total_seconds = int(server_data.get(b'total', b'0'))
            total_hours = total_seconds / 3600
            
            embed.add_field(
                name="⏱️ Total Time Tracked",
                value=f"{total_hours:.1f} hours",
                inline=True
            )
            
            # Count active users
            pattern = f"user_times:{self.guild_id}:*"
            cursor = 0
            user_count = 0
            
            while True:
                cursor, keys = await self.tracker.redis.scan(cursor, match=pattern, count=100)
                user_count += len(keys)
                if cursor == 0:
                    break
            
            embed.add_field(
                name="👥 Active Users",
                value=str(user_count),
                inline=True
            )
            
            # Top categories
            categories = {}
            for key, value in server_data.items():
                key_str = key.decode('utf-8')
                if key_str != 'total':
                    categories[key_str] = int(value)
            
            if categories:
                top_cats = sorted(categories.items(), key=lambda x: x[1], reverse=True)[:5]
                cat_text = "\n".join([
                    f"`{cat}`: {val/3600:.1f}h" for cat, val in top_cats
                ])
                embed.add_field(
                    name="📊 Top Categories",
                    value=cat_text,
                    inline=False
                )
        else:
            embed.description = "No time tracked yet on this server"
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="👥 Who's Clocked In", style=discord.ButtonStyle.gray, custom_id="whoclocked_btn")
    async def who_clocked_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """View who's clocked in button handler - works for any user"""
        await interaction.response.defer(ephemeral=True)
        await self._ensure_initialized()
        
        # Find all active sessions
        pattern = f"active_session:{self.guild_id}:*"
        cursor = 0
        active_sessions = []
        
        while True:
            cursor, keys = await self.tracker.redis.scan(cursor, match=pattern, count=100)
            
            for key in keys:
                session_data = await self.tracker.redis.get(key)
                if session_data:
                    import json
                    session = json.loads(session_data)
                    active_sessions.append(session)
            
            if cursor == 0:
                break
        
        embed = discord.Embed(
            title="👥 Currently Clocked In",
            color=discord.Color.blue()
        )
        
        if active_sessions:
            session_text = []
            for session in active_sessions[:10]:  # Limit to 10
                user = self.bot.get_user(session['user_id'])
                username = user.name if user else f"User {session['user_id']}"
                
                start_time = datetime.fromisoformat(session['start_time'])
                duration = int((datetime.now() - start_time).total_seconds())
                hours = duration / 3600
                
                session_text.append(
                    f"**{username}** - `{session['category']}` ({hours:.1f}h)"
                )
            
            embed.description = "\n".join(session_text)
            embed.set_footer(text=f"{len(active_sessions)} user(s) currently clocked in")
        else:
            embed.description = "No one is currently clocked in"
        
        await interaction.followup.send(embed=embed, ephemeral=True)


class PersonalDashboardView(discord.ui.View):
    """Personal ephemeral dashboard for a specific user"""
    
    def __init__(self, bot, guild_id: int, user_id: int):
        super().__init__(timeout=300)  # 5 minute timeout for personal dashboards
        self.bot = bot
        self.guild_id = guild_id
        self.user_id = user_id
        self.tracker = None
        self.clock = None
    
    async def _ensure_initialized(self):
        if not self.tracker or not self.clock:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
    
    @discord.ui.button(label="⏰ Clock In", style=discord.ButtonStyle.green)
    async def clock_in_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clock in button handler"""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This dashboard is not for you!", ephemeral=True)
            return
        
        await interaction.response.send_modal(ClockInModal(self.bot, self.guild_id))
    
    @discord.ui.button(label="🛑 Clock Out", style=discord.ButtonStyle.red)
    async def clock_out_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clock out button handler"""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This dashboard is not for you!", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        await self._ensure_initialized()
        
        result = await self.clock.clock_out(self.guild_id, self.user_id)
        
        if result['success']:
            embed = discord.Embed(
                title="✅ Clocked Out",
                description=f"Session: {result['session_duration_formatted']}\n"
                           f"Category: `{result['category']}`",
                color=discord.Color.green()
            )
        else:
            embed = discord.Embed(
                title="❌ Error",
                description=result['message'],
                color=discord.Color.red()
            )
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="📊 My Stats", style=discord.ButtonStyle.blurple)
    async def stats_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """View stats button handler"""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This dashboard is not for you!", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        await self._ensure_initialized()
        
        status = await self.clock.get_status(self.guild_id, self.user_id)
        
        embed = discord.Embed(
            title=f"📊 Your Stats",
            color=discord.Color.blue()
        )
        
        if status['clocked_in']:
            embed.add_field(
                name="⏰ Currently Clocked In",
                value=f"Category: `{status['category']}`\n"
                      f"Duration: {status['current_duration_formatted']}",
                inline=False
            )
        else:
            embed.add_field(
                name="Status",
                value="Not currently clocked in",
                inline=False
            )
        
        if status.get('total_time', 0) > 0:
            embed.add_field(
                name="⏱️ Total Time",
                value=status['total_time_formatted'],
                inline=True
            )
            
            if status.get('categories'):
                top_cats = sorted(
                    status['categories'].items(),
                    key=lambda x: x[1]['percentage'],
                    reverse=True
                )[:3]
                
                cat_text = "\n".join([f"`{cat}`: {data['time']}" for cat, data in top_cats])
                embed.add_field(
                    name="📈 Top Categories",
                    value=cat_text,
                    inline=True
                )
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="🌐 Server Total", style=discord.ButtonStyle.gray)
    async def server_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """View server total button handler"""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This dashboard is not for you!", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        await self._ensure_initialized()
        
        # Get server totals
        server_key = f"server_times:{self.guild_id}"
        server_data = await self.tracker.redis.hgetall(server_key)
        
        embed = discord.Embed(
            title=f"🌐 {interaction.guild.name} - Server Statistics",
            color=discord.Color.gold()
        )
        
        if server_data:
            total_seconds = int(server_data.get(b'total', b'0'))
            total_hours = total_seconds / 3600
            
            embed.add_field(
                name="⏱️ Total Time Tracked",
                value=f"{total_hours:.1f} hours",
                inline=True
            )
            
            # Count active users
            pattern = f"user_times:{self.guild_id}:*"
            cursor = 0
            user_count = 0
            
            while True:
                cursor, keys = await self.tracker.redis.scan(cursor, match=pattern, count=100)
                user_count += len(keys)
                if cursor == 0:
                    break
            
            embed.add_field(
                name="👥 Active Users",
                value=str(user_count),
                inline=True
            )
            
            # Top categories
            categories = {}
            for key, value in server_data.items():
                key_str = key.decode('utf-8')
                if key_str != 'total':
                    categories[key_str] = int(value)
            
            if categories:
                top_cats = sorted(categories.items(), key=lambda x: x[1], reverse=True)[:5]
                cat_text = "\n".join([
                    f"`{cat}`: {val/3600:.1f}h" for cat, val in top_cats
                ])
                embed.add_field(
                    name="📊 Top Categories",
                    value=cat_text,
                    inline=False
                )
        else:
            embed.description = "No time tracked yet on this server"
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="👥 Who's Clocked In", style=discord.ButtonStyle.gray)
    async def who_clocked_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """View who's clocked in button handler"""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This dashboard is not for you!", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        await self._ensure_initialized()
        
        # Find all active sessions
        pattern = f"active_session:{self.guild_id}:*"
        cursor = 0
        active_sessions = []
        
        while True:
            cursor, keys = await self.tracker.redis.scan(cursor, match=pattern, count=100)
            
            for key in keys:
                session_data = await self.tracker.redis.get(key)
                if session_data:
                    import json
                    session = json.loads(session_data)
                    active_sessions.append(session)
            
            if cursor == 0:
                break
        
        embed = discord.Embed(
            title="👥 Currently Clocked In",
            color=discord.Color.blue()
        )
        
        if active_sessions:
            session_text = []
            for session in active_sessions[:10]:  # Limit to 10
                user = self.bot.get_user(session['user_id'])
                username = user.name if user else f"User {session['user_id']}"
                
                start_time = datetime.fromisoformat(session['start_time'])
                duration = int((datetime.now() - start_time).total_seconds())
                hours = duration / 3600
                
                session_text.append(
                    f"**{username}** - `{session['category']}` ({hours:.1f}h)"
                )
            
            embed.description = "\n".join(session_text)
            embed.set_footer(text=f"{len(active_sessions)} user(s) currently clocked in")
        else:
            embed.description = "No one is currently clocked in"
        
        await interaction.followup.send(embed=embed, ephemeral=True)


class ClockInModal(discord.ui.Modal, title="Clock In"):
    """Modal for clock in with category selection"""
    
    category = discord.ui.TextInput(
        label="Category",
        placeholder="Enter category (e.g., main, work, break)",
        default="main",
        required=True,
        max_length=50
    )
    
    description = discord.ui.TextInput(
        label="Description (Optional)",
        placeholder="Brief description of what you'll be working on",
        required=False,
        max_length=200,
        style=discord.TextStyle.paragraph
    )
    
    def __init__(self, bot, guild_id: int):
        super().__init__()
        self.bot = bot
        self.guild_id = guild_id
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        tracker, clock = await get_shared_role_tracker(self.bot)
        
        # Determine role
        category_lower = self.category.value.lower().strip()
        role = "Break" if category_lower == "break" else "Clocked In"
        
        metadata = {
            'description': self.description.value if self.description.value else None,
            'guild_name': interaction.guild.name,
            'channel_id': interaction.channel_id
        }
        
        result = await clock.clock_in(
            server_id=self.guild_id,
            user_id=interaction.user.id,
            category=category_lower,
            role=role,
            interaction=interaction,
            metadata=metadata
        )
        
        if result['success']:
            embed = discord.Embed(
                title="✅ Clocked In",
                description=f"Category: `{result['category']}`\n"
                           f"Started: <t:{int(result['start_time'].timestamp())}:t>",
                color=discord.Color.green()
            )
        else:
            embed = discord.Embed(
                title="❌ Error",
                description=result['message'],
                color=discord.Color.red()
            )
        
        await interaction.followup.send(embed=embed, ephemeral=True)


class DashboardManager:
    """Manages persistent shared dashboards"""
    
    def __init__(self, bot):
        self.bot = bot
        self.shared_dashboards: Dict[str, Dict[str, Any]] = {}  # guild_channel key -> dashboard info
        self.tracker = None
        self.clock = None
        
        # Ensure data directory exists
        DASHBOARD_DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing dashboards
        self.load_dashboards()
    
    def load_dashboards(self):
        """Load dashboards from JSON file"""
        if DASHBOARD_DATA_FILE.exists():
            try:
                with open(DASHBOARD_DATA_FILE, 'r') as f:
                    self.shared_dashboards = json.load(f)
                logger.info(f"Loaded {len(self.shared_dashboards)} shared dashboards from file")
            except Exception as e:
                logger.error(f"Error loading dashboards: {e}")
                self.shared_dashboards = {}
        else:
            self.shared_dashboards = {}
    
    def save_dashboards(self):
        """Save dashboards to JSON file"""
        try:
            with open(DASHBOARD_DATA_FILE, 'w') as f:
                json.dump(self.shared_dashboards, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving dashboards: {e}")
    
    def add_dashboard(self, guild_id: int, channel_id: int, message_id: int):
        """Add a new shared dashboard"""
        key = f"{guild_id}_{channel_id}"
        self.shared_dashboards[key] = {
            'guild_id': guild_id,
            'channel_id': channel_id,
            'message_id': message_id,
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat()
        }
        self.save_dashboards()
        logger.info(f"Added shared dashboard in channel {channel_id} of guild {guild_id}")
    
    def remove_dashboard(self, guild_id: int, channel_id: int):
        """Remove a dashboard"""
        key = f"{guild_id}_{channel_id}"
        if key in self.shared_dashboards:
            del self.shared_dashboards[key]
            self.save_dashboards()
            logger.info(f"Removed shared dashboard from channel {channel_id} in guild {guild_id}")
    
    def get_dashboard(self, guild_id: int, channel_id: int) -> Optional[Dict[str, Any]]:
        """Get dashboard info"""
        key = f"{guild_id}_{channel_id}"
        return self.shared_dashboards.get(key)
    
    def get_guild_dashboards(self, guild_id: int) -> List[Dict[str, Any]]:
        """Get all dashboards for a guild"""
        return [
            dashboard for key, dashboard in self.shared_dashboards.items()
            if dashboard['guild_id'] == guild_id
        ]
    
    async def update_dashboard_embed(self, dashboard_info: Dict[str, Any]) -> discord.Embed:
        """Create updated shared dashboard embed"""
        if not self.tracker or not self.clock:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
        
        guild_id = dashboard_info['guild_id']
        
        # Get guild
        guild = self.bot.get_guild(guild_id)
        guild_name = guild.name if guild else f"Guild {guild_id}"
        
        embed = discord.Embed(
            title=f"⏰ {guild_name} - Time Tracking Dashboard",
            description="Use the buttons below to clock in/out and view stats.\n*This dashboard is shared - anyone can use it!*",
            color=discord.Color.blue(),
            timestamp=datetime.now()
        )
        
        # Get server-wide stats
        try:
            # Count clocked in users
            pattern = f"active_session:{guild_id}:*"
            cursor = 0
            active_count = 0
            
            while True:
                cursor, keys = await self.tracker.redis.scan(cursor, match=pattern, count=100)
                active_count += len(keys)
                if cursor == 0:
                    break
            
            # Get server totals
            server_key = f"server_times:{guild_id}"
            server_data = await self.tracker.redis.hgetall(server_key)
            
            if server_data:
                total_seconds = int(server_data.get(b'total', b'0'))
                total_hours = total_seconds / 3600
                
                embed.add_field(
                    name="🌐 Server Statistics",
                    value=f"**Total Time:** {total_hours:.1f} hours\n"
                          f"**Currently Active:** {active_count} user(s)",
                    inline=False
                )
                
                # Top categories
                categories = {}
                for key, value in server_data.items():
                    key_str = key.decode('utf-8')
                    if key_str != 'total':
                        categories[key_str] = int(value)
                
                if categories:
                    top_cats = sorted(categories.items(), key=lambda x: x[1], reverse=True)[:3]
                    cat_text = "\n".join([
                        f"`{cat}`: {val/3600:.1f}h" for cat, val in top_cats
                    ])
                    embed.add_field(
                        name="📊 Top 3 Categories",
                        value=cat_text,
                        inline=False
                    )
            else:
                embed.add_field(
                    name="🌐 Server Statistics",
                    value="No time tracked yet on this server",
                    inline=False
                )
                
        except Exception as e:
            logger.error(f"Error getting server stats for dashboard: {e}")
            embed.add_field(
                name="📊 Statistics",
                value="Unable to fetch server statistics",
                inline=False
            )
        
        embed.add_field(
            name="🎯 Quick Actions",
            value="• ⏰ **Clock In** - Start tracking your time\n"
                  "• 🛑 **Clock Out** - Stop tracking your time\n"
                  "• 📊 **My Stats** - View your personal statistics\n"
                  "• 🌐 **Server Total** - View server-wide stats\n"
                  "• 👥 **Who's Clocked In** - See who's currently active",
            inline=False
        )
        
        embed.set_footer(text=f"Shared Dashboard • Auto-updates every 60s • Last updated")
        
        return embed
    
    async def update_all_dashboards(self):
        """Update all active shared dashboards"""
        for key, dashboard_info in list(self.shared_dashboards.items()):
            try:
                # Get channel and message
                channel = self.bot.get_channel(dashboard_info['channel_id'])
                if not channel:
                    logger.warning(f"Channel {dashboard_info['channel_id']} not found, removing dashboard")
                    self.remove_dashboard(dashboard_info['guild_id'], dashboard_info['channel_id'])
                    continue
                
                try:
                    message = await channel.fetch_message(dashboard_info['message_id'])
                except discord.NotFound:
                    logger.warning(f"Message {dashboard_info['message_id']} not found, removing dashboard")
                    self.remove_dashboard(dashboard_info['guild_id'], dashboard_info['channel_id'])
                    continue
                
                # Update embed
                embed = await self.update_dashboard_embed(dashboard_info)
                
                # Create view
                view = SharedDashboardView(
                    self.bot,
                    dashboard_info['guild_id']
                )
                
                # Update message
                await message.edit(embed=embed, view=view)
                
                # Update last_updated timestamp
                dashboard_info['last_updated'] = datetime.now().isoformat()
                
            except Exception as e:
                logger.error(f"Error updating dashboard {key}: {e}")
        
        # Save updated timestamps
        self.save_dashboards()


class DashboardCog(commands.Cog):
    """Interactive persistent dashboard for time tracking"""
    
    def __init__(self, bot):
        self.bot = bot
        self.tracker = None
        self.clock = None
        self.dashboard_manager = DashboardManager(bot)
        logger.info("DashboardCog initialized with persistent storage")
    
    async def cog_load(self):
        try:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
            self.dashboard_manager.tracker = self.tracker
            self.dashboard_manager.clock = self.clock
            
            # Start dashboard update loop
            self.dashboard_update_loop.start()
            
            logger.info("DashboardCog connected to tracker system and started update loop")
        except Exception as e:
            logger.error(f"Failed to initialize dashboard: {e}")
    
    async def _ensure_initialized(self):
        """Ensure tracker and clock are initialized"""
        if not self.tracker or not self.clock:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
    
    async def cog_unload(self):
        """Stop update loop on unload"""
        self.dashboard_update_loop.cancel()
        logger.info("DashboardCog unloaded and update loop stopped")
    
    @tasks.loop(seconds=60)
    async def dashboard_update_loop(self):
        """Update all dashboards every 60 seconds"""
        try:
            await self.dashboard_manager.update_all_dashboards()
        except Exception as e:
            logger.error(f"Error in dashboard update loop: {e}")
    
    @dashboard_update_loop.before_loop
    async def before_dashboard_update(self):
        """Wait for bot to be ready before starting loop"""
        await self.bot.wait_until_ready()
        logger.info("Dashboard update loop ready to start")
    
    @app_commands.command(name="dashboard", description="📊 Open interactive time tracking dashboard")
    @app_commands.describe(
        personal="Make this a personal ephemeral dashboard (default: False)"
    )
    async def dashboard(self, interaction: discord.Interaction, personal: bool = False):
        """Open the interactive dashboard - shared by default, personal if specified"""
        await interaction.response.defer(ephemeral=personal)
        
        try:
            await self._ensure_initialized()
            
            if personal:
                # Create personal ephemeral dashboard
                embed = discord.Embed(
                    title=f"⏰ Personal Time Tracking Dashboard",
                    description="Use the buttons below to interact with the time tracking system.\n*This is your personal dashboard - only you can see it!*",
                    color=discord.Color.purple()
                )
                
                # Get current status
                status = await self.clock.get_status(interaction.guild.id, interaction.user.id)
                
                if status['clocked_in']:
                    embed.add_field(
                        name="📊 Current Status",
                        value=f"⏰ **Clocked In** to `{status['category']}`\n"
                              f"Duration: {status['current_duration_formatted']}\n"
                              f"Started: <t:{int(status['start_time'].timestamp())}:R>",
                        inline=False
                    )
                else:
                    embed.add_field(
                        name="📊 Current Status",
                        value="✅ Not currently clocked in",
                        inline=False
                    )
                
                # Add summary stats
                if status.get('total_time', 0) > 0:
                    embed.add_field(
                        name="⏱️ Total Time",
                        value=status['total_time_formatted'],
                        inline=True
                    )
                    
                    if status.get('categories'):
                        top_cat = max(status['categories'].items(), key=lambda x: x[1]['percentage'])
                        embed.add_field(
                            name="🏆 Top Category",
                            value=f"`{top_cat[0]}`: {top_cat[1]['time']}",
                            inline=True
                        )
                
                embed.add_field(
                    name="🎯 Quick Actions",
                    value="• ⏰ **Clock In** - Start tracking time\n"
                          "• 🛑 **Clock Out** - Stop tracking time\n"
                          "• 📊 **My Stats** - View detailed statistics\n"
                          "• 🌐 **Server Total** - View server-wide stats\n"
                          "• 👥 **Who's Clocked In** - See active users",
                    inline=False
                )
                
                embed.set_footer(text="Personal Dashboard • Expires after 5 minutes of inactivity")
                
                # Create personal view
                view = PersonalDashboardView(
                    self.bot,
                    interaction.guild.id,
                    interaction.user.id
                )
                
                await interaction.followup.send(embed=embed, view=view, ephemeral=True)
                logger.info(f"Created personal ephemeral dashboard for user {interaction.user.id}")
                
            else:
                # Create shared persistent dashboard
                # Check if channel already has a dashboard
                existing = self.dashboard_manager.get_dashboard(interaction.guild.id, interaction.channel.id)
                if existing:
                    try:
                        channel = self.bot.get_channel(existing['channel_id'])
                        if channel:
                            message = await channel.fetch_message(existing['message_id'])
                            
                            embed = discord.Embed(
                                title="📊 Dashboard Already Exists",
                                description=f"This channel already has an active shared dashboard.",
                                color=discord.Color.blue()
                            )
                            embed.add_field(
                                name="🔗 Jump to Dashboard",
                                value=f"[Click here]({message.jump_url})",
                                inline=False
                            )
                            embed.add_field(
                                name="💡 Options",
                                value="• Use the existing dashboard above\n"
                                      "• Remove it with `/dashboard-remove` (Admin only)\n"
                                      "• Create a personal dashboard with `/dashboard personal:True`",
                                inline=False
                            )
                            await interaction.followup.send(embed=embed, ephemeral=True)
                            return
                    except discord.NotFound:
                        # Old dashboard was deleted, remove from tracking
                        self.dashboard_manager.remove_dashboard(interaction.guild.id, interaction.channel.id)
                
                # Create initial embed
                embed = await self.dashboard_manager.update_dashboard_embed({
                    'guild_id': interaction.guild.id,
                    'channel_id': interaction.channel.id,
                    'message_id': 0  # Will be updated
                })
                
                # Create persistent view
                view = SharedDashboardView(
                    self.bot,
                    interaction.guild.id
                )
                
                # Send message (NOT ephemeral)
                message = await interaction.followup.send(embed=embed, view=view)
                
                # Store dashboard info
                self.dashboard_manager.add_dashboard(
                    interaction.guild.id,
                    interaction.channel.id,
                    message.id
                )
                
                logger.info(f"Created shared persistent dashboard in channel {interaction.channel.id} of guild {interaction.guild.id}")
            
        except Exception as e:
            logger.error(f"Error creating dashboard: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description=f"Failed to create dashboard: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
    
    @app_commands.command(name="dashboard-remove", description="🗑️ Remove the shared dashboard from this channel (Admin only)")
    async def dashboard_remove(self, interaction: discord.Interaction):
        """Remove shared dashboard from current channel"""
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Check admin permissions for shared dashboard removal
            if not interaction.user.guild_permissions.administrator:
                embed = discord.Embed(
                    title="🔒 Admin Only",
                    description="Only administrators can remove shared dashboards.\n\n*Personal dashboards expire automatically after 5 minutes.*",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
            
            dashboard = self.dashboard_manager.get_dashboard(interaction.guild.id, interaction.channel.id)
            
            if not dashboard:
                embed = discord.Embed(
                    title="ℹ️ No Dashboard",
                    description="There is no active shared dashboard in this channel.",
                    color=discord.Color.blue()
                )
                
                # Show other dashboards in the server
                guild_dashboards = self.dashboard_manager.get_guild_dashboards(interaction.guild.id)
                if guild_dashboards:
                    channels_list = []
                    for db in guild_dashboards[:5]:
                        channel = self.bot.get_channel(db['channel_id'])
                        if channel:
                            channels_list.append(f"• {channel.mention}")
                    
                    if channels_list:
                        embed.add_field(
                            name="📍 Dashboards in Other Channels",
                            value="\n".join(channels_list),
                            inline=False
                        )
                
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
            
            # Try to delete the message
            try:
                channel = self.bot.get_channel(dashboard['channel_id'])
                if channel:
                    message = await channel.fetch_message(dashboard['message_id'])
                    await message.delete()
            except:
                pass  # Message already deleted or inaccessible
            
            # Remove from tracking
            self.dashboard_manager.remove_dashboard(interaction.guild.id, interaction.channel.id)
            
            embed = discord.Embed(
                title="✅ Dashboard Removed",
                description="The shared dashboard has been removed from this channel.",
                color=discord.Color.green()
            )
            embed.add_field(
                name="💡 Create New Dashboard",
                value="Anyone can create a new shared dashboard with `/dashboard`",
                inline=False
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            
            logger.info(f"Admin {interaction.user.id} removed shared dashboard from channel {interaction.channel.id}")
            
        except Exception as e:
            logger.error(f"Error removing dashboard: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description=f"Failed to remove dashboard: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

    @app_commands.command(name="forceclockout", description="👑 Force clock out a user (Admin only)")
    @app_commands.describe(
        user="User to force clock out",
        reason="Reason for force clockout (optional)"
    )
    async def force_clockout(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        reason: Optional[str] = None
    ):
        """Force clock out a user"""
        await interaction.response.defer()
        
        try:
            # Check admin permissions
            if not interaction.user.guild_permissions.administrator:
                embed = discord.Embed(
                    title="🔒 Admin Only",
                    description="Only administrators can force clock out users.",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed)
                return
            
            if not user:
                embed = discord.Embed(
                    title="❌ Missing User",
                    description="Please specify a user to force clock out.",
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed)
                return
            
            await self.cog_load()
            
            result = await self.clock.clock_out(interaction.guild.id, user.id, force=True)
            
            if result['success']:
                embed = discord.Embed(
                    title="✅ User Force Clocked Out",
                    description=f"Successfully force clocked out {user.mention}",
                    color=discord.Color.green()
                )
                
                embed.add_field(
                    name="📊 Session Details",
                    value=f"**Category:** `{result['category']}`\n"
                          f"**Duration:** {result['session_duration_formatted']}",
                    inline=False
                )
                
                if reason:
                    embed.add_field(name="📝 Reason", value=reason, inline=False)
                
                logger.info(f"Admin {interaction.user.id} force clocked out user {user.id}")
            else:
                embed = discord.Embed(
                    title="❌ Force Clockout Failed",
                    description=result['message'],
                    color=discord.Color.red()
                )
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in force clockout: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed)
    
    @app_commands.command(name="whoclocked", description="👥 See who's currently clocked in")
    async def who_clocked(self, interaction: discord.Interaction):
        """Show who's currently clocked in"""
        await interaction.response.defer()
        
        try:
            await self.cog_load()
            
            pattern = f"active_session:{interaction.guild.id}:*"
            cursor = 0
            active_sessions = []
            
            while True:
                cursor, keys = await self.tracker.redis.scan(cursor, match=pattern, count=100)
                
                for key in keys:
                    session_data = await self.tracker.redis.get(key)
                    if session_data:
                        session = json.loads(session_data)
                        active_sessions.append(session)
                
                if cursor == 0:
                    break
            
            embed = discord.Embed(
                title="👥 Currently Clocked In",
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            if active_sessions:
                active_sessions.sort(
                    key=lambda s: datetime.now() - datetime.fromisoformat(s['start_time']),
                    reverse=True
                )
                
                session_text = []
                for session in active_sessions[:25]:
                    user = self.bot.get_user(session['user_id'])
                    username = user.mention if user else f"User {session['user_id']}"
                    
                    start_time = datetime.fromisoformat(session['start_time'])
                    duration = int((datetime.now() - start_time).total_seconds())
                    hours = duration / 3600
                    duration_str = f"{hours:.1f}h" if hours >= 1 else f"{duration/60:.0f}m"
                    
                    session_text.append(f"• {username} - `{session['category']}` ({duration_str})")
                
                chunk_size = 10
                for i in range(0, len(session_text), chunk_size):
                    chunk = session_text[i:i + chunk_size]
                    field_name = "Active Sessions" if i == 0 else f"Active Sessions (cont.)"
                    embed.add_field(name=field_name, value="\n".join(chunk), inline=False)
                
                embed.set_footer(text=f"{len(active_sessions)} user(s) currently clocked in")
            else:
                embed.description = "No one is currently clocked in"
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in whoclocked: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description=f"Failed to fetch clocked in users: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed)


async def setup(bot):
    await bot.add_cog(DashboardCog(bot))
    logger.info("DashboardCog loaded successfully with persistent storage")