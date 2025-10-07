import discord
from discord.ext import commands
from discord import app_commands
import logging
from typing import Optional, Dict, Any, List
import asyncio
from datetime import datetime

from Utils.timekeeper import get_shared_role_tracker

logger = logging.getLogger(__name__)


class DashboardView(discord.ui.View):
    """Interactive dashboard for time tracking"""
    
    def __init__(self, bot, guild_id: int, user_id: int):
        super().__init__(timeout=300)  # 5 minute timeout
        self.bot = bot
        self.guild_id = guild_id
        self.user_id = user_id
        self.tracker = None
        self.clock = None
    
    async def _ensure_initialized(self):
        if not self.tracker or not self.clock:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
    
    @discord.ui.button(label="⏰ Clock In", style=discord.ButtonStyle.green, custom_id="clockin_btn")
    async def clock_in_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clock in button handler"""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This dashboard is not for you!", ephemeral=True)
            return
        
        await interaction.response.send_modal(ClockInModal(self.bot, self.guild_id))
    
    @discord.ui.button(label="🛑 Clock Out", style=discord.ButtonStyle.red, custom_id="clockout_btn")
    async def clock_out_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Clock out button handler"""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This dashboard is not for you!", ephemeral=True)
            return
        
        await interaction.response.defer()
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
    
    @discord.ui.button(label="📊 My Stats", style=discord.ButtonStyle.blurple, custom_id="stats_btn")
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
    
    @discord.ui.button(label="🌐 Server Total", style=discord.ButtonStyle.gray, custom_id="server_btn")
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
    
    @discord.ui.button(label="👥 Who's Clocked In", style=discord.ButtonStyle.gray, custom_id="whoclocked_btn")
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


class DashboardCog(commands.Cog):
    """Interactive dashboard for time tracking"""
    
    def __init__(self, bot):
        self.bot = bot
        self.tracker = None
        self.clock = None
        logger.info("DashboardCog initialized")
    
    async def cog_load(self):
        try:
            self.tracker, self.clock = await get_shared_role_tracker(self.bot)
            logger.info("DashboardCog connected to tracker system")
        except Exception as e:
            logger.error(f"Failed to initialize dashboard: {e}")
    
    @app_commands.command(name="dashboard", description="📊 Open interactive time tracking dashboard")
    async def dashboard(self, interaction: discord.Interaction):
        """Open the interactive dashboard"""
        embed = discord.Embed(
            title="⏰ Time Tracking Dashboard",
            description="Use the buttons below to interact with the time tracking system",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="⏰ Clock In",
            value="Start tracking your time",
            inline=True
        )
        
        embed.add_field(
            name="🛑 Clock Out",
            value="Stop tracking your time",
            inline=True
        )
        
        embed.add_field(
            name="📊 My Stats",
            value="View your time tracking statistics",
            inline=True
        )
        
        embed.add_field(
            name="🌐 Server Total",
            value="View server-wide statistics",
            inline=True
        )
        
        embed.add_field(
            name="👥 Who's Clocked In",
            value="See who's currently tracking time",
            inline=True
        )
        
        view = DashboardView(self.bot, interaction.guild.id, interaction.user.id)
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


    @app_commands.command(name="forceclockout", description="👑 Force clock out a user (Admin only)")
    @app_commands.describe(
        user="User to force clock out (use @ mention or ID)",
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
                embed.add_field(
                    name="💡 Usage",
                    value="`/forceclockout user:@username`\nor\n`/forceclockout user:<user_id>`",
                    inline=False
                )
                await interaction.followup.send(embed=embed)
                return
            
            await self.cog_load()
            
            # Attempt to clock out the user
            result = await self.clock.clock_out(
                interaction.guild.id,
                user.id,
                force=True
            )
            
            if result['success']:
                embed = discord.Embed(
                    title="✅ User Force Clocked Out",
                    description=f"Successfully force clocked out {user.mention}",
                    color=discord.Color.green()
                )
                
                embed.add_field(
                    name="📊 Session Details",
                    value=f"**Category:** `{result['category']}`\n"
                          f"**Duration:** {result['session_duration_formatted']}\n"
                          f"**Session:** <t:{int(result['start_time'].timestamp())}:t> → <t:{int(result['end_time'].timestamp())}:t>",
                    inline=False
                )
                
                if reason:
                    embed.add_field(
                        name="📝 Reason",
                        value=reason,
                        inline=False
                    )
                
                embed.add_field(
                    name="👤 Admin",
                    value=interaction.user.mention,
                    inline=True
                )
                
                # Try to notify the user
                try:
                    dm_embed = discord.Embed(
                        title="⚠️ Force Clocked Out",
                        description=f"You were force clocked out from **{interaction.guild.name}** by an administrator.",
                        color=discord.Color.orange()
                    )
                    
                    dm_embed.add_field(
                        name="Session Details",
                        value=f"Category: `{result['category']}`\nDuration: {result['session_duration_formatted']}",
                        inline=False
                    )
                    
                    if reason:
                        dm_embed.add_field(
                            name="Reason",
                            value=reason,
                            inline=False
                        )
                    
                    await user.send(embed=dm_embed)
                    embed.set_footer(text="User has been notified via DM")
                except:
                    embed.set_footer(text="Could not send DM to user")
                
                logger.info(f"Admin {interaction.user.id} force clocked out user {user.id} in guild {interaction.guild.id}")
                
            else:
                embed = discord.Embed(
                    title="❌ Force Clockout Failed",
                    description=result['message'],
                    color=discord.Color.red()
                )
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in force clockout command: {e}")
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
            
            # Find all active sessions
            pattern = f"active_session:{interaction.guild.id}:*"
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
                color=discord.Color.blue(),
                timestamp=datetime.now()
            )
            
            if active_sessions:
                # Sort by duration (longest first)
                active_sessions.sort(
                    key=lambda s: datetime.now() - datetime.fromisoformat(s['start_time']),
                    reverse=True
                )
                
                session_text = []
                for session in active_sessions[:25]:  # Limit to 25
                    user = self.bot.get_user(session['user_id'])
                    username = user.mention if user else f"User {session['user_id']}"
                    
                    start_time = datetime.fromisoformat(session['start_time'])
                    duration = int((datetime.now() - start_time).total_seconds())
                    hours = duration / 3600
                    
                    # Format duration
                    if hours >= 1:
                        duration_str = f"{hours:.1f}h"
                    else:
                        minutes = duration / 60
                        duration_str = f"{minutes:.0f}m"
                    
                    session_text.append(
                        f"• {username} - `{session['category']}` ({duration_str})"
                    )
                
                # Split into chunks if too long
                chunk_size = 10
                for i in range(0, len(session_text), chunk_size):
                    chunk = session_text[i:i + chunk_size]
                    field_name = "Active Sessions" if i == 0 else f"Active Sessions (cont.)"
                    embed.add_field(
                        name=field_name,
                        value="\n".join(chunk),
                        inline=False
                    )
                
                embed.set_footer(text=f"{len(active_sessions)} user(s) currently clocked in")
            else:
                embed.description = "No one is currently clocked in"
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Error in whoclocked command: {e}")
            embed = discord.Embed(
                title="❌ Error",
                description=f"Failed to fetch clocked in users: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed)


async def setup(bot):
    await bot.add_cog(DashboardCog(bot))
    logger.info("DashboardCog loaded successfully")