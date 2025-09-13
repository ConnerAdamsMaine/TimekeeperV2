import discord
from discord import app_commands
from discord.ext import commands

import logging
import asyncio

from Utils.timekeeper import create_ultimate_system

logger = logging.getLogger("commands.clockin")
logger.setLevel(logging.INFO)

class ClockIn(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.tracker = None
        self.clock = None
        self._initialization_lock = asyncio.Lock()
        self._initialized = False

    async def _ensure_initialized(self):
        """Ensure the tracker and clock are initialized"""
        if self._initialized:
            return
        
        async with self._initialization_lock:
            if self._initialized:
                return
            
            try:
                self.tracker, self.clock = await create_ultimate_system()
                self._initialized = True
                logger.info("Time tracking system initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize time tracking system: {e}")
                raise

    async def _check_permissions(self, interaction: discord.Interaction) -> tuple[bool, str]:
        """Check if user has permission to use time tracking"""
        await self._ensure_initialized()
        
        # Get the config cog to use its permission checking
        config_cog = self.bot.get_cog("TimeTrackerConfig")
        if config_cog:
            return await config_cog.check_user_permissions(interaction)
        
        # Fallback if config cog not loaded
        return True, ""

    @app_commands.command(name="clockin", description="Clock in to start your work session.")
    @app_commands.describe(category="Work category to clock into (e.g., 'development', 'meetings', 'documentation')")
    async def clock_in(self, interaction: discord.Interaction, category: str = "main"):
        try:
            # Check permissions first
            can_use, reason = await self._check_permissions(interaction)
            if not can_use:
                embed = discord.Embed(
                    title="🚫 Permission Denied",
                    description=reason,
                    color=discord.Color.red()
                )
                embed.add_field(
                    name="💡 Need help?",
                    value="Contact a server administrator if you believe this is an error.",
                    inline=False
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                logger.info(f"User {interaction.user.id} denied access: {reason}")
                return

            # Ensure system is initialized
            await self._ensure_initialized()
            
            # Defer response to avoid timeout during processing
            await interaction.response.defer(ephemeral=True)
            
            # Attempt to clock in
            result = await self.clock.clock_in(interaction.guild.id, interaction.user.id, category)
            
            if result["success"]:
                # Create a nice embed for successful clock in
                embed = discord.Embed(
                    title="✅ Clocked In Successfully",
                    color=discord.Color.green(),
                    timestamp=result["start_time"]
                )
                
                embed.add_field(
                    name="📋 Category",
                    value=f"`{result['category']}`",
                    inline=True
                )
                
                embed.add_field(
                    name="🕐 Started At",
                    value=result["start_time"].strftime("%H:%M:%S"),
                    inline=True
                )
                
                # Add some motivational messages
                motivational_messages = [
                    "Let's get productive! 💪",
                    "Time to focus! 🎯",
                    "Great work starting your session! 🚀",
                    "Ready to tackle some tasks! ⚡",
                    "Focus mode activated! 🧠"
                ]
                import random
                embed.add_field(
                    name="💡 Motivation",
                    value=random.choice(motivational_messages),
                    inline=False
                )
                
                embed.set_footer(text=f"Good luck, {interaction.user.display_name}!")
                
                await interaction.followup.send(embed=embed)
                
                logger.info(f"User {interaction.user.id} clocked into '{category}' in server {interaction.guild.id}")
            
            else:
                # User is already clocked in
                embed = discord.Embed(
                    title="⚠️ Already Clocked In",
                    description=result["message"],
                    color=discord.Color.orange()
                )
                
                # Show current session info if available
                if "current_session" in result:
                    session = result["current_session"]
                    embed.add_field(
                        name="Current Session",
                        value=f"Category: `{session['category']}`\nStarted: {session['start_time'].strftime('%H:%M:%S')}",
                        inline=False
                    )
                    
                    embed.add_field(
                        name="💡 Tip",
                        value="Use `/clockout` to end your current session first!",
                        inline=False
                    )
                
                await interaction.followup.send(embed=embed)
                
        except Exception as e:
            # Enhanced error handling
            error_embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred while clocking in: {str(e)}",
                color=discord.Color.red()
            )
            
            # Add some helpful troubleshooting info
            error_embed.add_field(
                name="💡 What you can try:",
                value="• Wait a moment and try again\n• Make sure the category name is valid\n• Contact an admin if the problem persists",
                inline=False
            )
            
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(embed=error_embed, ephemeral=True)
                else:
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
            except:
                # Fallback if embed fails
                try:
                    await interaction.followup.send(f"An error occurred while clocking in: {str(e)}", ephemeral=True)
                except:
                    logger.error("Could not send error message to user")
            
            # Enhanced logging
            logger.error(f"Error in clock_in command: {e}", exc_info=True)
            
            if self.tracker:
                try:
                    health = await self.tracker.health_check()
                    logger.info(f"System health after error: {health['status']}")
                    logger.info(f"Connected: {self.tracker._connected}")
                    
                    # Log circuit breaker state if it's open
                    if health['components']['circuit_breaker']['status'] != 'healthy':
                        logger.warning(f"Circuit breaker state: {health['components']['circuit_breaker']}")
                        
                except Exception as health_error:
                    logger.error(f"Could not get system health check: {health_error}")
            
            logger.info(f"""Error context:
User: {interaction.user.id} ({interaction.user.name})
Guild: {interaction.guild.id if interaction.guild else 'DM'}
Category: {category}
Initialized: {self._initialized}
""")

    @app_commands.command(name="status", description="Check your current time tracking status.")
    async def status(self, interaction: discord.Interaction):
        """Check current status - no permission restrictions for viewing"""
        try:
            await self._ensure_initialized()
            await interaction.response.defer(ephemeral=True)
            
            status = await self.clock.get_status(interaction.guild.id, interaction.user.id)
            
            if status["clocked_in"]:
                # Currently clocked in
                embed = discord.Embed(
                    title="⏰ Currently Clocked In",
                    color=discord.Color.blue(),
                    timestamp=status["start_time"]
                )
                
                embed.add_field(
                    name="📋 Category",
                    value=f"`{status['category']}`",
                    inline=True
                )
                
                embed.add_field(
                    name="⏱️ Current Duration",
                    value=status["current_duration_formatted"],
                    inline=True
                )
                
                embed.add_field(
                    name="🕐 Started At",
                    value=status["start_time"].strftime("%H:%M:%S"),
                    inline=True
                )
                
            else:
                # Not clocked in, show summary
                embed = discord.Embed(
                    title="📊 Time Tracking Summary",
                    color=discord.Color.green()
                )
                
                embed.add_field(
                    name="🕐 Total Time",
                    value=status["total_time_formatted"],
                    inline=True
                )
                
                # Show top 3 categories
                if status["categories"]:
                    sorted_categories = sorted(
                        [(cat, time_str) for cat, time_str in status["categories"].items() if time_str != "0s"],
                        key=lambda x: int(x[1].split()[0]) if x[1].endswith('s') else 0,
                        reverse=True
                    )[:3]
                    
                    if sorted_categories:
                        category_text = "\n".join([f"`{cat}`: {time}" for cat, time in sorted_categories])
                        embed.add_field(
                            name="🏆 Top Categories",
                            value=category_text,
                            inline=False
                        )
                
                # Check if user can clock in
                can_use, reason = await self._check_permissions(interaction)
                if can_use:
                    embed.add_field(
                        name="💡 Ready to work?",
                        value="Use `/clockin` to start a new session!",
                        inline=False
                    )
                else:
                    embed.add_field(
                        name="🚫 Note",
                        value=f"Time tracking restricted: {reason}",
                        inline=False
                    )
            
            embed.set_footer(text=f"User: {interaction.user.display_name}")
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            error_embed = discord.Embed(
                title="❌ Error",
                description=f"Could not retrieve status: {str(e)}",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)
            logger.error(f"Error in status command: {e}")

    async def cog_unload(self):
        """Clean up when cog is unloaded"""
        if self.tracker:
            try:
                await self.tracker.close()
                logger.info("Time tracking system closed successfully")
            except Exception as e:
                logger.error(f"Error closing time tracking system: {e}")