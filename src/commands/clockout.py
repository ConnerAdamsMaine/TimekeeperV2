import discord
from discord import app_commands
from discord.ext import commands

import logging
import asyncio

from Utils.timekeeper import create_ultimate_system

logger = logging.getLogger("commands.clockout")
logger.setLevel(logging.INFO)

class ClockOut(commands.Cog):
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

    @app_commands.command(name="clockout", description="Clock out to end your work session.")
    async def clock_out(self, interaction: discord.Interaction):
        """Clock out command with permission checking"""
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
            
            # Attempt to clock out
            result = await self.clock.clock_out(interaction.guild.id, interaction.user.id)
            
            if result["success"]:
                # Create a nice embed for successful clock out
                embed = discord.Embed(
                    title="⏹️ Clocked Out Successfully",
                    color=discord.Color.green(),
                    timestamp=result["end_time"]
                )
                
                embed.add_field(
                    name="📋 Category",
                    value=result["category"],
                    inline=True
                )
                
                embed.add_field(
                    name="⏱️ Session Duration",
                    value=result["session_duration_formatted"],
                    inline=True
                )
                
                embed.add_field(
                    name="🎯 Total in Category",
                    value=result["category_total_formatted"],
                    inline=True
                )
                
                embed.add_field(
                    name="🕐 Session Time",
                    value=f"{result['start_time'].strftime('%H:%M')} - {result['end_time'].strftime('%H:%M')}",
                    inline=False
                )
                
                # Add productivity insights if available
                if self.tracker.analytics:
                    try:
                        insights = await self.tracker.get_insights(interaction.guild.id, interaction.user.id)
                        if insights:
                            embed.add_field(
                                name="📈 Productivity Score",
                                value=f"{insights.get('productivity_score', 0):.1f}/100",
                                inline=True
                            )
                    except:
                        pass  # Don't fail if insights unavailable
                
                embed.set_footer(text=f"Great work, {interaction.user.display_name}!")
                
                await interaction.followup.send(embed=embed)
                
                logger.info(f"User {interaction.user.id} clocked out: {result['session_duration']}s in {result['category']}")
            
            else:
                # User wasn't clocked in
                embed = discord.Embed(
                    title="❌ Not Clocked In",
                    description=result["message"],
                    color=discord.Color.orange()
                )
                
                # Show current status
                status = await self.clock.get_status(interaction.guild.id, interaction.user.id)
                if not status["clocked_in"] and status.get("total_time", 0) > 0:
                    embed.add_field(
                        name="📊 Your Total Time",
                        value=status["total_time_formatted"],
                        inline=False
                    )
                    
                    embed.add_field(
                        name="💡 Tip",
                        value="Use `/clockin` to start a new session!",
                        inline=False
                    )
                
                await interaction.followup.send(embed=embed)
                
        except Exception as e:
            # Enhanced error handling
            error_embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred while clocking out: {str(e)}",
                color=discord.Color.red()
            )
            
            try:
                if interaction.response.is_done():
                    await interaction.followup.send(embed=error_embed, ephemeral=True)
                else:
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
            except:
                # Fallback if embed fails
                await interaction.followup.send(f"An error occurred: {str(e)}", ephemeral=True)
            
            # Enhanced logging
            logger.error(f"Error in clock_out command: {e}", exc_info=True)
            
            if self.tracker:
                try:
                    health = await self.tracker.health_check()
                    logger.info(f"System health after error: {health['status']}")
                    logger.info(f"Connected: {self.tracker._connected}")
                except:
                    logger.error("Could not get system health check")
            
            logger.info(f"""Error context:
User: {interaction.user.id} ({interaction.user.name})
Guild: {interaction.guild.id if interaction.guild else 'DM'}
Initialized: {self._initialized}
""")

    async def cog_unload(self):
        """Clean up when cog is unloaded"""
        if self.tracker:
            try:
                await self.tracker.close()
                logger.info("Time tracking system closed successfully")
            except Exception as e:
                logger.error(f"Error closing time tracking system: {e}")



async def setup(bot: commands.Bot):
    await bot.add_cog(ClockOut(bot))
    logging.log(logging.INFO, "ClockOut Cog Loaded")