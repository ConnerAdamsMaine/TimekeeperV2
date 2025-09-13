import discord
from discord import app_commands
from discord.ext import commands

import logging
import asyncio

from Utils.timekeeper import create_clock_manager

logger = logging.getLogger("commands.clockout")
logger.setLevel(logging.INFO)

class ClockOut(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        
        self.tracker, self.clock = asyncio.run(create_clock_manager())
    
    @app_commands.command(name="clockout", description="Clock out to end your work session.")
    async def clock_out(self, interaction: discord.Interaction, category: str = "main"):
        try:
            await self.clock.clock_out(interaction.guild.id, interaction.user.id)
        except Exception as e:
            await interaction.response.send_message(f"An error occurred while clocking out: {e}", ephemeral=True)
            logger.error(f"Error in clock_out command: {e}")
            logger.info(f"""Relating to prior error:
1: Connected: {self.tracker._connected}
2: {interaction.user.id}: {interaction.user.name}
3: {interaction.context}
""")

async def setup(bot: commands.Bot):
    await bot.add_cog(ClockOut(bot))
    logging.log(logging.INFO, "ClockOut Cog Loaded")