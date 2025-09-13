import discord
from discord import app_commands
from discord.ext import commands

import logging
import asyncio

from Utils.timekeeper import create_clock_manager

logger = logging.getLogger("commands.clockin")
logger.setLevel(logging.INFO)

class ClockIn(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

        self.tracker, self.clock = asyncio.run(create_clock_manager())

    @app_commands.command(name="clockin", description="Clock in to start your work session.")
    async def clock_in(self, interaction: discord.Interaction, category: str = "main"):
        try:
            await self.clock.clock_in(interaction.guild.id, interaction.user.id, category)
            await interaction.response.send_message("You have successfully clocked in!", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"An error occurred while clocking in: {e}", ephemeral=True)
            logger.error(f"Error in clock_in command: {e}")
            logger.info(f"""Relating to prior error: 
1: {self.tracker._connected}
2: {interaction.user.id}: {interaction.user.name}
3: {interaction.context}
""")

async def setup(bot: commands.Bot):
    await bot.add_cog(ClockIn(bot))
    logging.log(logging.INFO, "ClockIn Cog Loaded")