import discord
from discord import app_commands
from discord.ext import commands

from Utils.timemanager import TimeManager

class ClockIn(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        
        self.TimeManager = TimeManager

    @app_commands.command(name="clockin", description="Clock in to start your work session.")
    async def clock_in(self, interaction: discord.Interaction):
        
        await interaction.response.send_message("You have successfully clocked in!", ephemeral=True)