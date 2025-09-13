import discord
from discord import app_commands
from discord.ext import commands

class ClockIn(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
    
    @app_commands.command(name="clockin", description="Clock in to start your work session.")
    async def clock_in(self, interaction: discord.Interaction):
        # Add clock in mechanism
        await interaction.response.send_message("You have successfully clocked in!", ephemeral=True)