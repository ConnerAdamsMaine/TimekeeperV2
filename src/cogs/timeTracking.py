import discord
from discord import app_commands
from discord.ext import commands
from Utils.Database import DBHelper
from Utils.dslParser import DSLParser
from pathlib import Path

class TimeTracking(commands.Cog):
    def __init__(self, bot: commands.Bot, dbPool):
        self.bot = bot
        self.db = DBHelper(dbPool)
    
    @app_commands.command(name="clockin", description="Clock in to start tracking your time.")
    async def clockIn(self, interaction: discord.Interaction, category: str):
        serverId = interaction.guild.id
        configPath = Path(f"/Servers/{serverId}/config.dsl")
        if not configPath.exists():
            await interaction.response.send_message("Server configuration not found. Please contact an administrator.", ephemeral=True)
            return
        config = DSLParser.parseFile(configPath)
        categories = config.get("settings", {}).get("attributes", {}).get("categories", [])
        if category not in categories:
            await interaction.response.send_message(f"Invalid category. Available categories: {', '.join(categories)}", ephemeral=True)
            return
        await self.db.createServerTable(serverId)
        await self.db.clockIn(serverId, interaction.user.id, category)
        await interaction.response.send_message(f"You have successfully clocked in under the category '{category}'.", ephemeral=True)
    
    @app_commands.command(name="clockout", description="Clock out to stop tracking your time.")
    async def clockOut(self, interaction: discord.Interaction):
        serverId = interaction.guild.id
        await self.db.createServerTable(serverId)
        success, category, duration = await self.db.clockOut(serverId, interaction.user.id)
        if not success:
            await interaction.response.send_message("You are not currently clocked in.", ephemeral=True)
            return
        hours, remainder = divmod(duration, 3600)
        minutes, seconds = divmod(remainder, 60)
        await interaction.response.send_message(f"You have successfully clocked out from the category '{category}'. Total time: {hours}h {minutes}m {seconds}s", ephemeral=True)

async def setup(bot: commands.Bot):
    bot.add_cog(TimeTracking(bot, bot.dbPool))