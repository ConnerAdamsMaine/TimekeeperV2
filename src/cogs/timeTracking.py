import discord
from discord import app_commands
from discord.ext import commands
from Utils.Database import DBHelper
from Utils.dslParser import DSLParser
from pathlib import Path

class TimeTracking(commands.Cog):
    def __init__(self, bot, db_pool):
        self.bot = bot
        self.db = DBHelper(db_pool)

    @app_commands.command(name="clockin", description="Clock in for a specific category.")
    async def clockin(self, interaction: discord.Interaction, category: str):
        server_id = interaction.guild.id
        config_path = Path(f"servers/{server_id}/config.dsl")
        if not config_path.exists():
            return await interaction.response.send_message("Server config missing.")
        config = DSLParser.parseFile(config_path)
        categories = config.get("settings", {}).get("attributes", {}).get("categories", [])
        if category not in categories:
            return await interaction.response.send_message(f"Category {category} not found on this server.")
        await self.db.createServerTable(server_id)
        await self.db.clockIn(server_id, interaction.user.id, category)
        await interaction.response.send_message(f"{interaction.user.mention} clocked in under {category}.")

    @app_commands.command(name="clockout", description="Clock out for a specific category.")
    async def clockout(self, interaction: discord.Interaction):
        server_id = interaction.guild.id
        await self.db.clockOut(server_id, interaction.user.id)
        await interaction.response.send_message(f"{interaction.user.mention} clocked out.")

def setup(bot):
    bot.add_cog(TimeTracking(bot, bot.dbPool))
