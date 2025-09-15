import discord
from discord.ext import commands

import logging
import pathlib
import os
import asyncio

from Utils.activation import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Bot(commands.Bot):
    def __init__(self, prefix: str, intents: discord.Intents):
        super().__init__(command_prefix=prefix, intents=intents)
        self.added_cogs = []
    
    async def setup_hook(self):
        await self.load_server_command()
        for dir in os.walk('commands'):
            for file in dir[2]:
                if file.endswith('.py') and not file.startswith('__'):
                    path = pathlib.Path(dir[0]) / file
                    cog = f"{path.parent.as_posix().replace('/', '.')}.{path.stem}"
                    try:
                        await self.load_extension(cog)
                        self.added_cogs.append(cog)
                        logger.log(logging.INFO, f'Loaded cog: {cog}')
                    except Exception as e:
                        logger.log(logging.ERROR, f'Failed to load cog {cog}: {e}')
        synced = await self.tree.sync()
        logger.log(logging.INFO, f'Synced {len(synced)} commands to the test guild.')
    
    async def on_ready(self):
        logger.log(logging.INFO, f'Logged in as {self.user} (ID: {self.user.id})')

    async def on_command_error(self, ctx: commands.Context, error):
        logger.log(logging.ERROR, f'Error occurred in command "{ctx.command}": {error}')
    
    async def load_server_command(self):
        @self.tree.command(name="activate", description="Activate the bot in this server")
        async def activate(interaction: discord.Interaction, key: str):
            if interaction.user.guild_permissions.administrator is False or interaction.user.id != interaction.guild.owner_id:
                await interaction.response.send_message("You need to be an administrator or the owner to activate the bot.")
                return
            await interaction.response.send_message(f"Activating bot with key: {key}")
            if validate_activation_key(interaction.guild.id, key):
                await interaction.followup.send("Activation successful!")
                mark_guild_activated(interaction.guild.id)
            else:
                await interaction.followup.send("Invalid activation key.")
        logger.log(logging.INFO, "Server command /activate loaded.")
        
        @self.tree.command(name="generate_key", description="Generate an activation key for this server")
        async def generate_key(interaction: discord.Interaction, secret: str, server_id: int = None):
            if interaction.user.id != int(os.getenv("DEV_ID")):
                await interaction.response.send_message("You do not have permission to generate keys.")
                return
            key = generate_activation_key((interaction.guild.id if not server_id else server_id) , secret)
            await interaction.response.send_message(f"Generated activation key: {key}", ephemeral=True)
        logger.log(logging.INFO, "Server command /generate_key loaded.")

    async def on_server_join(self, guild: discord.Guild):
        logger.log(logging.INFO, f'Joined new guild: {guild.name} (ID: {guild.id})')
        for channel in guild.text_channels:
            if channel.permissions_for(guild.me).send_messages:
                await channel.send("Hello! You have 10 minutes to activate the bot using /activate {secret_key}")
                break
            await asyncio.sleep(60*10)
            await guild.leave()
            logger.log(logging.INFO, f'Left guild {guild.name} due to no activation.')