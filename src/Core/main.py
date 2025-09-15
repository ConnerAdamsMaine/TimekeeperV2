import discord
from discord.ext import commands

import logging
import pathlib
import os
import argparse

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Bot(commands.Bot):
    def __init__(self, prefix: str, intents: discord.Intents):
        super().__init__(command_prefix=prefix, intents=intents)
        self.added_cogs = []
    
    async def setup_hook(self):
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