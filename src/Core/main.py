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
        self.start_database()
        self.added_cogs = []
    
    async def setup_hook(self):
        for dir in os.walk('cogs'):
            for file in dir[2]:
                if file.endswith('.py'):
                    path = pathlib.Path(dir[0]) / file
                    cog = f"{path.parent.as_posix().replace('/', '.')}.{path.stem}"
                    try:
                        await self.load_extension(cog)
                        self.added_cogs.append(cog)
                        logger.log(logging.INFO, f'Loaded cog: {cog}')
                    except Exception as e:
                        logger.log(logging.ERROR, f'Failed to load cog {cog}: {e}')
    
    async def on_ready(self):
        logger.log(logging.INFO, f'Logged in as {self.user} (ID: {self.user.id})')

    async def on_command_error(self, ctx: commands.Context, error):
        logger.log(logging.ERROR, f'Error occurred in command "{ctx.command}": {error}')

    def start_database(self):
        pass
    
    def soft_shutdown(self):
        pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Discord Bot", prefix_chars="/")
    parser.add_argument('/P', type=str, default='!', help='Command prefix for the bot')
    parser.add_argument('/T', type=str, required=True, help='Discord bot token')
    parser.add_argument('/LL', type=str, default='INFO', help='Logging level')
    parser.add_argument('/D', type=bool, default=False, help='Enable debug mode')
    
    args = parser.parse_args()

    logging.basicConfig(level=args.LL)
    logger.info("Starting bot...")