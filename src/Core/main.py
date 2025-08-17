import discord
from discord import app_commands
from discord.ext import commands
from discord.activity import Activity

from loguru import logger

from datetime import datetime
from time import perf_counter_ns

import os
import json


BotLogger = logger.bind(name="TimekeeperV2")
CommandLogger = logger.bind(name="TimekeeperV2.Commands")

commandsDict = json.load(open('C:\\Users\\conne\\Desktop\\TimekeeperV2\\docs\\commands.json', 'r', encoding='utf-8'))

class TimekeeperV2(commands.Bot):
    def __init__(self):
        logger.add("logs/bot.log", rotation="10 MB", retention="10 days", level="INFO")
        self.startTime = perf_counter_ns()
        BotActivity = Activity(
            name="Watching the time clock",
            url="timekeeper.404connernotfound.dev",
            details="Timekeeping Bot",
            type=discord.ActivityType.watching
        )
        super().__init__(
            command_prefix=".",
            intents=discord.Intents.all(),
            activity=BotActivity,
            status=discord.Status.online,
            help_command=None,
            case_insensitive=True,
            owner_ids=[473622504586477589, 211991776070729728]
        )

    async def on_ready(self):
        BotLogger.info(f"TimekeeperV2 is online! Logged in as {self.user.name} ({self.user.id})")
        BotLogger.info(f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        BotLogger.info(f"Startup time: {round((perf_counter_ns() - self.startTime) / 1_000_000)}ms")
        BotLogger.info(f"Latency: {round(self.latency * 1000)}ms")
        BotLogger.info("Bot is ready to track time!")
    
    async def setup_hook(self):
        for filename in os.listdir('./Commands'):
            if filename.endswith('.py') and not filename.startswith('_'):
                command = filename[:-3]
                try:
                    for commandDict in commandsDict['commands']:
                        if commandDict['name'] == command:
                            CommandLogger.info(f"""Loading command: {command}\nUsage: {commandDict['usage']} | Description: {commandDict['description']} | Aliases: {commandDict['aliases']}""")
                        await self.load_extension(f'Commands.{command}')
                except Exception as e:
                    CommandLogger.error(f"Failed to load command {command}: {e}")

    async def on_command_error(self, context, exception):
        CommandLogger.error(f"An error occurred while executing the command: {context.command} - {exception}")