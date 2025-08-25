from discord.ext import tasks, commands
from Utils.Database import DBHelper
from pathlib import Path
from Utils.dslParser import DSLParser

class Reminders(commands.Cog):
    def __init__(self, bot, dbPool):
        self.bot = bot
        self.db = DBHelper(dbPool)
        self.reminderLoop.start()

    @tasks.loop(minutes=1)
    async def reminderLoop(self):
        for serverPath in Path("servers").iterdir():
            if not serverPath.is_dir():
                continue
            serverId = int(serverPath.name)
            configPath = serverPath / "config.dsl"
            config = DSLParser.parseFile(configPath)
            interval = int(config.get("settings", {}).get("attributes", {}).get("reminderInterval", 1800))
            users = await self.db.getClockedInUsers(serverId)
            for userRow in users:
                user = self.bot.get_user(userRow["userId"])
                if user:
                    await user.send(f"You are still clocked in under {userRow['category']} on server {serverId}.")

    @reminderLoop.before_loop
    async def beforeLoop(self):
        await self.bot.wait_until_ready()

def setup(bot):
    bot.add_cog(Reminders(bot, bot.dbPool))
