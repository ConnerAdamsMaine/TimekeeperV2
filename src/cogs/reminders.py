import discord
from discord.ext import tasks, commands
from Utils.Database import DBHelper
from pathlib import Path
from Utils.dslParser import DSLParser

class Reminders(commands.Cog):
    def __init__(self, bot: commands.Bot, dbPool):
        self.bot = bot
        self.db = DBHelper(dbPool)
        self.remindersLoop.start()
    
    def getServerDb(serverId: int):
        dbPath = Path(f"servers/{serverId}/server.db")
        dbPath.parent.mkdir(parents=True, exist_ok=True)
        return DBHelper(dbPath)
    
    @tasks.loop(minutes=1)
    async def remindersLoop(self):
        for serverPath in Path("servers").iterdir():
            if serverPath.is_dir():
                serverId = int(serverPath.name)
                configPath = serverPath / "config.dsl"
                config = DSLParser.parseFile(configPath)
                interval = int(config.get("settings", {}).get("reminderInterval", 1800))
                users = await self.db.getClockedInUsers(serverId)
                for userRow in users:
                    user = self.bot.get_user(userRow['userId'])
                    if user:
                        await user.send(f"Reminder: You have been clocked in for category '{userRow['category']}' since {userRow['clockIn']}. Please remember to clock out if you are done.")
    
    @remindersLoop.before_loop
    async def beforeLoop(self):
        await self.bot.wait_until_ready()

async def setup(bot: commands.Bot):
    bot.add_cog(Reminders(bot, bot.dbPool))