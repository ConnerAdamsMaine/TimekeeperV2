import aiosqlite
from loguru import logger

DB_FILE = "Data/Timesheets.db"

class Database:
    def __init__(self):
        self.db = None
        logger.add(
            "bot.database"
            format="{name}:{function}:{line} - <cyan>{time:MM-DD-YY HH:mm:ss}</cyan>\n<blue>{extra[tag]}</blue>: {message}\n\n",
            )
    
    async def connect(self):
        self.db = await aiosqlite.connect(DB_FILE)
        await self.db.execute("PRAGMA foreign_keys = ON")
        await self.db.commit()
        