import sqlite3
from pathlib import Path
import datetime
from datetime import datetime as dt

class DBHelper:
    def __init__(self, dbPath: Path):
        self.dbPath = dbPath
        self.conn = sqlite3.connect(dbPath)
        self.conn.row_factory = sqlite3.Row
        self.createTable()

    def createTable(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS clockSessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                userId INTEGER NOT NULL,
                category TEXT NOT NULL,
                clockIn TIMESTAMP NOT NULL,
                clockOut TIMESTAMP
            )
        """)
        self.conn.commit()

    def clockIn(self, userId: int, category: str):
        self.conn.execute(
            "INSERT INTO clockSessions (userId, category, clockIn) VALUES (?, ?, ?)",
            (userId, category, dt.now(datetime.timezone.utc()))
        )
        self.conn.commit()

    def clockOut(self, userId: int):
        self.conn.execute(
            "UPDATE clockSessions SET clockOut=? WHERE userId=? AND clockOut IS NULL",
            (dt.now(datetime.timezone.utc()), userId)
        )
        self.conn.commit()

    def getClockedInUsers(self):
        cursor = self.conn.execute("SELECT * FROM clockSessions WHERE clockOut IS NULL")
        return cursor.fetchall()
