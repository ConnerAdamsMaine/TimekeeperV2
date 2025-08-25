import asyncpg

class DBHelper:
    def __init__(self, pool):
        self.pool = pool

    async def createServerTable(self, serverId: int):
        tableName = f"clockSessions_{serverId}"
        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {tableName} (
                    id SERIAL PRIMARY KEY,
                    userId BIGINT NOT NULL,
                    category VARCHAR(255) NOT NULL,
                    clockIn TIMESTAMP NOT NULL,
                    clockOut TIMESTAMP
                )
            """)

    async def clockIn(self, serverId: int, userId: int, category: str):
        tableName = f"clockSessions_{serverId}"
        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                INSERT INTO {tableName} (userId, category, clockIn)
                VALUES ($1, $2, NOW())
            """, userId, category)

    async def clockOut(self, serverId: int, userId: int):
        tableName = f"clockSessions_{serverId}"
        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                UPDATE {tableName}
                SET clockOut = NOW()
                WHERE userId = $1 AND clockOut IS NULL
            """, userId)

    async def getClockedInUsers(self, serverId: int):
        tableName = f"clockSessions_{serverId}"
        async with self.pool.acquire() as conn:
            return await conn.fetch(f"""
                SELECT userId, category, clockIn
                FROM {tableName}
                WHERE clockOut IS NULL
            """)
