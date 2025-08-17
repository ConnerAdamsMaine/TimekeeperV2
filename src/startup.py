import os
from dotenv import load_dotenv
from Core.main import TimekeeperV2

load_dotenv()

if __name__ == "__main__":
    token = os.getenv("DISCORD_AUTH_TOKEN")
    if not token:
        raise ValueError("DISCORD_AUTH_TOKEN not found in environment variables.")
    
    bot = TimekeeperV2(token)
    bot.run(
        asyncio_debug = True if os.getenv("DEBUG") == "true" else False,
        check_for_updates = True
    )