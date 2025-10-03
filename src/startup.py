import argparse
import os
from dotenv import load_dotenv
import logging
import discord
import tkinter as tk

from Core.main import Bot

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':    
    TOKEN = os.getenv("DISCORD_AUTH_TOKEN")
    PREFIX = os.getenv("COMMAND_PREFIX", ".")

#if __name__ == '__main__':
#    parser = argparse.ArgumentParser(description="Discord Bot")
#    parser.add_argument("--token", type=str, default=os.getenv("DISCORD_AUTH_TOKEN"), required=True, help="Discord bot token")
#    parser.add_argument("--prefix", type=str, default="!", help="Command prefix")
#    parser.add_argument("--lazy", action="store_true", help="Enable lazy loading of cogs")
#    parser.add_argument("--logging-level", type=int, default="20", help="Logging level")
#    
#    args = parser.parse_args()
#    print(args)
#    
#    token = args.token
#    prefix = args.prefix
#    lazy = args.lazy
#    level = args.logging_level
#    
#    if token.lower() == "env":
#        token = os.getenv("DISCORD_AUTH_TOKEN") or ""
#    
#    match level:
#        case 10:
#            level = logging.DEBUG
#        case 20:
#            level = logging.INFO
#        case 30:
#            level = logging.WARNING
#        case 40:
#            level = logging.ERROR
#        case 50:
#            level = logging.CRITICAL
#        case _:
#            level = logging.INFO
#            
#            
#    logging.basicConfig(level=level)
#    logger.info("Starting bot...")
#    intents = discord.Intents.all()
#    
#    bot = Bot(prefix=prefix, intents=intents)
#    if not token:
#        logger.error("No token provided. Exiting.")
#        exit(1)
#        
#    bot.run(token)