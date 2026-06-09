"""Hetman - On-demand Hetzner server management via discord bot.


This is the main module for the bot. It contains the bot class and the entry
point for the bot.

Typical usage example:
    For a standard startup, use start_bot.
    For a custom startup, use the code in the example below.
    ```py
    #!/usr/bin/env python3
    import asyncio
    import hetman
    loop = asyncio.get_event_loop()
    loop.run_until_complete(hetman.start_bot(config))
    ```
"""
# License: EPL-2.0
# SPDX-License-Identifier: EPL-2.0
# Copyright (c) 2023-present Tech. TTGames

import logging
import signal
import sys
import colorama

import discord
from discord.ext import commands

from hetman import bot
from hetman.data import config, const, interpreter


# pylint: disable=unused-argument
def sigint_handler(sign, frame):
    """Handles SIGINT (Ctrl+C)"""
    logging.info("SIGINT received. Exiting.")
    sys.exit(0)


signal.signal(signal.SIGINT, sigint_handler)


async def start_bot(conf: config.Config) -> None:
    """Starts the bot.

    Also sets up logging.
    Also handles neat shutdown.
    """
    colorama.just_fix_windows_console()
    print("Beggining setup...")
    try:
        # Set up logging
        dt_fmr = "%Y-%m-%d %H:%M:%S"
        const.HANDLER.setFormatter(
            logging.Formatter("%(asctime)s:%(levelname)s:%(name)s: %(message)s",
                              dt_fmr))

        # Set up bot logging
        logging.root.setLevel(logging.INFO)
        logging.root.addHandler(const.HANDLER)

        # Set up discord.py logging
        dscrd_logger = logging.getLogger("discord")
        dscrd_logger.setLevel(logging.INFO)
        dscrd_logger.addHandler(const.HANDLER)

        logging.info("Logging set up.")
    # pylint: disable=broad-except
    except Exception as e:
        logging.exception("Failed to set up logging.")
        print(f"LOGGING: {const.FAILED}")
        print(f"ERROR: {e}")
        print("Aborting...")
        return
    print(f"LOGGING: {const.OK}")

    # Create bot instance
    try:
        scrt = config.Secret()
        bot_instance = bot.Hetman(
            config=conf,
            secrets=scrt,
            intents=const.INTENTS,
            command_prefix=commands.when_mentioned,
            status=discord.Status.online,
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="the servers."),
        )
    # pylint: disable=broad-except
    except Exception as e:
        logging.exception("Failed to create bot instance.")
        print(f"BOT INITIALIZATION: {const.FAILED}")
        print(f"ERROR: {e}")
        print("Aborting...")
        return
    print(f"BOT INITIALIZATION: {const.OK}")

    print(const.ALL_OK)
    print("Starting bot...")
    try:
        await bot_instance.start(scrt.token())
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt detected. Shutting down...")
        print("Keyboard interrupt detected. Shutting down...")
        await bot_instance.close()
    except SystemExit as exc:
        logging.info("System exit code: %s detected. Closing bot...", exc.code)
        print(f"System exit code: {exc.code} detected. Closing bot...")
        await bot_instance.close()
    else:
        print("Internal bot shutdown. (/close was used.)")
        logging.info("Bot shutdown gracefully.")
    logging.info("Bot shutdown complete.")
    print("Thanks for using Hetman!")
