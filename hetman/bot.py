"""This main bot class module for the Hetman bot.

A module that contains purely the bot class - `Hetman`.
It's a subclass of `discord.ext.commands.AutoShardedBot`.
No other code is in this module at the moment.

Typical usage example:
    ```py
    from hetman import bot
    bot_instance = bot.Hetman(...)
    ```
"""
# License: EPL-2.0
# SPDX-License-Identifier: EPL-2.0
# Copyright (c) 2023-present Tech. TTGames

import logging

from cloudflare import AsyncCloudflare
from cloudflare import DefaultAioHttpClient
import discord
from discord.ext import commands
from hcloud import Client
from sqlalchemy.ext import asyncio as sa_asyncio

from hetman import cogs
from hetman.data import config
from hetman.data import const


class Hetman(commands.AutoShardedBot):
    """A bot class that is used to customize the bot.

    This is to allow us to add our own methods and attributes.
    In general, little is done in this class.
    Most of the work is done in the cogs.

    Attributes:
        stat_confg: The config for the bot.
        hcli: The Hetzner Cloud client.
        cfcli: The Cloudflare client.
        sessions: The database session maker.
    """

    stat_confg: config.Config
    hcli: Client
    cfcli: AsyncCloudflare
    sessions: sa_asyncio.async_sessionmaker

    def __init__(
        self,
        *args,
        db_engine: sa_asyncio.AsyncEngine,
        confg: config.Config,
        secrets: config.Secret,
        **kwargs,
    ) -> None:
        """Initializes the bot instance.

        This function is used to initialize the bot instance.
        We create prep some stuff for the bot to use.

        Args:
            *args: The arguments to pass to the super class.
            db_engine: The database engine.
            confg: The config for the bot.
            secrets: The secrets for the bot.
                Preferably with the bot token already consumed.
            **kwargs: The keyword arguments to pass to the super class.
        """
        super().__init__(*args, **kwargs)
        self._db_engine = db_engine
        self.stat_confg = confg
        self.hcli = Client(
            token=secrets.token("hetzner"),
            application_name="Hetman Server Manager",
            application_version=const.VERSION,
        )
        self.cfcli = AsyncCloudflare(
            api_token=secrets.token("cloudflare"),
            http_client=DefaultAioHttpClient(),
        )
        del secrets
        self.sessions = sa_asyncio.async_sessionmaker(self._db_engine, expire_on_commit=False)

    async def setup_hook(self) -> None:
        """Runs just before the bot connects to Discord.

        Sets up the bot, for actual use.
        This is used to load the cogs and sync the database.
        Generally, this function should not be called manually.
        """
        logging.info("Bot version: %s", const.VERSION)
        logging.info("Discord.py version: %s", discord.__version__)
        logging.info("Loading cogs...")
        for extension in cogs.EXTENSIONS:
            try:
                await self.load_extension(extension)
            except commands.ExtensionError as err:
                logging.error("Failed to load cog %s: %s", extension, err)
        logging.info("Finished loading cogs.")

    async def close(self) -> None:
        """Closes the bot.

        This function is used to close the bot.
        We additionally clean up the database engine/pool.
        """
        logging.info("Closing bot...")
        await self._db_engine.dispose()
        return await super().close()
