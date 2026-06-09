"""Configuration for the bot.

This file contains the configuration for the bot.
It loads from a TOML file, and provides a class to access the config.

Typical usage example:
    ```py
    from pkmntrpg.data import config
    cnfg = config.Config()
    print(cnfg["token"])
    ```
"""
# License: EPL-2.0
# SPDX-License-Identifier: EPL-2.0
# Copyright (c) 2023-present Tech. TTGames

from typing import overload
import discord
import tomli

from hetman.data import const

BURNABLE = False


class Config(dict):
    """Configuration class for the bot.

    This class is used to access the configuration for the bot.
    It loads from a TOML file, and provides a dictionary-like interface.

    Attributes:
       All attributes are inherited from the superclass.
    """

    def __init__(self):
        """Initializes the config.

        This method initializes the config.
        It loads the config from the TOML file.
        """
        super().__init__()
        with open(const.PROG_DIR.joinpath("config.toml"), "rb") as f:
            self.update(tomli.load(f))

    @overload
    async def dev_gld(self,
                      instance: discord.AutoShardedClient) -> discord.Guild:
        ...

    @overload
    async def dev_gld(self, instance: None) -> int:
        ...

    async def dev_gld(
        self,
        instance: discord.AutoShardedClient | None = None
    ) -> int | discord.Guild:
        """Returns the dev guild ID.

        This method returns the dev guild ID.
        It also deletes the ID from memory.
        """
        dev_gld = int(self["dev_gld"])
        if instance is not None:
            dev_gld = await instance.fetch_guild(dev_gld)
        return dev_gld


class Secret:
    """Class for sensitive data.

    This class is used to access sensitive data.
    It loads from a TOML file, and provides a throwaway access key. After
    the key is used the data in this class is deleted.

    Attributes:
        token: The bot token.
    """

    def __init__(self):
        """Initializes the secret.

        This method initializes the secret.
        It loads the secret from the TOML file.
        """
        global BURNABLE
        if BURNABLE:
            raise RuntimeError("Secret already burnt!")
        with open(const.PROG_DIR.joinpath("secret.toml"), "rb") as f:
            self.secrets = tomli.load(f)
        BURNABLE = True

    def token(self) -> str:
        """Returns the bot token.
    
        This method returns the bot token.
        It also deletes the token from memory.
        """
        if not self.secrets["token"]:
            raise RuntimeError("No token found!")
        token = self.secrets.pop("token")
        return token

    def htoken(self) -> str:
        """Returns the Hetzner cloud token.

        This method returns the Hetzner cloud token.
        It also deletes the token from memory.
        """
        if not self.secrets["htoken"]:
            raise RuntimeError("No Hetzner cloud token found!")
        token = self.secrets.pop("htoken")
        return token
