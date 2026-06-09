"""Generic discord.py views for Hetman.

Those are various buttons and select menus used throughout the bot.
Note that these are not cogs, but rather discord.py views.

Typical usage example:
    ```py
    from hetman.ext import views

    @bot.command()
    async def example(ctx):
        await ctx.send("Example", view=views.ExampleView())
        ...
    ```
"""
# License: EPL-2.0
# SPDX-License-Identifier: EPL-2.0
# Copyright (c) 2023-present Tech. TTGames

import discord


class Confirm(discord.ui.View):
    """A confirmation button set.

    Allows the user to confirm or cancel an action.

    Attributes:
        value: Whether the user confirmed or not.
            `None` if the user didn't confirm or cancel.
            `bool` if the user confirmed or canceled.
    """

    value: bool | None

    def __init__(self):
        """Initializes the view.

        We prep the button and set the return value to None.
        """
        super().__init__()
        self.value = None

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction,
                      button: discord.ui.Button):
        """The confirmation button was pressed.

        We set the return value to True, disable the button, and stop the view.

        Args:
            interaction: The interaction that triggered the button.
            button: The button that was pressed.
        """
        await interaction.response.send_message("Confirmed", ephemeral=True)
        self.value = True
        button.disabled = True
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction,
                     button: discord.ui.Button):
        """The cancel button.

        We set the return value to False, disable the button, and stop the view.

        Args:
            interaction: The interaction that triggered the button.
            button: The button that was pressed.
        """
        await interaction.response.send_message("Cancelled", ephemeral=True)
        self.value = False
        button.disabled = True
        self.stop()


class LocationSelectView(discord.ui.View):
    def __init__(self, valid_locations: list, server_type, author_id: int):
        super().__init__(timeout=60.0)
        self.value = None
        self.author_id = author_id

        options = []
        for loc in valid_locations:
            # Find the dynamic price for this specific location to display to the user
            price = 0.0
            for p in server_type.prices:
                if p['location'] == loc.name:
                    price = float(p['price_hourly']['gross'])
                    break

            options.append(
                discord.SelectOption(
                    label=f"{loc.description} ({loc.name})",
                    description=f"Cost: €{price:.3f} / hour",
                    value=loc.name,
                    emoji="🌍"
                )
            )

        self.select = discord.ui.Select(
            placeholder="Select a Datacenter Location...",
            min_values=1,
            max_values=1,
            options=options[:25]
        )
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("This menu is not for you.", ephemeral=True)
            return

        self.value = self.select.values[0]

        for item in self.children:
            item.disabled = True

        await interaction.response.edit_message(content=f"🌍 Selected **{self.value}**. Booting server...", view=self)
        self.stop()