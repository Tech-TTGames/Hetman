"""Manages server lifecycle operations, including monitoring and automated shutdown based on billing and activity considerations.

This module provides the functionality to track the status of servers running on Hetzner
nodes, ensuring cost-efficiency and optimal resource usage by enforcing shutdown decisions
based on server activity and credit balances.

Typical usage example:
"""
# License: EPL-2.0
# SPDX-License-Identifier: EPL-2.0
# Copyright (c) 2023-present Tech. TTGames

import asyncio
import datetime
import logging
from typing import Dict
import uuid
import re

import a2s
import discord
from discord import app_commands
from discord.ext import tasks, commands
from sqlalchemy import select
from hcloud.servers import BoundServer, ServerCreatePublicNetwork
from hcloud.images import BoundImage, CreateImageResponse

from hetman.bot import Hetman
from hetman.data import models
from hetman.ext import views

class ServerManager(commands.Cog):
    def __init__(self, bot: Hetman):
        self.bot = bot
        self._activity_flags: Dict[int, bool] = {}
        self.billing_watchdog.start()

    def cog_unload(self):
        self.billing_watchdog.cancel()

    @tasks.loop(minutes=1.0)
    async def billing_watchdog(self):
        """Monitors running servers and executes decisions based on time and credits."""
        lookahead = self.bot.stat_confg.get("shutdown_lookahead", 10)
        lookbehind = self.bot.stat_confg.get("shutdown_lookbehind", 5)

        # Calculate time windows dynamically
        spindown_minute = 60 - lookahead
        polling_start_minute = spindown_minute - lookbehind

        now = datetime.datetime.now(datetime.timezone.utc)

        async with self.bot.sessions.begin() as session:
            servers = await session.scalars(
                select(models.Server).where(models.Server.status == models.Status.ONLINE)
            )

            for server in servers:
                if not server.start_time:
                    await self.bot.loop.create_task(self.spindown(server.id))
                    logging.warning(f"[STARTUP] Server '{server.name}' has no start time. Forcing spindown.")
                    continue

                # Calculate current minute of the billed hour
                delta_seconds = (now - server.start_time).total_seconds()
                minute_of_hour = int((delta_seconds / 60) % 60)

                # --- PHASE 1: THE LOOKBEHIND WINDOW (Minutes 45 - 49) ---
                if polling_start_minute <= minute_of_hour <= spindown_minute:
                    try:
                        info = await a2s.ainfo((server.ip_address, server.a2s_port), timeout=2.0, encoding="utf-8")
                        if info.player_count > 0:
                            self._activity_flags[server.id] = True
                    except Exception as e:
                        # If server times out/reboots, treat as empty for this check tick
                        logging.warning(f"[ACTIVITY] Server '{server.name}' timed out or rebooted. Treating as empty. Details: {e}")
                        pass

                # --- PHASE 2: THE DECISION CROSSROADS (Minute 50) ---
                if minute_of_hour == spindown_minute:
                    # Credit check
                    if server.credits < (server.snapshot_reserve + server.cost_per_hour):
                        logging.warning(
                            f"[FINANCE] Server '{server.name}' has insufficient credits for another hour. Forcing spindown.")
                        self.bot.loop.create_task(self.spindown(server.id))
                        continue

                    # Activity check
                    was_active = self._activity_flags.get(server.id, False)

                    if not was_active or server.stop_requested:
                        logging.info(
                            f"[ACTIVITY] Server '{server.name}' remained empty during lookbehind or was requested to stop. Soft spindown triggered.")
                        self.bot.loop.create_task(self.spindown(server.id))
                    else:
                        self._activity_flags[server.id] = False

                # --- PHASE 3: THE RESET (Minute 0 of the next hour) ---
                elif minute_of_hour == 0 and delta_seconds > 60:
                    self._activity_flags.pop(server.id, None)
                    server.credits -= server.cost_per_hour
                    logging.info(f"[FINANCE] Server '{server.name}' has been reset for another hour.")

    @billing_watchdog.before_loop
    async def before_watchdog(self):
        await self.bot.wait_until_ready()

    async def spindown(self, server_db_id: int, forced: bool = False):
        """Handles the transition to Status.SNAPSHOTTING and tears down the Hetzner node safely."""

        # --- STEP 1: INITIAL STATE LOCK (Short Session) ---
        async with self.bot.sessions.begin() as session:
            server = await session.get(models.Server, server_db_id)
            if not server:
                logging.warning(f"[SHUTDOWN] Server with ID {server_db_id} not found.")
                return
            if server.hcloud_server_id is None:
                raise ValueError("Hetzner server ID is not set.")
            if server.status == models.Status.SNAPSHOTTING:
                logging.warning(f"[SHUTDOWN] Server '{server.name}' is already in snapshotting state.")
                return

            server.status = models.Status.SNAPSHOTTING

        logging.info(f"[SHUTDOWN] Server '{server.name}' set to SNAPSHOTTING. SQLite transaction released.")

        try:
            hetzner_server: BoundServer = await asyncio.to_thread(self.bot.hcli.servers.get_by_id, server.hcloud_server_id)

            # --- STEP 2: POWER DOWN (Using your forced flag) ---
            if forced:
                logging.info(f"[SHUTDOWN] Force flag detected. Killing power instantly for '{server.name}'.")
                shutdown_task = await asyncio.to_thread(hetzner_server.power_off)
            else:
                logging.info(f"[SHUTDOWN] Requesting graceful ACPI shutdown for '{server.name}'.")
                shutdown_task = await asyncio.to_thread(hetzner_server.shutdown)

            await asyncio.to_thread(shutdown_task.wait_until_finished, 1)

            # --- STEP 3: TRIGGER SNAPSHOT ---
            timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d-%H%M%S')
            snap_name = f"Hetman Snapshot {server.name} at {timestamp}"

            snapshot_task: CreateImageResponse = await asyncio.to_thread(hetzner_server.create_image, description=snap_name)

            # Fix Desync: Commit the NEW snapshot ID immediately before waiting
            new_snapshot_id = snapshot_task.image.id
            old_snapshot_id = server.current_snapshot_id
            async with self.bot.sessions.begin() as session:
                session.add(server)
                await session.refresh(server)
                server.current_snapshot_id = new_snapshot_id

            # Long running block happens completely OUTSIDE an open SQL transaction
            logging.info(f"[SHUTDOWN] Snapshot triggered. Polling cloud tracking action for '{server.name}'...")
            await asyncio.to_thread(snapshot_task.action.wait_until_finished, 1)

            # --- STEP 4: CLEAN UP OLD SNAPSHOT ---
            try:
                past_snapshot: BoundImage = await asyncio.to_thread(self.bot.hcli.images.get_by_id, old_snapshot_id)
                if past_snapshot:
                    await asyncio.to_thread(past_snapshot.delete)
                    logging.info(f"[SHUTDOWN] Old snapshot {old_snapshot_id} purged successfully.")
            except Exception as e:
                logging.warning(
                    f"[SHUTDOWN] Non-critical error clean-purging legacy snapshot {old_snapshot_id}: {e}")

            # --- STEP 5: TERMINATE NODE ---
            async with self.bot.sessions.begin() as session:
                session.add(server)
                await session.refresh(server)
                server.status = models.Status.DELETING

            logging.info(
                f"[SHUTDOWN] Retaining snapshot complete. Instructing Hetzner to drop server node '{server.name}'...")
            del_task = await asyncio.to_thread(hetzner_server.delete)
            await asyncio.to_thread(del_task.wait_until_finished, 1)

            # --- STEP 6: BIN DDNS ---
            try:
                if server.cloudflare_record_id:
                    await self.bot.cfcli.dns.records.delete(
                        dns_record_id=server.cloudflare_record_id,
                        zone_id=server.cloudflare_zone_id,
                    )
                    logging.info(f"[SHUTDOWN] DNS record for '{server.name}' successfully deleted.")
            except Exception as e:
                logging.warning(
                    f"[SHUTDOWN] Failed to delete DNS record for '{server.name}'. It may require manual cleanup. Error: {e}")

            # --- STEP 7: RESET RECORD FOR CHILLING ---
            async with self.bot.sessions.begin() as session:
                session.add(server)
                await session.refresh(server)
                server.status = models.Status.OFFLINE
                server.hcloud_server_id = None
                server.ip_address = None
                server.cloudflare_record_id = None

            logging.info(f"[SHUTDOWN] Server '{server.name}' is fully offline and deleted from cloud billing.")

        except Exception as exc:
            logging.exception(
                f"[SHUTDOWN] Critical error processing spindown loop sequence on server {server_db_id}: {exc}")
            # Reset server to a safe recovery state so it isn't locked up eternally
            async with self.bot.sessions.begin() as session:
                server = await session.get(models.Server, server_db_id)
                server.status = models.Status.OFFLINE
                server.hcloud_server_id = None
                server.ip_address = None
                server.stop_requested = False

    async def server_autocomplete_lim(self, ctx: discord.Interaction, current: str):
        """Autocomplete function for server commands."""
        async with self.bot.sessions.begin() as session:
            servers = await session.scalars(select(models.Server).where(models.Server.name.ilike(f"%{current}%"),models.Server.discord_id == ctx.guild_id).limit(25))
            return [app_commands.Choice(name=server.name, value=str(server.id)) for server in servers]

    @app_commands.command(name="stop", description="Requests a server to not renew on the next billing cycle.")
    @app_commands.describe(server_id="The server to stop.")
    @app_commands.autocomplete(server_id=server_autocomplete_lim)
    @app_commands.guild_only()
    async def stop(self, ctx: discord.Interaction, server_id: str) -> None:
        """Requests a server to does not renew on the next billing cycle.

        Args:
            ctx: The interaction calling the command.
            server_id: The ID of the server to stop.
        """
        await ctx.response.defer(ephemeral=True)

        try:
            server_uuid = uuid.UUID(server_id)
        except ValueError:
            embed = discord.Embed(description="Invalid server ID format.", color=discord.Color.red())
            await ctx.followup.send(embed=embed, ephemeral=True)
            return

        async with self.bot.sessions.begin() as session:
            server = await session.get(models.Server, server_uuid)
            if not server or server.discord_id != ctx.guild_id:
                embed = discord.Embed(description="Server not found.", color=discord.Color.red())
                await ctx.followup.send(embed=embed, ephemeral=True)
                return

            if server.status != models.Status.ONLINE:
                embed = discord.Embed(description="Server is not currently online.", color=discord.Color.orange())
                await ctx.followup.send(embed=embed, ephemeral=True)
                return

            if server.stop_requested:
                embed = discord.Embed(description="Server is already requested to stop.",
                                      color=discord.Color.orange())
                await ctx.followup.send(embed=embed, ephemeral=True)
                return

            is_owner = ctx.guild.owner_id == ctx.user.id
            has_role = server.role_id and any(r.id == server.role_id for r in ctx.user.roles)
            if not is_owner and not has_role:
                embed = discord.Embed(description="You do not have permission to stop this server.",
                                      color=discord.Color.red())
                await ctx.followup.send(embed=embed, ephemeral=True)
                return

            server.stop_requested = True

            embed = discord.Embed(
                title="Stop Requested",
                description=f"**{server.name}** has been flagged. It will safely shut down at the end of the current billing hour.",
                color=discord.Color.orange()
            )
            await ctx.followup.send(embed=embed, ephemeral=True)
            logging.info(f"[STOP] Server stop request sent for '{server.name}' by {ctx.user.name}.")

    @app_commands.command(name="start", description="Starts a server that was previously stopped.")
    @app_commands.describe(server_id="The server to start.")
    @app_commands.autocomplete(server_id=server_autocomplete_lim)
    @app_commands.guild_only()
    async def start(self, ctx: discord.Interaction, server_id: str) -> None:
        """Starts a server that was previously stopped.

        Args:
            ctx: The interaction calling the command.
            server_id: The ID of the server to start.
        """
        await ctx.response.defer()

        try:
            server_uuid = uuid.UUID(server_id)
        except ValueError:
            embed = discord.Embed(description="Invalid server ID format.", color=discord.Color.red())
            await ctx.followup.send(embed=embed, ephemeral=True)
            return

        async with self.bot.sessions.begin() as session:
            server = await session.get(models.Server, server_uuid)
            if not server or server.discord_id != ctx.guild_id:
                embed = discord.Embed(description="Server not found.", color=discord.Color.red())
                await ctx.followup.send(embed=embed, ephemeral=True)
                return

            if server.status != models.Status.OFFLINE:
                embed = discord.Embed(description="Server is not offline.", color=discord.Color.orange())
                await ctx.followup.send(embed=embed, ephemeral=True)
                return

            is_owner = ctx.guild.owner_id == ctx.user.id
            has_role = server.role_id and any(r.id == server.role_id for r in ctx.user.roles)
            if not is_owner and not has_role:
                embed = discord.Embed(description="You do not have permission to start this server.",
                                      color=discord.Color.red())
                await ctx.followup.send(embed=embed, ephemeral=True)
                return

            if server.credits < server.cost_per_hour + server.snapshot_reserve:
                embed = discord.Embed(description="Not enough credits to start this server.",
                                      color=discord.Color.red())
                await ctx.followup.send(embed=embed, ephemeral=True)
                return

            server.status = models.Status.ONLINE
            server.stop_requested = False

        embed_start = discord.Embed(
            description=f"Beginning startup sequence for **{server.name}**...",
            color=discord.Color.blue()
        )
        await ctx.followup.send(embed=embed_start, ephemeral=False)

        try:
            server_type = await asyncio.to_thread(self.bot.hcli.server_types.get_by_name, server.server_type)
            image = await asyncio.to_thread(self.bot.hcli.images.get_by_id, server.current_snapshot_id)

            valid_locations = [loc for loc in server_type.locations if loc.available]
            if not valid_locations:
                raise ValueError("No locations currently available for this server type.")

            view = views.LocationSelectView(valid_locations, server_type, ctx.user.id)

            embed_loc = discord.Embed(
                title="Select Datacenter",
                description="Where should we boot the server?",
                color=discord.Color.blue()
            )
            msg = await ctx.followup.send(embed=embed_loc, view=view, ephemeral=False)

            # Suspend execution until the user clicks or it times out
            await view.wait()

            # If they ignored it and let it timeout, abort cleanly.
            if view.value is None:
                embed_timeout = discord.Embed(description="Request timed out.", color=discord.Color.dark_grey())
                await msg.edit(embed=embed_timeout, view=None)
                async with self.bot.sessions.begin() as session:
                    server = await session.get(models.Server, server_uuid)
                    server.status = models.Status.OFFLINE
                return

            selected_location_name = view.value
            hourly_cost = 0.05
            for price_data in server_type.prices:
                if price_data['location'] == selected_location_name:
                    hourly_cost = float(price_data['price_hourly']['gross'])
                    break

            ipv4_cost = 0.00098
            total_cost = hourly_cost + ipv4_cost

            async with self.bot.sessions.begin() as session:
                server = await session.get(models.Server, server_uuid)
                server.cost_per_hour = total_cost
                if server.credits < total_cost + server.snapshot_reserve:
                    embed_broke = discord.Embed(
                        description="Not enough credits to start this server in this location.",
                        color=discord.Color.red())
                    await ctx.followup.send(embed=embed_broke, ephemeral=True)
                    server.status = models.Status.OFFLINE
                    return

            location = None
            for loc in server_type.locations:
                if loc.name == selected_location_name:
                    location = loc.location
                    break

            # Sanitize name
            safe_name = re.sub(r'[^a-z0-9-]', '', server.name.lower().replace(' ', '-'))
            safe_name = re.sub(r'-+', '-', safe_name).strip('-')[:50]

            network_cnfg = ServerCreatePublicNetwork(enable_ipv4=True, enable_ipv6=True)

            create_task = await asyncio.to_thread(
                self.bot.hcli.servers.create,
                name=f"hetman-{safe_name}",
                server_type=server_type,
                image=image,
                location=location,
                public_net=network_cnfg,
            )

            new_node = create_task.server
            await asyncio.to_thread(create_task.action.wait_until_finished, 1)
            new_ip = new_node.public_net.ipv4.ip

            try:
                record = await self.bot.cfcli.dns.records.create(
                    zone_id=server.cloudflare_zone_id,
                    name=f"hetman-{safe_name}.{self.bot.stat_confg['domain']}",
                    ttl=60,
                    type="A",
                    content=new_ip,
                    proxied=False,
                )
            except Exception as e:
                logging.warning(f"[STARTUP] Failed to create DNS record for {safe_name}: {e}")
                await ctx.followup.send(f"Failed to create DNS record. Contact bot admin.", ephemeral=True)

            # --- PHASE 6: FINALIZE DATABASE STATE & DEDUCT CREDIT ---
            async with self.bot.sessions.begin() as session:
                server = await session.get(models.Server, server_uuid)
                if server:
                    server.hcloud_server_id = new_node.id
                    server.ip_address = new_ip
                    server.cost_per_hour = total_cost
                    server.credits -= total_cost  # Secure upfront hour deduction
                    server.start_time = datetime.datetime.now(datetime.timezone.utc)
                    server.cloudflare_record_id = record.id

            embed_success = discord.Embed(
                title="Server Online",
                description=f"**{server.name}** is online in **{selected_location_name}**!",
                color=discord.Color.green()
            )
            embed_success.add_field(name="IP Address", value=f"`{new_ip}`", inline=False)
            embed_success.add_field(name="Domain (Wait ~60s)",
                                    value=f"`hetman-{safe_name}.{self.bot.stat_confg['domain']}`", inline=False)

            await ctx.followup.send(embed=embed_success)

        except Exception as e:
            logging.exception(f"[STARTUP] Critical error while spinning up {server.name}: {e}")
            async with self.bot.sessions.begin() as session:
                server = await session.get(models.Server, server_uuid)
                if server:
                    server.status = models.Status.OFFLINE

            embed_fail = discord.Embed(
                title="Startup Failed",
                description=f"A critical error occurred while starting **{server.name}**.\nThe operation was aborted and credits were protected.",
                color=discord.Color.red()
            )
            await ctx.followup.send(embed=embed_fail)


