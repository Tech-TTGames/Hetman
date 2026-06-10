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
from hetman.data import models, config
from hetman.ext import views, checks

_CNFG = config.Config()

class ServerManager(commands.Cog):
    def __init__(self, bot: Hetman):
        self.bot = bot
        self._activity_flags: Dict[int, bool] = {}
        self._monitor_tasks: dict[uuid.UUID, asyncio.Task] = {}
        self.billing_watchdog.start()

    def cog_unload(self):
        self.billing_watchdog.cancel()
        for task in self._monitor_tasks.values():
            task.cancel()
        self._monitor_tasks.clear()

    def _monitor_task_done(self, server_uuid: uuid.UUID, task: asyncio.Task) -> None:
        """Cleans up finished monitor tasks and logs unexpected worker failures."""
        self._monitor_tasks.pop(server_uuid, None)

        if task.cancelled():
            return
        try:
            task.result()
        except Exception as exc:
            logging.exception(f"[MONITOR] On-demand monitor task crashed for server {server_uuid}: {exc}")

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
            servers = (await session.scalars(
                select(models.Server).where(
                    models.Server.status.in_([
                        models.Status.ONLINE,
                        models.Status.STARTING,
                        models.Status.SNAPSHOTTING
                    ])
                )
            )).all()

        for server in servers:
            if not server.start_time:
                await self.bot.loop.create_task(
                    self.spindown(server.id,
                                  public_reason=f"**{server.name}** was forced offline due to a missing internal startup timestamp.")
                )
                logging.warning(f"[STARTUP] Server '{server.name}' has no start time. Forcing spindown.")
                continue

            # Calculate current minute of the billed hour
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=datetime.timezone.utc)
            delta_seconds = (now - server.start_time).total_seconds()
            minute_of_hour = int((delta_seconds / 60) % 60)

            # --- PHASE 1: THE LOOKBEHIND WINDOW (Minutes 45 - 49) ---
            if polling_start_minute <= minute_of_hour <= spindown_minute and server.status == models.Status.ONLINE:
                if minute_of_hour == polling_start_minute and server.log_channel_id:
                    try:
                        channel = self.bot.get_channel(server.log_channel_id) or await self.bot.fetch_channel(
                            server.log_channel_id)
                        if channel:
                            embed = discord.Embed(
                                title="Activity Check Monitoring ⏳",
                                description=f"**{server.name}** has entered its scheduled activity verification window.\nIf the server remains empty for the next **{lookbehind} minutes**, it will automatically spin down to preserve credits.",
                                color=discord.Color.blue()
                            )
                            await channel.send(embed=embed)
                    except Exception as e:
                        logging.warning(f"[NOTIFY] Failed to broadcast polling start notice: {e}")

                try:
                    info = await a2s.ainfo((server.ip_address, server.a2s_port), timeout=2.0, encoding="utf-8")
                    if info.player_count > 0:
                        self._activity_flags[server.id] = True
                except Exception as e:
                    logging.warning(
                        f"[ACTIVITY] Server '{server.name}' timed out or rebooted. Treating as empty. Details: {e}")
                    pass

            # --- PHASE 2: THE DECISION CROSSROADS (Minute 50) ---
            if minute_of_hour == spindown_minute and server.status == models.Status.ONLINE:
                was_active = self._activity_flags.get(server.id, False)

                if server.stop_requested:
                    reason = f"**{server.name}** has been shut down via user command request."
                    logging.info(f"[ACTIVITY] Server '{server.name}' stop requested. Soft spindown triggered.")
                    self.bot.loop.create_task(self.spindown(server.id, public_reason=reason))
                elif not was_active:
                    reason = f"**{server.name}** has gone to sleep due to inactivity during the lookbehind window."
                    logging.info(f"[ACTIVITY] Server '{server.name}' remained empty. Soft spindown triggered.")
                    self.bot.loop.create_task(self.spindown(server.id, public_reason=reason))
                else:
                    self._activity_flags[server.id] = False

            # Credit check - 5 minutes left in the billed hour
            elif minute_of_hour == 55 and server.status != models.Status.SNAPSHOTTING:
                if server.credits < (server.snapshot_reserve + server.cost_per_hour):
                    reason = f"**{server.name}** has insufficient credits to renew for another hour (Balance: €{server.credits:.2f}). Forcing shutdown."
                    logging.warning(
                        f"[FINANCE] Server '{server.name}' has insufficient credits. Forcing instant spindown.")
                    self.bot.loop.create_task(self.spindown(server.id, forced=True, public_reason=reason))

            # --- PHASE 3: THE RESET (Minute 0 of the next hour) ---
            elif minute_of_hour == 0 and delta_seconds > 60:
                self._activity_flags.pop(server.id, None)

                async with self.bot.sessions.begin() as session:
                    db_server = await session.get(models.Server, server.id)
                    if db_server:
                        db_server.credits -= db_server.cost_per_hour
                        server.credits = db_server.credits  # Sync local detached memory instance

                        if db_server.credits < (
                                db_server.snapshot_reserve + db_server.cost_per_hour) and server.log_channel_id:
                            try:
                                channel = self.bot.get_channel(server.log_channel_id) or await self.bot.fetch_channel(
                                    server.log_channel_id)
                                if channel:
                                    embed = discord.Embed(
                                        title="Low Credit Warning ⚠️",
                                        description=(
                                            f"**{server.name}** has successfully renewed, but its remaining balance (**€{db_server.credits:.2f}**) cannot cover another cycle.\n\n"
                                            f"This is officially the **final hour** of runtime unless the server is topped up.\n"
                                            "We recommend you request a stop manually so no data is lost due to a forced stop 5 minutes before timeout."),
                                        color=discord.Color.gold()
                                    )
                                    await channel.send(embed=embed)
                            except Exception as e:
                                logging.warning(f"[NOTIFY] Failed to broadcast low credit warning: {e}")

                logging.info(f"[FINANCE] Server '{server.name}' has been reset for another hour.")

    @billing_watchdog.before_loop
    async def before_watchdog(self):
        await self.bot.wait_until_ready()

    async def spindown(self, server_db_id: int, forced: bool = False, public_reason: str | None = None):
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

        if server.log_channel_id:
            if public_reason:
                public_reason = f"**{server.name}** has been shut down for the following reason:\n{public_reason}"
            else:
                public_reason = f"**{server.name}** has been shut down."
            embed = discord.Embed(
                title="Server Stopping",
                description=public_reason,
                color=discord.Color.red()
            )
            await self.send_log_dump(server.log_channel_id, embed)

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

            await asyncio.to_thread(shutdown_task.wait_until_finished)

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
            await asyncio.to_thread(snapshot_task.action.wait_until_finished)

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
            await asyncio.to_thread(del_task.wait_until_finished)

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
                if server:
                    server.status = models.Status.OFFLINE
                    server.stop_requested = False

    async def send_log_dump(self, log_channel_id: int | None, embed: discord.Embed) -> None:
        """Internal helper to dispatch embeds to the designated text log channel safely."""
        if not log_channel_id:
            return

        # Try to resolve from cache first, fall back to API call if cold
        channel = self.bot.get_channel(log_channel_id)
        if not channel:
            try:
                channel = await self.bot.fetch_channel(log_channel_id)
            except Exception:
                logging.warning(f"[NOTIFY] Could not access or resolve log channel ID {log_channel_id}")
                return

        try:
            await channel.send(embed=embed)
        except Exception as e:
            logging.error(f"[NOTIFY] Failed to broadcast alert to channel {log_channel_id}: {e}")

    async def wait_for_a2s(self, server_uuid: uuid.UUID):
        """Polls A2S until the game server comes up, then announces it."""
        async with self.bot.sessions.begin() as session:
            server = await session.get(models.Server, server_uuid)
            safe_name = re.sub(r'[^a-z0-9-]', '', server.name.lower().replace(' ', '-'))
            safe_name = re.sub(r'-+', '-', safe_name).strip('-')[:50]
            domain = f"hetman-{safe_name}.{self.bot.stat_confg['domain']}"

        retries = 0
        while retries < 40:  # E.g., 40 retries * 5s = ~3.3 minutes max wait
            try:
                await a2s.ainfo((server.ip_address, server.a2s_port), timeout=2.0, encoding="utf-8")

                async with self.bot.sessions.begin() as session:
                    server = await session.get(models.Server, server_uuid)
                    server.status = models.Status.ONLINE

                if server.log_channel_id:
                    embed = discord.Embed(
                        title="Server Online! 🟢",
                        description=f"**{server.name}** is fully booted and ready for players!",
                        color=discord.Color.green()
                    )
                    embed.add_field(name="Connect IP", value=f"`{server.ip_address}:{server.a2s_port-1}`", inline=False)
                    embed.add_field(name="Domain", value=f"`{domain}:{server.a2s_port-1}`", inline=False)
                    await self.send_log_dump(server.log_channel_id, embed)
                return

            except Exception:
                await asyncio.sleep(5)
                retries += 1

        async with self.bot.sessions.begin() as session:
            server = await session.get(models.Server, server_uuid)
            if server:
                server.status = models.Status.ONLINE

                if server.log_channel_id:
                    embed = discord.Embed(
                        title="Server Booted With Query Warning ⚠️",
                        description=(
                            f"**{server.name}** hardware is running, but the game server did not respond to A2S queries in time.\n"
                            "The server has been marked online so billing/activity checks can proceed, but live query data may be unavailable."
                        ),
                        color=discord.Color.orange()
                    )
                    self.bot.loop.create_task(self.send_log_dump(server.log_channel_id, embed))

        logging.warning(f"[A2S] Timeout waiting for game server {server.name} to boot. Marked ONLINE with warning.")

    async def server_autocomplete_lim(self, ctx: discord.Interaction, current: str):
        """Autocomplete function for server commands."""
        async with self.bot.sessions.begin() as session:
            servers = await session.scalars(select(models.Server).where(models.Server.name.ilike(f"%{current}%"),models.Server.discord_id == ctx.guild_id).limit(25))
            return [app_commands.Choice(name=server.name, value=str(server.id)) for server in servers]

    async def server_autocomplete(self, ctx: discord.Interaction, current: str):
        """Autocomplete function for server commands."""
        async with self.bot.sessions.begin() as session:
            servers = await session.scalars(select(models.Server).where(models.Server.name.ilike(f"%{current}%")).limit(25))
            return [app_commands.Choice(name=server.name, value=str(server.id)) for server in servers]

    @app_commands.command(name="stop", description="Requests a server to not renew on the next billing cycle.")
    @app_commands.describe(
        server_id="The server to stop.",
        cancel="Cancels the stop request if it's already in progress.",
    )
    @app_commands.autocomplete(server_id=server_autocomplete_lim)
    @app_commands.guild_only()
    async def stop(self, ctx: discord.Interaction, server_id: str, cancel: bool = False) -> None:
        """Requests a server to does not renew on the next billing cycle.

        Args:
            ctx: The interaction calling the command.
            server_id: The ID of the server to stop.
            cancel: Cancels a pending request.
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

            if server.stop_requested and not cancel:
                embed = discord.Embed(description="Server is already requested to stop.",
                                      color=discord.Color.orange())
                await ctx.followup.send(embed=embed, ephemeral=True)
                return
            if not server.stop_requested and cancel:
                embed = discord.Embed(description="Server is not currently requested to stop.",
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

            server.stop_requested = not cancel

            if cancel:
                title = "Shutdown Canceled 🟢"
                desc = f"The pending manual shutdown for **{server.name}** has been canceled by {ctx.user.mention}. The server will continue running normally."
                color = discord.Color.green()
                response_desc = f"The pending shutdown for **{server.name}** has been canceled."
            else:
                title = "Shutdown Requested ⚠️"
                desc = f"A manual stop request has been filed for **{server.name}** by {ctx.user.mention}.\nThe server will safely power down at the end of the current billing hour."
                color = discord.Color.orange()
                response_desc = f"**{server.name}** has been flagged. It will safely shut down at the end of the current billing hour."

            if server.log_channel_id:
                announce_embed = discord.Embed(title=title, description=desc, color=color)
                self.bot.loop.create_task(self.send_log_dump(server.log_channel_id, announce_embed))

            embed = discord.Embed(
                title=title,
                description=response_desc,
                color=color
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

            server.status = models.Status.PROVISIONING
            server.stop_requested = False

        if server.log_channel_id:
            boot_embed = discord.Embed(
                title="Server Booting",
                description=f"A startup sequence has been initiated for **{server.name}** by {ctx.user.mention}.\nWe are requesting hardware from the datacenter now...",
                color=discord.Color.blue()
            )
            self.bot.loop.create_task(self.send_log_dump(server.log_channel_id, boot_embed))

        embed_start = discord.Embed(
            description=f"Beginning startup sequence for **{server.name}**...",
            color=discord.Color.blue()
        )
        await ctx.followup.send(embed=embed_start, ephemeral=False)

        new_node = None
        try:
            server_type = await asyncio.to_thread(self.bot.hcli.server_types.get_by_name, server.server_type)
            image = await asyncio.to_thread(self.bot.hcli.images.get_by_id, server.current_snapshot_id)

            valid_locations = [loc for loc in server_type.locations if loc.available]
            if not valid_locations:
                embed_no_loc = discord.Embed(description="No available locations for this server type.", color=discord.Color.red())
                await ctx.followup.send(embed=embed_no_loc, ephemeral=True)
                async with self.bot.sessions.begin() as session:
                    server = await session.get(models.Server, server_uuid)
                    server.status = models.Status.OFFLINE
                return

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

            ipv4_cost = self.bot.stat_confg['ipv4_cost']
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
                if loc.location.name == selected_location_name:
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
            await asyncio.to_thread(create_task.action.wait_until_finished)
            new_ip = new_node.public_net.ipv4.ip

            async with self.bot.sessions.begin() as session:
                server = await session.get(models.Server, server_uuid)
                if server:
                    server.status = models.Status.STARTING
                    server.hcloud_server_id = new_node.id
                    server.cost_per_hour = total_cost
                    server.credits -= total_cost  # Secure upfront hour deduction
                    server.start_time = datetime.datetime.now(datetime.timezone.utc)
                    server.ip_address = new_ip

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
                record = None

            async with self.bot.sessions.begin() as session:
                server = await session.get(models.Server, server_uuid)
                if server:
                    server.cloudflare_record_id = record.id if record else None

            embed_provisioned = discord.Embed(
                title="Server Provisioned ⏳",
                description=f"**{server.name}** hardware is running in **{selected_location_name}**.\nWaiting for the game server to respond...",
                color=discord.Color.yellow()
            )
            await ctx.followup.send(embed=embed_provisioned)
            self.bot.loop.create_task(self.wait_for_a2s(server_uuid))

        except Exception as e:
            logging.exception(f"[STARTUP] Critical error while spinning up {server.name}: {e}")
            cleanup_succeeded = True
            if new_node is not None:
                try:
                    del_task = await asyncio.to_thread(new_node.delete)
                    await asyncio.to_thread(del_task.wait_until_finished)
                    logging.info(f"[STARTUP] Cleaned up orphaned Hetzner node {new_node.id} after startup failure.")
                except Exception as cleanup_error:
                    cleanup_succeeded = False
                    logging.warning(f"[STARTUP] Failed to delete server {new_node}: {cleanup_error}")
            async with self.bot.sessions.begin() as session:
                server = await session.get(models.Server, server_uuid)
                if server:
                    if cleanup_succeeded:
                        server.status = models.Status.OFFLINE
                        server.hcloud_server_id = None
                        server.ip_address = None
                        server.cloudflare_record_id = None
                    else:
                        server.status = models.Status.DELETING
                        if new_node is not None:
                            server.hcloud_server_id = new_node.id

            embed_fail = discord.Embed(
                title="Startup Failed",
                description=f"A critical error occurred while starting **{server.name}**.\nThe operation was aborted.",
                color=discord.Color.red()
            )
            await ctx.followup.send(embed=embed_fail)

    @app_commands.command(name="info", description="Displays real-time information about a server.")
    @app_commands.describe(server_id="The server to inspect.")
    @app_commands.autocomplete(server_id=server_autocomplete_lim)
    @app_commands.guild_only()
    async def info(self, ctx: discord.Interaction, server_id: str) -> None:
        await ctx.response.defer()

        try:
            server_uuid = uuid.UUID(server_id)
        except ValueError:
            await ctx.followup.send("Invalid server ID format.", ephemeral=True)
            return

        # Fetch and immediately release the database transaction
        async with self.bot.sessions.begin() as session:
            server = await session.get(models.Server, server_uuid)

            if not server or server.discord_id != ctx.guild_id:
                await ctx.followup.send("Server not found.", ephemeral=True)
                return

        # Determine color based on status
        colors = {
            models.Status.ONLINE: discord.Color.green(),
            models.Status.OFFLINE: discord.Color.dark_gray(),
            models.Status.PROVISIONING: discord.Color.blue(),
            models.Status.STARTING: discord.Color.yellow(),
            models.Status.SNAPSHOTTING: discord.Color.orange(),
            models.Status.DELETING: discord.Color.red(),
        }

        embed = discord.Embed(
            title=f"Server Info: {server.name}",
            color=colors.get(server.status, discord.Color.blue())
        )

        # Highlight if a manual stop request has been queued
        if server.stop_requested:
            embed.description = "⚠️ **Pending Shutdown:** A stop request has been filed. The server will safely power down at the end of the current billing cycle."

        embed.add_field(name="Status", value=f"**{server.status.name}**", inline=True)
        embed.add_field(name="Credits Remaining", value=f"€{server.credits:.2f}", inline=True)
        embed.add_field(name="Snapshot Reserve", value=f"€{server.snapshot_reserve:.2f}", inline=True)

        if server.status == models.Status.ONLINE:
            embed.add_field(name="Active Cost / Hour", value=f"€{server.cost_per_hour:.3f}", inline=True)

            # Reconstruct the safe domain name for player display
            safe_name = re.sub(r'[^a-z0-9-]', '', server.name.lower().replace(' ', '-'))
            safe_name = re.sub(r'-+', '-', safe_name).strip('-')[:50]
            domain = f"hetman-{safe_name}.{self.bot.stat_confg['domain']}"
            game_port = server.a2s_port - 1

            embed.add_field(
                name="Connection Details",
                value=f"**Domain:** `{domain}:{game_port}`\n**Direct IP:** `{server.ip_address}:{game_port}`",
                inline=False
            )
            embed.add_field(
                name="🔗 Join via Steam Browser",
                value=f"[Click to Connect](steam://connect/{domain}:{server.a2s_port})\n*Opens Steam dialog. Ignore the Steam password box; type your password inside Valheim once it launches.*",
                inline=False
            )
            embed.add_field(
                name="🚀 Direct Launch Game",
                value=f"[Click to Launch](steam://run/892970//%2Bconnect%20{domain}%3A{game_port})\n*Forces Valheim to bypass the main menu and connect directly to the domain on startup.*",
                inline=False
            )

            # Fetch Live A2S Data (Bumped timeout to 2.0s for safety)
            try:
                a2s_info = await a2s.ainfo((server.ip_address, server.a2s_port), timeout=2.0, encoding="utf-8")
                embed.add_field(name="Game", value=a2s_info.game, inline=True)
                embed.add_field(name="Players", value=f"{a2s_info.player_count} / {a2s_info.max_players}", inline=True)
                embed.add_field(name="Map", value=a2s_info.map_name, inline=True)
            except Exception:
                embed.add_field(name="Live Data", value="⚠️ Game server is not responding to queries.", inline=False)
        else:
            # Display stale cost when offline
            embed.add_field(name="Est. Cost / Hour", value=f"€{server.cost_per_hour:.3f}", inline=True)

        await ctx.followup.send(embed=embed)

    @app_commands.command(name="monitor",
                          description="Starts a short-term tracking sequence to watch for hardware availability.")
    @app_commands.describe(
        server_id="The server profile whose hardware type you want to track.",
        duration_minutes="How many minutes to poll before giving up (Max 60)."
    )
    @app_commands.autocomplete(server_id=server_autocomplete_lim)
    @app_commands.guild_only()
    async def monitor(self, ctx: discord.Interaction, server_id: str, duration_minutes: app_commands.Range[int, 1, 60] = 15) -> None:
        """Tracks hardware availability on-demand for a set duration, then pings when found."""
        await ctx.response.defer(ephemeral=True)

        try:
            server_uuid = uuid.UUID(server_id)
        except ValueError:
            await ctx.followup.send("Invalid server ID format.", ephemeral=True)
            return

        async with self.bot.sessions.begin() as session:
            server = await session.get(models.Server, server_uuid)
            if not server or server.discord_id != ctx.guild_id:
                await ctx.followup.send("Server profile not found.", ephemeral=True)
                return

            is_owner = ctx.guild.owner_id == ctx.user.id
            has_role = server.role_id and any(r.id == server.role_id for r in ctx.user.roles)
            if not is_owner and not has_role:
                embed = discord.Embed(description="You do not have permission to monitor this server.",
                                      color=discord.Color.red())
                await ctx.followup.send(embed=embed, ephemeral=True)
                return

            if server.status != models.Status.OFFLINE:
                await ctx.followup.send(
                    f"**{server.name}** is currently **{server.status.name}**. Monitoring is only available for offline profiles.",
                    ephemeral=True,
                )
                return

            target_hardware = server.server_type.lower()

        if not server.log_channel_id:
            await ctx.followup.send("This server profile does not have a valid log channel assigned.", ephemeral=True)
            return

        existing_task = self._monitor_tasks.get(server_uuid)
        if existing_task and not existing_task.done():
            await ctx.followup.send(
                f"A monitor is already running for **{server.name}**.",
                ephemeral=True,
            )
            return

        # Immediate user feedback confirming tracking thread assignment
        embed_tracking = discord.Embed(
            title="🛰️ Tracker Deployed",
            description=(
                f"Now scanning Hetzner inventories for a **{target_hardware.upper()}** footprint.\n"
                f"**Target Profile:** `{server.name}`\n"
                f"**Window:** Checking once a minute for the next **{duration_minutes} minutes**."
            ),
            color=discord.Color.blue()
        )
        await ctx.followup.send(embed=embed_tracking, ephemeral=True)

        # 2. Fire and forget the asynchronous background tracking task
        task = self.bot.loop.create_task(
            self._run_on_demand_monitor(
                server.log_channel_id, server.role_id, server.name, target_hardware, duration_minutes
            )
        )
        self._monitor_tasks[server_uuid] = task
        task.add_done_callback(lambda finished_task: self._monitor_task_done(server_uuid, finished_task))

    async def _run_on_demand_monitor(
            self, channel_id: int, role_id: int | None,
            server_name: str, target_hardware: str, duration_minutes: int
    ):
        """Internal worker task running the polling sequence outside SQL boundaries."""
        logging.info(
            f"[MONITOR] Starting short-term tracking instance for {server_name} ({target_hardware}) across {duration_minutes}m.")

        loops_remaining = duration_minutes
        role_ping = f"<@&{role_id}>" if role_id else "@here"

        while loops_remaining > 0:
            try:
                server_types = await asyncio.to_thread(self.bot.hcli.server_types.get_all)

                for s_type in server_types:
                    if s_type.name.lower() == target_hardware:
                        available_locs = [loc.name for loc in s_type.locations if loc.available]

                        if available_locs:
                            channel = self.bot.get_channel(channel_id) or await self.bot.fetch_channel(channel_id)
                            embed = discord.Embed(
                                title="⚡ Hetzner Stock Replenished!",
                                description=f"A **{target_hardware.upper()}** node allocation has opened up for **{server_name}**!",
                                color=discord.Color.brand_green()
                            )
                            embed.add_field(name="Locations",
                                            value=", ".join([f"`{l.upper()}`" for l in available_locs]))
                            embed.add_field(name="Action",
                                            value="Execute `/start` immediately to secure the resource allocation!")
                            allowed_mentions = discord.AllowedMentions(
                                roles=True,
                                everyone=role_id is None,
                                users=False,
                            )

                            await channel.send(content=f"{role_ping} 🚨 Hardware is available!", embed=embed, allowed_mentions=allowed_mentions)

                            logging.info(f"[MONITOR] Stock found for {server_name}. On-demand tracking complete.")
                            return

            except Exception as e:
                logging.warning(f"[MONITOR-TASK] Transient network anomaly during stock lookup: {e}")

            await asyncio.sleep(60.0)
            loops_remaining -= 1

        try:
            channel = self.bot.get_channel(channel_id) or await self.bot.fetch_channel(channel_id)
            await channel.send(
                embed=discord.Embed(
                    description=f"⏳ *On-demand tracking window for **{server_name}** ({target_hardware.upper()}) has expired without finding stock.*",
                    color=discord.Color.dark_gray()
                )
            )
        except Exception:
            pass

    @app_commands.command(name="register", description="[Owner] Registers a new Hetzner server configuration.")
    @app_commands.describe(
        name="A readable name for the server.",
        snapshot_id="The Hetzner image/snapshot ID to boot from.",
        target_guild_id="The ID of the Discord server this node belongs to.",
        server_type="The Hetzner server type (e.g., cx22, cpx31).",
        role_id="Optional: A Discord role id of the role required to start/stop this server.",
        log_channel_id="Optional: The Discord channel for player-facing status updates (e.g., online/offline alerts).",
    )
    @checks.is_owner_check()
    @app_commands.guilds(_CNFG["dev_guild_id"])
    async def register(
            self,
            ctx: discord.Interaction,
            name: str,
            snapshot_id: str,
            target_guild_id: str,
            server_type: str = "cx22",
            role_id: str | None = None,
            log_channel_id: str | None = None,
    ) -> None:
        """Registers a new server to the database.

        Args:
            ctx: The interaction calling the command.
            name: The name of the server.
            snapshot_id: The Hetzner image/snapshot ID to boot from.
            target_guild_id: The Discord ID of the guild this server belongs to.
            server_type: Hetzner server type to provision.
            role_id: The role id to assign permission to start/stop the server in the guild.
            log_channel_id: The de facto log channel for the server.
        """
        await ctx.response.defer(ephemeral=True)

        try:
            guild_id_int = int(target_guild_id)
        except ValueError:
            await ctx.followup.send("Target guild ID must be a valid integer.", ephemeral=True)
            return

        async with self.bot.sessions.begin() as session:
            existing = await session.scalar(select(models.Server).where(models.Server.name == name))
            role_id = int(role_id) if role_id else None
            snapshot_id = int(snapshot_id)
            log_channel_id = int(log_channel_id) if log_channel_id else None
            if existing:
                await ctx.followup.send(f"A server named `{name}` already exists!", ephemeral=True)
                return

            new_server = models.Server(
                name=name,
                discord_id=guild_id_int,
                role_id=role_id,
                current_snapshot_id=snapshot_id,
                server_type=server_type,
                cloudflare_zone_id=self.bot.stat_confg['cloudflare_zone_id'],
                status=models.Status.OFFLINE,
                credits=0.0,
                log_channel_id=log_channel_id,
            )
            session.add(new_server)

        embed = discord.Embed(
            title="Server Registered",
            description=f"**{name}** has been added to Hetman.",
            color=discord.Color.green()
        )
        embed.add_field(name="Target Guild", value=f"`{guild_id_int}`")
        embed.add_field(name="Snapshot ID", value=str(snapshot_id))

        await ctx.followup.send(embed=embed)
        logging.info(f"[ADMIN] Server '{name}' registered for guild {guild_id_int} by {ctx.user.name}.")

    @app_commands.command(name="edit", description="[Owner] Edits an existing Hetzner server configuration.")
    @app_commands.describe(
        server_id="The server to edit.",
        name="New readable name.",
        snapshot_id="New Hetzner snapshot ID.",
        target_guild_id="New Discord guild ID.",
        server_type="New server type (e.g., cx22).",
        role_id="New bound role.",
        clear_role="Set to True to remove the existing role requirement.",
        a2s_port="New A2S port.",
        snapshot_reserve="New snapshot reserve (in credits).",
        log_channel_id="New log channel ID.",
    )
    @app_commands.autocomplete(server_id=server_autocomplete)
    @checks.is_owner_check()
    @app_commands.guilds(_CNFG["dev_guild_id"])
    async def edit(
            self,
            ctx: discord.Interaction,
            server_id: str,
            name: str | None = None,
            snapshot_id: str | None = None,
            target_guild_id: str | None = None,
            server_type: str | None = None,
            role_id: str | None = None,
            clear_role: bool = False,
            a2s_port: str | None = None,
            snapshot_reserve: float | None = None,
            log_channel_id: str | None = None,
    ) -> None:
        """Edits an existing server in the database.

        Args:
            ctx: The interaction calling the command.
            server_id: The ID of the server to edit.
            name: The name of the server.
            snapshot_id: The Hetzner image/snapshot ID to boot from.
            target_guild_id: The Discord ID of the guild this server belongs to.
            server_type: Hetzner server type to provision.
            role_id: The role id to assign permission to start/stop the server in the guild.
            clear_role: Set to True to remove the existing role requirement.
            a2s_port: New A2S port.
            snapshot_reserve: Custom snapshot reserve (in credits).
            log_channel_id: New log channel discord ID.
        """
        await ctx.response.defer(ephemeral=True)

        try:
            server_uuid = uuid.UUID(server_id)
        except ValueError:
            await ctx.followup.send("Invalid server ID format.", ephemeral=True)
            return

        # Pre-validate guild ID if provided
        guild_id_int = None
        if target_guild_id is not None:
            try:
                guild_id_int = int(target_guild_id)
            except ValueError:
                await ctx.followup.send("Target guild ID must be a valid integer.", ephemeral=True)
                return

        async with self.bot.sessions.begin() as session:
            server = await session.get(models.Server, server_uuid)

            if not server:
                await ctx.followup.send("Server not found.", ephemeral=True)
                return

            # Check name collision if name is being changed
            if name is not None and name != server.name:
                existing = await session.scalar(select(models.Server).where(models.Server.name == name))
                if existing:
                    await ctx.followup.send(f"A server named `{name}` already exists!", ephemeral=True)
                    return
                server.name = name

            # Apply updates
            if snapshot_id is not None:
                server.current_snapshot_id = int(snapshot_id)
            if guild_id_int is not None:
                server.discord_id = guild_id_int
            if server_type is not None:
                server.server_type = server_type
            if a2s_port is not None:
                server.a2s_port = int(a2s_port)
            if snapshot_reserve is not None:
                server.snapshot_reserve = snapshot_reserve
            if log_channel_id is not None:
                server.log_channel_id = int(log_channel_id)

            if role_id is not None:
                server.role_id = int(role_id)
            elif clear_role:
                server.role_id = None

        embed = discord.Embed(
            title="Server Updated",
            description=f"Configuration for **{server.name}** has been updated.",
            color=discord.Color.blue()
        )
        embed.add_field(name="Target Guild", value=f"`{server.discord_id}`")
        embed.add_field(name="Server Type", value=server.server_type)
        embed.add_field(name="Snapshot ID", value=str(server.current_snapshot_id))
        embed.add_field(name="Role Bound", value=f"<@&{server.role_id}>" if server.role_id else "None (Owner Only)")

        await ctx.followup.send(embed=embed)
        logging.info(f"[ADMIN] Server '{server.name}' edited by {ctx.user.name}.")

    @app_commands.command(name="force_stop", description="[Owner] Instantly kills a server and takes a snapshot.")
    @app_commands.describe(server_id="The server to force-stop.")
    @app_commands.autocomplete(server_id=server_autocomplete)
    @checks.is_owner_check()
    @app_commands.guilds(_CNFG["dev_guild_id"])
    async def force_stop(self, ctx: discord.Interaction, server_id: str) -> None:
        """Instantly forces a server to spin down.

        Args:
            ctx: The interaction calling the command.
            server_id: The ID of the server to force-stop.
        """
        await ctx.response.defer(ephemeral=True)

        try:
            server_uuid = uuid.UUID(server_id)
        except ValueError:
            await ctx.followup.send("Invalid server ID format.", ephemeral=True)
            return

        async with self.bot.sessions.begin() as session:
            server = await session.get(models.Server, server_uuid)

            # Note: Removed the local guild check here so you can hit global servers
            if not server:
                await ctx.followup.send("Server not found.", ephemeral=True)
                return

            if server.status != models.Status.ONLINE:
                await ctx.followup.send(f"Server is currently **{server.status.name}**. Cannot force stop.",
                                        ephemeral=True)
                return

        reason = "An administrator has initiated an emergency force-stop. The server is dropping connection immediately."
        self.bot.loop.create_task(self.spindown(server.id, forced=True, public_reason=reason))

        embed = discord.Embed(
            title="Force Stop Initiated",
            description=f"Powering off **{server.name}** instantly and dropping node. A snapshot will be saved.",
            color=discord.Color.red()
        )
        await ctx.followup.send(embed=embed)
        logging.warning(f"[ADMIN] Force-stop executed on '{server.name}' by {ctx.user.name}.")

    @app_commands.command(name="add_credits", description="[Owner] Adds billing credits to a server.")
    @app_commands.describe(
        server_id="The server to fund.",
        amount="The amount of credits to add."
    )
    @app_commands.autocomplete(server_id=server_autocomplete)
    @checks.is_owner_check()
    @app_commands.guilds(_CNFG["dev_guild_id"])
    async def add_credits(self, ctx: discord.Interaction, server_id: str, amount: float):
        """
        Adds billing credits to a specified server. This command is restricted to
        users with owner permissions and allows adding a specific number of
        credits to the account balance of a server.

        Args:
            ctx: The interaction context that contains
                information about the command invocation.
            server_id: The unique identifier of the server to which credits
                will be added.
            amount: The amount of billing credits to add to the server.
        """
        await ctx.response.defer(ephemeral=True)
        try:
            server_uuid = uuid.UUID(server_id)
            async with self.bot.sessions.begin() as session:
                server = await session.get(models.Server, server_uuid)
                if server:
                    server.credits += amount
                    await ctx.followup.send(
                        f"Added €{amount:.2f} to **{server.name}**. New balance: €{server.credits:.2f}")
                else:
                    await ctx.followup.send("Server not found.", ephemeral=True)
        except Exception as e:
            logging.exception(f"[ADMIN] Failed to add credits: {e}")
            await ctx.followup.send("Failed to add credits due to an internal error.", ephemeral=True)


async def setup(bot_instance: Hetman) -> None:
    """Sets up the error handler.

    This function is called when the cog is loaded.
    It is used to add the cog to the bot.

    Args:
        bot_instance: The bot instance.
    """
    await bot_instance.add_cog(ServerManager(bot_instance))