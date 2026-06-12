"""File for database models.

This file contains the SQLAlchemy models for the database.
These models are used to create the database tables and
to interact with the database.
For more information on SQLAlchemy, please see the docs:
https://docs.sqlalchemy.org/en/21/

Typical usage example:
    ```py
    from hetman.data import models
    models.Base.metadata.create_all(...)
    ```
"""
# License: EPL-2.0
# SPDX-License-Identifier: EPL-2.0
# Copyright (c) 2023-present Tech. TTGames

import datetime
import enum
import uuid

import sqlalchemy
from sqlalchemy import orm


class Status(int, enum.Enum):
    """Enum for the status of a server."""
    OFFLINE = 0
    ONLINE = 1
    SNAPSHOTTING = 2
    DELETING = 3
    PROVISIONING = 4
    STARTING = 5


class Base(orm.DeclarativeBase):
    """Base of SQLAlchemy models

    This is the base class for all SQLAlchemy models.
    It is used to define the metadata object for the database.
    A detailed database schema is available in our docs.
    It includes a diagram of the database tables.
    Please see the SQLAlchemy docs for more information about
    how to use this class.
    """
    pass


class Server(Base):
    __tablename__ = "servers"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(sqlalchemy.UUID, primary_key=True, default=uuid.uuid7)
    name: orm.Mapped[str] = orm.mapped_column(sqlalchemy.String, unique=True)
    discord_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger, index=True)
    role_id: orm.Mapped[int | None] = orm.mapped_column(sqlalchemy.BigInteger, nullable=True)
    log_channel_id: orm.Mapped[int | None] = orm.mapped_column(sqlalchemy.BigInteger, nullable=True)

    # Hetzner Data
    hcloud_server_id: orm.Mapped[int | None] = orm.mapped_column(sqlalchemy.BigInteger, nullable=True)
    current_snapshot_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger)
    status: orm.Mapped[Status] = orm.mapped_column(sqlalchemy.Enum(Status), default=Status.OFFLINE)
    server_type: orm.Mapped[str] = orm.mapped_column(sqlalchemy.String, default="cx23")

    # Billing Data
    start_time: orm.Mapped[datetime.datetime | None] = orm.mapped_column(sqlalchemy.DateTime, nullable=True)
    credits: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger, default=0)
    snapshot_reserve: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger, default=50000)
    cost_per_hour: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger, default=5000)
    stop_requested: orm.Mapped[bool] = orm.mapped_column(sqlalchemy.Boolean, default=False)

    # Connection Data
    ip_address: orm.Mapped[str | None] = orm.mapped_column(sqlalchemy.String, nullable=True)
    a2s_port: orm.Mapped[int] = orm.mapped_column(sqlalchemy.Integer, default=27015)

    # Cloudflare DDNS Data
    cloudflare_zone_id: orm.Mapped[str | None] = orm.mapped_column(sqlalchemy.String, nullable=True)
    cloudflare_record_id: orm.Mapped[str | None] = orm.mapped_column(sqlalchemy.String, nullable=True)
