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

import sqlalchemy
from sqlalchemy import orm

class Status(int, enum.Enum):
    """Enum for the status of a server."""
    OFFLINE = 0
    ONLINE = 1
    SNAPSHOTTING = 2
    DELETING = 3


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

    id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.Integer, primary_key=True)
    name: orm.Mapped[str] = orm.mapped_column(sqlalchemy.String, unique=True)
    discord_id: orm.Mapped[int] = orm.mapped_column(sqlalchemy.BigInteger, index=True)
    role_id: orm.Mapped[int | None] = orm.mapped_column(sqlalchemy.BigInteger, nullable=True)

    # Hetzner Data
    hcloud_server_id: orm.Mapped[int | None] = orm.mapped_column(sqlalchemy.BigInteger, nullable=True)
    current_snapshot_id: orm.Mapped[int | None] = orm.mapped_column(sqlalchemy.BigInteger, nullable=True)
    status: orm.Mapped[Status] = orm.mapped_column(sqlalchemy.Enum(Status), default=Status.OFFLINE)

    # Billing Data
    start_time: orm.Mapped[datetime.datetime | None] = orm.mapped_column(sqlalchemy.DateTime, nullable=True)
    credits: orm.Mapped[float] = orm.mapped_column(sqlalchemy.Float, default=0.0)
    snapshot_reserve: orm.Mapped[float] = orm.mapped_column(sqlalchemy.Float, default=0.5)

    # Connection Data
    ip_address: orm.Mapped[str | None] = orm.mapped_column(sqlalchemy.String, nullable=True)
    a2s_port: orm.Mapped[int] = orm.mapped_column(sqlalchemy.Integer, default=27015)

    # Cloudflare DDNS Data
    cloudflare_zone_id: orm.Mapped[str | None] = orm.mapped_column(sqlalchemy.String, nullable=True)
    cloudflare_record_id: orm.Mapped[str | None] = orm.mapped_column(sqlalchemy.String, nullable=True)