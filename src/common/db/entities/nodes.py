import enum
from typing import Callable

import geoalchemy2
from sqlalchemy import Sequence, Table, Column, Integer, Enum, TIMESTAMP, text, Text, String
from sqlalchemy.dialects.postgresql import JSONB

from src.common.db import metadata


class NodeTypeEnum(enum.Enum):
    DRIVE = "DRIVE"
    TRAIN = "TRAIN"
    PLATFORM = "PLATFORM"
    WALK = "WALK"
    TRAM = "TRAM"
    BUS = "BUS"
    TROLLEYBUS = "TROLLEYBUS"
    SUBWAY = "SUBWAY"
    WATER = "WATER"


func: Callable

nodes_id_seq = Sequence("node_id_seq")
nodes = Table(
    "nodes",
    metadata,
    Column("id", Integer, primary_key=True, server_default=nodes_id_seq.next_value()),
    Column("type", Enum(NodeTypeEnum), nullable=False, default=NodeTypeEnum.DRIVE),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("route", String(200), nullable=False, default=''),
    Column(
        "point",
        geoalchemy2.types.Geometry(
            geometry_type="POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
        ),
        nullable=False,
        default=None,
    ),
    Column("created_at", TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False),
)
