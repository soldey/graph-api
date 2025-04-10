import enum
from typing import Callable

import geoalchemy2
from sqlalchemy import Sequence, Table, Column, Integer, Enum, TIMESTAMP, text, ForeignKey, Text, Float, String
from sqlalchemy.dialects.postgresql import JSONB

from src.common.db import metadata
from src.common.db.entities.nodes import nodes


class EdgeTypeEnum(enum.Enum):
    DRIVE = "DRIVE"
    TRAIN = "TRAIN"
    BOARDING = "BOARDING"
    WALK = "WALK"
    TRAM = "TRAM"
    BUS = "BUS"
    TROLLEYBUS = "TROLLEYBUS"
    SUBWAY = "SUBWAY"
    WATERCHANNEL = "WATERCHANNEL"


class WeightTypeEnum(enum.Enum):
    DISTANCE = "DISTANCE"
    TIME = "TIME"
    VOLUME = "VOLUME"


class EdgeLevelEnum(enum.Enum):
    NONE = "NONE"
    LOCAL = "LOCAL"
    REGIONAL = "REGIONAL"
    FEDERAL = "FEDERAL"


func: Callable

edges_id_seq = Sequence("edges_id_seq")
edges = Table(
    "edges",
    metadata,
    Column("id", Integer, primary_key=True, server_default=edges_id_seq.next_value()),
    Column("u", Integer, ForeignKey(nodes.c.id, ondelete="CASCADE"), nullable=False),
    Column("v", Integer, ForeignKey(nodes.c.id, ondelete="CASCADE"), nullable=False),
    Column("type", Enum(EdgeTypeEnum), nullable=False, default=EdgeTypeEnum.DRIVE),
    Column("weight", Float, nullable=False),
    Column("weight_type", Enum(WeightTypeEnum), nullable=False, default=WeightTypeEnum.DISTANCE),
    Column("level", Enum(EdgeLevelEnum), nullable=False, default=EdgeLevelEnum.NONE),
    Column("speed", Integer, nullable=False),
    Column("route", String(50), nullable=True, default=None),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column(
        "geometry",
        geoalchemy2.types.Geometry(
            spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=True
        ),
        nullable=True,
        default=None,
    ),
    Column("created_at", TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False),
)
