import enum
from typing import Callable

from sqlalchemy import Sequence, Table, Column, Integer, Enum, TIMESTAMP, text, Text, String
from sqlalchemy.dialects.postgresql import JSONB

from src.common.db import metadata


class GraphTypeEnum(enum.Enum):
    ROAD = "ROAD"
    WATER = "WATER"


func: Callable

graphs_id_seq = Sequence("graphs_id_seq")
graphs = Table(
    "graphs",
    metadata,
    Column("id", Integer, primary_key=True, server_default=graphs_id_seq.next_value()),
    Column("name", String(100)),
    Column("type", Enum(GraphTypeEnum), nullable=False, default=GraphTypeEnum.ROAD),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False),
)
