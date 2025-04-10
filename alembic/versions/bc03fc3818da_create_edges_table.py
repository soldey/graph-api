"""create_edges_table

Revision ID: bc03fc3818da
Revises: 0064760f2bd5
Create Date: 2025-01-27 16:44:57.433367

"""
from typing import Sequence as seq, Union

import geoalchemy2
from alembic import op
import sqlalchemy as sa
from sqlalchemy import Sequence, Column, Integer, ForeignKey, Enum, TIMESTAMP, text, Text, Float, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.ddl import CreateSequence, DropSequence

from src.common.db.entities.edges import EdgeTypeEnum, WeightTypeEnum, EdgeLevelEnum
from src.common.db.entities.graphs import graphs
from src.common.db.entities.nodes import nodes

# revision identifiers, used by Alembic.
revision: str = 'bc03fc3818da'
down_revision: Union[str, None] = '0064760f2bd5'
branch_labels: Union[str, seq[str], None] = None
depends_on: Union[str, seq[str], None] = None


def upgrade() -> None:
    op.execute(CreateSequence(Sequence("edges_id_seq"), if_not_exists=True))
    op.create_table(
        "edges",
        Column("id", Integer, primary_key=True),
        Column("u", Integer, ForeignKey(nodes.c.id, ondelete="CASCADE"), nullable=False),
        Column("v", Integer, ForeignKey(nodes.c.id, ondelete="CASCADE"), nullable=False),
        Column("type", Enum(EdgeTypeEnum, name="edgetypeenum"), nullable=False, default=EdgeTypeEnum.DRIVE),
        Column("weight", Float, nullable=False),
        Column("weight_type", Enum(WeightTypeEnum, name="weighttypeenum"), nullable=False, default=WeightTypeEnum.DISTANCE),
        Column("level", Enum(EdgeLevelEnum, name="edgelevelenum"), nullable=False, default=EdgeLevelEnum.NONE),
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
    op.create_unique_constraint("edges_multiunique", "edges", ["u", "v", "type", "geometry", "route"])


def downgrade() -> None:
    op.execute(DropSequence(Sequence("edges_id_seq"), if_exists=True))
    op.drop_constraint("edges_multiunique", "edges")
    op.drop_table("edges")
    op.execute("DROP TYPE IF EXISTS edgetypeenum")
    op.execute("DROP TYPE IF EXISTS weighttypeenum")
    op.execute("DROP TYPE IF EXISTS edgelevelenum")
