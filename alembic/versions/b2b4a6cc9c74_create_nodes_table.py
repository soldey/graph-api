"""create_nodes_table

Revision ID: b2b4a6cc9c74
Revises: 
Create Date: 2025-01-23 15:41:23.749865

"""
from typing import Sequence as seq, Union

import geoalchemy2
from alembic import op
from sqlalchemy import Sequence, Column, Integer, Enum, TIMESTAMP, text, Text, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.ddl import CreateSequence, DropSequence

from src.common.db.entities.nodes import NodeTypeEnum

# revision identifiers, used by Alembic.
revision: str = 'b2b4a6cc9c74'
down_revision: Union[str, None] = None
branch_labels: Union[str, seq[str], None] = None
depends_on: Union[str, seq[str], None] = None


def upgrade() -> None:
    op.execute(CreateSequence(Sequence("node_id_seq"), if_not_exists=True))
    op.create_table(
        "nodes",
        Column("id", Integer, primary_key=True),
        Column("type", Enum(NodeTypeEnum, name="nodetypeenum"), nullable=False, default=NodeTypeEnum.DRIVE),
        Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
        Column("route", String(50), nullable=True, default=None),
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
    op.create_unique_constraint("node_multiunique", "nodes", ["type", "point", "route"])


def downgrade() -> None:
    op.execute(DropSequence(Sequence("node_id_seq"), if_exists=True))
    op.drop_constraint("node_multiunique", "nodes")
    op.drop_table("nodes")
    op.execute("DROP TYPE IF EXISTS nodetypeenum")
