"""create_graphs_table

Revision ID: 0064760f2bd5
Revises: b2b4a6cc9c74
Create Date: 2025-01-27 14:56:17.894460

"""
from typing import Sequence as seq, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import Sequence, Column, Integer, Enum, String, TIMESTAMP, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.ddl import CreateSequence, DropSequence

from src.common.db.entities.graphs import GraphTypeEnum

# revision identifiers, used by Alembic.
revision: str = '0064760f2bd5'
down_revision: Union[str, None] = 'b2b4a6cc9c74'
branch_labels: Union[str, seq[str], None] = None
depends_on: Union[str, seq[str], None] = None


def upgrade() -> None:
    op.execute(CreateSequence(Sequence("graphs_id_seq"), if_not_exists=True))
    op.create_table(
        "graphs",
        Column("id", Integer, primary_key=True),
        Column("name", String(100)),
        Column("type", Enum(GraphTypeEnum, name="graphtypeenum"), nullable=False, default=GraphTypeEnum.ROAD),
        Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
        Column("created_at", TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False),
        Column("updated_at", TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False),
    )


def downgrade() -> None:
    op.execute(DropSequence(Sequence("graphs_id_seq"), if_exists=True))
    op.drop_table("graphs")
    op.execute("DROP TYPE IF EXISTS graphtypeenum")
