"""create_graph_edges_table

Revision ID: ef78a299d768
Revises: bc03fc3818da
Create Date: 2025-03-20 12:31:12.091926

"""
from typing import Sequence as seq, Union

import geoalchemy2
from alembic import op
import sqlalchemy as sa
from sqlalchemy import Sequence, Column, Integer, ForeignKey, Enum, TIMESTAMP, text
from sqlalchemy.sql.ddl import CreateSequence, DropSequence

from src.common.db.entities.edges import EdgeTypeEnum, WeightTypeEnum, EdgeLevelEnum, edges
from src.common.db.entities.graphs import graphs
from src.common.db.entities.nodes import nodes


# revision identifiers, used by Alembic.
revision: str = 'ef78a299d768'
down_revision: Union[str, None] = 'bc03fc3818da'
branch_labels: Union[str, seq[str], None] = None
depends_on: Union[str, seq[str], None] = None


def upgrade() -> None:
    op.execute(CreateSequence(Sequence("graph_edges_id_seq"), if_not_exists=True))
    op.create_table(
        "graph_edges",
        Column("id", Integer, primary_key=True),
        Column("graph", Integer, ForeignKey(graphs.c.id, ondelete="CASCADE"), nullable=False),
        Column("edge", Integer, ForeignKey(edges.c.id, ondelete="CASCADE"), nullable=False),
    )
    op.create_unique_constraint("graph_edges_multiunique", "graph_edges", ["graph", "edge"])


def downgrade() -> None:
    op.execute(DropSequence(Sequence("graph_edges_id_seq"), if_exists=True))
    op.drop_constraint("graph_edges_multiunique", "graph_edges")
    op.drop_table("graph_edges")
