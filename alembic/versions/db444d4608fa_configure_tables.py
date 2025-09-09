"""configure_tables

Revision ID: db444d4608fa
Revises: ef78a299d768
Create Date: 2025-09-09 13:10:12.497060

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'db444d4608fa'
down_revision: Union[str, None] = 'ef78a299d768'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE graphs SET (
            autovacuum_vacuum_scale_factor = 0.02,
            autovacuum_analyze_scale_factor = 0.02,
            autovacuum_vacuum_threshold = 1000,
            autovacuum_analyze_threshold = 1000
        );
    """)
    op.execute("""
        ALTER TABLE edges SET (
            autovacuum_vacuum_scale_factor = 0.02,
            autovacuum_analyze_scale_factor = 0.02,
            autovacuum_vacuum_threshold = 1000,
            autovacuum_analyze_threshold = 1000
        );
    """)
    op.execute("""
        ALTER TABLE nodes SET (
            autovacuum_vacuum_scale_factor = 0.02,
            autovacuum_analyze_scale_factor = 0.02,
            autovacuum_vacuum_threshold = 1000,
            autovacuum_analyze_threshold = 1000
        );
    """)
    op.execute("""
        ALTER TABLE graph_edges SET (
            autovacuum_vacuum_scale_factor = 0.02,
            autovacuum_analyze_scale_factor = 0.02,
            autovacuum_vacuum_threshold = 1000,
            autovacuum_analyze_threshold = 1000
        );
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE graph_edges RESET (
            autovacuum_vacuum_scale_factor,
            autovacuum_analyze_scale_factor,
            autovacuum_vacuum_threshold,
            autovacuum_analyze_threshold
        );
    """)
    op.execute("""
        ALTER TABLE nodes RESET (
            autovacuum_vacuum_scale_factor,
            autovacuum_analyze_scale_factor,
            autovacuum_vacuum_threshold,
            autovacuum_analyze_threshold
        );
    """)
    op.execute("""
        ALTER TABLE edges RESET (
            autovacuum_vacuum_scale_factor,
            autovacuum_analyze_scale_factor,
            autovacuum_vacuum_threshold,
            autovacuum_analyze_threshold
        );
    """)
    op.execute("""
        ALTER TABLE graphs RESET (
            autovacuum_vacuum_scale_factor,
            autovacuum_analyze_scale_factor,
            autovacuum_vacuum_threshold,
            autovacuum_analyze_threshold
        );
    """)
