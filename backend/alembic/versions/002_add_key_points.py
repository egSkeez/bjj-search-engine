"""add key_points column to chunks

Revision ID: 002
Revises: 001
Create Date: 2026-02-26
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "chunks",
        sa.Column("key_points", ARRAY(sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("chunks", "key_points")
