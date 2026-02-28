"""add chunk_type column

Revision ID: 003
Revises: 002
Create Date: 2026-02-26

"""
import sqlalchemy as sa
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "chunks",
        sa.Column("chunk_type", sa.String(20), nullable=False, server_default="granular"),
    )
    op.create_index("idx_chunks_chunk_type", "chunks", ["chunk_type"])


def downgrade() -> None:
    op.drop_index("idx_chunks_chunk_type", table_name="chunks")
    op.drop_column("chunks", "chunk_type")
