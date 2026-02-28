"""Initial schema

Revision ID: 001
Revises:
Create Date: 2025-01-01 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "dvds",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("instructor", sa.Text()),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()")),
    )

    op.create_table(
        "volumes",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("dvd_id", UUID(as_uuid=True), sa.ForeignKey("dvds.id"), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("file_path", sa.Text()),
        sa.Column("duration_seconds", sa.Float()),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()")),
    )

    op.create_table(
        "chunks",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("volume_id", UUID(as_uuid=True), sa.ForeignKey("volumes.id"), nullable=False),
        sa.Column("start_time", sa.Float(), nullable=False),
        sa.Column("end_time", sa.Float(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("position", sa.Text()),
        sa.Column("technique", sa.Text()),
        sa.Column("technique_type", sa.String(50)),
        sa.Column("aliases", sa.ARRAY(sa.String())),
        sa.Column("description", sa.Text()),
        sa.Column("embedding_id", sa.Text()),
        sa.Column("llm_raw_response", JSONB),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()")),
    )

    op.create_index("idx_chunks_technique", "chunks", ["technique"])
    op.create_index("idx_chunks_position", "chunks", ["position"])
    op.create_index("idx_chunks_type", "chunks", ["technique_type"])

    op.create_table(
        "ingest_jobs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("dvd_title", sa.Text(), nullable=False),
        sa.Column("volume_name", sa.Text(), nullable=False),
        sa.Column("file_path", sa.Text(), nullable=False),
        sa.Column("status", sa.String(50), server_default="queued"),
        sa.Column("progress", sa.Float(), server_default="0"),
        sa.Column("error_message", sa.Text()),
        sa.Column("volume_id", UUID(as_uuid=True), sa.ForeignKey("volumes.id")),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("now()")),
    )


def downgrade() -> None:
    op.drop_table("ingest_jobs")
    op.drop_index("idx_chunks_type", "chunks")
    op.drop_index("idx_chunks_position", "chunks")
    op.drop_index("idx_chunks_technique", "chunks")
    op.drop_table("chunks")
    op.drop_table("volumes")
    op.drop_table("dvds")
