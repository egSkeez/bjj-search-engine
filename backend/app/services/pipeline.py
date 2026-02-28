import json
import logging
import uuid
from pathlib import Path

from sqlalchemy import select

from app.config import settings
from app.database import async_session
from app.models import Chunk, DVD, IngestJob, Volume

logger = logging.getLogger(__name__)


async def _update_job(job_id: str, **kwargs):
    async with async_session() as session:
        stmt = select(IngestJob).where(IngestJob.id == uuid.UUID(job_id))
        result = await session.execute(stmt)
        job = result.scalar_one()
        for key, value in kwargs.items():
            setattr(job, key, value)
        await session.commit()


async def run_pipeline(
    job_id: str,
    file_path: str,
    dvd_title: str,
    volume_name: str,
    instructor: str | None = None,
):
    """Full ingest pipeline: transcribe -> chunk -> tag -> embed -> index."""
    data_dir = Path(settings.data_dir)
    transcripts_dir = data_dir / "transcripts"
    chunks_dir = data_dir / "chunks"
    transcripts_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    safe_name = f"{dvd_title}_{volume_name}".replace(" ", "_").replace("/", "_")
    transcript_path = transcripts_dir / f"{safe_name}.json"
    chunks_path = chunks_dir / f"{safe_name}.json"

    try:
        # --- Transcribe ---
        await _update_job(job_id, status="transcribing", progress=5.0)

        from app.services.transcription import transcribe_video

        segments = transcribe_video(file_path, output_path=str(transcript_path))
        logger.info("Transcribed %d segments", len(segments))

        await _update_job(job_id, status="chunking", progress=30.0)

        # --- Chunk ---
        from app.services.chunker import chunk_segments

        raw_chunks = chunk_segments(segments, dvd_title=dvd_title, volume_name=volume_name)
        logger.info("Created %d chunks", len(raw_chunks))

        with open(chunks_path, "w", encoding="utf-8") as f:
            json.dump(raw_chunks, f, indent=2)

        await _update_job(job_id, status="tagging", progress=45.0)

        # --- Tag ---
        from app.services.tagger import tag_chunks

        tagged_chunks = tag_chunks(raw_chunks)

        with open(chunks_path, "w", encoding="utf-8") as f:
            json.dump(tagged_chunks, f, indent=2)

        await _update_job(job_id, status="embedding", progress=70.0)

        # --- Embed ---
        from app.services.embedder import embed_chunks
        from app.services.vector_store import upsert_chunks

        embedded_chunks = embed_chunks(tagged_chunks)
        upsert_chunks(embedded_chunks)

        await _update_job(job_id, status="indexing", progress=90.0)

        # --- Store in PostgreSQL ---
        async with async_session() as session:
            # Find or create DVD
            dvd_stmt = select(DVD).where(DVD.title == dvd_title)
            dvd_result = await session.execute(dvd_stmt)
            dvd = dvd_result.scalar_one_or_none()
            if not dvd:
                dvd = DVD(title=dvd_title, instructor=instructor)
                session.add(dvd)
                await session.flush()

            vol = Volume(
                dvd_id=dvd.id,
                name=volume_name,
                file_path=file_path,
            )
            session.add(vol)
            await session.flush()

            for chunk_data in embedded_chunks:
                chunk = Chunk(
                    id=uuid.UUID(chunk_data["id"]),
                    volume_id=vol.id,
                    start_time=chunk_data["start_time"],
                    end_time=chunk_data["end_time"],
                    text=chunk_data["text"],
                    position=chunk_data.get("position"),
                    technique=chunk_data.get("technique"),
                    technique_type=chunk_data.get("technique_type"),
                    aliases=chunk_data.get("aliases"),
                    description=chunk_data.get("description"),
                    key_points=chunk_data.get("key_points"),
                    chunk_type=chunk_data.get("chunk_type", "granular"),
                    embedding_id=chunk_data.get("embedding_id"),
                    llm_raw_response=chunk_data.get("llm_raw_response"),
                )
                session.add(chunk)

            # Update job with volume_id
            job_stmt = select(IngestJob).where(IngestJob.id == uuid.UUID(job_id))
            job_result = await session.execute(job_stmt)
            job = job_result.scalar_one()
            job.volume_id = vol.id
            job.status = "complete"
            job.progress = 100.0

            await session.commit()

        logger.info("Pipeline complete for %s - %s: %d chunks indexed", dvd_title, volume_name, len(embedded_chunks))

    except Exception as e:
        logger.exception("Pipeline failed for job %s", job_id)
        await _update_job(job_id, status="failed", error_message=str(e))
