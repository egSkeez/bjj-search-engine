"""Batch ingest script for John Danaher - Back Attacks Enter The System."""

import json
import logging
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

DVD_TITLE = "Back Attacks - Enter The System"
INSTRUCTOR = "John Danaher"
SOURCE_DIR = Path(r"D:\BJJ\John Danaher\Back Attacks - John Danaher")

VOLUMES = [
    ("Vol 1 - Straitjacket System", "Back Attacks Enter The System Vol 1 - Straitjacket System.mp4"),
    ("Vol 2 - 10 Critical Principles", "Back Attacks Enter The System Vol 2 10 Critical Principles.mp4"),
    ("Vol 3 - Workings of Straitjacket System", "Back Attacks Enter The System Vol 3 Workings of Straitjacket System.mp4"),
    ("Vol 4 - Workings of Straitjacket System 2", "Back Attacks Enter The System Vol 4 Workings of Straitjacket System 2.mp4"),
    ("Vol 5 - Auxiliary Systems", "Back Attacks Enter The System Vol 5 Auxiliary Systems.mp4"),
    ("Vol 6 - Auxiliary Systems 2", "Back Attacks Enter The System Vol 6 Auxiliary Systems2.mp4"),
    ("Vol 7 - Establishing Hooks and Rear Mount", "Back Attacks Enter The System Vol 7 Establishing Hooks and Rear Mount.mp4"),
    ("Vol 8 - Establishing Hooks and Rear Mount 2", "Back Attacks Enter The System Vol 8 Establishing Hooks and Rear Mount 2.mp4"),
]


def main():
    from app.config import settings

    data_dir = Path(settings.data_dir)
    transcripts_dir = data_dir / "transcripts"
    chunks_dir = data_dir / "chunks"
    transcripts_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    start_from = 0
    if len(sys.argv) > 1:
        start_from = int(sys.argv[1]) - 1
        logger.info("Starting from volume %d", start_from + 1)

    for idx, (vol_name, filename) in enumerate(VOLUMES):
        if idx < start_from:
            continue

        video_path = SOURCE_DIR / filename
        if not video_path.exists():
            logger.error("File not found: %s", video_path)
            continue

        safe_name = f"{DVD_TITLE}_{vol_name}".replace(" ", "_").replace("/", "_")
        transcript_path = transcripts_dir / f"{safe_name}.json"
        chunks_path = chunks_dir / f"{safe_name}.json"

        logger.info("=" * 60)
        logger.info("VOLUME %d/%d: %s", idx + 1, len(VOLUMES), vol_name)
        logger.info("=" * 60)

        # --- Transcription ---
        if transcript_path.exists():
            logger.info("Transcript already exists, loading from %s", transcript_path)
            from app.services.transcription import load_transcription
            segments = load_transcription(str(transcript_path))
        else:
            logger.info("Transcribing %s...", filename)
            t0 = time.time()
            from app.services.transcription import transcribe_video
            segments = transcribe_video(str(video_path), output_path=str(transcript_path))
            elapsed = time.time() - t0
            logger.info("Transcription took %.1f minutes (%d segments)", elapsed / 60, len(segments))

        # --- Chunking ---
        logger.info("Chunking %d segments...", len(segments))
        from app.services.chunker import chunk_segments
        chunks = chunk_segments(segments, dvd_title=DVD_TITLE, volume_name=vol_name)
        logger.info("Created %d chunks", len(chunks))

        with open(chunks_path, "w", encoding="utf-8") as f:
            json.dump(chunks, f, indent=2)

        # --- Tagging ---
        logger.info("Tagging %d chunks with OpenAI...", len(chunks))
        t0 = time.time()
        from app.services.tagger import tag_chunks
        chunks = tag_chunks(chunks)
        elapsed = time.time() - t0
        logger.info("Tagging took %.1f seconds", elapsed)

        with open(chunks_path, "w", encoding="utf-8") as f:
            json.dump(chunks, f, indent=2)

        # --- Embedding ---
        logger.info("Embedding %d chunks...", len(chunks))
        from app.services.embedder import embed_chunks
        from app.services.vector_store import upsert_chunks
        chunks = embed_chunks(chunks)
        upsert_chunks(chunks)

        with open(chunks_path, "w", encoding="utf-8") as f:
            json.dump(chunks, f, indent=2)

        # --- Store in PostgreSQL ---
        logger.info("Storing in database...")
        _store_in_db_sync(chunks, vol_name, str(video_path))

        logger.info("Volume %d complete: %d chunks indexed\n", idx + 1, len(chunks))

    logger.info("ALL DONE! %d volumes processed.", len(VOLUMES))


def _store_in_db_sync(chunks: list[dict], vol_name: str, file_path: str):
    import uuid as uuid_mod

    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import Session

    from app.config import settings
    from app.models import DVD, Volume, Chunk

    sync_url = settings.database_url.replace("+asyncpg", "")
    engine = create_engine(sync_url)

    with Session(engine) as session:
        dvd_stmt = select(DVD).where(DVD.title == DVD_TITLE)
        dvd = session.execute(dvd_stmt).scalar_one_or_none()
        if not dvd:
            dvd = DVD(title=DVD_TITLE, instructor=INSTRUCTOR)
            session.add(dvd)
            session.flush()

        vol = Volume(
            dvd_id=dvd.id,
            name=vol_name,
            file_path=file_path,
        )
        session.add(vol)
        session.flush()

        for chunk_data in chunks:
            chunk = Chunk(
                id=uuid_mod.UUID(chunk_data["id"]),
                volume_id=vol.id,
                start_time=float(chunk_data["start_time"]),
                end_time=float(chunk_data["end_time"]),
                text=chunk_data["text"],
                position=chunk_data.get("position"),
                technique=chunk_data.get("technique"),
                technique_type=chunk_data.get("technique_type"),
                aliases=chunk_data.get("aliases"),
                description=chunk_data.get("description"),
                embedding_id=chunk_data.get("embedding_id"),
                llm_raw_response=chunk_data.get("llm_raw_response"),
            )
            session.add(chunk)

        session.commit()
    engine.dispose()


if __name__ == "__main__":
    main()
