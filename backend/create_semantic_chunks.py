"""Create semantic (music-boundary) chunks for all ingested volumes.

For each volume that has granular chunks but no semantic chunks yet:
  1. Extract audio from the video file with FFmpeg
  2. Detect music segment timestamps with librosa
  3. Merge existing granular chunks into technique-level sections
  4. Tag the merged chunks with GPT-4o
  5. Embed and upsert to Qdrant  (chunk_type='semantic' in payload)
  6. Store in Postgres

No re-transcription needed — reuses existing transcript text from
the granular chunks already in the database.

Usage:
    python create_semantic_chunks.py               # process all pending volumes
    python create_semantic_chunks.py --limit 5     # only first 5 volumes
    python create_semantic_chunks.py --dry-run     # show what would be done
    python create_semantic_chunks.py --volume-id <uuid>  # single volume
"""

import argparse
import logging
import os
import sys
import uuid as uuid_mod
from pathlib import Path

from dotenv import load_dotenv

for _v in ("ALL_PROXY", "all_proxy", "HTTP_PROXY", "http_proxy",
           "HTTPS_PROXY", "https_proxy", "SOCKS_PROXY", "SOCKS5_PROXY"):
    os.environ.pop(_v, None)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("semantic_chunks.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def get_volumes_needing_semantic(engine, volume_id: str | None = None):
    """Return volumes that have granular chunks but no semantic chunks yet."""
    from sqlalchemy import text

    query = """
        SELECT DISTINCT
            v.id::text        AS volume_id,
            v.name            AS volume_name,
            v.file_path       AS file_path,
            v.duration_seconds AS duration,
            d.title           AS dvd_title,
            d.instructor      AS instructor,
            COUNT(c.id)       AS granular_count
        FROM volumes v
        JOIN dvds d ON d.id = v.dvd_id
        JOIN chunks c ON c.volume_id = v.id AND c.chunk_type = 'granular'
        WHERE NOT EXISTS (
            SELECT 1 FROM chunks sc
            WHERE sc.volume_id = v.id AND sc.chunk_type = 'semantic'
        )
        {volume_filter}
        GROUP BY v.id, v.name, v.file_path, v.duration_seconds, d.title, d.instructor
        ORDER BY d.title, v.name
    """.format(
        volume_filter=f"AND v.id = '{volume_id}'" if volume_id else ""
    )

    with engine.connect() as conn:
        rows = conn.execute(text(query)).fetchall()

    return [dict(r._mapping) for r in rows]


def get_granular_chunks(engine, volume_id: str) -> list[dict]:
    """Fetch all granular chunks for a volume, sorted by start_time."""
    from sqlalchemy import text

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                id::text, start_time, end_time, text,
                position, technique, technique_type, aliases,
                description, key_points
            FROM chunks
            WHERE volume_id = :vid AND chunk_type = 'granular'
            ORDER BY start_time
        """), {"vid": volume_id}).fetchall()

    return [dict(r._mapping) for r in rows]


def store_semantic_chunks(engine, volume_id: str, chunks: list[dict]):
    """Persist semantic chunks to Postgres."""
    from sqlalchemy import text

    with engine.begin() as conn:
        for c in chunks:
            conn.execute(text("""
                INSERT INTO chunks (
                    id, volume_id, start_time, end_time, text,
                    position, technique, technique_type, aliases,
                    description, key_points, chunk_type, embedding_id,
                    llm_raw_response, created_at
                ) VALUES (
                    :id, :volume_id, :start_time, :end_time, :text,
                    :position, :technique, :technique_type, :aliases,
                    :description, :key_points, 'semantic', :embedding_id,
                    :llm_raw_response, NOW()
                )
                ON CONFLICT (id) DO NOTHING
            """), {
                "id": c["id"],
                "volume_id": volume_id,
                "start_time": c["start_time"],
                "end_time": c["end_time"],
                "text": c["text"],
                "position": c.get("position") or None,
                "technique": c.get("technique") or None,
                "technique_type": c.get("technique_type") or None,
                "aliases": c.get("aliases") or None,
                "description": c.get("description") or None,
                "key_points": c.get("key_points") or None,
                "embedding_id": c.get("embedding_id") or None,
                "llm_raw_response": None,
            })

    logger.info("Stored %d semantic chunks for volume %s", len(chunks), volume_id[:8])


def process_volume(engine, vol: dict, dry_run: bool = False, detect_only: bool = False) -> bool:
    """Process one volume: detect music, merge chunks, tag, embed, store.

    detect_only=True skips tagging and embedding (no OpenAI required).
    Chunks are stored in Postgres with empty tags, ready to be tagged later.
    """
    volume_id = vol["volume_id"]
    dvd_title = vol["dvd_title"]
    volume_name = vol["volume_name"]
    file_path = vol["file_path"]
    duration = vol.get("duration") or 0.0

    logger.info("--- Processing: %s / %s (granular chunks: %d)",
                dvd_title, volume_name, vol["granular_count"])

    if not file_path or not Path(file_path).exists():
        logger.warning("  SKIP: video file not found: %s", file_path)
        return False

    if dry_run:
        logger.info("  DRY RUN: would process %s", file_path)
        return True

    # --- 1. Detect music boundaries ---
    from app.services.music_detector import detect_music_from_video, music_to_chunk_boundaries

    music_segs = detect_music_from_video(file_path)

    if music_segs:
        logger.info("  Found %d music segments", len(music_segs))
    else:
        logger.info("  No music detected — will use single-section grouping with sub-splits")

    section_windows = music_to_chunk_boundaries(music_segs, duration or 99999.0)
    logger.info("  Section windows: %d", len(section_windows))

    # --- 2. Get existing granular chunks ---
    granular = get_granular_chunks(engine, volume_id)
    if not granular:
        logger.warning("  SKIP: no granular chunks found")
        return False

    # --- 3. Build semantic chunks ---
    from app.services.semantic_chunker import build_semantic_chunks

    semantic_chunks = build_semantic_chunks(
        granular_chunks=granular,
        section_windows=section_windows,
        dvd_title=dvd_title,
        volume_name=volume_name,
    )
    logger.info("  Semantic chunks: %d (from %d granular)", len(semantic_chunks), len(granular))

    if not semantic_chunks:
        logger.warning("  SKIP: no semantic chunks produced")
        return False

    if detect_only:
        # Store untagged chunks to Postgres only — no OpenAI needed.
        # Run with --tag-only later to complete tagging + embedding.
        store_semantic_chunks(engine, volume_id, semantic_chunks)
        logger.info("  Stored %d untagged semantic chunks (detect-only mode)", len(semantic_chunks))
        return True

    # --- 4. Tag with GPT-4o ---
    from app.services.tagger import tag_chunks

    tagged = tag_chunks(semantic_chunks)
    logger.info("  Tagged %d chunks", len(tagged))

    # --- 5. Embed ---
    from app.services.embedder import embed_chunks
    from app.services.vector_store import upsert_chunks

    for c in tagged:
        c["chunk_type"] = "semantic"

    embedded = embed_chunks(tagged)
    upsert_chunks(embedded)
    logger.info("  Embedded and upserted %d chunks to Qdrant", len(embedded))

    # --- 6. Store in Postgres ---
    store_semantic_chunks(engine, volume_id, embedded)

    logger.info("  Done: %s / %s -> %d semantic chunks", dvd_title, volume_name, len(embedded))
    return True


def _tag_untagged_semantic_chunks(engine, limit: int = 0):
    """Tag and embed semantic chunks that were stored without tags (--detect-only mode)."""
    from sqlalchemy import text

    query = """
        SELECT c.id::text, c.text, c.start_time, c.end_time,
               v.name AS volume, d.title AS dvd_title
        FROM chunks c
        JOIN volumes v ON v.id = c.volume_id
        JOIN dvds d ON d.id = v.dvd_id
        WHERE c.chunk_type = 'semantic'
          AND (c.technique IS NULL OR c.technique = '')
        ORDER BY d.title, v.name, c.start_time
    """
    if limit:
        query += f" LIMIT {limit}"

    with engine.connect() as conn:
        rows = conn.execute(text(query)).fetchall()

    logger.info("Untagged semantic chunks to process: %d", len(rows))

    if not rows:
        logger.info("Nothing to tag.")
        return

    from app.services.tagger import tag_single_chunk, _get_client as _get_tagger_client
    from app.services.embedder import embed_chunks
    from app.services.vector_store import upsert_chunks

    tagger_client = _get_tagger_client()
    ok = 0
    for i, row in enumerate(rows, 1):
        chunk_dict = {
            "id": row[0],
            "text": row[1],
            "start_time": row[2],
            "end_time": row[3],
            "volume": row[4],
            "dvd_title": row[5],
            "chunk_type": "semantic",
        }
        try:
            tagged = tag_single_chunk(tagger_client, chunk_dict)
            tagged["chunk_type"] = "semantic"
            embedded = embed_chunks([tagged])
            upsert_chunks(embedded)

            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE chunks SET
                        technique = :technique,
                        technique_type = :technique_type,
                        position = :position,
                        aliases = :aliases,
                        description = :description,
                        key_points = :key_points,
                        embedding_id = :embedding_id
                    WHERE id = :id
                """), {
                    "id": row[0],
                    "technique": tagged.get("technique"),
                    "technique_type": tagged.get("technique_type"),
                    "position": tagged.get("position"),
                    "aliases": tagged.get("aliases"),
                    "description": tagged.get("description"),
                    "key_points": tagged.get("key_points"),
                    "embedding_id": embedded[0].get("embedding_id") if embedded else None,
                })

            ok += 1
            if i % 10 == 0:
                logger.info("  Tagged %d/%d", i, len(rows))
        except Exception as e:
            logger.error("  FAILED chunk %s: %s", row[0][:8], e)

    logger.info("Tag-only done. Tagged: %d / %d", ok, len(rows))


def main():
    parser = argparse.ArgumentParser(description="Create semantic chunks for all volumes")
    parser.add_argument("--limit", type=int, default=0, help="Process only N volumes (0=all)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
    parser.add_argument("--detect-only", action="store_true",
                        help="Run music detection + merge only (no OpenAI). Store untagged chunks "
                             "to Postgres. Run without this flag later to tag+embed them.")
    parser.add_argument("--tag-only", action="store_true",
                        help="Tag and embed existing untagged semantic chunks (needs OpenAI). "
                             "Run this after --detect-only once OpenAI quota is restored.")
    parser.add_argument("--volume-id", type=str, default=None, help="Process a single volume by UUID")
    args = parser.parse_args()

    from sqlalchemy import create_engine
    from app.config import settings

    engine = create_engine(settings.database_url.replace("+asyncpg", ""))

    if args.tag_only:
        _tag_untagged_semantic_chunks(engine, limit=args.limit)
        engine.dispose()
        return

    volumes = get_volumes_needing_semantic(engine, volume_id=args.volume_id)
    logger.info("Volumes needing semantic chunks: %d", len(volumes))

    if args.limit:
        volumes = volumes[: args.limit]
        logger.info("Limited to first %d volumes", args.limit)

    ok = 0
    skip = 0
    fail = 0

    for i, vol in enumerate(volumes, 1):
        logger.info("[%d/%d] %s - %s", i, len(volumes), vol["dvd_title"], vol["volume_name"])
        try:
            success = process_volume(engine, vol, dry_run=args.dry_run, detect_only=args.detect_only)
            if success:
                ok += 1
            else:
                skip += 1
        except Exception as e:
            logger.exception("  FAILED: %s", e)
            fail += 1

    engine.dispose()
    logger.info("Done. Processed: %d  Skipped: %d  Failed: %d", ok, skip, fail)


if __name__ == "__main__":
    main()
