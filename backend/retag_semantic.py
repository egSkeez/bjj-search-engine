"""
Re-tag and re-embed ALL semantic chunks.
Fixes: empty tags, missing descriptions, missing embeddings.

Usage:
  python retag_semantic.py              # process all
  python retag_semantic.py --limit 20   # test on first 20
  python retag_semantic.py --dry-run    # show what would be done
"""
import os
import sys
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

for v in ("ALL_PROXY","all_proxy","HTTP_PROXY","http_proxy",
          "HTTPS_PROXY","https_proxy","SOCKS_PROXY","SOCKS5_PROXY"):
    os.environ.pop(v, None)

import argparse
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("retag_semantic.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

from sqlalchemy import create_engine, text
from app.config import settings
from app.services.tagger import (
    tag_single_chunk, _get_client as get_tagger_client,
    QuotaExhaustedError, CONCURRENCY,
)
from app.services.embedder import embed_chunks
from app.services.vector_store import upsert_chunks


def is_music_only(txt: str) -> bool:
    cleaned = re.sub(r"\(upbeat music\)", "", txt, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\(music\)", "", cleaned, flags=re.IGNORECASE).strip()
    return len(cleaned) < 30


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    engine = create_engine(settings.database_url.replace("+asyncpg", ""))

    query = """
        SELECT c.id::text, c.start_time, c.end_time, c.text,
               c.technique, c.position, c.description,
               v.name AS volume_name, d.title AS dvd_title,
               c.volume_id::text
        FROM chunks c
        JOIN volumes v ON v.id = c.volume_id
        JOIN dvds d ON d.id = v.dvd_id
        WHERE c.chunk_type = 'semantic'
        ORDER BY d.title, v.name, c.start_time
    """
    if args.limit:
        query += f" LIMIT {args.limit}"

    with engine.connect() as conn:
        rows = conn.execute(text(query)).fetchall()

    logger.info("Total semantic chunks: %d", len(rows))

    # Separate music-only chunks
    music_only_ids = []
    to_process = []
    for r in rows:
        if is_music_only(r.text or ""):
            music_only_ids.append(r.id)
        else:
            to_process.append(r)

    logger.info("Music-only (will delete): %d", len(music_only_ids))
    logger.info("To tag + embed: %d", len(to_process))

    if args.dry_run:
        logger.info("DRY RUN - no changes made.")
        for r in to_process[:5]:
            logger.info("  Would tag: %s / %s [%s]", r.dvd_title, r.volume_name, r.id[:8])
        engine.dispose()
        return

    # Delete music-only chunks from DB and Qdrant
    if music_only_ids:
        logger.info("Deleting %d music-only chunks...", len(music_only_ids))
        with engine.begin() as conn:
            for cid in music_only_ids:
                conn.execute(text("DELETE FROM chunks WHERE id = :id"), {"id": cid})
        from qdrant_client.models import PointIdsList
        from app.services.vector_store import _get_client as get_qdrant, COLLECTION_NAME
        try:
            qc = get_qdrant()
            qc.delete(collection_name=COLLECTION_NAME,
                       points_selector=PointIdsList(points=music_only_ids))
        except Exception as e:
            logger.warning("Qdrant delete failed (non-fatal): %s", e)
        logger.info("Deleted %d music-only chunks.", len(music_only_ids))

    # Tag all remaining semantic chunks with Gemini
    client = get_tagger_client()
    tagged_count = 0
    failed_count = 0
    batch_size = 20

    for batch_start in range(0, len(to_process), batch_size):
        batch = to_process[batch_start:batch_start + batch_size]
        chunk_dicts = []
        for r in batch:
            chunk_dicts.append({
                "id": r.id,
                "text": r.text,
                "start_time": r.start_time,
                "end_time": r.end_time,
                "volume": r.volume_name,
                "dvd_title": r.dvd_title,
                "chunk_type": "semantic",
            })

        # Parallel tagging
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            futures = {pool.submit(tag_single_chunk, client, c): c for c in chunk_dicts}
            for future in as_completed(futures):
                try:
                    future.result()
                except QuotaExhaustedError:
                    logger.error("QUOTA EXHAUSTED - stopping.")
                    pool.shutdown(wait=False, cancel_futures=True)
                    engine.dispose()
                    sys.exit(1)
                except Exception as e:
                    logger.error("Tag error: %s", e)

        # Embed the batch
        try:
            embedded = embed_chunks(chunk_dicts)
            upsert_chunks(embedded)
        except QuotaExhaustedError:
            logger.error("EMBEDDING QUOTA EXHAUSTED - stopping.")
            engine.dispose()
            sys.exit(1)
        except Exception as e:
            logger.error("Embed/upsert error: %s", e)

        # Update Postgres
        with engine.begin() as conn:
            for c in chunk_dicts:
                try:
                    aliases_val = c.get("aliases")
                    if isinstance(aliases_val, list):
                        aliases_val = aliases_val if aliases_val else None
                    else:
                        aliases_val = None

                    key_points_val = c.get("key_points")
                    if isinstance(key_points_val, list):
                        key_points_val = key_points_val if key_points_val else None
                    else:
                        key_points_val = None

                    conn.execute(text("""
                        UPDATE chunks SET
                            technique = :technique,
                            technique_type = :technique_type,
                            position = :position,
                            aliases = :aliases,
                            description = :description,
                            key_points = :key_points,
                            embedding_id = :embedding_id,
                            llm_raw_response = :llm_raw
                        WHERE id = :id
                    """), {
                        "id": c["id"],
                        "technique": c.get("technique") or None,
                        "technique_type": c.get("technique_type") or None,
                        "position": c.get("position") or None,
                        "aliases": aliases_val,
                        "description": c.get("description") or None,
                        "key_points": key_points_val,
                        "embedding_id": c.get("embedding_id") or None,
                        "llm_raw": json.dumps(c.get("llm_raw_response")) if c.get("llm_raw_response") else None,
                    })
                    tagged_count += 1
                except Exception as e:
                    logger.error("DB update failed for %s: %s", c["id"][:8], e)
                    failed_count += 1

        logger.info(
            "Progress: %d/%d tagged, %d failed",
            tagged_count, len(to_process), failed_count,
        )

    engine.dispose()
    logger.info("DONE. Tagged: %d  Failed: %d  Music-only deleted: %d",
                tagged_count, failed_count, len(music_only_ids))


if __name__ == "__main__":
    main()
