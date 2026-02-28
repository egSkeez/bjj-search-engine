"""Re-tag existing chunks to populate the key_points field.

Fetches all chunks from PostgreSQL that have no key_points, sends them to
GPT-4o with the updated prompt, updates both PostgreSQL and Qdrant.

Usage:
  python retag_key_points.py              # re-tag all chunks without key_points
  python retag_key_points.py --all        # re-tag ALL chunks (overwrite existing)
  python retag_key_points.py --limit 20   # test with first 20
"""

import argparse
import json
import logging
import time

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Re-tag all chunks, not just those missing key_points")
    parser.add_argument("--limit", type=int, default=None, help="Only process first N chunks (for testing)")
    args = parser.parse_args()

    from sqlalchemy import create_engine, select, update
    from sqlalchemy.orm import Session
    from app.config import settings
    from app.models import Chunk, Volume
    from app.services.tagger import tag_single_chunk, _get_client
    from app.services.embedder import build_search_text, embed_texts
    from app.services.vector_store import _get_client as qdrant_client, COLLECTION_NAME, ensure_collection
    from qdrant_client.models import PointStruct

    engine = create_engine(settings.database_url.replace("+asyncpg", ""))

    with Session(engine) as session:
        stmt = (
            select(Chunk, Volume.name.label("volume_name"))
            .join(Volume, Chunk.volume_id == Volume.id)
        )
        if not args.all:
            stmt = stmt.where(
                (Chunk.key_points == None) |  # noqa: E711
                (Chunk.key_points == [])
            )
        rows = session.execute(stmt).all()

    chunks_with_vol = [(row.Chunk, row.volume_name) for row in rows]

    if args.limit:
        chunks_with_vol = chunks_with_vol[:args.limit]

    total = len(chunks_with_vol)
    logger.info("Chunks to re-tag: %d", total)

    if total == 0:
        logger.info("Nothing to do.")
        return

    oai_client = _get_client()
    qdrant = qdrant_client()
    ensure_collection()

    updated = 0
    failed = 0

    with Session(engine) as session:
        for i, (chunk, vol_name) in enumerate(chunks_with_vol, 1):
            chunk_dict = {
                "id": str(chunk.id),
                "dvd_title": "",  # not needed for re-tagging
                "volume": vol_name,
                "start_time": chunk.start_time,
                "end_time": chunk.end_time,
                "text": chunk.text,
                "technique": chunk.technique or "",
                "position": chunk.position or "",
                "technique_type": chunk.technique_type or "",
                "aliases": chunk.aliases or [],
                "description": chunk.description or "",
                "key_points": [],
            }

            try:
                tagged = tag_single_chunk(oai_client, chunk_dict)

                new_key_points = tagged.get("key_points") or []
                new_description = tagged.get("description") or chunk.description or ""

                # Update PostgreSQL
                session.execute(
                    update(Chunk)
                    .where(Chunk.id == chunk.id)
                    .values(
                        key_points=new_key_points,
                        description=new_description,
                        llm_raw_response=tagged.get("llm_raw_response"),
                    )
                )

                # Re-embed with new key_points included in search text
                search_text = build_search_text({
                    **chunk_dict,
                    "key_points": new_key_points,
                    "description": new_description,
                })
                new_embedding = embed_texts([search_text])[0]

                # Upsert into Qdrant
                qdrant.upsert(
                    collection_name=COLLECTION_NAME,
                    points=[PointStruct(
                        id=str(chunk.id),
                        vector=new_embedding,
                        payload={
                            "technique": chunk.technique,
                            "position": chunk.position,
                            "technique_type": chunk.technique_type,
                            "aliases": chunk.aliases or [],
                            "description": new_description,
                            "key_points": new_key_points,
                            "start_time": chunk.start_time,
                            "end_time": chunk.end_time,
                            "volume_id": str(chunk.volume_id),
                            "volume_name": vol_name,
                        },
                    )],
                )

                updated += 1
                logger.info("[%d/%d] Updated: %s — %s (%d key points)",
                            i, total, chunk.technique or "unidentified", vol_name, len(new_key_points))

                # Commit every 10 chunks
                if i % 10 == 0:
                    session.commit()
                    logger.info("Committed batch at %d/%d", i, total)

                # Rate limiting
                if i % 10 == 0 and i < total:
                    time.sleep(1.0)

            except Exception as e:
                logger.error("[%d/%d] FAILED chunk %s: %s", i, total, chunk.id, e)
                failed += 1
                continue

        session.commit()

    engine.dispose()
    logger.info("Done. Updated: %d, Failed: %d out of %d total.", updated, failed, total)


if __name__ == "__main__":
    main()
