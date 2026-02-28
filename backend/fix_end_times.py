"""Fix chunk end_times to extend to the next chunk's start_time.

Updates both PostgreSQL and Qdrant payload so the video player covers
the full technique demonstration including any silent drill sections.

Usage: python fix_end_times.py
"""

import logging
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    from sqlalchemy import create_engine, text
    from app.config import settings

    engine = create_engine(settings.database_url.replace("+asyncpg", ""))

    # --- Step 1: Compute new end_times in Postgres ---
    with engine.connect() as conn:
        # Find all chunk pairs where we can extend
        rows = conn.execute(text("""
            SELECT
                c1.id::text        AS chunk_id,
                c1.end_time        AS old_end,
                c2.start_time      AS new_end,
                (c2.start_time - c1.end_time) AS gained_sec
            FROM chunks c1
            JOIN chunks c2 ON c2.volume_id = c1.volume_id
            WHERE c2.start_time = (
                SELECT MIN(c3.start_time) FROM chunks c3
                WHERE c3.volume_id = c1.volume_id
                  AND c3.start_time > c1.start_time
            )
            AND c2.start_time > c1.end_time
            ORDER BY gained_sec DESC
        """)).fetchall()

    if not rows:
        logger.info("No chunks need updating.")
        return

    total = len(rows)
    gained = sum(r[3] for r in rows)
    logger.info("Updating %d chunks (total %.0f seconds of demonstration recovered)", total, gained)
    logger.info("Largest extensions:")
    for r in rows[:5]:
        logger.info("  chunk %s: %.1fs -> %.1fs (+%.1fs)", r[0][:8], r[1], r[2], r[3])

    # --- Step 2: Bulk update Postgres ---
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE chunks c1
            SET end_time = c2.start_time
            FROM chunks c2
            WHERE c2.volume_id = c1.volume_id
              AND c2.start_time = (
                  SELECT MIN(c3.start_time) FROM chunks c3
                  WHERE c3.volume_id = c1.volume_id
                    AND c3.start_time > c1.start_time
              )
              AND c2.start_time > c1.end_time
        """))
    logger.info("Postgres updated.")

    # --- Step 3: Update Qdrant payloads ---
    try:
        from app.services.vector_store import _get_client, COLLECTION_NAME, ensure_collection
        from qdrant_client.models import SetPayload, PointIdsList

        qdrant = _get_client()
        ensure_collection()

        # Fetch updated end_times from Postgres for all affected chunks
        with engine.connect() as conn:
            updated = conn.execute(text("""
                SELECT id::text, end_time FROM chunks
                WHERE id::text = ANY(:ids)
            """), {"ids": [r[0] for r in rows]}).fetchall()

        logger.info("Updating %d Qdrant payloads...", len(updated))

        # Qdrant batch update - set payload field per point
        batch_size = 100
        for i in range(0, len(updated), batch_size):
            batch = updated[i:i + batch_size]
            for chunk_id, new_end in batch:
                qdrant.set_payload(
                    collection_name=COLLECTION_NAME,
                    payload={"end_time": new_end},
                    points=[chunk_id],
                )
            logger.info("  Qdrant: updated %d/%d", min(i + batch_size, len(updated)), len(updated))

        logger.info("Qdrant updated.")
    except Exception as e:
        logger.warning("Qdrant update failed (non-fatal): %s", e)

    engine.dispose()
    logger.info("Done. %d chunks extended, %.0fs of demonstration recovered.", total, gained)


if __name__ == "__main__":
    main()
