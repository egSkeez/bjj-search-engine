"""
Normalize all existing chunks to the canonical taxonomy.
Updates technique_type, position, and technique in Postgres + Qdrant payload.

Usage: python normalize_all.py
       python normalize_all.py --dry-run
"""
import os, sys
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
for v in ("ALL_PROXY","all_proxy","HTTP_PROXY","http_proxy",
          "HTTPS_PROXY","https_proxy","SOCKS_PROXY","SOCKS5_PROXY"):
    os.environ.pop(v, None)

import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

from sqlalchemy import create_engine, text
from app.config import settings
from app.services.taxonomy import normalize_category, normalize_position, normalize_technique


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    engine = create_engine(settings.database_url.replace("+asyncpg", ""))

    # Fetch all chunks
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT id::text, technique_type, position, technique
            FROM chunks
            ORDER BY created_at
        """)).fetchall()

    logger.info("Total chunks to normalize: %d", len(rows))

    changes = 0
    updates = []

    for r in rows:
        old_type = r.technique_type or ""
        old_pos  = r.position or ""
        old_tech = r.technique or ""

        new_type = normalize_category(old_type)
        new_pos  = normalize_position(old_pos)
        new_tech = normalize_technique(old_tech)

        if new_type != old_type or new_pos != old_pos or new_tech != old_tech:
            changes += 1
            updates.append({
                "id": r.id,
                "technique_type": new_type,
                "position": new_pos,
                "technique": new_tech,
            })

    logger.info("Chunks that will change: %d / %d (%.1f%%)",
                changes, len(rows), changes / len(rows) * 100 if rows else 0)

    if args.dry_run:
        # Show sample changes
        for u in updates[:20]:
            orig = next(r for r in rows if r.id == u["id"])
            print(f"  {u['id'][:8]}:")
            if u["technique_type"] != (orig.technique_type or ""):
                print(f"    type:  [{orig.technique_type}] -> [{u['technique_type']}]")
            if u["position"] != (orig.position or ""):
                print(f"    pos:   [{orig.position}] -> [{u['position']}]")
            if u["technique"] != (orig.technique or ""):
                print(f"    tech:  [{orig.technique}] -> [{u['technique']}]")
        logger.info("DRY RUN — no changes written.")
        engine.dispose()
        return

    # Batch update Postgres
    batch_size = 500
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        with engine.begin() as c:
            for u in batch:
                c.execute(text("""
                    UPDATE chunks SET
                        technique_type = :technique_type,
                        position = :position,
                        technique = :technique
                    WHERE id = :id
                """), u)
        logger.info("  Postgres: updated %d/%d", min(i + batch_size, len(updates)), len(updates))

    # Update Qdrant payloads
    logger.info("Updating Qdrant payloads...")
    try:
        from qdrant_client import QdrantClient
        qc = QdrantClient(url=settings.qdrant_url)
        COLLECTION = "bjj_chunks"

        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            for u in batch:
                try:
                    qc.set_payload(
                        collection_name=COLLECTION,
                        payload={
                            "technique": u["technique"],
                            "technique_type": u["technique_type"],
                            "position": u["position"],
                        },
                        points=[u["id"]],
                    )
                except Exception:
                    pass  # point may not exist in Qdrant
            logger.info("  Qdrant: updated %d/%d", min(i + batch_size, len(updates)), len(updates))
    except Exception as ex:
        logger.warning("Qdrant update failed (non-fatal): %s", ex)

    engine.dispose()
    logger.info("DONE. Normalized %d chunks.", changes)


if __name__ == "__main__":
    main()
