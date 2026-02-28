"""
Re-embed all chunks already in Postgres using the current embedding model
(Gemini text-embedding-004) and upsert them into Qdrant.

Run this once after switching from OpenAI to Gemini embeddings:
    python reembed_all.py

The Qdrant collection will be recreated automatically if the dimension
changed (1536 → 768). Nothing in Postgres is modified.
"""
import logging
import os
import sys
import time
from pathlib import Path

for _v in ("ALL_PROXY", "all_proxy", "HTTP_PROXY", "http_proxy",
           "HTTPS_PROXY", "https_proxy", "SOCKS_PROXY", "SOCKS5_PROXY"):
    os.environ.pop(_v, None)

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("reembed_all")

from sqlalchemy import create_engine, text
from app.config import settings
from app.services.embedder import embed_texts, build_search_text, EMBEDDING_DIMENSIONS
from app.services.vector_store import ensure_collection, upsert_chunks, COLLECTION_NAME, _get_client
from qdrant_client.models import VectorParams, Distance

BATCH = 200  # chunks per embedding call (Gemini batch limit ~100, we chunk below)


def main():
    engine = create_engine(settings.database_url.replace("+asyncpg", ""))

    # Count total
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM chunks")).scalar()
    logger.info("Found %d chunks in Postgres to re-embed", total)

    if total == 0:
        logger.info("Nothing to do.")
        return

    # Ensure collection exists with correct dimensions (will recreate if mismatched)
    ensure_collection()

    offset = 0
    done = 0
    t0 = time.time()

    while True:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT
                    c.id::text,
                    c.technique,
                    c.position,
                    c.technique_type,
                    c.aliases,
                    c.description,
                    c.key_points,
                    c.text,
                    c.start_time,
                    c.end_time,
                    c.chunk_type,
                    v.name  AS volume,
                    d.title AS dvd_title
                FROM chunks c
                JOIN volumes v ON c.volume_id = v.id
                JOIN dvds d ON v.dvd_id = d.id
                ORDER BY c.id
                OFFSET :offset LIMIT :limit
            """), {"offset": offset, "limit": BATCH}).fetchall()

        if not rows:
            break

        chunks = [
            {
                "id":             r[0],
                "technique":      r[1] or "",
                "position":       r[2] or "",
                "technique_type": r[3] or "",
                "aliases":        r[4] or [],
                "description":    r[5] or "",
                "key_points":     r[6] or [],
                "text":           r[7] or "",
                "start_time":     float(r[8]),
                "end_time":       float(r[9]),
                "chunk_type":     r[10] or "granular",
                "volume":         r[11] or "",
                "dvd_title":      r[12] or "",
            }
            for r in rows
        ]

        texts = [build_search_text(c) for c in chunks]
        embeddings = embed_texts(texts)

        for chunk, emb in zip(chunks, embeddings):
            chunk["_embedding"] = emb
            chunk["embedding_id"] = chunk["id"]

        upsert_chunks(chunks)
        done += len(chunks)
        offset += len(chunks)

        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0
        remaining = total - done
        eta = remaining / rate if rate > 0 else 0
        logger.info(
            "Progress: %d/%d (%.0f%%) — %.1f chunks/s — ETA %.0fs",
            done, total, 100 * done / total, rate, eta,
        )

    logger.info("Done. Re-embedded %d chunks with %d-dim Gemini vectors.", done, EMBEDDING_DIMENSIONS)
    engine.dispose()


if __name__ == "__main__":
    main()
