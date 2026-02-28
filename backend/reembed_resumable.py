"""
Resumable re-embedding of all chunks with the new search text formula.

Tracks progress in a local JSON file so it can resume after quota exhaustion.
Run it, let it go until Gemini quota runs out, then run it again tomorrow —
it picks up exactly where it left off.

Usage:
    python reembed_resumable.py           # start or resume
    python reembed_resumable.py --reset   # start fresh (deletes progress)
    python reembed_resumable.py --status  # show progress without running
"""
import json
import logging
import os
import sys
import time
from pathlib import Path

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

for _v in ("ALL_PROXY", "all_proxy", "HTTP_PROXY", "http_proxy",
           "HTTPS_PROXY", "https_proxy", "SOCKS_PROXY", "SOCKS5_PROXY"):
    os.environ.pop(_v, None)

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("reembed")

from sqlalchemy import create_engine, text
from app.config import settings
from app.services.embedder import embed_texts, build_search_text, EMBEDDING_DIMENSIONS
from app.services.vector_store import ensure_collection, COLLECTION_NAME, _get_client
from qdrant_client.models import PointStruct

BATCH = 50
PROGRESS_FILE = Path(__file__).parent / "data" / "reembed_progress.json"


def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"done_ids": [], "done_count": 0, "total": 0, "started_at": None, "last_run": None}


def _save_progress(prog: dict):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(prog, indent=2))


def _show_status():
    prog = _load_progress()
    if not prog.get("total"):
        print("No re-embedding in progress. Run without --status to start.")
        return
    done = prog["done_count"]
    total = prog["total"]
    pct = done / total * 100 if total else 0
    print(f"Re-embedding progress: {done}/{total} ({pct:.1f}%)")
    print(f"Started: {prog.get('started_at', 'unknown')}")
    print(f"Last run: {prog.get('last_run', 'unknown')}")
    remaining = total - done
    print(f"Remaining: {remaining} chunks")
    if done > 0:
        print(f"Completed chunk IDs tracked: {len(prog['done_ids'])}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Start fresh, delete progress")
    parser.add_argument("--status", action="store_true", help="Show progress and exit")
    args = parser.parse_args()

    if args.status:
        _show_status()
        return

    if args.reset and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        logger.info("Progress reset. Starting fresh.")

    engine = create_engine(settings.database_url.replace("+asyncpg", ""))

    # Get all chunk IDs ordered consistently
    with engine.connect() as conn:
        all_ids = [
            r[0] for r in conn.execute(
                text("SELECT id::text FROM chunks ORDER BY id")
            ).fetchall()
        ]

    total = len(all_ids)
    logger.info("Total chunks in DB: %d", total)

    if total == 0:
        logger.info("Nothing to do.")
        return

    # Load progress
    prog = _load_progress()
    done_set = set(prog.get("done_ids", []))

    # Filter out already-done IDs
    remaining_ids = [cid for cid in all_ids if cid not in done_set]
    already_done = total - len(remaining_ids)

    if not prog.get("started_at"):
        prog["started_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    prog["total"] = total
    prog["done_count"] = already_done
    _save_progress(prog)

    logger.info(
        "Already done: %d/%d (%.1f%%). Remaining: %d",
        already_done, total, already_done / total * 100, len(remaining_ids),
    )

    if not remaining_ids:
        logger.info("All chunks already re-embedded!")
        return

    ensure_collection()
    qc = _get_client()

    session_done = 0
    session_start = time.time()
    quota_hit = False

    for batch_start in range(0, len(remaining_ids), BATCH):
        batch_ids = remaining_ids[batch_start: batch_start + BATCH]

        # Fetch chunk data from Postgres
        placeholders = ", ".join(f"'{cid}'" for cid in batch_ids)
        with engine.connect() as conn:
            rows = conn.execute(text(f"""
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
                WHERE c.id IN ({placeholders})
            """)).fetchall()

        if not rows:
            continue

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

        # Embed — this is where quota can be exhausted
        try:
            embeddings = embed_texts(texts)
        except Exception as e:
            err_str = str(e)
            if "quota" in err_str.lower() or "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                logger.warning("=" * 60)
                logger.warning("QUOTA EXHAUSTED — saving progress and stopping.")
                logger.warning("Run this script again tomorrow to resume.")
                logger.warning("=" * 60)
                quota_hit = True
                break
            raise

        # Upsert to Qdrant
        points = []
        for chunk, emb in zip(chunks, embeddings):
            points.append(PointStruct(
                id=chunk["id"],
                vector=emb,
                payload={
                    "dvd_title": chunk.get("dvd_title", ""),
                    "volume": chunk.get("volume", ""),
                    "position": chunk.get("position", ""),
                    "technique": chunk.get("technique", ""),
                    "technique_type": chunk.get("technique_type", ""),
                    "start_time": chunk.get("start_time", 0),
                    "end_time": chunk.get("end_time", 0),
                    "chunk_type": chunk.get("chunk_type", "granular"),
                },
            ))

        if points:
            qc.upsert(collection_name=COLLECTION_NAME, points=points)

        # Track progress
        batch_done_ids = [c["id"] for c in chunks]
        done_set.update(batch_done_ids)
        session_done += len(chunks)
        total_done = already_done + session_done

        prog["done_ids"] = list(done_set)
        prog["done_count"] = total_done
        prog["last_run"] = time.strftime("%Y-%m-%d %H:%M:%S")
        _save_progress(prog)

        elapsed = time.time() - session_start
        rate = session_done / elapsed if elapsed > 0 else 0
        remaining_count = total - total_done
        eta = remaining_count / rate if rate > 0 else 0
        eta_min = eta / 60

        logger.info(
            "Progress: %d/%d (%.1f%%) | this session: %d | %.1f chunks/s | ETA: %.0fm",
            total_done, total, total_done / total * 100,
            session_done, rate, eta_min,
        )

    # Final summary
    total_done = already_done + session_done
    logger.info("=" * 60)
    if quota_hit:
        logger.info(
            "PAUSED at %d/%d (%.1f%%). Run again to resume.",
            total_done, total, total_done / total * 100,
        )
    elif total_done >= total:
        logger.info("COMPLETE! All %d chunks re-embedded.", total)
        # Clean up progress file
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
            logger.info("Progress file cleaned up.")
    else:
        logger.info(
            "Session done: %d/%d (%.1f%%).",
            total_done, total, total_done / total * 100,
        )
    logger.info("=" * 60)

    engine.dispose()


if __name__ == "__main__":
    main()
