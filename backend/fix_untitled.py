"""
Fix untitled chunks in 3 steps:
  1. Delete junk chunks (<50 chars text) from Postgres + Qdrant
  2. Re-tag chunks where tagging previously failed
  3. Auto-title remaining untitled chunks from their description
  4. Re-embed all affected chunks

Resumable: tracks progress in data/fix_untitled_progress.json
"""
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

for _v in ("ALL_PROXY", "all_proxy", "HTTP_PROXY", "http_proxy",
           "HTTPS_PROXY", "https_proxy", "SOCKS_PROXY", "SOCKS5_PROXY"):
    os.environ.pop(_v, None)

sys.path.insert(0, str(Path(__file__).parent))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("fix_untitled")

from sqlalchemy import create_engine, text
from app.config import settings
from app.services.taxonomy import normalize_chunk

PROGRESS_FILE = Path(__file__).parent / "data" / "fix_untitled_progress.json"


def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"step1_done": False, "step2_done": False, "step2_tagged_ids": [],
            "step3_done": False, "step4_done": False, "step4_embedded_ids": []}


def _save_progress(prog: dict):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(prog, indent=2))


def step1_delete_junk(engine):
    """Delete chunks with text < 50 chars — pure noise."""
    logger.info("=" * 60)
    logger.info("STEP 1: Deleting junk chunks (<50 chars)")
    logger.info("=" * 60)

    with engine.connect() as c:
        junk_ids = [r[0] for r in c.execute(text("""
            SELECT id::text FROM chunks
            WHERE LENGTH(text) < 50
            AND (technique IS NULL OR technique = '' OR technique = 'no instructional content')
        """)).fetchall()]

    logger.info("Found %d junk chunks to delete", len(junk_ids))

    if not junk_ids:
        return

    # Delete from Qdrant
    try:
        from qdrant_client import QdrantClient
        qc = QdrantClient(url=settings.qdrant_url)
        BATCH = 100
        for i in range(0, len(junk_ids), BATCH):
            batch = junk_ids[i:i + BATCH]
            try:
                qc.delete(collection_name="bjj_chunks", points_selector=batch)
            except Exception:
                pass
        logger.info("Deleted %d points from Qdrant", len(junk_ids))
    except Exception as ex:
        logger.warning("Qdrant delete failed (non-fatal): %s", ex)

    # Delete from Postgres
    BATCH = 200
    for i in range(0, len(junk_ids), BATCH):
        batch = junk_ids[i:i + BATCH]
        placeholders = ", ".join(f"'{cid}'" for cid in batch)
        with engine.begin() as c:
            c.execute(text(f"DELETE FROM chunks WHERE id IN ({placeholders})"))
        logger.info("  Postgres: deleted %d/%d", min(i + BATCH, len(junk_ids)), len(junk_ids))

    logger.info("STEP 1 DONE: Deleted %d junk chunks", len(junk_ids))


def step2_retag_failed(engine):
    """Re-tag chunks where tagging previously failed."""
    logger.info("=" * 60)
    logger.info("STEP 2: Re-tagging failed chunks via Gemini")
    logger.info("=" * 60)

    prog = _load_progress()
    already_done = set(prog.get("step2_tagged_ids", []))

    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT c.id::text, c.text, c.technique_type, c.position,
                   c.start_time, c.end_time,
                   v.name as volume_name, d.title as dvd_title
            FROM chunks c
            JOIN volumes v ON c.volume_id = v.id
            JOIN dvds d ON v.dvd_id = d.id
            WHERE (c.technique IS NULL OR c.technique = '' OR c.technique = 'no instructional content')
            AND (c.description IS NULL OR c.description = '' OR c.description LIKE '%Tagging failed%' OR c.description LIKE '%No instructional%')
            AND LENGTH(c.text) >= 50
            ORDER BY c.id
        """)).fetchall()

    to_tag = [r for r in rows if r[0] not in already_done]
    logger.info("Total failed chunks: %d, already re-tagged: %d, remaining: %d",
                len(rows), len(already_done), len(to_tag))

    if not to_tag:
        return

    from app.services.tagger import tag_single_chunk, _get_client, QuotaExhaustedError, CONCURRENCY

    client = _get_client()
    tagged_count = 0
    quota_hit = False

    chunk_dicts = [
        {
            "id": r[0],
            "text": r[1],
            "technique_type": r[2] or "",
            "position": r[3] or "",
            "start_time": float(r[4]),
            "end_time": float(r[5]),
            "volume": r[6] or "",
            "dvd_title": r[7] or "",
        }
        for r in to_tag
    ]

    BATCH = 50
    for batch_start in range(0, len(chunk_dicts), BATCH):
        batch = chunk_dicts[batch_start:batch_start + BATCH]

        results = []
        try:
            with ThreadPoolExecutor(max_workers=min(CONCURRENCY, 8)) as pool:
                futures = {pool.submit(tag_single_chunk, client, c): c for c in batch}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except QuotaExhaustedError:
                        pool.shutdown(wait=False, cancel_futures=True)
                        raise
        except QuotaExhaustedError:
            logger.warning("QUOTA EXHAUSTED at %d/%d. Will resume next run.",
                           tagged_count, len(chunk_dicts))
            quota_hit = True
            break

        # Save to Postgres
        with engine.begin() as c:
            for ch in results:
                aliases = ch.get("aliases", [])
                if isinstance(aliases, str):
                    aliases = [a.strip() for a in aliases.split(",") if a.strip()]
                kp = ch.get("key_points", [])
                if isinstance(kp, str):
                    kp = [kp]
                c.execute(text("""
                    UPDATE chunks SET
                        technique = :technique,
                        technique_type = :technique_type,
                        position = :position,
                        aliases = :aliases,
                        description = :description,
                        key_points = :key_points
                    WHERE id = :id
                """), {
                    "id": ch["id"],
                    "technique": ch.get("technique", ""),
                    "technique_type": ch.get("technique_type", ""),
                    "position": ch.get("position", ""),
                    "aliases": aliases,
                    "description": ch.get("description", ""),
                    "key_points": kp,
                })

        new_ids = [ch["id"] for ch in results]
        already_done.update(new_ids)
        tagged_count += len(results)

        prog["step2_tagged_ids"] = list(already_done)
        _save_progress(prog)

        logger.info("  Re-tagged %d/%d (batch %d)",
                     tagged_count, len(chunk_dicts), batch_start // BATCH + 1)

    if quota_hit:
        logger.warning("Step 2 paused. Run again to resume.")
    else:
        logger.info("STEP 2 DONE: Re-tagged %d chunks", tagged_count)


def step3_auto_title(engine):
    """Auto-title remaining untitled chunks from their description."""
    logger.info("=" * 60)
    logger.info("STEP 3: Auto-titling from descriptions")
    logger.info("=" * 60)

    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT id::text, description, technique_type, text
            FROM chunks
            WHERE (technique IS NULL OR technique = '' OR technique = 'no instructional content')
            AND LENGTH(text) >= 50
        """)).fetchall()

    logger.info("Found %d untitled chunks to auto-title", len(rows))

    if not rows:
        return

    updates = []
    for r in rows:
        chunk_id, desc, ttype, txt = r[0], r[1] or "", r[2] or "concept", r[3] or ""

        title = ""
        if desc and "Tagging failed" not in desc and "No instructional" not in desc and len(desc) > 10:
            # Extract a short title from the description
            title = desc.strip()
            # Remove common prefixes
            for prefix in ["The coach ", "The instructor ", "This segment ", "A ", "An "]:
                if title.startswith(prefix):
                    title = title[len(prefix):]
                    break
            # Truncate to first sentence or 60 chars
            for sep in [". ", " - ", " -- ", "; "]:
                if sep in title:
                    title = title[:title.index(sep)]
                    break
            if len(title) > 60:
                title = title[:57] + "..."
            # Capitalize first letter
            if title:
                title = title[0].upper() + title[1:]
        else:
            # No usable description — extract from transcript
            clean = txt.strip()[:200]
            # Try to find a meaningful opening phrase
            clean = re.sub(r'\([^)]*\)', '', clean).strip()
            if len(clean) > 15:
                # Take first sentence
                for sep in [". ", "? ", "! "]:
                    if sep in clean:
                        clean = clean[:clean.index(sep)]
                        break
                if len(clean) > 60:
                    clean = clean[:57] + "..."
                title = clean[0].upper() + clean[1:] if clean else ""

        if not title:
            title = f"{ttype} segment" if ttype else "untitled segment"

        updates.append({"id": chunk_id, "technique": title})

    # Batch update
    BATCH = 500
    for i in range(0, len(updates), BATCH):
        batch = updates[i:i + BATCH]
        with engine.begin() as c:
            for u in batch:
                c.execute(text("""
                    UPDATE chunks SET technique = :technique WHERE id = :id
                """), u)
        logger.info("  Updated %d/%d", min(i + BATCH, len(updates)), len(updates))

    logger.info("STEP 3 DONE: Auto-titled %d chunks", len(updates))


def step4_reembed(engine):
    """Re-embed all chunks that were modified (re-tagged or auto-titled)."""
    logger.info("=" * 60)
    logger.info("STEP 4: Re-embedding affected chunks")
    logger.info("=" * 60)

    prog = _load_progress()
    already_embedded = set(prog.get("step4_embedded_ids", []))

    # Collect all affected IDs: anything that was retagged, auto-titled,
    # or still has a long/truncated technique name
    all_affected_ids = set()
    with engine.connect() as c:
        # Chunks with long technique names (auto-titled from description)
        rows = c.execute(text("""
            SELECT id::text FROM chunks
            WHERE technique IS NOT NULL AND technique != ''
            AND (LENGTH(technique) > 40 OR technique LIKE '%...'
                 OR technique = 'unidentified'
                 OR technique LIKE '%segment')
        """)).fetchall()
        for r in rows:
            all_affected_ids.add(r[0])

    # Also add any IDs from the step2 retag list
    retagged_ids = set(prog.get("step2_tagged_ids", []))
    all_affected_ids.update(retagged_ids)

    to_embed = list(all_affected_ids - already_embedded)
    logger.info("Chunks to re-embed: %d (already done: %d)", len(to_embed), len(already_embedded))

    if not to_embed:
        logger.info("STEP 4: Nothing to re-embed")
        return

    from app.services.embedder import embed_texts, build_search_text
    from app.services.vector_store import COLLECTION_NAME, _get_client as get_qdrant
    from qdrant_client.models import PointStruct

    qc = get_qdrant()
    BATCH = 50

    for batch_start in range(0, len(to_embed), BATCH):
        batch_ids = to_embed[batch_start:batch_start + BATCH]
        placeholders = ", ".join(f"'{cid}'" for cid in batch_ids)

        with engine.connect() as c:
            rows = c.execute(text(f"""
                SELECT c.id::text, c.technique, c.position, c.technique_type,
                       c.aliases, c.description, c.key_points, c.text,
                       c.start_time, c.end_time, c.chunk_type,
                       v.name as volume, d.title as dvd_title
                FROM chunks c
                JOIN volumes v ON c.volume_id = v.id
                JOIN dvds d ON v.dvd_id = d.id
                WHERE c.id IN ({placeholders})
            """)).fetchall()

        if not rows:
            continue

        chunks = [
            {
                "id": r[0], "technique": r[1] or "", "position": r[2] or "",
                "technique_type": r[3] or "", "aliases": r[4] or [],
                "description": r[5] or "", "key_points": r[6] or [],
                "text": r[7] or "", "start_time": float(r[8]),
                "end_time": float(r[9]), "chunk_type": r[10] or "granular",
                "volume": r[11] or "", "dvd_title": r[12] or "",
            }
            for r in rows
        ]

        texts = [build_search_text(c) for c in chunks]
        try:
            embeddings = embed_texts(texts)
        except Exception as e:
            if "quota" in str(e).lower() or "429" in str(e):
                logger.warning("QUOTA EXHAUSTED during embedding. Run again to resume.")
                break
            raise

        points = [
            PointStruct(
                id=ch["id"], vector=emb,
                payload={
                    "dvd_title": ch["dvd_title"], "volume": ch["volume"],
                    "position": ch["position"], "technique": ch["technique"],
                    "technique_type": ch["technique_type"],
                    "start_time": ch["start_time"], "end_time": ch["end_time"],
                    "chunk_type": ch["chunk_type"],
                },
            )
            for ch, emb in zip(chunks, embeddings)
        ]
        if points:
            qc.upsert(collection_name=COLLECTION_NAME, points=points)

        already_embedded.update(ch["id"] for ch in chunks)
        prog["step4_embedded_ids"] = list(already_embedded)
        _save_progress(prog)

        done = len(already_embedded)
        total = len(all_affected_ids)
        logger.info("  Embedded %d/%d (%.1f%%)", done, total, done / total * 100 if total else 0)

    logger.info("STEP 4 DONE")


def main():
    engine = create_engine(settings.database_url.replace("+asyncpg", ""))
    prog = _load_progress()

    if not prog.get("step1_done"):
        step1_delete_junk(engine)
        prog["step1_done"] = True
        _save_progress(prog)

    if not prog.get("step2_done"):
        step2_retag_failed(engine)
        # Check if all were tagged
        with engine.connect() as c:
            remaining = c.execute(text("""
                SELECT COUNT(*) FROM chunks
                WHERE (technique IS NULL OR technique = '' OR technique = 'no instructional content')
                AND (description IS NULL OR description = '' OR description LIKE '%Tagging failed%' OR description LIKE '%No instructional%')
                AND LENGTH(text) >= 50
            """)).scalar()
        if remaining == 0:
            prog["step2_done"] = True
            _save_progress(prog)
        else:
            logger.info("Step 2 incomplete: %d chunks still need re-tagging. Run again.", remaining)

    if not prog.get("step3_done"):
        step3_auto_title(engine)
        prog["step3_done"] = True
        _save_progress(prog)

    if not prog.get("step4_done"):
        step4_reembed(engine)
        prog["step4_done"] = True
        _save_progress(prog)

    # Final stats
    with engine.connect() as c:
        total = c.execute(text("SELECT COUNT(*) FROM chunks")).scalar()
        untitled = c.execute(text("SELECT COUNT(*) FROM chunks WHERE technique IS NULL OR technique = ''")).scalar()

    logger.info("=" * 60)
    logger.info("ALL DONE")
    logger.info("Total chunks: %d", total)
    logger.info("Still untitled: %d (%.1f%%)", untitled, untitled / total * 100 if total else 0)
    logger.info("=" * 60)

    # Clean up progress file
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()

    engine.dispose()


if __name__ == "__main__":
    main()
