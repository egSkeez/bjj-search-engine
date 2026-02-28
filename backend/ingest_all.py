"""Bulk ingestion script - scans D:\\BJJ and indexes all BJJ instructional videos.

Folder structure handled:
  D:\\BJJ\\<Instructor>\\<DVD Title>\\*.mp4    → instructor from folder name
  D:\\BJJ\\<DVD Title>\\*.mp4                  → instructor extracted from DVD name

Usage:
  python ingest_all.py                   # ingest everything not yet in DB
  python ingest_all.py --dry-run         # print what would be ingested
  python ingest_all.py --dvd "Back Attacks"   # ingest only matching DVDs
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

# Clear any injected proxy vars before any network library is imported.
# Cursor IDE injects a local sandbox proxy (127.0.0.1:51367) that only works
# inside the Cursor sandbox process — spawned external processes must bypass it.
for _v in ("ALL_PROXY", "all_proxy", "HTTP_PROXY", "http_proxy",
           "HTTPS_PROXY", "https_proxy", "SOCKS_PROXY", "SOCKS5_PROXY"):
    os.environ.pop(_v, None)

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BJJ_ROOT = Path(r"D:\BJJ")
VIDEO_EXTS = {".mp4", ".mkv", ".avi"}

# Folders to skip — non-BJJ content
SKIP_KEYWORDS = [
    "kubernetes",
    "friday the 13th",
    "personal",
    "joe hippensteel",
    "ultimate human performance",
    "certif",
]

# Known instructor folder names → canonical display name
INSTRUCTOR_MAP = {
    "john danaher": "John Danaher",
    "gordon ryan": "Gordon Ryan",
    "gordon king ryan": "Gordon Ryan",
    "craig jones": "Craig Jones",
    "joseph chen": "Jozef Chen",
    "jozef chen": "Jozef Chen",
    "nicky ryan": "Nicky Ryan",
    "lachlan giles": "Lachlan Giles",
    "kade & tye ruotolo": "Kade & Tye Ruotolo",
    "kade ruotolo": "Kade & Tye Ruotolo",
    "tye ruotolo": "Kade & Tye Ruotolo",
    "ruotolo": "Kade & Tye Ruotolo",
    "garry tonon": "Garry Tonon",
}


def _should_skip(path: Path) -> bool:
    name_lower = path.name.lower()
    return any(kw in name_lower for kw in SKIP_KEYWORDS)


def _extract_instructor_from_name(name: str) -> str | None:
    """Try to extract instructor from 'DVD Title by Instructor Name'."""
    # Check known instructor names embedded in the title first (most reliable)
    name_lower = name.lower()
    for key, display in INSTRUCTOR_MAP.items():
        if key in name_lower:
            return display
    # Fall back to "by X Y" pattern
    m = re.search(r"\bby\s+([A-Z][a-zA-Z]+(?:\s+(?:&\s+)?[A-Z][a-zA-Z]+){0,4})", name)
    if m:
        candidate = m.group(1).strip()
        return INSTRUCTOR_MAP.get(candidate.lower(), candidate)
    return None


def _normalize_instructor(raw: str) -> str:
    return INSTRUCTOR_MAP.get(raw.lower().strip(), raw.strip().title())


def discover_volumes(root: Path) -> list[dict]:
    """Walk root and return list of {file_path, dvd_title, volume_name, instructor}."""
    volumes = []

    for level1 in sorted(root.iterdir()):
        if not level1.is_dir() or _should_skip(level1):
            continue

        # Collect MP4s directly in level1
        direct_files = sorted([f for f in level1.iterdir() if f.suffix.lower() in VIDEO_EXTS])

        # Collect subdirs of level1
        subdirs = sorted([d for d in level1.iterdir() if d.is_dir() and not _should_skip(d)])

        if direct_files and not subdirs:
            # D:\BJJ\<DVD>\*.mp4 — level1 is the DVD, no instructor subfolder
            dvd_title = level1.name
            instructor = _extract_instructor_from_name(dvd_title) or "Unknown"
            for f in direct_files:
                vol_name = f.stem
                volumes.append({
                    "file_path": str(f),
                    "dvd_title": dvd_title,
                    "volume_name": vol_name,
                    "instructor": instructor,
                })
        else:
            # Check if level1 is an instructor folder (subdirs are DVDs)
            level1_is_instructor = any(
                level1.name.lower() in INSTRUCTOR_MAP
                or level1.name.lower().startswith(("john ", "gordon ", "craig ", "nicky ", "lachlan ", "joseph "))
                for _ in [None]
            )

            if level1_is_instructor or subdirs:
                instructor = _normalize_instructor(level1.name)

                for level2 in subdirs:
                    if _should_skip(level2):
                        continue

                    # Check for video files directly in level2
                    vid_files = sorted([f for f in level2.iterdir() if f.suffix.lower() in VIDEO_EXTS])
                    level3_dirs = sorted([d for d in level2.iterdir() if d.is_dir() and not _should_skip(d)])

                    if vid_files:
                        dvd_title = level2.name
                        for f in vid_files:
                            vol_name = f.stem
                            volumes.append({
                                "file_path": str(f),
                                "dvd_title": dvd_title,
                                "volume_name": vol_name,
                                "instructor": instructor,
                            })
                    else:
                        # One more level: D:\BJJ\Instructor\DVD\Volume\*.mp4
                        for level3 in level3_dirs:
                            vid_files_3 = sorted([f for f in level3.iterdir() if f.suffix.lower() in VIDEO_EXTS])
                            if vid_files_3:
                                dvd_title = level2.name
                                for f in vid_files_3:
                                    volumes.append({
                                        "file_path": str(f),
                                        "dvd_title": dvd_title,
                                        "volume_name": f.stem,
                                        "instructor": instructor,
                                    })

            elif direct_files:
                # Files directly in level1 with no recognized instructor parent
                dvd_title = level1.name
                instructor = _extract_instructor_from_name(dvd_title) or "Unknown"
                for f in direct_files:
                    volumes.append({
                        "file_path": str(f),
                        "dvd_title": dvd_title,
                        "volume_name": f.stem,
                        "instructor": instructor,
                    })

    return volumes


def get_already_ingested() -> set[str]:
    """Return set of file_paths already in the database."""
    from sqlalchemy import create_engine, text
    from app.config import settings

    engine = create_engine(settings.database_url.replace("+asyncpg", ""))
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT file_path FROM volumes WHERE file_path IS NOT NULL")).fetchall()
    engine.dispose()
    return {r[0] for r in rows}


def ingest_volume(entry: dict, data_dir: Path):
    """Run the full pipeline for a single volume."""
    file_path = entry["file_path"]
    dvd_title = entry["dvd_title"]
    vol_name = entry["volume_name"]
    instructor = entry["instructor"]

    transcripts_dir = data_dir / "transcripts"
    chunks_dir = data_dir / "chunks"
    transcripts_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    safe_name = f"{dvd_title}_{vol_name}".replace(" ", "_").replace("/", "_").replace("\\", "_")
    transcript_path = transcripts_dir / f"{safe_name}.json"
    chunks_path = chunks_dir / f"{safe_name}.json"

    # --- Transcription ---
    if transcript_path.exists():
        logger.info("  Transcript cached, loading from %s", transcript_path.name)
        from app.services.transcription import load_transcription
        segments = load_transcription(str(transcript_path))
    else:
        logger.info("  Transcribing...")
        t0 = time.time()
        from app.services.transcription import transcribe_video
        segments = transcribe_video(str(file_path), output_path=str(transcript_path))
        logger.info("  Transcription: %.1f min, %d segments", (time.time() - t0) / 60, len(segments))

    # --- Chunking ---
    from app.services.chunker import chunk_segments
    chunks = chunk_segments(segments, dvd_title=dvd_title, volume_name=vol_name)
    logger.info("  Chunked into %d chunks", len(chunks))

    with open(chunks_path, "w", encoding="utf-8") as f:
        json.dump(chunks, f, indent=2)

    # --- Tagging ---
    logger.info("  Tagging with Gemini...")
    t0 = time.time()
    from app.services.tagger import tag_chunks
    chunks = tag_chunks(chunks)
    logger.info("  Tagging: %.1fs", time.time() - t0)

    with open(chunks_path, "w", encoding="utf-8") as f:
        json.dump(chunks, f, indent=2)

    # --- Embedding ---
    logger.info("  Embedding...")
    from app.services.embedder import embed_chunks
    from app.services.vector_store import upsert_chunks
    chunks = embed_chunks(chunks)
    upsert_chunks(chunks)

    with open(chunks_path, "w", encoding="utf-8") as f:
        json.dump(chunks, f, indent=2)

    # --- Store in PostgreSQL ---
    logger.info("  Storing in database...")
    _store_in_db(chunks, dvd_title, vol_name, instructor, file_path)

    logger.info("  Done: %d chunks indexed", len(chunks))


def _store_in_db(chunks: list[dict], dvd_title: str, vol_name: str, instructor: str, file_path: str):
    import uuid as uuid_mod
    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import Session
    from app.config import settings
    from app.models import DVD, Volume, Chunk

    engine = create_engine(settings.database_url.replace("+asyncpg", ""))
    with Session(engine) as session:
        dvd = session.execute(select(DVD).where(DVD.title == dvd_title)).scalar_one_or_none()
        if not dvd:
            dvd = DVD(title=dvd_title, instructor=instructor)
            session.add(dvd)
            session.flush()

        vol = Volume(dvd_id=dvd.id, name=vol_name, file_path=file_path)
        session.add(vol)
        session.flush()

        for cd in chunks:
            session.add(Chunk(
                id=uuid_mod.UUID(cd["id"]),
                volume_id=vol.id,
                start_time=float(cd["start_time"]),
                end_time=float(cd["end_time"]),
                text=cd["text"],
                position=cd.get("position"),
                technique=cd.get("technique"),
                technique_type=cd.get("technique_type"),
                aliases=cd.get("aliases"),
                description=cd.get("description"),
                key_points=cd.get("key_points"),
                chunk_type=cd.get("chunk_type", "granular"),
                embedding_id=cd.get("embedding_id"),
                llm_raw_response=cd.get("llm_raw_response"),
            ))

        session.commit()
    engine.dispose()


def main():
    parser = argparse.ArgumentParser(description="Bulk-ingest all BJJ instructionals from D:\\BJJ")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be ingested without doing it")
    parser.add_argument("--dvd", type=str, default=None, help="Only ingest DVDs whose title contains this string")
    parser.add_argument("--skip-existing", action="store_true", default=True, help="Skip already-ingested videos (default: on)")
    parser.add_argument("--force", action="store_true", help="Re-ingest even if already in DB")
    args = parser.parse_args()

    logger.info("Scanning %s...", BJJ_ROOT)
    volumes = discover_volumes(BJJ_ROOT)
    logger.info("Found %d video files across %d unique paths",
                len(volumes),
                len({v["dvd_title"] for v in volumes}))

    if args.dvd:
        volumes = [v for v in volumes if args.dvd.lower() in v["dvd_title"].lower()]
        logger.info("Filtered to %d volumes matching '%s'", len(volumes), args.dvd)

    if not args.force:
        already = get_already_ingested()
        pending = [v for v in volumes if v["file_path"] not in already]
        skipped = len(volumes) - len(pending)
        if skipped:
            logger.info("Skipping %d already-ingested volumes, %d remaining", skipped, len(pending))
        volumes = pending

    if not volumes:
        logger.info("Nothing to ingest.")
        return

    if args.dry_run:
        logger.info("=== DRY RUN - would ingest %d volumes ===", len(volumes))
        current_dvd = None
        for v in volumes:
            if v["dvd_title"] != current_dvd:
                current_dvd = v["dvd_title"]
                logger.info("  DVD: %s  [%s]", current_dvd, v["instructor"])
            logger.info("    Vol: %s", v["volume_name"].encode("ascii", "replace").decode())
            logger.info("         %s", v["file_path"].encode("ascii", "replace").decode())
        return

    from app.config import settings
    data_dir = Path(settings.data_dir)

    from app.services.tagger import QuotaExhaustedError

    total = len(volumes)
    failed = 0
    for i, entry in enumerate(volumes, 1):
        logger.info("=" * 60)
        logger.info("[%d/%d] %s — %s", i, total, entry["dvd_title"], entry["volume_name"])
        logger.info("       %s", entry["file_path"])
        logger.info("=" * 60)
        try:
            ingest_volume(entry, data_dir)
        except QuotaExhaustedError as e:
            logger.error("FAILED: %s — %s: %s", entry["dvd_title"], entry["volume_name"], e)
            logger.error("")
            logger.error("*** OPENAI QUOTA EXHAUSTED — stopping immediately ***")
            logger.error("Transcripts are cached on disk; no GPU work will be wasted.")
            logger.error("Top up credits at: https://platform.openai.com/account/billing")
            logger.error("Then re-run this script — it will skip already-ingested volumes")
            logger.error("and retry only the %d failed volume(s).", failed + 1)
            failed += 1
            break
        except Exception as e:
            logger.error("FAILED: %s — %s: %s", entry["dvd_title"], entry["volume_name"], e)
            logger.exception("Full traceback:")
            failed += 1
            continue

    done = i - failed
    logger.info("\nALL DONE. %d/%d volumes processed, %d failed.", done, total, failed)


if __name__ == "__main__":
    main()
