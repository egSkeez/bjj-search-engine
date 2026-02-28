"""
Test LLM-based technique boundary detection on one volume.
Shows detected sections with full metadata.

Usage: python test_llm_boundaries.py
"""
import os
import sys
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
for v in ("ALL_PROXY","all_proxy","HTTP_PROXY","http_proxy",
          "HTTPS_PROXY","https_proxy","SOCKS_PROXY","SOCKS5_PROXY"):
    os.environ.pop(v, None)

import json
import logging
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

from sqlalchemy import create_engine, text
from app.config import settings
from app.services.llm_boundary_detector import detect_technique_boundaries

DVD_TITLE = "Back Attacks - Enter The System"
DVD_ID = "8597297a-76f5-46be-a9a8-570c55b60310"

SECTION_TYPE_ICONS = {
    "demonstration": "D",
    "theory": "T",
    "drilling": "R",
    "rolling_footage": "F",
    "intro_outro": "I",
}

def fmt_time(secs: float) -> str:
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def main():
    engine = create_engine(settings.database_url.replace("+asyncpg", ""))

    # Get volumes for this DVD
    with engine.connect() as c:
        vols = c.execute(text("""
            SELECT v.id::text, v.name, v.file_path
            FROM volumes v
            WHERE v.dvd_id = :did
            ORDER BY v.name
        """), {"did": DVD_ID}).fetchall()

    # Process first 2 volumes as test
    test_vols = vols[:2]

    for vol in test_vols:
        vol_id = vol.id
        vol_name = vol.name

        print(f"\n{'='*70}")
        print(f"  {DVD_TITLE}")
        print(f"  {vol_name}")
        print(f"{'='*70}\n")

        # Get granular chunks
        with engine.connect() as c:
            rows = c.execute(text("""
                SELECT c.id::text, c.start_time, c.end_time, c.text
                FROM chunks c
                WHERE c.volume_id = :vid AND c.chunk_type = 'granular'
                ORDER BY c.start_time
            """), {"vid": vol_id}).fetchall()

        granular = [dict(r._mapping) for r in rows]
        print(f"  Granular chunks: {len(granular)}")
        print(f"  Duration: {fmt_time(granular[-1]['end_time'])}")

        # Skip music detection for speed — LLM will handle boundaries from transcript
        music_segs = []

        # Run LLM boundary detection
        print(f"\n  Sending transcript to Gemini...")
        sections = detect_technique_boundaries(
            granular_chunks=granular,
            dvd_title=DVD_TITLE,
            volume_name=vol_name,
            music_segments=music_segs,
        )

        print(f"\n  Detected {len(sections)} technique sections:\n")

        for i, sec in enumerate(sections, 1):
            stype = sec.get("section_type", "?")
            icon = SECTION_TYPE_ICONS.get(stype, "?")
            duration = sec["end_time"] - sec["start_time"]

            print(f"  [{icon}] Section {i}: {fmt_time(sec['start_time'])} -> {fmt_time(sec['end_time'])} ({int(duration)}s)")
            print(f"      Technique:  {sec.get('technique', '?')}")
            print(f"      Position:   {sec.get('position', '?')}")
            print(f"      Type:       {sec.get('technique_type', '?')} ({stype})")
            desc = sec.get("description", "")[:120]
            print(f"      Description:{desc}")

            kp = sec.get("key_points", [])
            if kp:
                print(f"      Key Points:")
                for j, pt in enumerate(kp[:4], 1):
                    print(f"        {j}. {str(pt)[:110]}")

            aliases = sec.get("aliases", [])
            if aliases:
                print(f"      Aliases:    {', '.join(str(a) for a in aliases[:6])}")
            print()

        # Compare with existing semantic chunks
        with engine.connect() as c:
            existing = c.execute(text("""
                SELECT c.start_time, c.end_time, c.technique
                FROM chunks c
                WHERE c.volume_id = :vid AND c.chunk_type = 'semantic'
                ORDER BY c.start_time
            """), {"vid": vol_id}).fetchall()

        if existing:
            print(f"\n  --- Existing semantic chunks (music-based) for comparison ---")
            for ex in existing:
                print(f"    {fmt_time(ex.start_time)} -> {fmt_time(ex.end_time)} | {ex.technique or '(untagged)'}")

        print(f"\n  {'='*70}\n")

    engine.dispose()
    print("Done. Review the sections above.")
    print("NOTE: No changes were saved to the database.")


if __name__ == "__main__":
    main()
