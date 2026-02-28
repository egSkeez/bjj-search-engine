"""
Live ingestion progress from the database.
Run: python progress.py
Press Ctrl+C to stop.
"""
import os
for v in ("ALL_PROXY","all_proxy","HTTP_PROXY","http_proxy",
          "HTTPS_PROXY","https_proxy","SOCKS_PROXY","SOCKS5_PROXY"):
    os.environ.pop(v, None)

import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from sqlalchemy import create_engine, text
from app.config import settings

RESET  = "\033[0m"; BOLD  = "\033[1m"; DIM = "\033[2m"
GREEN  = "\033[92m"; YELLOW= "\033[93m"; CYAN= "\033[96m"
RED    = "\033[91m"; WHITE = "\033[97m"; BLUE= "\033[94m"

engine = create_engine(settings.database_url.replace("+asyncpg", ""), pool_pre_ping=True)

def query(sql, **kw):
    with engine.connect() as c:
        return c.execute(text(sql), kw).fetchall()

def scalar(sql, **kw):
    with engine.connect() as c:
        return c.execute(text(sql), kw).scalar()

def bar(pct: float, width: int = 40, color: str = GREEN) -> str:
    filled = max(0, min(width, int(width * pct / 100)))
    return f"{color}{'█'*filled}{'░'*(width-filled)}{RESET} {WHITE}{pct:5.1f}%{RESET}"

def fmt_eta(done, total, started_epoch):
    if done < 2 or not started_epoch:
        return "calculating…"
    elapsed = time.time() - started_epoch
    rate = done / elapsed
    rem  = (total - done) / rate
    h, r = divmod(int(rem), 3600)
    m, s = divmod(r, 60)
    return f"~{h}h {m:02d}m" if h else f"~{m}m {s:02d}s"

def fmt_elapsed(started_epoch):
    if not started_epoch:
        return "--:--:--"
    secs = int(time.time() - started_epoch)
    h, r = divmod(secs, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def gather():
    total_dvds  = scalar("SELECT COUNT(*) FROM dvds")
    total_vols  = scalar("SELECT COUNT(*) FROM volumes")
    total_chunks= scalar("SELECT COUNT(*) FROM chunks")
    gran        = scalar("SELECT COUNT(*) FROM chunks WHERE chunk_type='granular'")
    sem         = scalar("SELECT COUNT(*) FROM chunks WHERE chunk_type='semantic'")
    tagged      = scalar("""
        SELECT COUNT(*) FROM chunks
        WHERE chunk_type='granular'
          AND technique IS NOT NULL
          AND technique <> 'unidentified'
    """)
    embedded    = scalar("SELECT COUNT(*) FROM chunks WHERE embedding_id IS NOT NULL")

    # Volumes that have at least one granular chunk = ingested
    ingested_vols = scalar("""
        SELECT COUNT(DISTINCT volume_id) FROM chunks WHERE chunk_type='granular'
    """)

    # Last 5 recently updated volumes
    recent = query("""
        SELECT d.title, v.title, MAX(c.created_at) as last_chunk
        FROM chunks c
        JOIN volumes v ON v.id = c.volume_id
        JOIN dvds d ON d.id = v.dvd_id
        WHERE c.chunk_type='granular'
        GROUP BY d.title, v.title
        ORDER BY last_chunk DESC
        LIMIT 5
    """)

    # Jobs in progress
    active_jobs = query("""
        SELECT j.status, j.volume_name, j.dvd_title
        FROM ingest_jobs j
        WHERE j.status IN ('running','pending')
        ORDER BY j.updated_at DESC
        LIMIT 3
    """)

    # Failed jobs
    failed_jobs = query("""
        SELECT j.dvd_title, j.volume_name, j.error_message
        FROM ingest_jobs j
        WHERE j.status = 'failed'
        ORDER BY j.updated_at DESC
        LIMIT 5
    """)

    # Earliest chunk creation time as proxy for run start
    started_at_str = scalar("SELECT MIN(created_at) FROM chunks")

    return dict(
        total_dvds=total_dvds, total_vols=total_vols,
        total_chunks=total_chunks, gran=gran, sem=sem,
        tagged=tagged, embedded=embedded,
        ingested_vols=ingested_vols,
        recent=recent, active_jobs=active_jobs,
        failed_jobs=failed_jobs,
        started_at_str=started_at_str,
    )


_start_epoch = None

def render(d):
    global _start_epoch
    now = datetime.now().strftime("%H:%M:%S")

    # Determine run start epoch from earliest chunk
    if _start_epoch is None and d["started_at_str"]:
        try:
            if hasattr(d["started_at_str"], "timestamp"):
                _start_epoch = d["started_at_str"].timestamp()
            else:
                from datetime import datetime as dt
                _start_epoch = dt.fromisoformat(str(d["started_at_str"])).timestamp()
        except Exception:
            pass

    tv = d["total_vols"] or 1
    iv = d["ingested_vols"] or 0
    pct = iv / tv * 100

    gran       = d["gran"] or 0
    tagged     = d["tagged"] or 0
    tag_pct    = (tagged / gran * 100) if gran else 0
    embedded   = d["embedded"] or 0
    emb_pct    = (embedded / gran * 100) if gran else 0
    gran       = gran or 0
    embedded   = embedded or 0

    os.system("cls")
    print(f"\n{BOLD}{WHITE}  ╔══════════════════════════════════════════════════════╗")
    print(f"  ║   BJJ Pipeline — Live DB Progress                   ║")
    print(f"  ╚══════════════════════════════════════════════════════╝{RESET}")
    print(f"  {DIM}Updated: {now}   Ctrl+C to quit{RESET}\n")

    # Overall
    print(f"  {BOLD}Volumes ingested{RESET}")
    print(f"  {bar(pct, 44)}")
    print(f"  {WHITE}{iv}{RESET}/{d['total_vols']} volumes  "
          f"({d['total_dvds']} DVDs)   "
          f"Elapsed {CYAN}{fmt_elapsed(_start_epoch)}{RESET}  "
          f"ETA {YELLOW}{fmt_eta(iv, tv, _start_epoch)}{RESET}")

    # Chunks
    print(f"\n  {BOLD}Chunks{RESET}")
    print(f"  Total   {WHITE}{d['total_chunks']:>7,}{RESET}  "
          f"(Granular: {gran:,}  |  Semantic: {d['sem']:,})")

    # Tagging
    print(f"\n  {BOLD}Tagging  {DIM}(Gemini 2.5 Flash){RESET}")
    print(f"  {bar(tag_pct, 44, YELLOW)}")
    print(f"  {tagged:,} / {gran:,} granular chunks tagged")

    # Embeddings
    print(f"\n  {BOLD}Embeddings  {DIM}(Gemini embedding-001){RESET}")
    print(f"  {bar(emb_pct, 44, BLUE)}")
    print(f"  {embedded:,} / {gran:,} chunks embedded in Qdrant")

    # Active jobs
    if d["active_jobs"]:
        print(f"\n  {BOLD}Active jobs{RESET}")
        for status, vol, dvd in d["active_jobs"]:
            col = CYAN if status == "running" else YELLOW
            dvd_s = (dvd or "")[:35]
            vol_s = (vol or "")[:40]
            print(f"  {col}[{status}]{RESET}  {dvd_s} / {DIM}{vol_s}{RESET}")

    # Recent
    if d["recent"]:
        print(f"\n  {BOLD}Recently completed{RESET}")
        for dvd, vol, ts in d["recent"]:
            dvd_s = (dvd or "")[:35]
            vol_s = (vol or "")[:35]
            ts_s  = str(ts)[:19] if ts else ""
            print(f"  {GREEN}✓{RESET}  {dvd_s[:30]} / {DIM}{vol_s[:28]}{RESET}  {DIM}{ts_s}{RESET}")

    # Failures
    if d["failed_jobs"]:
        print(f"\n  {RED}{BOLD}Failed volumes:{RESET}")
        for dvd, vol, err in d["failed_jobs"]:
            dvd_s = (dvd or "")[:30]
            vol_s = (vol or "")[:30]
            err_s = (err or "")[:60]
            print(f"  {RED}✗{RESET}  {dvd_s} / {DIM}{vol_s}{RESET}")
            if err_s:
                print(f"     {DIM}{err_s}{RESET}")

    print(f"\n  {DIM}Refreshing every 10s…{RESET}\n")


def main():
    os.system("color")
    print("Connecting to database…")
    try:
        scalar("SELECT 1")
    except Exception as e:
        print(f"Cannot connect: {e}")
        sys.exit(1)

    try:
        while True:
            try:
                d = gather()
                render(d)
            except Exception as ex:
                print(f"\nError: {ex}")
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nStopped.")

if __name__ == "__main__":
    main()
