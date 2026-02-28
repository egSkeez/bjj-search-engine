"""Real-time progress monitor for the ingestion pipeline.

Usage:
  python watch_progress.py                     # auto-find latest log
  python watch_progress.py logs/ingest2.log    # watch specific log file
"""

import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RED    = "\033[91m"
WHITE  = "\033[97m"
BLUE   = "\033[94m"

LOGS_DIR = Path(__file__).parent.parent / "logs"

LOG_PATTERNS = {
    "volume_start":    re.compile(r"\[(\d+)/(\d+)\]"),
    "volume_name":     re.compile(r"\[(\d+)/(\d+)\] (.+)"),
    "transcribing":    re.compile(r"Transcribing\.\.\."),
    "cached":          re.compile(r"Transcript cached"),
    "whisper_cpp_done":re.compile(r"whisper\.cpp transcription: (\d+) segments"),
    "chunked":         re.compile(r"Chunked into (\d+) chunks"),
    "tagging":         re.compile(r"Tagging with"),
    "tagged_progress": re.compile(r"Tagged (\d+)/(\d+) chunks"),
    "embedding":       re.compile(r"Embedding\.\.\."),
    "storing":         re.compile(r"Storing in database"),
    "done":            re.compile(r"Done: (\d+) chunks indexed"),
    "failed":          re.compile(r"FAILED:"),
    "gpu":             re.compile(r"Vulkan backend"),
    "all_done":        re.compile(r"ALL DONE\.\s*(\d+)/(\d+) volumes processed"),
}

STAGES      = ["transcribe", "chunk", "tag", "embed", "store", "done"]
STAGE_ORDER = {s: i for i, s in enumerate(STAGES)}
STAGE_LABEL = {
    "transcribe": "Transcribe",
    "chunk":      "Chunk",
    "tag":        "Tag",
    "embed":      "Embed",
    "store":      "Store",
    "done":       "Done",
}


def find_latest_log():
    if LOGS_DIR.exists():
        logs = sorted(LOGS_DIR.glob("ingest*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
        if logs:
            return logs[0]
    return None


def parse_log(content: str) -> dict:
    state = {
        "total": 0, "current_num": 0, "current_label": "",
        "stage": "", "chunks_in_vol": 0, "tagged": 0, "tag_total": 0,
        "completed": 0, "failed": [], "using_gpu": False,
        "started_at": None, "last_ts": None, "finished": False,
        "final_done": 0, "final_total": 0, "final_failed": 0,
    }

    for line in content.splitlines():
        ts = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
        if ts:
            try:
                dt = datetime.strptime(ts.group(1), "%Y-%m-%d %H:%M:%S")
                state["last_ts"] = dt
                if not state["started_at"]:
                    state["started_at"] = dt
            except Exception:
                pass

        m = LOG_PATTERNS["volume_name"].search(line)
        if m:
            state["current_num"] = int(m.group(1))
            state["total"]       = int(m.group(2))
            # label is everything after [N/T]
            rest = m.group(3).strip()
            state["current_label"] = rest[:80] + ("…" if len(rest) > 80 else "")
            state["stage"] = "transcribe"
            state["chunks_in_vol"] = 0
            state["tagged"] = 0
            state["tag_total"] = 0
            continue

        if LOG_PATTERNS["gpu"].search(line):         state["using_gpu"] = True
        if LOG_PATTERNS["cached"].search(line):      state["stage"] = "chunk"
        if LOG_PATTERNS["transcribing"].search(line):state["stage"] = "transcribe"
        if LOG_PATTERNS["whisper_cpp_done"].search(line): state["stage"] = "chunk"

        mc = LOG_PATTERNS["chunked"].search(line)
        if mc:
            state["chunks_in_vol"] = int(mc.group(1))
            state["stage"] = "tag"

        if LOG_PATTERNS["tagging"].search(line): state["stage"] = "tag"

        mt = LOG_PATTERNS["tagged_progress"].search(line)
        if mt:
            state["tagged"]    = int(mt.group(1))
            state["tag_total"] = int(mt.group(2))
            state["stage"] = "tag"

        if LOG_PATTERNS["embedding"].search(line): state["stage"] = "embed"
        if LOG_PATTERNS["storing"].search(line):   state["stage"] = "store"

        md = LOG_PATTERNS["done"].search(line)
        if md:
            state["stage"] = "done"
            state["completed"] += 1

        if LOG_PATTERNS["failed"].search(line):
            m2 = re.search(r"FAILED: (.+?): (.+)$", line)
            if m2:
                state["failed"].append(m2.group(1).strip()[:70])

        ma = LOG_PATTERNS["all_done"].search(line)
        if ma:
            state["finished"]     = True
            state["final_done"]   = int(ma.group(1))
            state["final_total"]  = int(ma.group(2))
            state["final_failed"] = len(state["failed"])

    return state


def bar(pct: float, width: int = 36, color: str = GREEN) -> str:
    filled = max(0, min(width, int(width * pct / 100)))
    return f"{color}{'█' * filled}{'░' * (width - filled)}{RESET} {WHITE}{pct:5.1f}%{RESET}"


def eta_str(started_at, current, total):
    if not started_at or current < 2:
        return "calculating…"
    secs = (datetime.now() - started_at).total_seconds()
    rate = current / secs
    rem  = (total - current) / rate
    h, r = divmod(int(rem), 3600)
    m, s = divmod(r, 60)
    return f"~{h}h {m:02d}m" if h else f"~{m}m {s:02d}s"


def elapsed_str(started_at):
    if not started_at:
        return "--:--:--"
    secs = int((datetime.now() - started_at).total_seconds())
    h, r = divmod(secs, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def render(state: dict, log_path: Path, refresh: int):
    now   = datetime.now().strftime("%H:%M:%S")
    total = state["total"] or 1
    cur   = state["current_num"]
    pct   = cur / total * 100

    os.system("cls")

    print(f"\n{BOLD}{WHITE}  ╔══════════════════════════════════════════════════════╗")
    print(f"  ║  BJJ Instructional — Ingestion Pipeline Monitor     ║")
    print(f"  ╚══════════════════════════════════════════════════════╝{RESET}")
    print(f"  {DIM}Updated: {now}   Log: {log_path.name}   Ctrl+C to quit{RESET}\n")

    # ── Overall bar ──────────────────────────────────────────────
    print(f"  {BOLD}Overall Progress{RESET}")
    print(f"  {bar(pct, 44)}")
    parts = [f"{WHITE}{cur}{RESET}/{total} volumes"]
    parts.append(f"{GREEN}{state['completed']} done{RESET}")
    if state["failed"]:
        parts.append(f"{RED}{len(state['failed'])} failed{RESET}")
    gpu_str = f"  •  {'GPU 🟢' if state['using_gpu'] else 'CPU'}"
    print(f"  {'  •  '.join(parts)}{gpu_str}")
    print(f"  Elapsed {CYAN}{elapsed_str(state['started_at'])}{RESET}"
          f"  •  ETA {YELLOW}{eta_str(state['started_at'], cur, total)}{RESET}")

    # ── Current volume ────────────────────────────────────────────
    print(f"\n  {BOLD}Current  [{cur}/{total}]{RESET}")
    label = state["current_label"] or "—"
    print(f"  {CYAN}{label}{RESET}")

    # ── Stage pipeline ────────────────────────────────────────────
    stage   = state["stage"]
    cur_idx = STAGE_ORDER.get(stage, -1)
    print(f"\n  ", end="")
    for s in STAGES:
        idx = STAGE_ORDER[s]
        if idx < cur_idx:
            col, sym = GREEN, "✓"
        elif idx == cur_idx:
            col, sym = YELLOW, "●"
        else:
            col, sym = DIM, "○"
        print(f"{col}{sym} {STAGE_LABEL[s]}{RESET}", end="  ")
    print()

    # Stage-specific detail
    if stage == "transcribe":
        backend = f"{GREEN}GPU (RX 9070 XT){RESET}" if state["using_gpu"] else f"{YELLOW}CPU{RESET}"
        print(f"\n  {YELLOW}▶ Transcribing…{RESET}   backend: {backend}")
    elif stage == "tag":
        if state["tag_total"] > 0:
            tp = state["tagged"] / state["tag_total"] * 100
            print(f"\n  {bar(tp, 36, YELLOW)}")
            print(f"  {state['tagged']}/{state['tag_total']} chunks tagged  •  {CYAN}Gemini 2.5 Flash (×10 parallel){RESET}")
        else:
            print(f"\n  {YELLOW}▶ Tagging…{RESET}")
    elif stage == "chunk":
        print(f"\n  {BLUE}▶ Chunking transcript…{RESET}")
    elif stage == "embed":
        print(f"\n  {BLUE}▶ Embedding & writing to Qdrant…{RESET}")
    elif stage == "store":
        print(f"\n  {BLUE}▶ Storing in PostgreSQL…{RESET}")
    elif stage == "done" and state["chunks_in_vol"]:
        print(f"\n  {GREEN}✓ {state['chunks_in_vol']} chunks indexed{RESET}")

    # ── Recent failures ───────────────────────────────────────────
    if state["failed"]:
        print(f"\n  {RED}{BOLD}Failed volumes:{RESET}")
        for f in state["failed"][-5:]:
            print(f"  {RED}✗ {DIM}{f}{RESET}")

    # ── Finished banner ───────────────────────────────────────────
    if state["finished"]:
        print(f"\n  {GREEN}{BOLD}{'═'*54}")
        print(f"  ALL DONE  —  {state['final_done']}/{state['final_total']} volumes ingested")
        if state["final_failed"]:
            print(f"  {state['final_failed']} volumes failed (see log for details)")
        print(f"{'═'*54}{RESET}")

    activity_ago = ""
    if state["last_ts"]:
        ago = int((datetime.now() - state["last_ts"]).total_seconds())
        activity_ago = f"last activity {ago}s ago"
    print(f"\n  {DIM}{activity_ago}   refreshing every {refresh}s{RESET}\n")


def main():
    os.system("color")  # Enable ANSI on Windows

    refresh = 3

    log_path = None
    if len(sys.argv) > 1:
        log_path = Path(sys.argv[1])
        if not log_path.is_absolute():
            log_path = Path(__file__).parent / log_path
    else:
        log_path = find_latest_log()

    if not log_path or not log_path.exists():
        print("No ingestion log found.")
        print(f"Looked in: {LOGS_DIR}")
        print("Is ingest_all.py running?  Or pass the log path as an argument.")
        sys.exit(1)

    print(f"Watching: {log_path}")
    time.sleep(0.5)

    try:
        while True:
            try:
                content = log_path.read_text(encoding="utf-8", errors="replace")
                state = parse_log(content)
                render(state, log_path, refresh)
                if state["finished"]:
                    break
            except Exception as ex:
                print(f"\nError: {ex}")
            time.sleep(refresh)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")


if __name__ == "__main__":
    main()
