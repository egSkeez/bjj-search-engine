"""Semantic chunker — builds technique-level chunks from music boundaries.

Takes the already-stored granular chunks for a volume and merges them into
larger units based on where music plays (the production's natural chapter
markers). Each resulting chunk covers one complete technique section.

If a section has no music breaks and runs longer than MAX_SECTION_DURATION,
it is sub-split at granular chunk boundaries so no single semantic chunk
becomes unplayably long.
"""

import logging
import uuid

logger = logging.getLogger(__name__)

MAX_SECTION_DURATION = 720.0   # 12 minutes — sub-split beyond this


def build_semantic_chunks(
    granular_chunks: list[dict],
    section_windows: list[tuple[float, float]],
    dvd_title: str,
    volume_name: str,
) -> list[dict]:
    """Merge granular chunks into semantic (technique-level) chunks.

    Args:
        granular_chunks: Existing silence-based chunks, sorted by start_time.
                         Each dict has at minimum: start_time, end_time, text.
        section_windows: List of (start, end) tuples defining technique
                         sections (i.e. the gaps between music segments).
        dvd_title: DVD title string (for metadata).
        volume_name: Volume name string (for metadata).

    Returns:
        List of chunk dicts ready for tagging and embedding.
        chunk_type is set to 'semantic'.
    """
    if not granular_chunks:
        return []

    sorted_chunks = sorted(granular_chunks, key=lambda c: c["start_time"])

    if not section_windows:
        # No music detected — wrap everything in one section (will sub-split if needed)
        total_end = sorted_chunks[-1]["end_time"]
        section_windows = [(sorted_chunks[0]["start_time"], total_end)]

    semantic: list[dict] = []

    for sec_start, sec_end in section_windows:
        # Collect granular chunks that belong to this section window.
        # sec_start may be at a music intro, so include chunks from there.
        in_section = [
            c for c in sorted_chunks
            if c["start_time"] >= sec_start - 0.5 and c["start_time"] < sec_end
        ]

        if not in_section:
            continue

        # Use the section window's start (music intro) as the chunk start,
        # even if the first granular chunk starts a bit later.
        music_start = sec_start

        groups = _sub_split(in_section, max_duration=MAX_SECTION_DURATION)

        for gi, group in enumerate(groups):
            chunk = _merge_group(group, dvd_title, volume_name)
            # First group in each section starts at the music intro
            if gi == 0:
                chunk["start_time"] = music_start
            semantic.append(chunk)

    logger.info(
        "Built %d semantic chunks from %d granular chunks across %d sections",
        len(semantic),
        len(sorted_chunks),
        len(section_windows),
    )
    return semantic


def _sub_split(chunks: list[dict], max_duration: float) -> list[list[dict]]:
    """Split a list of granular chunks into groups not exceeding max_duration."""
    groups: list[list[dict]] = []
    current: list[dict] = []

    for chunk in chunks:
        if not current:
            current.append(chunk)
            continue

        group_start = current[0]["start_time"]
        projected_end = chunk["end_time"]

        if projected_end - group_start > max_duration:
            groups.append(current)
            current = [chunk]
        else:
            current.append(chunk)

    if current:
        groups.append(current)

    return groups


def _merge_group(chunks: list[dict], dvd_title: str, volume_name: str) -> dict:
    """Merge a list of granular chunks into a single semantic chunk dict."""
    text = " ".join(c["text"] for c in chunks).strip()
    start_time = chunks[0]["start_time"]
    end_time = chunks[-1]["end_time"]

    return {
        "id": str(uuid.uuid4()),
        "dvd_title": dvd_title,
        "volume": volume_name,
        "start_time": start_time,
        "end_time": end_time,
        "text": text,
        "chunk_type": "semantic",
        # Tag fields — filled in by tagger
        "position": "",
        "technique": "",
        "technique_type": "",
        "aliases": [],
        "description": "",
        "key_points": [],
    }
