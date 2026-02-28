import logging
import re
import uuid

logger = logging.getLogger(__name__)

TRANSITION_PHRASES = re.compile(
    r"^\s*("
    r"okay so now|ok so now|alright now|alright so now|"
    r"let's look at|let's take a look|let's move on|"
    r"next thing|next up|next we|the next|"
    r"moving on|now let's|now we're going to|now I want to|"
    r"another option|another variation|another way|"
    r"from this position|from here|so from here|"
    r"so the next|so now|so what we|"
    r"one more thing|one thing I want|"
    r"the second|the third|the fourth|"
    r"here's another|here is another|"
    r"a different|an alternative"
    r")",
    re.IGNORECASE,
)

SILENCE_GAP_THRESHOLD = 2.0  # seconds
MAX_CHUNK_DURATION = 120.0  # seconds


def chunk_segments(
    segments: list[dict],
    dvd_title: str = "",
    volume_name: str = "",
) -> list[dict]:
    """Split Whisper segments into technique-sized chunks.

    Uses three signals:
      1. Silence gaps > 2s between consecutive segments
      2. Transition phrases at the start of a segment
      3. Hard cap at 120s (force-split at nearest sentence boundary)
    """
    if not segments:
        return []

    chunks: list[dict] = []
    current_segments: list[dict] = [segments[0]]

    def _flush(segs: list[dict]) -> dict:
        text = " ".join(s["text"] for s in segs)
        return {
            "id": str(uuid.uuid4()),
            "dvd_title": dvd_title,
            "volume": volume_name,
            "start_time": segs[0]["start"],
            "end_time": segs[-1]["end"],
            "text": text,
            "position": "",
            "technique": "",
            "technique_type": "",
            "aliases": [],
            "description": "",
        }

    for i in range(1, len(segments)):
        seg = segments[i]
        prev = segments[i - 1]

        gap = seg["start"] - prev["end"]
        has_transition = bool(TRANSITION_PHRASES.match(seg["text"]))
        chunk_duration = seg["end"] - current_segments[0]["start"]

        should_split = False

        if gap >= SILENCE_GAP_THRESHOLD:
            should_split = True
        elif has_transition:
            should_split = True
        elif chunk_duration >= MAX_CHUNK_DURATION:
            should_split = True

        if should_split and current_segments:
            chunks.append(_flush(current_segments))
            current_segments = [seg]
        else:
            current_segments.append(seg)

    if current_segments:
        chunks.append(_flush(current_segments))

    _enforce_max_duration(chunks, dvd_title, volume_name)
    _extend_end_times(chunks)

    logger.info("Chunked %d segments into %d chunks", len(segments), len(chunks))
    return chunks


def _extend_end_times(chunks: list[dict]) -> None:
    """Extend each chunk's end_time to the start_time of the next chunk.

    The chunker splits on speech gaps, so end_time is where the coach's last
    word was spoken. The actual technique demonstration often continues in
    silence for many seconds after that. Extending to the next chunk's
    start_time ensures the full visual demonstration is included.
    """
    for i in range(len(chunks) - 1):
        next_start = chunks[i + 1]["start_time"]
        if next_start > chunks[i]["end_time"]:
            chunks[i]["end_time"] = next_start


def _enforce_max_duration(chunks: list[dict], dvd_title: str, volume_name: str) -> None:
    """Post-pass: split any chunks still exceeding MAX_CHUNK_DURATION at sentence boundaries."""
    i = 0
    while i < len(chunks):
        chunk = chunks[i]
        duration = chunk["end_time"] - chunk["start_time"]
        if duration <= MAX_CHUNK_DURATION:
            i += 1
            continue

        text = chunk["text"]
        sentences = re.split(r"(?<=[.!?])\s+", text)
        if len(sentences) <= 1:
            i += 1
            continue

        mid = len(sentences) // 2
        first_text = " ".join(sentences[:mid])
        second_text = " ".join(sentences[mid:])

        total_len = len(text)
        split_ratio = len(first_text) / total_len if total_len > 0 else 0.5
        split_time = chunk["start_time"] + (duration * split_ratio)

        first_chunk = {
            **chunk,
            "id": str(uuid.uuid4()),
            "end_time": split_time,
            "text": first_text,
        }
        second_chunk = {
            **chunk,
            "id": str(uuid.uuid4()),
            "start_time": split_time,
            "text": second_text,
        }

        chunks[i : i + 1] = [first_chunk, second_chunk]
        # don't increment i — re-check the first half
