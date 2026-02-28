"""LLM-based technique boundary detection.

Sends the full volume transcript (with timestamps) to Gemini and asks it
to identify where each distinct technique/concept starts and ends.

Each section is classified as: demonstration, theory, drilling, rolling_footage, intro_outro.
The LLM also provides full tagging (technique name, position, etc.) in the same pass,
eliminating the need for a separate tagging step.
"""

import json
import logging
import time

from google import genai
from google.genai import types
from google.genai import errors as gerrors

from app.config import settings
from app.services.taxonomy import normalize_category, normalize_position, normalize_technique

logger = logging.getLogger(__name__)

BOUNDARY_MODEL = "gemini-2.5-flash-lite"

BOUNDARY_PROMPT = """You are an expert BJJ (Brazilian Jiu-Jitsu) instructional analyst. You are given the FULL timestamped transcript of one volume from an elite BJJ instructional DVD.

Your job: identify every distinct technique or concept section in this video, with precise start and end timestamps.

CRITICAL RULES for timestamp boundaries:
- Each section must cover the COMPLETE technique from start to finish — including the coach's introduction of the move, the full demonstration, any variations shown, and any recap.
- If the coach says "let's look at X" or "now we're going to look at Y", that's the START of a new section.
- If the coach says "so that's X" or transitions to a new topic, that's the END of the previous section.
- Do NOT split a single technique into multiple sections. If the coach shows a move, then shows a variation of the same move, keep it as ONE section.
- Theory/concept sections (general principles, frameworks) are valid sections too — label them accordingly.
- Intro/outro sections (greetings, general DVD overview) should be their own section.
- If there's music between sections, the NEXT section should start at the music timestamp (so the video clip includes the music intro).

For each section, provide:
1. start_time: when this section begins (seconds, float)
2. end_time: when this section ends (seconds, float)
3. section_type: one of "demonstration", "theory", "drilling", "rolling_footage", "intro_outro"
4. technique: specific technique name (use coach's terminology when possible)
5. position: the guard/position being played
6. technique_type: MUST be exactly one of "submission", "sweep", "guard pass", "guard retention", "escape", "takedown", "counter", "control", "concept"
7. description: one sentence — what is being taught and why it matters
8. key_points: 3-5 bullet points with the coach's most valuable insights
9. aliases: alternative names for the technique

Respond with ONLY valid JSON — an array of section objects:
[
  {
    "start_time": 0.0,
    "end_time": 164.4,
    "section_type": "theory",
    "technique": "Why the Back Position is Dominant",
    "position": "back mount",
    "technique_type": "concept",
    "description": "...",
    "key_points": ["...", "..."],
    "aliases": ["..."]
  },
  ...
]

IMPORTANT:
- Sections must be in chronological order
- Sections must NOT overlap
- Every second of the transcript should belong to exactly one section
- There should be NO gaps between sections (end of one = start of next)
- Use the timestamps from the transcript markers [XXXs] to determine boundaries"""


def _get_client() -> genai.Client:
    return genai.Client(api_key=settings.gemini_api_key)


def build_timestamped_transcript(granular_chunks: list[dict]) -> str:
    """Build a single transcript string with timestamp markers from granular chunks."""
    sorted_chunks = sorted(granular_chunks, key=lambda c: c["start_time"])
    parts = []
    for c in sorted_chunks:
        t = c["start_time"]
        m, s = divmod(int(t), 60)
        h, m = divmod(m, 60)
        ts = f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"
        parts.append(f"[{t:.0f}s | {ts}] {c['text']}")
    return "\n\n".join(parts)


def detect_technique_boundaries(
    granular_chunks: list[dict],
    dvd_title: str,
    volume_name: str,
    music_segments: list[tuple[float, float]] | None = None,
) -> list[dict]:
    """Send the full transcript to Gemini and get technique section boundaries.

    Returns a list of section dicts with start_time, end_time, technique, etc.
    """
    transcript = build_timestamped_transcript(granular_chunks)

    music_info = ""
    if music_segments:
        music_lines = []
        for ms, me in music_segments:
            music_lines.append(f"  Music: {ms:.0f}s - {me:.0f}s")
        music_info = (
            "\n\nDetected music segments (transition breaks between techniques):\n"
            + "\n".join(music_lines)
        )

    user_msg = (
        f"DVD: {dvd_title}\n"
        f"Volume: {volume_name}\n"
        f"{music_info}\n\n"
        f"FULL TRANSCRIPT:\n\n{transcript}"
    )

    client = _get_client()

    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model=BOUNDARY_MODEL,
                contents=user_msg,
                config=types.GenerateContentConfig(
                    system_instruction=BOUNDARY_PROMPT,
                    temperature=0.15,
                    max_output_tokens=16384,
                ),
            )
            break
        except gerrors.ClientError as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                raise
            if attempt < 2 and any(code in str(e) for code in ("500", "502", "503")):
                logger.warning("Transient error (attempt %d/3): %s", attempt + 1, e)
                time.sleep(5 * (attempt + 1))
                continue
            raise

    raw_text = response.text or ""
    if not raw_text and response.candidates:
        for part in response.candidates[0].content.parts:
            if getattr(part, "text", None):
                raw_text = part.text
                break

    # Clean up markdown code fences
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

    sections = json.loads(cleaned)

    if not isinstance(sections, list):
        raise ValueError(f"Expected JSON array, got {type(sections)}")

    # Validate and clean up
    video_end = granular_chunks[-1]["end_time"] if granular_chunks else 0
    validated = []
    for i, sec in enumerate(sections):
        if not isinstance(sec, dict):
            continue
        sec["start_time"] = float(sec.get("start_time", 0))
        sec["end_time"] = float(sec.get("end_time", video_end))
        sec.setdefault("technique", "unidentified")
        sec.setdefault("position", "")
        sec.setdefault("technique_type", "concept")
        sec.setdefault("description", "")
        sec.setdefault("key_points", [])
        sec.setdefault("aliases", [])
        sec.setdefault("section_type", "demonstration")

        # Normalize to canonical taxonomy
        sec["technique_type"] = normalize_category(sec.get("technique_type"))
        sec["position"] = normalize_position(sec.get("position"))
        sec["technique"] = normalize_technique(sec.get("technique"))

        validated.append(sec)

    # Ensure no gaps — extend each section's end to the next section's start
    for i in range(len(validated) - 1):
        if validated[i]["end_time"] < validated[i + 1]["start_time"]:
            validated[i]["end_time"] = validated[i + 1]["start_time"]

    # Ensure last section covers to end of video
    if validated and validated[-1]["end_time"] < video_end:
        validated[-1]["end_time"] = video_end

    logger.info(
        "LLM detected %d technique sections for %s / %s (transcript: %d chars)",
        len(validated), dvd_title, volume_name, len(transcript),
    )

    return validated
