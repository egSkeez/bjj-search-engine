import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from google import genai
from google.genai import types
from google.genai import errors as gerrors

from app.config import settings
from app.services.taxonomy import normalize_chunk

logger = logging.getLogger(__name__)

# Parallel tagging workers — Gemini paid tier allows 2000 RPM so 10 is safe
CONCURRENCY = 10


class QuotaExhaustedError(RuntimeError):
    """Raised when the Gemini account has hit its quota / no billing active."""


SYSTEM_PROMPT = """You are an expert BJJ (Brazilian Jiu-Jitsu) instructional analyst working with transcripts from elite-level instructional DVDs.

Your job: extract structured metadata AND the coach's most valuable insights from each transcript segment.

Respond with ONLY valid JSON, no markdown, no explanation:
{
    "position": "<MUST be one of: closed guard, open guard, half guard, butterfly guard, deep half guard, de la riva guard, reverse de la riva guard, x guard, single leg x, spider guard, lasso guard, rubber guard, octopus guard, z guard, mount, side control, back control, north south, turtle, front headlock, leg entanglement, standing, seated guard, top position, bottom position, crucifix, truck, 50/50>",
    "technique": "<the specific technique name — use the most common English name. Group related moves: e.g. all heel hooks/toe holds/knee bars are 'leglocks', darce/brabo are 'darce choke', all guillotine variants are 'guillotine'. Use the coach's own name when they give a specific one.>",
    "technique_type": "<MUST be exactly one of: submission, sweep, guard pass, guard retention, escape, takedown, counter, control, concept>",
    "aliases": ["ALL alternative names including Japanese, Portuguese, nicknames, abbreviations, and coach-specific terminology"],
    "description": "One sentence: what is being taught and why it matters.",
    "key_points": ["3 to 5 bullet points with the coach's most valuable specific insights."]
}

CATEGORY DEFINITIONS (technique_type):
- submission: goal is to break/choke the opponent and get them to tap
- sweep: goal is to reverse position and get to an advantageous position (includes reversals)
- guard pass: goal is to pass the opponent's guard
- guard retention: goal is to prevent the opponent from passing your guard
- escape: goal is to get out of a bad position or a submission hold
- takedown: goal is to bring a standing opponent down to the mat
- counter: going from being submitted to submitting the opponent
- control: positional dominance — pins, grips, pressure, transitions between positions
- concept: coach explaining theory/principles/strategy without demonstrating a specific technique

Rules:
- position MUST be from the fixed list above. Pick the closest match.
- technique_type MUST be exactly one value from the list above. Never combine multiple.
- Group related techniques under one name (e.g. all leg entanglement attacks = "heel hook" or "knee bar", not "outside ashi garami heel hook").
- Include all known aliases for searchability.
- If the segment is conceptual, use technique_type "concept".
- If you cannot identify the technique, set technique to "unidentified"."""

TAGGING_MODEL = "gemini-2.5-flash"
BATCH_SIZE = 10
BATCH_DELAY = 0.3


def _get_client() -> genai.Client:
    return genai.Client(api_key=settings.gemini_api_key)


def tag_single_chunk(client: genai.Client, chunk: dict) -> dict:
    """Send a single chunk to Gemini for BJJ metadata extraction."""
    user_msg = (
        f"DVD: {chunk.get('dvd_title', 'Unknown')}\n"
        f"Volume: {chunk.get('volume', 'Unknown')}\n"
        f"Timestamp: {chunk.get('start_time', 0):.1f}s - {chunk.get('end_time', 0):.1f}s\n\n"
        f"Transcript:\n{chunk.get('text', '')}"
    )

    raw_text = ""
    try:
        response = None
        for attempt in range(3):
            try:
                response = client.models.generate_content(
                    model=TAGGING_MODEL,
                    contents=user_msg,
                    config=types.GenerateContentConfig(
                        system_instruction=SYSTEM_PROMPT,
                        temperature=0.2,
                        max_output_tokens=4096,
                    ),
                )
                break
            except gerrors.ClientError as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    raise QuotaExhaustedError(
                        "Gemini quota exhausted or billing not enabled."
                    ) from e
                if attempt < 2 and ("502" in str(e) or "503" in str(e) or "500" in str(e)):
                    logger.warning("Transient Gemini error (attempt %d/3): %s", attempt + 1, e)
                    time.sleep(5 * (attempt + 1))
                    continue
                raise

        if response is None:
            raise RuntimeError("No response after 3 attempts")

        raw_text = response.text or ""
        if not raw_text and response.candidates:
            for part in response.candidates[0].content.parts:
                if getattr(part, "text", None):
                    raw_text = part.text
                    break

        cleaned = raw_text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

        parsed = json.loads(cleaned)

        # Gemini sometimes wraps the response in an array
        if isinstance(parsed, list) and len(parsed) > 0:
            parsed = parsed[0]

        if not isinstance(parsed, dict):
            raise json.JSONDecodeError("Expected JSON object", cleaned, 0)

        chunk["position"] = parsed.get("position", "")
        chunk["technique"] = parsed.get("technique", "")
        chunk["technique_type"] = parsed.get("technique_type", "")
        chunk["aliases"] = parsed.get("aliases", [])
        chunk["description"] = parsed.get("description", "")
        chunk["key_points"] = parsed.get("key_points", [])
        chunk["llm_raw_response"] = parsed

        normalize_chunk(chunk)

    except json.JSONDecodeError as e:
        logger.error("Failed to parse Gemini response for chunk %s: %s", chunk.get("id"), e)
        chunk["technique"] = "unidentified"
        chunk["description"] = "Tagging failed — could not parse LLM response"
        chunk["llm_raw_response"] = {"error": "json_parse_error", "raw": raw_text}

    except gerrors.ClientError as e:
        if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
            raise QuotaExhaustedError(
                "Gemini quota exhausted or billing not enabled."
            ) from e
        logger.error("Gemini API error for chunk %s: %s", chunk.get("id"), e)
        chunk["technique"] = "unidentified"
        chunk["description"] = f"Tagging failed — API error: {e}"
        chunk["llm_raw_response"] = {"error": "api_error", "message": str(e)}

    except QuotaExhaustedError:
        raise

    except Exception as e:
        logger.error("Unexpected error tagging chunk %s: %s", chunk.get("id"), e)
        chunk["technique"] = "unidentified"
        chunk["description"] = f"Tagging failed — error: {e}"
        chunk["llm_raw_response"] = {"error": "unexpected", "message": str(e)}

    return chunk


def tag_chunks(chunks: list[dict], force: bool = False) -> list[dict]:
    """Tag all chunks with BJJ metadata via Gemini API.

    Uses a thread pool for concurrent tagging (Gemini paid tier allows
    2000 RPM, so CONCURRENCY=10 is well within limits).
    """
    client = _get_client()
    total = len(chunks)

    to_tag = []
    skipped = 0
    for chunk in chunks:
        already_tagged = chunk.get("technique") and chunk["technique"] != "unidentified"
        if already_tagged and not force:
            skipped += 1
        else:
            to_tag.append(chunk)

    if skipped:
        logger.info("Skipping %d already-tagged chunks", skipped)

    if not to_tag:
        logger.info("All %d chunks already tagged", total)
        return chunks

    tagged = 0
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = {pool.submit(tag_single_chunk, client, c): c for c in to_tag}
        for future in as_completed(futures):
            try:
                future.result()
            except QuotaExhaustedError:
                pool.shutdown(wait=False, cancel_futures=True)
                raise
            tagged += 1
            if tagged % 10 == 0 or tagged == len(to_tag):
                logger.info("Tagged %d/%d chunks (%d skipped)", tagged, total, skipped)

    logger.info("Tagging complete: %d tagged, %d skipped out of %d total", tagged, skipped, total)
    return chunks
