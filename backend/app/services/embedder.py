import logging
import time

from google import genai
from google.genai import types
from google.genai import errors as gerrors

from app.config import settings

logger = logging.getLogger(__name__)

EMBEDDING_MODEL = "models/gemini-embedding-001"
EMBEDDING_DIMENSIONS = 768
BATCH_SIZE = 50

# Words that carry no semantic weight in BJJ queries.
_STOPWORDS = {
    "a", "an", "the", "from", "to", "in", "on", "at", "with", "for",
    "how", "what", "where", "when", "do", "does", "did", "can", "could",
    "i", "my", "me", "show", "find", "get", "using", "and", "or", "is",
    "are", "was", "were", "be", "been", "of", "that", "this", "it", "its",
    "by", "so", "as", "if", "then", "but", "not", "no", "into", "about",
}


def expand_query(query: str) -> str:
    """Weight meaningful query terms by repeating them before embedding.

    "armbar from the back" -> the embedding model sees "armbar" and "back"
    three times each, making them dominate the vector direction rather than
    being diluted by stopwords like "from" and "the".
    """
    words = query.lower().split()
    key = [w for w in words if w not in _STOPWORDS and len(w) > 2]
    if not key:
        return query
    return f"{query} {' '.join(key)} {' '.join(key)}"


def build_search_text(chunk: dict) -> str:
    """Build the text string that gets embedded for a chunk.

    Focused on the 5 fields that matter most for BJJ search:
      1. technique name  — repeated 2x so it anchors the vector
      2. position        — repeated 2x for positional context
      3. technique_type  — category context (submission vs escape vs concept)
      4. aliases         — synonym coverage (mata leao → rear naked choke)
      5. description     — richest semantic summary sentence
    """
    parts = []

    technique = (chunk.get("technique") or "").strip()
    if technique:
        parts.append(f"{technique}. {technique}.")

    position = (chunk.get("position") or "").strip()
    if position:
        parts.append(f"{position}. {position}.")

    ttype = (chunk.get("technique_type") or "").strip()
    if ttype:
        parts.append(ttype)

    aliases = chunk.get("aliases")
    if aliases and isinstance(aliases, list) and aliases:
        parts.append("Also known as: " + ", ".join(aliases))

    description = (chunk.get("description") or "").strip()
    if description:
        parts.append(description)

    return "\n".join(parts)


def _get_client() -> genai.Client:
    return genai.Client(api_key=settings.gemini_api_key)


def embed_texts(texts: list[str], task_type: str = "RETRIEVAL_DOCUMENT") -> list[list[float]]:
    """Generate embeddings for a list of texts using Gemini embedding model."""
    from app.services.tagger import QuotaExhaustedError

    client = _get_client()
    all_embeddings = []

    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i : i + BATCH_SIZE]
        for attempt in range(4):
            try:
                result = client.models.embed_content(
                    model=EMBEDDING_MODEL,
                    contents=batch,
                    config=types.EmbedContentConfig(
                        task_type=task_type,
                        output_dimensionality=EMBEDDING_DIMENSIONS,
                    ),
                )
                embeddings = [e.values for e in result.embeddings]
                all_embeddings.extend(embeddings)
                break
            except gerrors.ClientError as e:
                if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                    if attempt < 3:
                        wait = 30 * (attempt + 1)
                        logger.warning("Embedding rate limited, waiting %ds (attempt %d/4)...", wait, attempt + 1)
                        time.sleep(wait)
                        continue
                    # All retries exhausted
                    raise QuotaExhaustedError(
                        "Gemini embedding quota exhausted after retries. "
                        "Check billing at https://aistudio.google.com"
                    ) from e
                raise

        logger.info("Embedded batch %d-%d of %d", i, i + len(batch), len(texts))
        # Free tier: 100 requests/min — stay safely under by sleeping between batches
        if i + BATCH_SIZE < len(texts):
            time.sleep(0.7)

    return all_embeddings


def embed_query(query: str) -> list[float]:
    """Generate an embedding for a search query with query expansion."""
    from app.services.tagger import QuotaExhaustedError

    client = _get_client()
    expanded = expand_query(query)
    if expanded != query:
        logger.debug("Query expanded: %r -> %r", query, expanded)
    try:
        result = client.models.embed_content(
            model=EMBEDDING_MODEL,
            contents=expanded,
            config=types.EmbedContentConfig(
                task_type="RETRIEVAL_QUERY",
                output_dimensionality=EMBEDDING_DIMENSIONS,
            ),
        )
        return result.embeddings[0].values
    except gerrors.ClientError as e:
        if e.status_code == 429:
            raise QuotaExhaustedError("Gemini quota exhausted during query embedding.") from e
        raise


def embed_chunks(chunks: list[dict]) -> list[dict]:
    """Generate embeddings for all chunks and store the embedding_id."""
    texts = [build_search_text(c) for c in chunks]
    embeddings = embed_texts(texts)

    for chunk, embedding in zip(chunks, embeddings):
        chunk["_embedding"] = embedding
        chunk["embedding_id"] = chunk["id"]

    logger.info("Generated embeddings for %d chunks", len(chunks))
    return chunks
