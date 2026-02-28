import logging

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PointStruct,
    VectorParams,
)

from app.config import settings
from app.services.embedder import EMBEDDING_DIMENSIONS, embed_query

logger = logging.getLogger(__name__)

COLLECTION_NAME = "bjj_chunks"

_client: QdrantClient | None = None


def _get_client() -> QdrantClient:
    global _client
    if _client is None:
        _client = QdrantClient(url=settings.qdrant_url)
    return _client


def ensure_collection():
    """Create the Qdrant collection if it doesn't exist.

    If the existing collection was built with a different embedding model
    (dimension mismatch), it is deleted and recreated so new embeddings fit.
    The caller should run reembed_all.py to repopulate vectors from Postgres.
    """
    client = _get_client()
    collections = {c.name for c in client.get_collections().collections}

    if COLLECTION_NAME in collections:
        info = client.get_collection(COLLECTION_NAME)
        existing_size = info.config.params.vectors.size
        if existing_size != EMBEDDING_DIMENSIONS:
            logger.warning(
                "Qdrant collection '%s' has %d-dim vectors but embedder now uses %d dims. "
                "Recreating collection — run reembed_all.py to repopulate.",
                COLLECTION_NAME, existing_size, EMBEDDING_DIMENSIONS,
            )
            client.delete_collection(COLLECTION_NAME)
            collections.discard(COLLECTION_NAME)

    if COLLECTION_NAME not in collections:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(
                size=EMBEDDING_DIMENSIONS,
                distance=Distance.COSINE,
            ),
        )
        logger.info("Created Qdrant collection '%s' (%d dims)", COLLECTION_NAME, EMBEDDING_DIMENSIONS)


def upsert_chunks(chunks: list[dict]):
    """Insert or update chunk embeddings in Qdrant."""
    client = _get_client()
    ensure_collection()

    points = []
    for chunk in chunks:
        embedding = chunk.get("_embedding")
        if not embedding:
            continue

        points.append(
            PointStruct(
                id=chunk["id"],
                vector=embedding,
                payload={
                    "dvd_title": chunk.get("dvd_title", ""),
                    "volume": chunk.get("volume", ""),
                    "position": chunk.get("position", ""),
                    "technique": chunk.get("technique", ""),
                    "technique_type": chunk.get("technique_type", ""),
                    "start_time": chunk.get("start_time", 0),
                    "end_time": chunk.get("end_time", 0),
                    "chunk_type": chunk.get("chunk_type", "granular"),
                },
            )
        )

    if points:
        QDRANT_BATCH = 100
        for i in range(0, len(points), QDRANT_BATCH):
            batch = points[i : i + QDRANT_BATCH]
            client.upsert(collection_name=COLLECTION_NAME, points=batch)
            logger.info("Upserted %d-%d of %d points", i, i + len(batch), len(points))

    logger.info("Upserted %d chunks to Qdrant", len(points))


def search_chunks(
    query: str,
    limit: int = 20,
    filters: dict | None = None,
) -> list[dict]:
    """Semantic search for chunks matching a query."""
    client = _get_client()
    ensure_collection()

    query_vector = embed_query(query)

    qdrant_filter = None
    if filters:
        conditions = []
        for key, value in filters.items():
            if value:
                conditions.append(FieldCondition(key=key, match=MatchValue(value=value)))
        if conditions:
            qdrant_filter = Filter(must=conditions)

    results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        query_filter=qdrant_filter,
        limit=limit,
    )

    return [
        {
            "id": str(hit.id),
            "score": hit.score,
            **hit.payload,
        }
        for hit in results.points
    ]
