"""Search API — structured-first with vector fallback.

The search strategy:
  1. Parse the natural language query into structured intent
     (technique, position, type)
  2. Query Postgres directly on structured fields with weighted scoring:
       - technique match  → highest weight
       - position match   → high weight
       - type match       → moderate weight
       - alias match      → bonus
       - description/text → weak fallback
  3. If the query is too vague for structured search, fall back to
     vector similarity via Qdrant
  4. Score is a 0–1 relevance value with clear differentiation
"""

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, cast, Float as SAFloat, func, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.database import get_db
from app.models import Chunk, DVD, Volume
from app.schemas import BrowseResponse, ChunkOut, SearchResponse, SearchResult
from app.services.query_parser import parse_query

router = APIRouter(tags=["search"])


def _chunk_to_out(chunk: Chunk) -> ChunkOut:
    volume = chunk.volume
    dvd = volume.dvd if volume else None
    return ChunkOut(
        id=chunk.id,
        volume_id=chunk.volume_id,
        start_time=chunk.start_time,
        end_time=chunk.end_time,
        text=chunk.text,
        position=chunk.position,
        technique=chunk.technique,
        technique_type=chunk.technique_type,
        aliases=chunk.aliases,
        description=chunk.description,
        key_points=chunk.key_points,
        chunk_type=chunk.chunk_type,
        created_at=chunk.created_at,
        dvd_title=dvd.title if dvd else None,
        volume_name=volume.name if volume else None,
        instructor=dvd.instructor if dvd else None,
    )


# ────────────────────────────────────────────────────────────────
# Structured search — the primary search path
# ────────────────────────────────────────────────────────────────

def _build_structured_query(
    parsed, mode: str, position_filter: str | None, type_filter: str | None, limit: int, offset: int
):
    """Build a Postgres query that scores chunks by structured field match.

    Returns (stmt, count_stmt) — the results query and a count query.
    """
    alias_str = func.lower(func.array_to_string(Chunk.aliases, " "))

    # ── Technique score (0 or 0.45) ──────────────────────────
    if parsed.technique:
        tech_term = parsed.technique.lower()
        # Exact technique name match
        tech_exact = case(
            (func.lower(Chunk.technique) == tech_term, literal(0.45)),
            # technique name contains the search term
            (func.lower(Chunk.technique).contains(tech_term), literal(0.38)),
            # search term contains the technique name (query is more specific)
            (literal(tech_term).contains(func.lower(Chunk.technique)), literal(0.30)),
            # any alias matches
            (alias_str.contains(tech_term), literal(0.32)),
            # technique word overlap
            *[
                (func.lower(Chunk.technique).contains(w), literal(0.15))
                for w in tech_term.split() if len(w) > 2
            ],
            # description mentions it
            (func.lower(Chunk.description).contains(tech_term), literal(0.10)),
            # transcript mentions it
            (func.lower(Chunk.text).contains(tech_term), literal(0.05)),
            else_=literal(0.0),
        )
    else:
        tech_exact = literal(0.0)

    # ── Position score (0 or 0.30) ───────────────────────────
    if parsed.position and parsed.position_variants:
        variants = parsed.position_variants
        pos_exact = case(
            *[
                (func.lower(Chunk.position) == v.lower(), literal(0.30))
                for v in variants
            ],
            *[
                (func.lower(Chunk.position).contains(v.lower()), literal(0.20))
                for v in variants
            ],
            else_=literal(0.0),
        )
    elif parsed.position:
        pos_term = parsed.position.lower()
        pos_exact = case(
            (func.lower(Chunk.position) == pos_term, literal(0.30)),
            (func.lower(Chunk.position).contains(pos_term), literal(0.20)),
            else_=literal(0.0),
        )
    else:
        pos_exact = literal(0.0)

    # ── Type score (0 or 0.15) ───────────────────────────────
    if parsed.technique_type:
        type_score = case(
            (func.lower(Chunk.technique_type) == parsed.technique_type.lower(), literal(0.15)),
            else_=literal(0.0),
        )
    else:
        type_score = literal(0.0)

    # ── Bonus for text/description mentioning raw query (0 or 0.10) ──
    raw_lower = parsed.raw_query.lower()
    text_bonus = case(
        (func.lower(Chunk.description).contains(raw_lower), literal(0.10)),
        (func.lower(Chunk.text).contains(raw_lower), literal(0.05)),
        else_=literal(0.0),
    )

    total_score = cast(tech_exact + pos_exact + type_score + text_bonus, SAFloat).label("relevance")

    # ── Build WHERE clause ────────────────────────────────────
    # Must match at least one structured field to be a candidate
    conditions = []
    if parsed.technique:
        tech_term = parsed.technique.lower()
        tech_words = [w for w in tech_term.split() if len(w) > 2]
        tech_conditions = [
            func.lower(Chunk.technique).contains(tech_term),
            alias_str.contains(tech_term),
            func.lower(Chunk.description).contains(tech_term),
        ]
        # Also match individual technique words
        for w in tech_words:
            tech_conditions.append(func.lower(Chunk.technique).contains(w))
            tech_conditions.append(alias_str.contains(w))
        conditions.append(or_(*tech_conditions))

    if parsed.position and parsed.position_variants:
        pos_conditions = []
        for v in parsed.position_variants:
            pos_conditions.append(func.lower(Chunk.position).contains(v.lower()))
        if pos_conditions:
            conditions.append(or_(*pos_conditions))

    if parsed.technique_type:
        conditions.append(func.lower(Chunk.technique_type) == parsed.technique_type.lower())

    # Always filter by chunk_type (granular/semantic)
    conditions.append(Chunk.chunk_type == mode)

    # Apply user's explicit filters
    if position_filter:
        conditions.append(func.lower(Chunk.position).contains(position_filter.lower()))
    if type_filter:
        conditions.append(func.lower(Chunk.technique_type) == type_filter.lower())

    # If we have technique + position, require BOTH to match (AND logic)
    # But each individual field uses OR on its variants
    if parsed.technique and parsed.position:
        # Build separate conditions and AND them
        tech_term = parsed.technique.lower()
        tech_words = [w for w in tech_term.split() if len(w) > 2]
        tech_or = [
            func.lower(Chunk.technique).contains(tech_term),
            alias_str.contains(tech_term),
        ]
        for w in tech_words:
            tech_or.append(func.lower(Chunk.technique).contains(w))
            tech_or.append(alias_str.contains(w))

        pos_or = [
            func.lower(Chunk.position).contains(v.lower())
            for v in parsed.position_variants
        ]

        # Primary: both match. Secondary: just technique matches (lower score).
        # We fetch both and let the scoring sort them.
        combined_where = [
            Chunk.chunk_type == mode,
            or_(
                # Both match (will score high)
                or_(*tech_or) & or_(*pos_or) if pos_or else or_(*tech_or),
                # Just technique (will score lower due to missing position score)
                or_(*tech_or),
            ),
        ]
        if position_filter:
            combined_where.append(func.lower(Chunk.position).contains(position_filter.lower()))
        if type_filter:
            combined_where.append(func.lower(Chunk.technique_type) == type_filter.lower())
        conditions = combined_where

    stmt = (
        select(Chunk, total_score)
        .options(joinedload(Chunk.volume).joinedload(Volume.dvd))
        .where(*conditions)
        .order_by(total_score.desc(), Chunk.technique)
        .limit(limit)
        .offset(offset)
    )

    count_conditions = [c for c in conditions]
    count_stmt = select(func.count(Chunk.id)).where(*count_conditions)

    return stmt, count_stmt


# ────────────────────────────────────────────────────────────────
# Vector search — fallback for conceptual/vague queries
# ────────────────────────────────────────────────────────────────

async def _vector_search(
    q: str, mode: str, position: str | None, type_filter: str | None,
    limit: int, offset: int, db: AsyncSession,
) -> SearchResponse | None:
    """Attempt vector similarity search via Qdrant. Returns None if unavailable."""
    try:
        from app.services.vector_store import search_chunks as vector_search

        filters: dict = {"chunk_type": mode}
        if position:
            filters["position"] = position
        if type_filter:
            filters["technique_type"] = type_filter

        fetch_limit = (limit + offset) * 3
        vector_results = vector_search(q, limit=fetch_limit, filters=filters)

        if not vector_results:
            return None

        chunk_ids = [UUID(r["id"]) for r in vector_results]
        vector_scores = {r["id"]: r["score"] for r in vector_results}

        stmt = (
            select(Chunk)
            .options(joinedload(Chunk.volume).joinedload(Volume.dvd))
            .where(Chunk.id.in_(chunk_ids))
        )
        result = await db.execute(stmt)
        chunks_by_id = {str(c.id): c for c in result.scalars().unique().all()}

        scored: list[SearchResult] = []
        for cid_uuid in chunk_ids:
            cid = str(cid_uuid)
            if cid not in chunks_by_id:
                continue
            vec_score = vector_scores.get(cid, 0.0)
            scored.append(SearchResult(
                chunk=_chunk_to_out(chunks_by_id[cid]),
                score=round(vec_score * 0.85, 4),  # Scale down so vector results don't appear inflated
            ))

        scored.sort(key=lambda r: r.score, reverse=True)
        page = scored[offset: offset + limit]
        return SearchResponse(query=q, results=page, total=len(scored))
    except Exception:
        return None


# ────────────────────────────────────────────────────────────────
# Main search endpoint
# ────────────────────────────────────────────────────────────────

@router.get("/search", response_model=SearchResponse)
async def search(
    q: str = Query(..., min_length=1),
    position: str | None = Query(None),
    type: str | None = Query(None, alias="type"),
    mode: str = Query("granular", pattern="^(granular|semantic)$"),
    limit: int = Query(20, le=100),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Search across all indexed BJJ chunks.

    Strategy:
      1. Parse the query into structured intent (technique, position, type).
      2. If structured fields found → query Postgres with weighted scoring.
      3. Otherwise → fall back to vector similarity search.
    """
    parsed = parse_query(q)

    # ── Path 1: Structured search ───────────────────────────
    if parsed.is_structured:
        stmt, count_stmt = _build_structured_query(
            parsed, mode, position, type, limit, offset
        )

        result = await db.execute(stmt)
        rows = result.unique().all()

        count_result = await db.execute(count_stmt)
        total = count_result.scalar() or 0

        if rows:
            # Normalize scores: highest result → 1.0
            max_score = max(r[1] for r in rows) or 1.0
            results = [
                SearchResult(
                    chunk=_chunk_to_out(chunk),
                    score=round(raw_score / max_score, 4) if max_score > 0 else 0.0,
                )
                for chunk, raw_score in rows
            ]
            return SearchResponse(query=q, results=results, total=total)

        # Structured search found nothing — try vector as fallback
        vector_resp = await _vector_search(q, mode, position, type, limit, offset, db)
        if vector_resp:
            return vector_resp

        return SearchResponse(query=q, results=[], total=0)

    # ── Path 2: No structure detected → vector search ──────
    vector_resp = await _vector_search(q, mode, position, type, limit, offset, db)
    if vector_resp:
        return vector_resp

    # ── Path 3: Last resort — raw text ILIKE ───────────────
    search_term = f"%{q}%"
    base_where = [
        or_(
            Chunk.technique.ilike(search_term),
            Chunk.description.ilike(search_term),
            Chunk.text.ilike(search_term),
            Chunk.position.ilike(search_term),
            func.array_to_string(Chunk.aliases, " ").ilike(search_term),
        ),
        Chunk.chunk_type == mode,
    ]

    stmt = (
        select(Chunk)
        .options(joinedload(Chunk.volume).joinedload(Volume.dvd))
        .where(*base_where)
    )
    if position:
        stmt = stmt.where(Chunk.position.ilike(f"%{position}%"))
    if type:
        stmt = stmt.where(Chunk.technique_type == type)
    stmt = stmt.order_by(Chunk.created_at.desc()).offset(offset).limit(limit)

    result = await db.execute(stmt)
    chunks = result.scalars().unique().all()

    count_stmt = select(func.count(Chunk.id)).where(*base_where)
    if position:
        count_stmt = count_stmt.where(Chunk.position.ilike(f"%{position}%"))
    if type:
        count_stmt = count_stmt.where(Chunk.technique_type == type)
    total_result = await db.execute(count_stmt)
    total = total_result.scalar() or 0

    results = [SearchResult(chunk=_chunk_to_out(c), score=0.5) for c in chunks]
    return SearchResponse(query=q, results=results, total=total)


# ────────────────────────────────────────────────────────────────
# Browse and filter endpoints (unchanged)
# ────────────────────────────────────────────────────────────────

@router.get("/browse", response_model=BrowseResponse)
async def browse(
    position: str | None = Query(None),
    type: str | None = Query(None, alias="type"),
    limit: int = Query(50, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Browse chunks by structured tags (position, technique type)."""
    stmt = (
        select(Chunk)
        .options(joinedload(Chunk.volume).joinedload(Volume.dvd))
    )

    if position:
        stmt = stmt.where(Chunk.position.ilike(f"%{position}%"))
    if type:
        stmt = stmt.where(Chunk.technique_type == type)

    stmt = stmt.order_by(Chunk.position, Chunk.technique).offset(offset).limit(limit)

    result = await db.execute(stmt)
    chunks = result.scalars().unique().all()

    count_stmt = select(func.count(Chunk.id))
    if position:
        count_stmt = count_stmt.where(Chunk.position.ilike(f"%{position}%"))
    if type:
        count_stmt = count_stmt.where(Chunk.technique_type == type)

    total_result = await db.execute(count_stmt)
    total = total_result.scalar() or 0

    return BrowseResponse(
        position=position,
        technique_type=type,
        results=[_chunk_to_out(c) for c in chunks],
        total=total,
    )


@router.get("/positions")
async def list_positions(db: AsyncSession = Depends(get_db)):
    """Return distinct positions in the database for filter dropdowns."""
    stmt = select(Chunk.position).where(Chunk.position.isnot(None)).distinct().order_by(Chunk.position)
    result = await db.execute(stmt)
    return [row[0] for row in result.all() if row[0]]


@router.get("/technique-types")
async def list_technique_types(db: AsyncSession = Depends(get_db)):
    """Return distinct technique types for filter checkboxes."""
    stmt = (
        select(Chunk.technique_type)
        .where(Chunk.technique_type.isnot(None))
        .distinct()
        .order_by(Chunk.technique_type)
    )
    result = await db.execute(stmt)
    return [row[0] for row in result.all() if row[0]]
