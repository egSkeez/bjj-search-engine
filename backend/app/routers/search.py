"""Search API — vector-first with structured re-ranking.

The search strategy:
  1. Always start with vector similarity search via Qdrant (semantic matching)
  2. Parse the query for structured intent (technique, position, type)
  3. Re-rank vector results by boosting chunks that also match structured fields
  4. If vector search is unavailable, fall back to structured Postgres search
  5. Last resort: raw text ILIKE
"""

import logging
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, cast, Float as SAFloat, func, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.database import get_db
from app.models import Chunk, DVD, Volume
from app.schemas import BrowseResponse, ChunkOut, SearchResponse, SearchResult
from app.services.query_parser import parse_query
from app.services.taxonomy import CATEGORIES, POSITIONS, normalize_category, normalize_position

logger = logging.getLogger(__name__)

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


def _compute_struct_boost(chunk: Chunk, parsed) -> float:
    """Compute a structured-field boost (0.0–0.30) for re-ranking vector results.

    This rewards chunks whose metadata explicitly matches the parsed query intent,
    on top of the vector similarity score.
    """
    boost = 0.0

    if parsed.technique:
        tech_term = parsed.technique.lower()
        chunk_tech = (chunk.technique or "").lower()
        chunk_aliases = " ".join(chunk.aliases or []).lower()

        if chunk_tech == tech_term:
            boost += 0.15
        elif tech_term in chunk_tech or chunk_tech in tech_term:
            boost += 0.10
        elif tech_term in chunk_aliases:
            boost += 0.08

    if parsed.position and parsed.position_variants:
        chunk_pos = (chunk.position or "").lower()
        for v in parsed.position_variants:
            if chunk_pos == v.lower():
                boost += 0.10
                break
            if v.lower() in chunk_pos:
                boost += 0.05
                break

    if parsed.technique_type:
        if (chunk.technique_type or "").lower() == parsed.technique_type.lower():
            boost += 0.05

    return boost


# ────────────────────────────────────────────────────────────────
# Vector search — the primary search path
# ────────────────────────────────────────────────────────────────

async def _vector_search(
    q: str,
    parsed,
    mode: str,
    position: str | None,
    type_filter: str | None,
    limit: int,
    offset: int,
    db: AsyncSession,
    instructor: str | None = None,
    dvd_id: str | None = None,
    hide_concepts: bool = False,
) -> SearchResponse | None:
    """Vector similarity search via Qdrant with structured re-ranking."""
    try:
        from app.services.vector_store import search_chunks as vector_search

        filters: dict = {"chunk_type": mode}
        if position:
            filters["position"] = position
        if type_filter:
            filters["technique_type"] = type_filter

        fetch_limit = max((limit + offset) * 3, 60)
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
            chunk = chunks_by_id[cid]

            if hide_concepts and (chunk.technique_type or "").lower() == "concept":
                continue
            if instructor:
                dvd = chunk.volume.dvd if chunk.volume else None
                if not dvd or (dvd.instructor or "").lower() != instructor.lower():
                    continue
            if dvd_id:
                if not chunk.volume or str(chunk.volume.dvd_id) != dvd_id:
                    continue

            vec_score = vector_scores.get(cid, 0.0)
            struct_boost = _compute_struct_boost(chunk, parsed)
            final_score = vec_score + struct_boost

            scored.append(SearchResult(
                chunk=_chunk_to_out(chunk),
                score=round(final_score, 4),
            ))

        scored.sort(key=lambda r: r.score, reverse=True)

        if scored:
            max_score = scored[0].score or 1.0
            for s in scored:
                s.score = round(s.score / max_score, 4)

        page = scored[offset: offset + limit]
        return SearchResponse(query=q, results=page, total=len(scored))
    except Exception as exc:
        logger.warning("Vector search failed, will fall back: %s", exc)
        return None


# ────────────────────────────────────────────────────────────────
# Structured search — fallback when vector search is unavailable
# ────────────────────────────────────────────────────────────────

def _build_structured_query(
    parsed, mode: str, position_filter: str | None, type_filter: str | None, limit: int, offset: int
):
    """Build a Postgres query that scores chunks by structured field match."""
    alias_str = func.lower(func.array_to_string(Chunk.aliases, " "))

    if parsed.technique:
        tech_term = parsed.technique.lower()
        tech_exact = case(
            (func.lower(Chunk.technique) == tech_term, literal(0.45)),
            (func.lower(Chunk.technique).contains(tech_term), literal(0.38)),
            (literal(tech_term).contains(func.lower(Chunk.technique)), literal(0.30)),
            (alias_str.contains(tech_term), literal(0.32)),
            *[
                (func.lower(Chunk.technique).contains(w), literal(0.15))
                for w in tech_term.split() if len(w) > 2
            ],
            (func.lower(Chunk.description).contains(tech_term), literal(0.10)),
            (func.lower(Chunk.text).contains(tech_term), literal(0.05)),
            else_=literal(0.0),
        )
    else:
        tech_exact = literal(0.0)

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

    if parsed.technique_type:
        type_score = case(
            (func.lower(Chunk.technique_type) == parsed.technique_type.lower(), literal(0.15)),
            else_=literal(0.0),
        )
    else:
        type_score = literal(0.0)

    raw_lower = parsed.raw_query.lower()
    text_bonus = case(
        (func.lower(Chunk.description).contains(raw_lower), literal(0.10)),
        (func.lower(Chunk.text).contains(raw_lower), literal(0.05)),
        else_=literal(0.0),
    )

    total_score = cast(tech_exact + pos_exact + type_score + text_bonus, SAFloat).label("relevance")

    conditions = []
    if parsed.technique:
        tech_term = parsed.technique.lower()
        tech_words = [w for w in tech_term.split() if len(w) > 2]
        tech_conditions = [
            func.lower(Chunk.technique).contains(tech_term),
            alias_str.contains(tech_term),
            func.lower(Chunk.description).contains(tech_term),
        ]
        for w in tech_words:
            tech_conditions.append(func.lower(Chunk.technique).contains(w))
            tech_conditions.append(alias_str.contains(w))
        conditions.append(or_(*tech_conditions))

    if parsed.position and parsed.position_variants:
        pos_conditions = [
            func.lower(Chunk.position).contains(v.lower())
            for v in parsed.position_variants
        ]
        if pos_conditions:
            conditions.append(or_(*pos_conditions))

    if parsed.technique_type:
        conditions.append(func.lower(Chunk.technique_type) == parsed.technique_type.lower())

    conditions.append(Chunk.chunk_type == mode)

    if position_filter:
        conditions.append(func.lower(Chunk.position) == position_filter.lower())
    if type_filter:
        conditions.append(func.lower(Chunk.technique_type) == type_filter.lower())

    stmt = (
        select(Chunk, total_score)
        .options(joinedload(Chunk.volume).joinedload(Volume.dvd))
        .where(*conditions)
        .order_by(total_score.desc(), Chunk.technique)
        .limit(limit)
        .offset(offset)
    )

    count_stmt = select(func.count(Chunk.id)).where(*conditions)
    return stmt, count_stmt


# ────────────────────────────────────────────────────────────────
# Main search endpoint
# ────────────────────────────────────────────────────────────────

def _apply_extra_filters(
    stmt, chunk_ids_or_stmt, *,
    instructor: str | None,
    dvd_id: str | None,
    hide_concepts: bool,
):
    """Apply instructor / DVD / hide-concepts filters to a query."""
    if hide_concepts:
        stmt = stmt.where(func.lower(Chunk.technique_type) != "concept")
    if instructor:
        stmt = stmt.where(func.lower(DVD.instructor) == instructor.lower())
    if dvd_id:
        stmt = stmt.where(Volume.dvd_id == dvd_id)
    return stmt


@router.get("/search", response_model=SearchResponse)
async def search(
    q: str = Query(..., min_length=1),
    position: str | None = Query(None),
    type: str | None = Query(None, alias="type"),
    mode: str = Query("granular", pattern="^(granular|semantic)$"),
    instructor: str | None = Query(None),
    dvd_id: str | None = Query(None),
    hide_concepts: bool = Query(False),
    limit: int = Query(20, le=100),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Search across all indexed BJJ chunks."""
    parsed = parse_query(q)

    if position:
        position = normalize_position(position) or position
    if type:
        type = normalize_category(type) or type

    extra = dict(instructor=instructor, dvd_id=dvd_id, hide_concepts=hide_concepts)

    # ── Path 1: Vector search (primary) ──────────────────────
    vector_resp = await _vector_search(
        q, parsed, mode, position, type, limit, offset, db, **extra
    )
    if vector_resp and vector_resp.results:
        return vector_resp

    # ── Path 2: Structured search (fallback) ─────────────────
    if parsed.is_structured:
        stmt, count_stmt = _build_structured_query(
            parsed, mode, position, type, limit, offset
        )
        stmt = _apply_extra_filters(stmt, None, **extra)
        count_stmt = _apply_extra_filters(count_stmt, None, **extra)

        result = await db.execute(stmt)
        rows = result.unique().all()

        count_result = await db.execute(count_stmt)
        total = count_result.scalar() or 0

        if rows:
            max_score = max(r[1] for r in rows) or 1.0
            results = [
                SearchResult(
                    chunk=_chunk_to_out(chunk),
                    score=round(raw_score / max_score, 4) if max_score > 0 else 0.0,
                )
                for chunk, raw_score in rows
            ]
            return SearchResponse(query=q, results=results, total=total)

    # ── Path 3: Last resort -- raw text ILIKE ─────────────────
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
    if hide_concepts:
        base_where.append(func.lower(Chunk.technique_type) != "concept")

    stmt = (
        select(Chunk)
        .options(joinedload(Chunk.volume).joinedload(Volume.dvd))
        .where(*base_where)
    )
    if position:
        stmt = stmt.where(func.lower(Chunk.position) == position.lower())
    if type:
        stmt = stmt.where(func.lower(Chunk.technique_type) == type.lower())
    if instructor:
        stmt = stmt.join(Volume, Chunk.volume_id == Volume.id).join(DVD, Volume.dvd_id == DVD.id)
        stmt = stmt.where(func.lower(DVD.instructor) == instructor.lower())
    if dvd_id:
        stmt = stmt.join(Volume, Chunk.volume_id == Volume.id) if not instructor else stmt
        stmt = stmt.where(Volume.dvd_id == dvd_id)
    stmt = stmt.order_by(Chunk.created_at.desc()).offset(offset).limit(limit)

    result = await db.execute(stmt)
    chunks = result.scalars().unique().all()

    count_stmt = select(func.count(Chunk.id)).where(*base_where)
    if position:
        count_stmt = count_stmt.where(func.lower(Chunk.position) == position.lower())
    if type:
        count_stmt = count_stmt.where(func.lower(Chunk.technique_type) == type.lower())
    total_result = await db.execute(count_stmt)
    total = total_result.scalar() or 0

    results = [SearchResult(chunk=_chunk_to_out(c), score=0.5) for c in chunks]
    return SearchResponse(query=q, results=results, total=total)


# ────────────────────────────────────────────────────────────────
# Browse and filter endpoints
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
        stmt = stmt.where(func.lower(Chunk.position) == position.lower())
    if type:
        stmt = stmt.where(func.lower(Chunk.technique_type) == type.lower())

    stmt = stmt.order_by(Chunk.position, Chunk.technique).offset(offset).limit(limit)

    result = await db.execute(stmt)
    chunks = result.scalars().unique().all()

    count_stmt = select(func.count(Chunk.id))
    if position:
        count_stmt = count_stmt.where(func.lower(Chunk.position) == position.lower())
    if type:
        count_stmt = count_stmt.where(func.lower(Chunk.technique_type) == type.lower())

    total_result = await db.execute(count_stmt)
    total = total_result.scalar() or 0

    return BrowseResponse(
        position=position,
        technique_type=type,
        results=[_chunk_to_out(c) for c in chunks],
        total=total,
    )


@router.get("/positions")
async def list_positions():
    """Return the fixed canonical positions for filter dropdowns."""
    return [p for p in POSITIONS if p]


@router.get("/technique-types")
async def list_technique_types():
    """Return the fixed canonical technique types for filter checkboxes."""
    return CATEGORIES


@router.get("/instructors")
async def list_instructors(db: AsyncSession = Depends(get_db)):
    """Return distinct instructors for filter dropdown."""
    stmt = (
        select(DVD.instructor)
        .where(DVD.instructor.isnot(None), DVD.instructor != "")
        .distinct()
        .order_by(DVD.instructor)
    )
    result = await db.execute(stmt)
    return [row[0] for row in result.all() if row[0]]


@router.get("/dvds-list")
async def list_dvds_for_filter(db: AsyncSession = Depends(get_db)):
    """Return DVDs with id+title for filter dropdown."""
    stmt = select(DVD.id, DVD.title, DVD.instructor).order_by(DVD.title)
    result = await db.execute(stmt)
    return [
        {"id": str(row[0]), "title": row[1], "instructor": row[2]}
        for row in result.all()
    ]
