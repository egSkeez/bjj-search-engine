from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.database import get_db
from app.models import Chunk, DVD, Volume
from app.schemas import ChunkOut, DVDOut, VolumeOut

router = APIRouter(tags=["library"])


@router.get("/dvds", response_model=list[DVDOut])
async def list_dvds(db: AsyncSession = Depends(get_db)):
    """List all indexed DVDs with volume counts."""
    stmt = (
        select(DVD, func.count(Volume.id).label("volume_count"))
        .outerjoin(Volume, DVD.id == Volume.dvd_id)
        .group_by(DVD.id)
        .order_by(DVD.title)
    )
    result = await db.execute(stmt)
    rows = result.all()

    return [
        DVDOut(
            id=dvd.id,
            title=dvd.title,
            instructor=dvd.instructor,
            created_at=dvd.created_at,
            volume_count=count,
        )
        for dvd, count in rows
    ]


@router.get("/dvds/{dvd_id}", response_model=DVDOut)
async def get_dvd(dvd_id: UUID, db: AsyncSession = Depends(get_db)):
    """Get a single DVD by ID."""
    stmt = (
        select(DVD, func.count(Volume.id).label("volume_count"))
        .outerjoin(Volume, DVD.id == Volume.dvd_id)
        .where(DVD.id == dvd_id)
        .group_by(DVD.id)
    )
    result = await db.execute(stmt)
    row = result.first()
    if not row:
        raise HTTPException(status_code=404, detail="DVD not found")

    dvd, count = row
    return DVDOut(
        id=dvd.id,
        title=dvd.title,
        instructor=dvd.instructor,
        created_at=dvd.created_at,
        volume_count=count,
    )


@router.get("/dvds/{dvd_id}/volumes", response_model=list[VolumeOut])
async def list_volumes(dvd_id: UUID, db: AsyncSession = Depends(get_db)):
    """List all volumes for a DVD."""
    stmt = select(Volume).where(Volume.dvd_id == dvd_id).order_by(Volume.name)
    result = await db.execute(stmt)
    return result.scalars().all()


@router.get("/dvds/{dvd_id}/chunks", response_model=list[ChunkOut])
async def list_dvd_chunks(dvd_id: UUID, db: AsyncSession = Depends(get_db)):
    """List all chunks for a DVD, ordered by volume and timestamp (table of contents)."""
    stmt = (
        select(Chunk)
        .join(Volume, Chunk.volume_id == Volume.id)
        .options(joinedload(Chunk.volume).joinedload(Volume.dvd))
        .where(Volume.dvd_id == dvd_id)
        .order_by(Volume.name, Chunk.start_time)
    )
    result = await db.execute(stmt)
    chunks = result.scalars().unique().all()

    return [
        ChunkOut(
            id=c.id,
            volume_id=c.volume_id,
            start_time=c.start_time,
            end_time=c.end_time,
            text=c.text,
            position=c.position,
            technique=c.technique,
            technique_type=c.technique_type,
            aliases=c.aliases,
            description=c.description,
            key_points=c.key_points,
            chunk_type=c.chunk_type,
            created_at=c.created_at,
            dvd_title=c.volume.dvd.title if c.volume and c.volume.dvd else None,
            volume_name=c.volume.name if c.volume else None,
            instructor=c.volume.dvd.instructor if c.volume and c.volume.dvd else None,
        )
        for c in chunks
    ]
