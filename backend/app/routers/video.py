import os
import stat
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Volume

router = APIRouter(tags=["video"])

CHUNK_SIZE = 1024 * 1024  # 1 MB


@router.get("/volumes/{volume_id}/video")
async def stream_video(
    volume_id: UUID,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Stream a volume's video file with HTTP range request support."""
    stmt = select(Volume).where(Volume.id == volume_id)
    result = await db.execute(stmt)
    volume = result.scalar_one_or_none()
    if not volume or not volume.file_path:
        raise HTTPException(status_code=404, detail="Volume or video file not found")

    file_path = volume.file_path
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail=f"Video file not found on disk")

    file_size = os.stat(file_path)[stat.ST_SIZE]
    content_type = "video/mp4"
    if file_path.lower().endswith(".mkv"):
        content_type = "video/x-matroska"
    elif file_path.lower().endswith(".avi"):
        content_type = "video/x-msvideo"

    range_header = request.headers.get("range")

    if range_header:
        range_spec = range_header.replace("bytes=", "")
        range_start_str, range_end_str = range_spec.split("-", 1)
        range_start = int(range_start_str)
        range_end = int(range_end_str) if range_end_str else min(range_start + CHUNK_SIZE - 1, file_size - 1)
        range_end = min(range_end, file_size - 1)
        content_length = range_end - range_start + 1

        def iter_range():
            with open(file_path, "rb") as f:
                f.seek(range_start)
                remaining = content_length
                while remaining > 0:
                    read_size = min(CHUNK_SIZE, remaining)
                    data = f.read(read_size)
                    if not data:
                        break
                    remaining -= len(data)
                    yield data

        return StreamingResponse(
            iter_range(),
            status_code=206,
            media_type=content_type,
            headers={
                "Content-Range": f"bytes {range_start}-{range_end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(content_length),
            },
        )

    def iter_file():
        with open(file_path, "rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                yield chunk

    return StreamingResponse(
        iter_file(),
        media_type=content_type,
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size),
        },
    )
