import uuid
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models import IngestJob
from app.schemas import IngestJobOut, IngestRequest

router = APIRouter(tags=["ingest"])


@router.post("/ingest", response_model=IngestJobOut)
async def create_ingest_job(
    file: UploadFile,
    dvd_title: str,
    volume_name: str,
    instructor: str | None = None,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    db: AsyncSession = Depends(get_db),
):
    """Upload a video file and start the ingestion pipeline."""
    upload_dir = Path(settings.data_dir) / "videos"
    upload_dir.mkdir(parents=True, exist_ok=True)

    file_id = str(uuid.uuid4())
    ext = Path(file.filename or "video.mp4").suffix
    file_path = upload_dir / f"{file_id}{ext}"

    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)

    job = IngestJob(
        dvd_title=dvd_title,
        volume_name=volume_name,
        file_path=str(file_path),
        status="queued",
        progress=0.0,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    background_tasks.add_task(
        _run_pipeline,
        job_id=str(job.id),
        file_path=str(file_path),
        dvd_title=dvd_title,
        volume_name=volume_name,
        instructor=instructor,
    )

    return job


@router.get("/ingest/{job_id}/status", response_model=IngestJobOut)
async def get_ingest_status(job_id: str, db: AsyncSession = Depends(get_db)):
    """Check the status of an ingestion job."""
    try:
        uid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid job ID")

    stmt = select(IngestJob).where(IngestJob.id == uid)
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/ingest", response_model=list[IngestJobOut])
async def list_ingest_jobs(db: AsyncSession = Depends(get_db)):
    """List all ingest jobs."""
    stmt = select(IngestJob).order_by(IngestJob.created_at.desc())
    result = await db.execute(stmt)
    return result.scalars().all()


async def _run_pipeline(
    job_id: str,
    file_path: str,
    dvd_title: str,
    volume_name: str,
    instructor: str | None,
):
    """Run the full ingest pipeline as a background task."""
    from app.services.pipeline import run_pipeline

    await run_pipeline(
        job_id=job_id,
        file_path=file_path,
        dvd_title=dvd_title,
        volume_name=volume_name,
        instructor=instructor,
    )
