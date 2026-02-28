from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class DVDOut(BaseModel):
    id: UUID
    title: str
    instructor: str | None
    created_at: datetime
    volume_count: int = 0

    model_config = {"from_attributes": True}


class VolumeOut(BaseModel):
    id: UUID
    dvd_id: UUID
    name: str
    file_path: str | None
    duration_seconds: float | None
    created_at: datetime

    model_config = {"from_attributes": True}


class ChunkOut(BaseModel):
    id: UUID
    volume_id: UUID
    start_time: float
    end_time: float
    text: str
    position: str | None
    technique: str | None
    technique_type: str | None
    aliases: list[str] | None
    description: str | None
    key_points: list[str] | None = None
    chunk_type: str = "granular"
    created_at: datetime

    dvd_title: str | None = None
    volume_name: str | None = None
    instructor: str | None = None

    model_config = {"from_attributes": True}


class SearchResult(BaseModel):
    chunk: ChunkOut
    score: float = 0.0


class SearchResponse(BaseModel):
    query: str
    results: list[SearchResult]
    total: int


class IngestRequest(BaseModel):
    dvd_title: str
    volume_name: str
    instructor: str | None = None


class IngestJobOut(BaseModel):
    id: UUID
    dvd_title: str
    volume_name: str
    status: str
    progress: float
    error_message: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class BrowseResponse(BaseModel):
    position: str | None
    technique_type: str | None
    results: list[ChunkOut]
    total: int
