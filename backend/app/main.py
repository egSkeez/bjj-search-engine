from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import ingest, library, search, video


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title="BJJ Instructional Search Engine",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(search.router, prefix="/api")
app.include_router(library.router, prefix="/api")
app.include_router(ingest.router, prefix="/api")
app.include_router(video.router, prefix="/api")


@app.get("/api/health")
async def health():
    return {"status": "ok"}
