# BJJ Instructional Search Engine

Search across your BJJ instructional DVD library by technique, position, or situation — and get back the exact DVD, volume, and timestamp where that technique is taught.

## Architecture

```
Video files → [Whisper] → [Chunker] → [OpenAI GPT-4o] → [Vector DB] → [Web App]
```

- **Whisper** transcribes audio to timestamped segments
- **Chunker** splits transcripts into technique-sized segments using topic detection
- **OpenAI GPT-4o** tags each chunk with BJJ metadata (position, technique, aliases)
- **Qdrant** stores vector embeddings for semantic search
- **PostgreSQL** stores structured metadata for filtering and browsing
- **Next.js** frontend with dark theme for search, browse, and library views

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+ (for CLI usage)
- Node.js 20+ (for frontend development)
- API key: `OPENAI_API_KEY`

### 1. Environment Setup

```bash
cp .env.example .env
# Edit .env with your API keys
```

### 2. Start Services

```bash
docker-compose up -d db qdrant
```

### 3. Run Database Migrations

```bash
cd backend
pip install -r requirements.txt
alembic upgrade head
```

### 4. Ingest a Video (CLI)

```bash
cd backend
python cli.py ingest path/to/video.mp4 --title "Guard Mastery" --volume "Vol 1 - Closed Guard" --instructor "Gordon Ryan"
```

### 5. Start the Backend

```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

### 6. Start the Frontend

```bash
cd frontend
npm install
npm run dev
```

Visit `http://localhost:3000` to search.

## CLI Commands

```bash
# Full ingest pipeline
python cli.py ingest video.mp4 --title "Title" --volume "Vol 1"

# Skip steps
python cli.py ingest video.mp4 --title "Title" --volume "Vol 1" --skip-transcribe
python cli.py ingest video.mp4 --title "Title" --volume "Vol 1" --skip-tag --skip-embed

# Tag existing chunks
python cli.py tag data/chunks/Title_Vol_1.json

# Embed and index existing chunks
python cli.py embed data/chunks/Title_Vol_1.json
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/ingest` | Upload video + start pipeline |
| GET | `/api/ingest/{id}/status` | Check ingestion progress |
| GET | `/api/search?q=...` | Semantic search |
| GET | `/api/browse?position=...&type=...` | Browse by tags |
| GET | `/api/dvds` | List all DVDs |
| GET | `/api/dvds/{id}/chunks` | DVD table of contents |
| GET | `/api/positions` | List distinct positions |
| GET | `/api/technique-types` | List technique types |

## Project Structure

```
├── docker-compose.yml
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI app
│   │   ├── config.py            # Settings
│   │   ├── database.py          # SQLAlchemy async session
│   │   ├── models.py            # ORM models
│   │   ├── schemas.py           # Pydantic schemas
│   │   ├── routers/             # API endpoints
│   │   └── services/            # Business logic
│   │       ├── transcription.py # Whisper wrapper
│   │       ├── chunker.py       # Topic-based chunking
│   │       ├── tagger.py        # OpenAI GPT-4o tagging
│   │       ├── embedder.py      # OpenAI embeddings
│   │       ├── vector_store.py  # Qdrant client
│   │       └── pipeline.py      # Orchestration
│   └── cli.py                   # CLI entry point
├── frontend/
│   └── src/
│       ├── app/                 # Next.js pages
│       ├── components/          # React components
│       └── lib/api.ts           # API client
└── data/                        # Videos, transcripts, chunks
```
