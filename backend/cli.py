"""CLI entry point for ingesting BJJ instructional videos."""

import json
import logging
from pathlib import Path

import click
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """BJJ Instructional Search Engine CLI."""
    pass


@cli.command()
@click.argument("video_path", type=click.Path(exists=True))
@click.option("--title", required=True, help="DVD title (e.g. 'Guard Mastery')")
@click.option("--volume", required=True, help="Volume name (e.g. 'Vol 1 - Closed Guard')")
@click.option("--instructor", default=None, help="Instructor name")
@click.option("--skip-transcribe", is_flag=True, help="Skip transcription (use existing transcript)")
@click.option("--skip-tag", is_flag=True, help="Skip LLM tagging")
@click.option("--skip-embed", is_flag=True, help="Skip embedding generation")
def ingest(video_path: str, title: str, volume: str, instructor: str | None, skip_transcribe: bool, skip_tag: bool, skip_embed: bool):
    """Ingest a video file: transcribe, chunk, tag, and embed."""
    from app.config import settings

    data_dir = Path(settings.data_dir)
    transcripts_dir = data_dir / "transcripts"
    chunks_dir = data_dir / "chunks"
    transcripts_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    safe_name = f"{title}_{volume}".replace(" ", "_").replace("/", "_")
    transcript_path = transcripts_dir / f"{safe_name}.json"
    chunks_path = chunks_dir / f"{safe_name}.json"

    # --- Transcription ---
    if skip_transcribe and transcript_path.exists():
        logger.info("Loading existing transcription from %s", transcript_path)
        from app.services.transcription import load_transcription
        segments = load_transcription(str(transcript_path))
    else:
        from app.services.transcription import transcribe_video
        segments = transcribe_video(video_path, output_path=str(transcript_path))

    logger.info("Got %d segments", len(segments))

    # --- Chunking ---
    from app.services.chunker import chunk_segments
    chunks = chunk_segments(segments, dvd_title=title, volume_name=volume)
    logger.info("Created %d chunks", len(chunks))

    with open(chunks_path, "w", encoding="utf-8") as f:
        json.dump(chunks, f, indent=2)
    logger.info("Saved chunks to %s", chunks_path)

    # --- Tagging ---
    if not skip_tag:
        from app.services.tagger import tag_chunks
        chunks = tag_chunks(chunks)
        with open(chunks_path, "w", encoding="utf-8") as f:
            json.dump(chunks, f, indent=2)
        logger.info("Tagged and saved %d chunks", len(chunks))
    else:
        logger.info("Skipping tagging")

    # --- Embedding ---
    if not skip_embed:
        from app.services.embedder import embed_chunks
        from app.services.vector_store import upsert_chunks
        chunks = embed_chunks(chunks)
        upsert_chunks(chunks)
        with open(chunks_path, "w", encoding="utf-8") as f:
            json.dump(chunks, f, indent=2)
        logger.info("Embedded and indexed %d chunks", len(chunks))
    else:
        logger.info("Skipping embedding")

    click.echo(f"\nDone! Processed {len(chunks)} chunks from '{title} - {volume}'")


@cli.command()
@click.argument("chunks_path", type=click.Path(exists=True))
def tag(chunks_path: str):
    """Run OpenAI tagging on an existing chunks JSON file."""
    with open(chunks_path, "r", encoding="utf-8") as f:
        chunks = json.load(f)

    from app.services.tagger import tag_chunks
    chunks = tag_chunks(chunks)

    with open(chunks_path, "w", encoding="utf-8") as f:
        json.dump(chunks, f, indent=2)

    click.echo(f"Tagged {len(chunks)} chunks")


@cli.command()
@click.argument("chunks_path", type=click.Path(exists=True))
def embed(chunks_path: str):
    """Generate embeddings and index an existing chunks JSON file."""
    with open(chunks_path, "r", encoding="utf-8") as f:
        chunks = json.load(f)

    from app.services.embedder import embed_chunks
    from app.services.vector_store import upsert_chunks

    chunks = embed_chunks(chunks)
    upsert_chunks(chunks)

    with open(chunks_path, "w", encoding="utf-8") as f:
        json.dump(chunks, f, indent=2)

    click.echo(f"Embedded and indexed {len(chunks)} chunks")


if __name__ == "__main__":
    cli()
