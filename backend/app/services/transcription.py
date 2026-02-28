"""Transcription service.

Uses whisper.cpp with Vulkan (AMD GPU) when available, falls back to
faster-whisper on CPU otherwise.

whisper.cpp binary path and model are configured via settings:
  WHISPER_CPP_BIN  - path to whisper-cli.exe (default: C:/whisper.cpp/build/bin/Release/whisper-cli.exe)
  WHISPER_CPP_MODEL - path to GGML model file (default: C:/whisper.cpp/models/ggml-small.en.bin)
  WHISPER_MODEL    - faster-whisper model name for CPU fallback (default: small)
"""

import json
import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path

from app.config import settings

logger = logging.getLogger(__name__)

WHISPER_CPP_BIN = os.environ.get(
    "WHISPER_CPP_BIN",
    r"C:\whisper.cpp\build\bin\Release\whisper-cli.exe",
)
WHISPER_CPP_MODEL = os.environ.get(
    "WHISPER_CPP_MODEL",
    r"C:\whisper.cpp\models\ggml-small.en.bin",
)

_faster_whisper_model = None


def _whisper_cpp_available() -> bool:
    return Path(WHISPER_CPP_BIN).exists() and Path(WHISPER_CPP_MODEL).exists()


def _ffmpeg_available() -> bool:
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def _get_faster_whisper_model():
    global _faster_whisper_model
    if _faster_whisper_model is None:
        from faster_whisper import WhisperModel
        device = "cuda" if _cuda_available() else "cpu"
        compute_type = "float16" if device == "cuda" else "int8"
        logger.info("Loading faster-whisper '%s' on %s (%s)", settings.whisper_model, device, compute_type)
        _faster_whisper_model = WhisperModel(settings.whisper_model, device=device, compute_type=compute_type)
    return _faster_whisper_model


def _cuda_available() -> bool:
    try:
        import torch
        return torch.cuda.is_available()
    except ImportError:
        return False


def _transcribe_with_whisper_cpp(video_path: str) -> list[dict]:
    """Transcribe using whisper.cpp Vulkan binary. Returns list of segment dicts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wav_path = os.path.join(tmpdir, "audio.wav")
        json_base = os.path.join(tmpdir, "out")
        json_path = json_base + ".json"

        # Extract audio as 16kHz mono WAV
        logger.info("Extracting audio from %s", video_path)
        ffmpeg_cmd = [
            "ffmpeg", "-y", "-i", video_path,
            "-ar", "16000", "-ac", "1", "-f", "wav",
            wav_path, "-loglevel", "error",
        ]
        subprocess.run(ffmpeg_cmd, check=True)

        # Run whisper.cpp with Vulkan (device 0 = RX 9070 XT)
        logger.info("Transcribing with whisper.cpp Vulkan (device 0)...")
        whisper_cmd = [
            WHISPER_CPP_BIN,
            "-m", WHISPER_CPP_MODEL,
            "-f", wav_path,
            "-oj",            # output JSON
            "-of", json_base,
            "-l", "en",
            "--device", "0",  # RX 9070 XT
        ]
        result = subprocess.run(whisper_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.warning("whisper.cpp stderr: %s", result.stderr[-500:])
            raise RuntimeError(f"whisper.cpp failed with code {result.returncode}")

        # Parse JSON output
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

    segments = []
    for i, seg in enumerate(data.get("transcription", [])):
        # whisper.cpp timestamps are in format [HH:MM:SS.mmm --> HH:MM:SS.mmm]
        # The JSON has "timestamps": {"from": "00:00:00,000", "to": "00:00:05,160"}
        # and "offsets": {"from": 0, "to": 5160} (milliseconds)
        offsets = seg.get("offsets", {})
        start_ms = offsets.get("from", 0)
        end_ms = offsets.get("to", 0)
        text = seg.get("text", "").strip()
        if text:
            segments.append({
                "id": i,
                "start": round(start_ms / 1000.0, 3),
                "end": round(end_ms / 1000.0, 3),
                "text": text,
            })

    logger.info("whisper.cpp transcription: %d segments", len(segments))
    return segments


def _transcribe_with_faster_whisper(video_path: str) -> list[dict]:
    """Transcribe using faster-whisper (CPU fallback)."""
    model = _get_faster_whisper_model()
    logger.info("Transcribing with faster-whisper (CPU): %s", video_path)
    raw_segments, info = model.transcribe(video_path, language="en", word_timestamps=True)

    segments = []
    for i, seg in enumerate(raw_segments):
        segments.append({
            "id": i,
            "start": round(seg.start, 3),
            "end": round(seg.end, 3),
            "text": seg.text.strip(),
        })
        if (i + 1) % 100 == 0:
            logger.info("  ...processed %d segments (%.0fs)", i + 1, seg.end)

    logger.info("Transcription complete: %d segments, %.0fs", len(segments), info.duration)
    return segments


def transcribe_video(video_path: str, output_path: str | None = None) -> list[dict]:
    """Transcribe a video. Uses whisper.cpp Vulkan if available, else faster-whisper."""
    if _whisper_cpp_available() and _ffmpeg_available():
        logger.info("Using whisper.cpp Vulkan backend (AMD RX 9070 XT)")
        segments = _transcribe_with_whisper_cpp(video_path)
    else:
        logger.info("Using faster-whisper CPU backend")
        segments = _transcribe_with_faster_whisper(video_path)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"segments": segments}, f, indent=2)
        logger.info("Saved transcription to %s", output_path)

    return segments


def load_transcription(path: str) -> list[dict]:
    """Load a previously saved transcription JSON."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["segments"]
