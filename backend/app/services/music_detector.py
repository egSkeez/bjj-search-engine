"""Music segment detection for BJJ instructional videos.

Uses audio feature analysis to find sections where background music plays
between technique sections. These music transitions are the natural chapter
boundaries editors baked into the production.

Algorithm:
  Per 0.5-second frame, compute:
    - Zero-crossing rate  (speech high ~0.04-0.15, music low ~0.01-0.06)
    - Spectral flatness   (noise=1.0, tone=0.0; music is tonal so low)
    - RMS energy          (to exclude silence)
  Smooth with a 5-second median window, threshold, then collect runs
  of music >= MIN_MUSIC_DURATION seconds.
"""

import logging
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Tunable thresholds — conservative so we don't falsely split techniques
FRAME_HOP_SEC = 0.5          # analysis frame size
MUSIC_ZCR_MAX = 0.07         # frames with ZCR above this are "speech-like"
MUSIC_FLATNESS_MAX = 0.25    # frames with flatness above this are "noise-like"
SILENCE_RMS_MIN = 0.003      # frames below this are silence (ignore them)
SMOOTH_WINDOW_SEC = 5.0      # median smoothing window
MIN_MUSIC_DURATION = 4.0     # discard detected music shorter than this
MIN_SPEECH_GAP = 1.0         # merge music segments closer than this


def extract_wav(video_path: str, wav_path: str) -> bool:
    """Extract mono 16 kHz WAV from a video file using FFmpeg."""
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-ac", "1",
        "-ar", "16000",
        "-vn",
        "-f", "wav",
        wav_path,
    ]
    result = subprocess.run(cmd, capture_output=True)
    return result.returncode == 0


def detect_music_segments(audio_path: str) -> list[tuple[float, float]]:
    """Return list of (start_sec, end_sec) for music segments in an audio file.

    Accepts WAV or any format supported by librosa (MP4, etc.).
    """
    try:
        import numpy as np
        import librosa
        from scipy.ndimage import median_filter
    except ImportError as e:
        logger.warning("librosa/scipy not installed, music detection unavailable: %s", e)
        return []

    logger.info("Loading audio for music detection: %s", audio_path)
    y, sr = librosa.load(audio_path, sr=16000, mono=True)

    hop_length = int(sr * FRAME_HOP_SEC)

    # --- Feature extraction ---
    zcr = librosa.feature.zero_crossing_rate(y, hop_length=hop_length)[0]
    flatness = librosa.feature.spectral_flatness(y=y, hop_length=hop_length)[0]
    rms = librosa.feature.rms(y=y, hop_length=hop_length)[0]

    frame_times = librosa.frames_to_time(
        np.arange(len(zcr)), sr=sr, hop_length=hop_length
    )

    # --- Per-frame music score (higher = more music-like) ---
    # Invert ZCR and flatness so that low values (music) become high scores
    zcr_score = np.clip(1.0 - zcr / MUSIC_ZCR_MAX, 0, 1)
    flat_score = np.clip(1.0 - flatness / MUSIC_FLATNESS_MAX, 0, 1)
    not_silence = (rms > SILENCE_RMS_MIN).astype(float)

    music_score = zcr_score * flat_score * not_silence

    # --- Smooth ---
    smooth_frames = max(1, int(SMOOTH_WINDOW_SEC / FRAME_HOP_SEC))
    music_smooth = median_filter(music_score, size=smooth_frames)

    # --- Binary classification (threshold at 0.5 of combined score) ---
    is_music = music_smooth >= 0.5

    # --- Collect contiguous music runs ---
    segments: list[tuple[float, float]] = []
    in_music = False
    seg_start = 0.0

    for i, flag in enumerate(is_music):
        t = frame_times[i]
        if flag and not in_music:
            seg_start = t
            in_music = True
        elif not flag and in_music:
            seg_end = t
            if seg_end - seg_start >= MIN_MUSIC_DURATION:
                segments.append((seg_start, seg_end))
            in_music = False

    if in_music:
        seg_end = frame_times[-1]
        if seg_end - seg_start >= MIN_MUSIC_DURATION:
            segments.append((seg_start, seg_end))

    # --- Merge segments with tiny speech gaps between them ---
    merged: list[tuple[float, float]] = []
    for seg in segments:
        if merged and seg[0] - merged[-1][1] < MIN_SPEECH_GAP:
            merged[-1] = (merged[-1][0], seg[1])
        else:
            merged.append(seg)

    logger.info(
        "Detected %d music segments (total %.0fs) in %s",
        len(merged),
        sum(e - s for s, e in merged),
        Path(audio_path).name,
    )
    return merged


def detect_music_from_video(video_path: str) -> list[tuple[float, float]]:
    """Extract audio from video then detect music segments.

    Returns list of (start_sec, end_sec) music segment tuples.
    Returns empty list if FFmpeg or librosa is unavailable.
    """
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        wav_path = tmp.name

    try:
        ok = extract_wav(video_path, wav_path)
        if not ok:
            logger.warning("FFmpeg failed to extract audio from %s", video_path)
            return []
        return detect_music_segments(wav_path)
    finally:
        Path(wav_path).unlink(missing_ok=True)


def music_to_chunk_boundaries(
    music_segments: list[tuple[float, float]],
    video_duration: float,
) -> list[tuple[float, float]]:
    """Convert music segments into technique section windows.

    Each section starts at the *beginning* of its preceding music intro
    so the video clip plays the music first, then the technique content.
    Returns list of (section_start, section_end) tuples.
    """
    if not music_segments:
        return [(0.0, video_duration)]

    sections: list[tuple[float, float]] = []

    # Before first music — content before any music plays
    if music_segments[0][0] > 1.0:
        sections.append((0.0, music_segments[0][0]))

    # Each music segment introduces the next technique section.
    # Section = [music_start, next_music_start) or [music_start, video_end)
    for i, (m_start, m_end) in enumerate(music_segments):
        if i + 1 < len(music_segments):
            section_end = music_segments[i + 1][0]
        else:
            section_end = video_duration

        # Only include if there's meaningful speech content after the music
        speech_duration = section_end - m_end
        if speech_duration > 5.0:
            sections.append((m_start, section_end))

    return sections
