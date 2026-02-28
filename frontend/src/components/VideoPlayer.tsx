"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { formatTimestamp } from "@/lib/api";

interface VideoPlayerProps {
  volumeId: string;
  startTime: number;
  endTime: number;
  title: string;
  subtitle: string;
  onClose: () => void;
}

const TRAIL_BUFFER_SEC = 15;
const EXTEND_STEP_SEC  = 60;   // each press adds 1 minute
const MAX_EXTENSIONS   = 2;    // max 2 presses → +2min total

export default function VideoPlayer({
  volumeId,
  startTime,
  endTime,
  title,
  subtitle,
  onClose,
}: VideoPlayerProps) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const [isReady, setIsReady]       = useState(false);
  const [currentTime, setCurrentTime] = useState(startTime);
  const [isPlaying, setIsPlaying]   = useState(false);
  const [extensions, setExtensions] = useState(0);
  const hasSeenInitialSeek = useRef(false);

  const extraSec  = extensions * EXTEND_STEP_SEC;
  const playUntil = endTime + extraSec + TRAIL_BUFFER_SEC;
  const displayEnd = endTime + extraSec;

  const videoUrl = `/api/volumes/${volumeId}/video`;

  const handleLoadedMetadata = useCallback(() => {
    const video = videoRef.current;
    if (!video) return;
    video.currentTime = startTime;
    setIsReady(true);
  }, [startTime]);

  const handleSeeked = useCallback(() => {
    const video = videoRef.current;
    if (!video || hasSeenInitialSeek.current) return;
    hasSeenInitialSeek.current = true;
    video.play().catch(() => {});
  }, []);

  const handleTimeUpdate = useCallback(() => {
    const video = videoRef.current;
    if (!video) return;
    setCurrentTime(video.currentTime);
    setIsPlaying(!video.paused);

    if (video.currentTime >= playUntil) {
      video.pause();
      setIsPlaying(false);
    }
  }, [playUntil]);

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  useEffect(() => {
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = "";
    };
  }, []);

  const handleReplay = () => {
    const video = videoRef.current;
    if (!video) return;
    setExtensions(0);
    video.currentTime = startTime;
    video.play().catch(() => {});
  };

  const handleExtend = () => {
    if (extensions >= MAX_EXTENSIONS) return;
    const next = extensions + 1;
    setExtensions(next);
    // If video is paused at the old end, resume it
    const video = videoRef.current;
    if (video && video.paused) {
      video.play().catch(() => {});
    }
  };

  const segmentDuration = playUntil - startTime;
  const segmentProgress = Math.min(
    ((currentTime - startTime) / (playUntil - startTime)) * 100,
    100
  );

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/90 backdrop-blur-sm"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="relative w-full max-w-5xl mx-4">
        <button
          onClick={onClose}
          className="absolute -top-10 right-0 text-gray-400 hover:text-white transition-colors text-sm flex items-center gap-1"
        >
          <span>Close</span>
          <kbd className="px-1.5 py-0.5 text-xs bg-gray-800 border border-gray-700 rounded">Esc</kbd>
        </button>

        <div className="bg-gray-900 rounded-xl overflow-hidden border border-gray-800">
          <div className="relative bg-black aspect-video">
            {!isReady && (
              <div className="absolute inset-0 flex items-center justify-center">
                <div className="flex flex-col items-center gap-3">
                  <div className="w-8 h-8 border-2 border-bjj-500 border-t-transparent rounded-full animate-spin" />
                  <p className="text-gray-500 text-sm">Loading video...</p>
                </div>
              </div>
            )}
            <video
              ref={videoRef}
              src={videoUrl}
              onLoadedMetadata={handleLoadedMetadata}
              onSeeked={handleSeeked}
              onTimeUpdate={handleTimeUpdate}
              onPlay={() => setIsPlaying(true)}
              onPause={() => setIsPlaying(false)}
              className="w-full h-full"
              controls
              preload="metadata"
            />
          </div>

          <div className="px-5 py-4">
            {/* Title row */}
            <div className="flex items-start justify-between gap-4 mb-3">
              <div>
                <h3 className="text-white font-semibold text-lg">{title}</h3>
                <p className="text-gray-400 text-sm mt-0.5">{subtitle}</p>
              </div>

              {/* Buttons */}
              <div className="flex items-center gap-2 shrink-0">
                {/* Extend button */}
                <button
                  onClick={handleExtend}
                  disabled={extensions >= MAX_EXTENSIONS}
                  title={
                    extensions >= MAX_EXTENSIONS
                      ? "Maximum extension reached (+2min)"
                      : `Extend clip by 1min (${MAX_EXTENSIONS - extensions} left)`
                  }
                  className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg border transition-colors ${
                    extensions >= MAX_EXTENSIONS
                      ? "text-gray-600 bg-gray-900/20 border-gray-800 cursor-not-allowed"
                      : "text-emerald-300 bg-emerald-900/20 border-emerald-800 hover:bg-emerald-900/40"
                  }`}
                >
                  <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                      d="M12 4v16m8-8H4" />
                  </svg>
                  {extensions >= MAX_EXTENSIONS ? (
                    <span>+2min max</span>
                  ) : (
                    <span>+1min{extensions > 0 ? ` (${extensions}/${MAX_EXTENSIONS})` : ""}</span>
                  )}
                </button>

                {/* Replay button */}
                <button
                  onClick={handleReplay}
                  className="px-3 py-1.5 text-xs font-medium text-bjj-300 bg-bjj-900/30 border border-bjj-800 rounded-lg hover:bg-bjj-900/50 transition-colors"
                >
                  Replay
                </button>
              </div>
            </div>

            {/* Progress bar */}
            <div className="flex items-center gap-3 text-xs text-gray-500">
              <span className="font-mono text-bjj-300">
                {formatTimestamp(currentTime)}
              </span>
              <div className="flex-1 h-1.5 bg-gray-800 rounded-full overflow-hidden">
                <div
                  className="h-full bg-bjj-500 rounded-full transition-all duration-200"
                  style={{ width: `${Math.max(0, segmentProgress)}%` }}
                />
              </div>
              <span className="font-mono text-gray-600">
                {formatTimestamp(playUntil)}
              </span>
            </div>

            {/* Segment info */}
            <div className="flex items-center justify-between mt-2">
              <p className="text-xs text-gray-600">
                {formatTimestamp(startTime)} — {formatTimestamp(playUntil)}
                {" "}({Math.round(segmentDuration)}s)
              </p>
              {extensions > 0 && (
                <p className="text-xs text-emerald-600">
                  +{extensions * EXTEND_STEP_SEC}s extended
                </p>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
