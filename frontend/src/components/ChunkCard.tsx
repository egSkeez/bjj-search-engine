"use client";

import { useState } from "react";
import { formatTimestamp, type ChunkResult } from "@/lib/api";
import VideoPlayer from "./VideoPlayer";

const TYPE_COLORS: Record<string, string> = {
  submission: "bg-red-900/50 text-red-300 border-red-800",
  sweep: "bg-green-900/50 text-green-300 border-green-800",
  "guard pass": "bg-blue-900/50 text-blue-300 border-blue-800",
  "guard retention": "bg-teal-900/50 text-teal-300 border-teal-800",
  escape: "bg-yellow-900/50 text-yellow-300 border-yellow-800",
  takedown: "bg-purple-900/50 text-purple-300 border-purple-800",
  counter: "bg-pink-900/50 text-pink-300 border-pink-800",
  control: "bg-orange-900/50 text-orange-300 border-orange-800",
  concept: "bg-gray-800/50 text-gray-300 border-gray-700",
};

function RelevanceBar({ score }: { score: number }) {
  const pct = Math.round(score * 100);
  let label: string;
  let barColor: string;
  let filled: number;

  if (score >= 0.85) {
    label = "Exact match";  barColor = "bg-green-500"; filled = 5;
  } else if (score >= 0.65) {
    label = "Strong match"; barColor = "bg-bjj-500";   filled = 4;
  } else if (score >= 0.45) {
    label = "Good match";   barColor = "bg-yellow-500"; filled = 3;
  } else if (score >= 0.25) {
    label = "Related";      barColor = "bg-orange-500"; filled = 2;
  } else {
    label = "Loose";        barColor = "bg-gray-600";   filled = 1;
  }

  return (
    <div className="flex flex-col items-end gap-1" title={`Relevance: ${pct}%`}>
      <span className="text-xs text-gray-500">{label}</span>
      <div className="flex gap-0.5">
        {[1, 2, 3, 4, 5].map((n) => (
          <div
            key={n}
            className={`w-1.5 rounded-full transition-colors ${
              n <= filled ? barColor : "bg-gray-800"
            }`}
            style={{ height: `${4 + n * 2}px` }}
          />
        ))}
      </div>
    </div>
  );
}

interface ChunkCardProps {
  chunk: ChunkResult;
  score?: number;
}

export default function ChunkCard({ chunk, score }: ChunkCardProps) {
  const [showPlayer, setShowPlayer] = useState(false);
  const [showTranscript, setShowTranscript] = useState(false);

  const typeColor = chunk.technique_type
    ? TYPE_COLORS[chunk.technique_type] || TYPE_COLORS.concept
    : TYPE_COLORS.concept;

  const playerTitle = chunk.technique || "Untitled technique";
  const playerSubtitle = [chunk.dvd_title, chunk.volume_name, chunk.instructor]
    .filter(Boolean)
    .join(" / ");

  const hasKeyPoints = chunk.key_points && chunk.key_points.length > 0;

  return (
    <>
      <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden hover:border-gray-700 transition-colors">
        {/* Header */}
        <div className="p-5">
          <div className="flex items-start justify-between gap-4">
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 flex-wrap mb-1">
                <h3 className="text-white font-semibold text-base truncate">
                  {chunk.technique || "Untitled technique"}
                </h3>
                {chunk.technique_type && (
                  <span className={`inline-flex px-2 py-0.5 text-xs font-medium rounded border ${typeColor}`}>
                    {chunk.technique_type}
                  </span>
                )}
                {chunk.chunk_type === "semantic" && (
                  <span className="inline-flex items-center gap-1 px-2 py-0.5 text-xs font-medium rounded border bg-violet-900/30 text-violet-300 border-violet-800">
                    <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19V6l12-3v13M9 19c0 1.105-1.343 2-3 2s-3-.895-3-2 1.343-2 3-2 3 .895 3 2zm12-3c0 1.105-1.343 2-3 2s-3-.895-3-2 1.343-2 3-2 3 .895 3 2zM9 10l12-3" />
                    </svg>
                    technique
                  </span>
                )}
              </div>

              {chunk.position && (
                <p className="text-bjj-400 text-sm font-medium mb-2">{chunk.position}</p>
              )}

              {chunk.description && (
                <p className="text-gray-300 text-sm mb-3 leading-relaxed">{chunk.description}</p>
              )}

              {chunk.aliases && chunk.aliases.length > 0 && (
                <div className="flex gap-1.5 flex-wrap mb-3">
                  {chunk.aliases.map((alias, i) => (
                    <span key={i} className="text-xs text-gray-500 bg-gray-800 px-2 py-0.5 rounded">
                      {alias}
                    </span>
                  ))}
                </div>
              )}
            </div>

            <div className="flex flex-col items-end gap-2 shrink-0">
              {score !== undefined && score > 0 && (
                <RelevanceBar score={score} />
              )}
              <button
                onClick={() => setShowPlayer(true)}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-bjj-300 bg-bjj-900/30 border border-bjj-800 rounded-lg hover:bg-bjj-900/50 hover:text-bjj-200 transition-colors"
                title="Play this segment"
              >
                <svg className="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 24 24">
                  <path d="M8 5v14l11-7z" />
                </svg>
                Play
              </button>
            </div>
          </div>

          {/* Key Points */}
          {hasKeyPoints && (
            <div className="mt-3 bg-gray-800/50 rounded-lg p-3 border border-gray-700/50">
              <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">
                Coach&apos;s Key Points
              </p>
              <ul className="space-y-1.5">
                {chunk.key_points!.map((point, i) => (
                  <li key={i} className="flex gap-2 text-sm text-gray-300 leading-relaxed">
                    <span className="text-bjj-500 font-bold shrink-0 mt-0.5">›</span>
                    <span>{point}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center gap-3 text-xs text-gray-500 px-5 py-3 border-t border-gray-800 bg-gray-900/50">
          <button
            onClick={() => setShowPlayer(true)}
            className="font-mono bg-gray-800 px-2 py-1 rounded text-bjj-300 hover:bg-gray-700 hover:text-bjj-200 transition-colors"
            title="Play from this timestamp"
          >
            {formatTimestamp(chunk.start_time)}
          </button>
          {chunk.dvd_title && <span className="truncate">{chunk.dvd_title}</span>}
          {chunk.volume_name && (
            <>
              <span className="text-gray-700">/</span>
              <span className="truncate">{chunk.volume_name}</span>
            </>
          )}
          {chunk.instructor && (
            <>
              <span className="text-gray-700">-</span>
              <span>{chunk.instructor}</span>
            </>
          )}
          <div className="ml-auto">
            <button
              onClick={() => setShowTranscript(!showTranscript)}
              className="text-gray-600 hover:text-gray-400 transition-colors text-xs"
            >
              {showTranscript ? "Hide transcript" : "Show transcript"}
            </button>
          </div>
        </div>

        {/* Expandable transcript */}
        {showTranscript && (
          <div className="px-5 py-4 border-t border-gray-800 bg-gray-950">
            <p className="text-xs text-gray-600 uppercase tracking-wider mb-2 font-semibold">Transcript</p>
            <p className="text-gray-400 text-sm leading-relaxed whitespace-pre-wrap">{chunk.text}</p>
          </div>
        )}
      </div>

      {showPlayer && (
        <VideoPlayer
          volumeId={chunk.volume_id}
          startTime={chunk.start_time}
          endTime={chunk.end_time}
          title={playerTitle}
          subtitle={playerSubtitle}
          onClose={() => setShowPlayer(false)}
        />
      )}
    </>
  );
}
