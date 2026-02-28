"use client";

import { useEffect, useState } from "react";
import { getIngestJobStatus, type IngestJob } from "@/lib/api";

interface IngestionStatusProps {
  jobId: string;
  onComplete?: () => void;
}

const STATUS_LABELS: Record<string, string> = {
  queued: "Queued",
  transcribing: "Transcribing audio...",
  chunking: "Splitting into techniques...",
  tagging: "Analyzing with AI...",
  embedding: "Generating embeddings...",
  indexing: "Indexing in database...",
  complete: "Complete",
  failed: "Failed",
};

export default function IngestionStatus({ jobId, onComplete }: IngestionStatusProps) {
  const [job, setJob] = useState<IngestJob | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const data = await getIngestJobStatus(jobId);
        if (cancelled) return;
        setJob(data);

        if (data.status === "complete") {
          onComplete?.();
          return;
        }
        if (data.status === "failed") return;

        setTimeout(poll, 2000);
      } catch (e) {
        if (!cancelled) setError(String(e));
      }
    }

    poll();
    return () => { cancelled = true; };
  }, [jobId, onComplete]);

  if (error) {
    return (
      <div className="bg-red-900/30 border border-red-800 rounded-lg p-4">
        <p className="text-red-300 text-sm">Error checking status: {error}</p>
      </div>
    );
  }

  if (!job) {
    return (
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-4 animate-pulse">
        <div className="h-4 bg-gray-800 rounded w-48"></div>
      </div>
    );
  }

  const isFailed = job.status === "failed";
  const isComplete = job.status === "complete";

  return (
    <div
      className={`border rounded-lg p-4 ${
        isFailed
          ? "bg-red-900/20 border-red-800"
          : isComplete
            ? "bg-green-900/20 border-green-800"
            : "bg-gray-900 border-gray-800"
      }`}
    >
      <div className="flex items-center justify-between mb-2">
        <div>
          <p className="text-white font-medium text-sm">
            {job.dvd_title} - {job.volume_name}
          </p>
          <p className={`text-xs mt-0.5 ${isFailed ? "text-red-400" : isComplete ? "text-green-400" : "text-gray-400"}`}>
            {STATUS_LABELS[job.status] || job.status}
          </p>
        </div>
        <span className="text-sm font-mono text-gray-500">{Math.round(job.progress)}%</span>
      </div>

      <div className="w-full h-2 bg-gray-800 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all duration-500 ${
            isFailed ? "bg-red-600" : isComplete ? "bg-green-500" : "bg-bjj-500"
          }`}
          style={{ width: `${job.progress}%` }}
        />
      </div>

      {job.error_message && (
        <p className="text-red-400 text-xs mt-2">{job.error_message}</p>
      )}
    </div>
  );
}
