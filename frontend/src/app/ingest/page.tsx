"use client";

import { useEffect, useRef, useState } from "react";
import IngestionStatus from "@/components/IngestionStatus";
import { createIngestJob, getIngestJobs, type IngestJob } from "@/lib/api";

export default function IngestPage() {
  const [dvdTitle, setDvdTitle] = useState("");
  const [volumeName, setVolumeName] = useState("");
  const [instructor, setInstructor] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [activeJobIds, setActiveJobIds] = useState<string[]>([]);
  const [pastJobs, setPastJobs] = useState<IngestJob[]>([]);
  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    getIngestJobs()
      .then(setPastJobs)
      .catch(() => {});
  }, []);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!file || !dvdTitle || !volumeName) return;

    setSubmitting(true);
    try {
      const job = await createIngestJob(file, dvdTitle, volumeName, instructor || undefined);
      setActiveJobIds((prev) => [job.id, ...prev]);
      setDvdTitle("");
      setVolumeName("");
      setInstructor("");
      setFile(null);
      if (fileInputRef.current) fileInputRef.current.value = "";
    } catch (err) {
      alert(`Failed to start ingestion: ${err}`);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div>
      <h1 className="text-3xl font-bold text-white mb-2">Ingest Video</h1>
      <p className="text-gray-500 mb-8">
        Upload a BJJ instructional video to transcribe, analyze, and index it for search.
      </p>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <form onSubmit={handleSubmit} className="space-y-5">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">DVD Title</label>
            <input
              type="text"
              value={dvdTitle}
              onChange={(e) => setDvdTitle(e.target.value)}
              placeholder="e.g. Guard Mastery"
              required
              className="w-full bg-gray-900 border border-gray-700 rounded-lg px-4 py-2.5 text-white placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-bjj-500"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">Volume Name</label>
            <input
              type="text"
              value={volumeName}
              onChange={(e) => setVolumeName(e.target.value)}
              placeholder="e.g. Volume 1 - Closed Guard"
              required
              className="w-full bg-gray-900 border border-gray-700 rounded-lg px-4 py-2.5 text-white placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-bjj-500"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">Instructor</label>
            <input
              type="text"
              value={instructor}
              onChange={(e) => setInstructor(e.target.value)}
              placeholder="e.g. Gordon Ryan"
              className="w-full bg-gray-900 border border-gray-700 rounded-lg px-4 py-2.5 text-white placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-bjj-500"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">Video File</label>
            <input
              ref={fileInputRef}
              type="file"
              accept=".mp4,.mkv,.avi,.mov"
              onChange={(e) => setFile(e.target.files?.[0] || null)}
              required
              className="w-full text-sm text-gray-400 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border file:border-gray-700 file:text-sm file:font-medium file:bg-gray-800 file:text-gray-300 hover:file:bg-gray-700 file:cursor-pointer"
            />
          </div>

          <button
            type="submit"
            disabled={submitting || !file || !dvdTitle || !volumeName}
            className="w-full py-3 px-4 bg-bjj-600 hover:bg-bjj-700 disabled:bg-gray-700 disabled:text-gray-500 text-white font-medium rounded-lg transition-colors"
          >
            {submitting ? "Uploading..." : "Start Ingestion"}
          </button>
        </form>

        <div className="space-y-4">
          <h2 className="text-lg font-semibold text-gray-200">Processing Jobs</h2>

          {activeJobIds.map((id) => (
            <IngestionStatus
              key={id}
              jobId={id}
              onComplete={() => {
                getIngestJobs().then(setPastJobs).catch(() => {});
              }}
            />
          ))}

          {pastJobs.length > 0 && (
            <div className="space-y-2 mt-6">
              <h3 className="text-sm font-medium text-gray-400">Recent Jobs</h3>
              {pastJobs.slice(0, 10).map((job) => (
                <div
                  key={job.id}
                  className={`flex items-center justify-between px-4 py-3 rounded-lg border text-sm ${
                    job.status === "complete"
                      ? "bg-green-900/10 border-green-900"
                      : job.status === "failed"
                        ? "bg-red-900/10 border-red-900"
                        : "bg-gray-900 border-gray-800"
                  }`}
                >
                  <div>
                    <span className="text-white">{job.dvd_title}</span>
                    <span className="text-gray-600 mx-1">-</span>
                    <span className="text-gray-400">{job.volume_name}</span>
                  </div>
                  <span
                    className={`text-xs font-medium capitalize ${
                      job.status === "complete"
                        ? "text-green-400"
                        : job.status === "failed"
                          ? "text-red-400"
                          : "text-gray-500"
                    }`}
                  >
                    {job.status}
                  </span>
                </div>
              ))}
            </div>
          )}

          {activeJobIds.length === 0 && pastJobs.length === 0 && (
            <p className="text-gray-600 text-sm py-4">
              No ingestion jobs yet. Upload a video to get started.
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
