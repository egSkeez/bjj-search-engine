"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import ChunkCard from "@/components/ChunkCard";
import { getDVD, getDVDChunks, type ChunkResult, type DVD } from "@/lib/api";

export default function DVDDetailPage() {
  const params = useParams();
  const id = params.id as string;

  const [dvd, setDVD] = useState<DVD | null>(null);
  const [chunks, setChunks] = useState<ChunkResult[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([getDVD(id), getDVDChunks(id)])
      .then(([d, c]) => {
        setDVD(d);
        setChunks(c);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) {
    return (
      <div className="space-y-4">
        <div className="h-8 bg-gray-800 rounded w-64 animate-pulse"></div>
        <div className="h-4 bg-gray-800 rounded w-48 animate-pulse"></div>
        {[...Array(5)].map((_, i) => (
          <div key={i} className="bg-gray-900 border border-gray-800 rounded-lg p-5 animate-pulse">
            <div className="h-5 bg-gray-800 rounded w-64 mb-3"></div>
            <div className="h-3 bg-gray-800 rounded w-full"></div>
          </div>
        ))}
      </div>
    );
  }

  if (!dvd) {
    return (
      <div className="text-center py-16">
        <p className="text-gray-400">DVD not found</p>
        <Link href="/library" className="text-bjj-500 hover:text-bjj-400 text-sm mt-2 inline-block">
          Back to library
        </Link>
      </div>
    );
  }

  const volumeGroups = chunks.reduce<Record<string, ChunkResult[]>>((acc, chunk) => {
    const vol = chunk.volume_name || "Unknown volume";
    if (!acc[vol]) acc[vol] = [];
    acc[vol].push(chunk);
    return acc;
  }, {});

  return (
    <div>
      <Link href="/library" className="text-bjj-500 hover:text-bjj-400 text-sm mb-4 inline-flex items-center gap-1">
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
        </svg>
        Back to library
      </Link>

      <div className="mb-8 mt-2">
        <h1 className="text-3xl font-bold text-white">{dvd.title}</h1>
        {dvd.instructor && <p className="text-gray-400 text-lg mt-1">{dvd.instructor}</p>}
        <p className="text-gray-600 text-sm mt-2">
          {dvd.volume_count} volume{dvd.volume_count !== 1 ? "s" : ""} &middot;{" "}
          {chunks.length} technique{chunks.length !== 1 ? "s" : ""}
        </p>
      </div>

      {Object.entries(volumeGroups).map(([volumeName, volumeChunks]) => (
        <div key={volumeName} className="mb-8">
          <h2 className="text-xl font-semibold text-gray-200 mb-4 pb-2 border-b border-gray-800">
            {volumeName}
          </h2>
          <div className="space-y-3">
            {volumeChunks.map((chunk) => (
              <ChunkCard key={chunk.id} chunk={chunk} />
            ))}
          </div>
        </div>
      ))}

      {chunks.length === 0 && (
        <p className="text-gray-500 text-center py-8">
          No techniques indexed yet for this DVD.
        </p>
      )}
    </div>
  );
}
