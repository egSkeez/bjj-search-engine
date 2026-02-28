"use client";

import { useEffect, useState } from "react";
import DVDCard from "@/components/DVDCard";
import { listDVDs, type DVD } from "@/lib/api";

export default function LibraryPage() {
  const [dvds, setDVDs] = useState<DVD[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    listDVDs()
      .then(setDVDs)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <div>
      <h1 className="text-3xl font-bold text-white mb-2">DVD Library</h1>
      <p className="text-gray-500 mb-8">
        All your indexed BJJ instructionals in one place.
      </p>

      {loading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="bg-gray-900 border border-gray-800 rounded-lg p-5 animate-pulse">
              <div className="h-6 bg-gray-800 rounded w-48 mb-3"></div>
              <div className="h-4 bg-gray-800 rounded w-32 mb-4"></div>
              <div className="h-3 bg-gray-800 rounded w-24"></div>
            </div>
          ))}
        </div>
      ) : dvds.length === 0 ? (
        <div className="text-center py-16 bg-gray-900 border border-gray-800 rounded-lg">
          <p className="text-gray-400 text-lg mb-2">No DVDs indexed yet</p>
          <p className="text-gray-600 text-sm">
            Use the Ingest page or CLI to add your first instructional.
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {dvds.map((dvd) => (
            <DVDCard key={dvd.id} dvd={dvd} />
          ))}
        </div>
      )}
    </div>
  );
}
