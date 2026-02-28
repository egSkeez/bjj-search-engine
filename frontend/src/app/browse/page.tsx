"use client";

import { useCallback, useEffect, useState } from "react";
import ChunkCard from "@/components/ChunkCard";
import { browse, getPositions, getTechniqueTypes, type ChunkResult } from "@/lib/api";

export default function BrowsePage() {
  const [positions, setPositions] = useState<string[]>([]);
  const [types, setTypes] = useState<string[]>([]);
  const [selectedPosition, setSelectedPosition] = useState<string | null>(null);
  const [selectedType, setSelectedType] = useState<string | null>(null);
  const [results, setResults] = useState<ChunkResult[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    getPositions().then(setPositions).catch(() => {});
    getTechniqueTypes().then(setTypes).catch(() => {});
  }, []);

  const doBrowse = useCallback(async (pos: string | null, typ: string | null) => {
    setLoading(true);
    try {
      const data = await browse({
        position: pos || undefined,
        type: typ || undefined,
        limit: 50,
      });
      setResults(data.results);
      setTotal(data.total);
    } catch {
      setResults([]);
      setTotal(0);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (selectedPosition || selectedType) {
      doBrowse(selectedPosition, selectedType);
    }
  }, [selectedPosition, selectedType, doBrowse]);

  return (
    <div>
      <h1 className="text-3xl font-bold text-white mb-2">Browse Techniques</h1>
      <p className="text-gray-500 mb-8">
        Explore techniques by position and type across all your instructionals.
      </p>

      <div className="flex gap-8">
        <aside className="w-64 shrink-0 space-y-6">
          <div>
            <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-3">
              Positions
            </h3>
            <div className="space-y-1">
              <button
                onClick={() => setSelectedPosition(null)}
                className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-colors ${
                  !selectedPosition ? "bg-bjj-900/50 text-bjj-300" : "text-gray-400 hover:text-gray-200 hover:bg-gray-800"
                }`}
              >
                All positions
              </button>
              {positions.map((p) => (
                <button
                  key={p}
                  onClick={() => setSelectedPosition(p)}
                  className={`w-full text-left px-3 py-2 rounded-lg text-sm transition-colors ${
                    selectedPosition === p ? "bg-bjj-900/50 text-bjj-300" : "text-gray-400 hover:text-gray-200 hover:bg-gray-800"
                  }`}
                >
                  {p}
                </button>
              ))}
            </div>
          </div>

          <div>
            <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-3">
              Technique Type
            </h3>
            <div className="flex flex-wrap gap-2">
              {types.map((t) => (
                <button
                  key={t}
                  onClick={() => setSelectedType(selectedType === t ? null : t)}
                  className={`px-3 py-1.5 rounded-full text-xs font-medium border transition-colors capitalize ${
                    selectedType === t
                      ? "bg-bjj-900/50 text-bjj-300 border-bjj-700"
                      : "text-gray-400 border-gray-700 hover:border-gray-600"
                  }`}
                >
                  {t}
                </button>
              ))}
            </div>
          </div>
        </aside>

        <div className="flex-1 min-w-0">
          {!selectedPosition && !selectedType ? (
            <div className="text-center py-16">
              <p className="text-gray-500">Select a position or technique type to browse</p>
            </div>
          ) : loading ? (
            <div className="space-y-3">
              {[...Array(3)].map((_, i) => (
                <div key={i} className="bg-gray-900 border border-gray-800 rounded-lg p-5 animate-pulse">
                  <div className="h-5 bg-gray-800 rounded w-64 mb-3"></div>
                  <div className="h-3 bg-gray-800 rounded w-full"></div>
                </div>
              ))}
            </div>
          ) : (
            <>
              <p className="text-sm text-gray-500 mb-4">{total} techniques found</p>
              <div className="space-y-3">
                {results.map((chunk) => (
                  <ChunkCard key={chunk.id} chunk={chunk} />
                ))}
              </div>
              {results.length === 0 && (
                <p className="text-center text-gray-500 py-8">No techniques found for this filter</p>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
