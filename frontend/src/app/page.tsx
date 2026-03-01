"use client";

import { useCallback, useState } from "react";
import SearchBar from "@/components/SearchBar";
import SearchResults from "@/components/SearchResults";
import FilterSidebar from "@/components/FilterSidebar";
import { search, type SearchResponse, type SearchMode } from "@/lib/api";

const MODE_CONFIG: Record<SearchMode, { label: string; description: string; icon: string }> = {
  granular: {
    label: "Segments",
    description: "Short clips - precise moment-level results",
    icon: "M4 6h16M4 10h16M4 14h16M4 18h16",
  },
  semantic: {
    label: "Techniques",
    description: "Complete technique sections detected by music breaks",
    icon: "M9 19V6l12-3v13M9 19c0 1.105-1.343 2-3 2s-3-.895-3-2 1.343-2 3-2 3 .895 3 2zm12-3c0 1.105-1.343 2-3 2s-3-.895-3-2 1.343-2 3-2 3 .895 3 2zM9 10l12-3",
  },
};

export default function HomePage() {
  const [results, setResults] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [currentQuery, setCurrentQuery] = useState("");
  const [position, setPosition] = useState<string | null>(null);
  const [type, setType] = useState<string | null>(null);
  const [instructor, setInstructor] = useState<string | null>(null);
  const [dvdId, setDvdId] = useState<string | null>(null);
  const [hideConcepts, setHideConcepts] = useState(false);
  const [mode, setMode] = useState<SearchMode>("granular");

  const doSearch = useCallback(
    async (
      q: string,
      overrides?: {
        pos?: string | null;
        typ?: string | null;
        mod?: SearchMode;
        inst?: string | null;
        dvd?: string | null;
        hide?: boolean;
      }
    ) => {
      setLoading(true);
      setCurrentQuery(q);
      try {
        const data = await search(q, {
          position: (overrides?.pos ?? position) || undefined,
          type: (overrides?.typ ?? type) || undefined,
          mode: overrides?.mod ?? mode,
          instructor: (overrides?.inst ?? instructor) || undefined,
          dvd_id: (overrides?.dvd ?? dvdId) || undefined,
          hide_concepts: overrides?.hide ?? hideConcepts,
        });
        setResults(data);
      } catch {
        setResults(null);
      } finally {
        setLoading(false);
      }
    },
    [position, type, mode, instructor, dvdId, hideConcepts]
  );

  const reSearch = (overrides: Record<string, unknown>) => {
    if (currentQuery) doSearch(currentQuery, overrides as never);
  };

  return (
    <div>
      <div className="text-center mb-10 pt-8">
        <h1 className="text-4xl font-bold text-white mb-3">
          <span className="text-bjj-500">BJJ</span> Instructional Search
        </h1>
        <p className="text-gray-500 text-lg max-w-xl mx-auto">
          Search across your entire instructional library. Find the exact DVD, volume, and timestamp for any technique.
        </p>
      </div>

      <SearchBar
        onSearch={(q) => doSearch(q)}
        onClear={() => {
          setResults(null);
          setCurrentQuery("");
        }}
      />

      {/* Search mode toggle */}
      <div className="flex items-center justify-center gap-2 mt-4">
        {(Object.entries(MODE_CONFIG) as [SearchMode, typeof MODE_CONFIG[SearchMode]][]).map(([key, cfg]) => (
          <button
            key={key}
            onClick={() => {
              setMode(key);
              reSearch({ mod: key });
            }}
            title={cfg.description}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium border transition-all ${
              mode === key
                ? "bg-bjj-900/60 border-bjj-600 text-bjj-300"
                : "bg-gray-900/40 border-gray-700 text-gray-500 hover:border-gray-500 hover:text-gray-300"
            }`}
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d={cfg.icon} />
            </svg>
            {cfg.label}
          </button>
        ))}
        <span className="text-xs text-gray-600 ml-2 hidden sm:inline">
          {MODE_CONFIG[mode].description}
        </span>
      </div>

      <div className="mt-10 flex gap-8">
        <aside className="hidden lg:block w-56 shrink-0">
          <FilterSidebar
            selectedPosition={position}
            selectedType={type}
            selectedInstructor={instructor}
            selectedDvdId={dvdId}
            hideConcepts={hideConcepts}
            onPositionChange={(p) => { setPosition(p); reSearch({ pos: p }); }}
            onTypeChange={(t) => { setType(t); reSearch({ typ: t }); }}
            onInstructorChange={(i) => { setInstructor(i); reSearch({ inst: i }); }}
            onDvdIdChange={(d) => { setDvdId(d); reSearch({ dvd: d }); }}
            onHideConceptsChange={(h) => { setHideConcepts(h); reSearch({ hide: h }); }}
          />
        </aside>

        <div className="flex-1 min-w-0">
          {loading ? (
            <div className="space-y-3">
              {[...Array(3)].map((_, i) => (
                <div key={i} className="bg-gray-900 border border-gray-800 rounded-lg p-5 animate-pulse">
                  <div className="h-5 bg-gray-800 rounded w-64 mb-3"></div>
                  <div className="h-3 bg-gray-800 rounded w-32 mb-3"></div>
                  <div className="h-3 bg-gray-800 rounded w-full"></div>
                </div>
              ))}
            </div>
          ) : results ? (
            <SearchResults results={results.results} total={results.total} query={results.query} />
          ) : null}
        </div>
      </div>
    </div>
  );
}
