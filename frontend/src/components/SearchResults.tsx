import type { SearchResult } from "@/lib/api";
import ChunkCard from "./ChunkCard";

interface SearchResultsProps {
  results: SearchResult[];
  total: number;
  query: string;
}

export default function SearchResults({ results, total, query }: SearchResultsProps) {
  if (results.length === 0) {
    return (
      <div className="text-center py-16">
        <p className="text-gray-500 text-lg">
          No results found for &ldquo;{query}&rdquo;
        </p>
        <p className="text-gray-600 text-sm mt-2">
          Try a different search term or broaden your filters
        </p>
      </div>
    );
  }

  return (
    <div>
      <p className="text-sm text-gray-500 mb-4">
        {total} result{total !== 1 ? "s" : ""} for &ldquo;{query}&rdquo;
      </p>
      <div className="space-y-3">
        {results.map((r) => (
          <ChunkCard key={r.chunk.id} chunk={r.chunk} score={r.score} />
        ))}
      </div>
    </div>
  );
}
