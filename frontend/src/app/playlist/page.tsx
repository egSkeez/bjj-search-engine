"use client";

import { useEffect, useState } from "react";
import { type Bookmark, getBookmarks, removeBookmark, clearBookmarks } from "@/lib/bookmarks";
import ChunkCard from "@/components/ChunkCard";

export default function PlaylistPage() {
  const [bookmarks, setBookmarks] = useState<Bookmark[]>([]);

  useEffect(() => {
    setBookmarks(getBookmarks());
    const handler = () => setBookmarks(getBookmarks());
    window.addEventListener("bookmarks-changed", handler);
    return () => window.removeEventListener("bookmarks-changed", handler);
  }, []);

  const grouped = bookmarks.reduce<Record<string, Bookmark[]>>((acc, b) => {
    const key = b.chunk.dvd_title || "Unknown DVD";
    if (!acc[key]) acc[key] = [];
    acc[key].push(b);
    return acc;
  }, {});

  return (
    <div>
      <div className="flex items-center justify-between mb-8">
        <div>
          <h1 className="text-3xl font-bold text-white">
            <span className="text-amber-500">My</span> Playlist
          </h1>
          <p className="text-gray-500 mt-1">
            {bookmarks.length} saved clip{bookmarks.length !== 1 ? "s" : ""}
          </p>
        </div>
        {bookmarks.length > 0 && (
          <button
            onClick={() => {
              if (confirm("Remove all saved clips from your playlist?")) {
                clearBookmarks();
              }
            }}
            className="px-4 py-2 text-sm text-red-400 border border-red-900 rounded-lg hover:bg-red-900/20 transition-colors"
          >
            Clear all
          </button>
        )}
      </div>

      {bookmarks.length === 0 ? (
        <div className="text-center py-20">
          <svg className="w-16 h-16 mx-auto text-gray-700 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M5 5a2 2 0 012-2h10a2 2 0 012 2v16l-7-3.5L5 21V5z" />
          </svg>
          <p className="text-gray-500 text-lg">No saved clips yet</p>
          <p className="text-gray-600 text-sm mt-2">
            Click the bookmark icon on any search result to save it here
          </p>
        </div>
      ) : (
        <div className="space-y-8">
          {Object.entries(grouped).map(([dvdTitle, items]) => (
            <div key={dvdTitle}>
              <h2 className="text-lg font-semibold text-gray-300 mb-3 flex items-center gap-2">
                <span className="w-1 h-5 bg-amber-500 rounded-full" />
                {dvdTitle}
                <span className="text-xs text-gray-600 font-normal">
                  ({items.length} clip{items.length !== 1 ? "s" : ""})
                </span>
              </h2>
              <div className="space-y-3">
                {items.map((b) => (
                  <div key={b.chunk.id} className="relative group">
                    <ChunkCard chunk={b.chunk} />
                    <button
                      onClick={() => removeBookmark(b.chunk.id)}
                      className="absolute top-3 right-3 opacity-0 group-hover:opacity-100 p-1.5 text-red-400 bg-gray-900/90 border border-red-900 rounded-lg hover:bg-red-900/30 transition-all"
                      title="Remove from playlist"
                    >
                      <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
