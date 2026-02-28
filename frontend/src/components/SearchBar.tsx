"use client";

import { useCallback, useEffect, useRef, useState } from "react";

interface SearchBarProps {
  onSearch: (query: string) => void;
  onClear?: () => void;
  initialQuery?: string;
  placeholder?: string;
}

export default function SearchBar({ onSearch, onClear, initialQuery = "", placeholder }: SearchBarProps) {
  const [query, setQuery] = useState(initialQuery);
  const inputRef = useRef<HTMLInputElement>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout>>();

  const debouncedSearch = useCallback(
    (value: string) => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = setTimeout(() => {
        if (value.trim()) {
          onSearch(value.trim());
        } else {
          onClear?.();
        }
      }, 300);
    },
    [onSearch, onClear]
  );

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        inputRef.current?.focus();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  return (
    <div className="relative w-full max-w-2xl mx-auto">
      <div className="relative">
        <svg
          className="absolute left-4 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-500"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
        </svg>
        <input
          ref={inputRef}
          type="text"
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            debouncedSearch(e.target.value);
          }}
          onKeyDown={(e) => {
            if (e.key === "Escape") {
              if (debounceRef.current) clearTimeout(debounceRef.current);
              setQuery("");
              onClear?.();
            }
            if (e.key === "Enter" && query.trim()) {
              if (debounceRef.current) clearTimeout(debounceRef.current);
              onSearch(query.trim());
            }
          }}
          placeholder={placeholder || 'Search techniques... (e.g. "omoplata from closed guard")'}
          className="w-full pl-12 pr-20 py-4 bg-gray-900 border border-gray-700 rounded-xl text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-bjj-500 focus:border-transparent text-lg transition-shadow"
        />
        {query ? (
          <button
            onClick={() => {
              if (debounceRef.current) clearTimeout(debounceRef.current);
              setQuery("");
              onClear?.();
              inputRef.current?.focus();
            }}
            className="absolute right-4 top-1/2 -translate-y-1/2 p-1 text-gray-500 hover:text-gray-300 transition-colors"
            aria-label="Clear search"
          >
            <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        ) : (
          <kbd className="absolute right-4 top-1/2 -translate-y-1/2 hidden sm:inline-flex items-center gap-1 px-2 py-1 text-xs text-gray-500 bg-gray-800 border border-gray-700 rounded">
            <span className="text-xs">Ctrl</span>K
          </kbd>
        )}
      </div>
    </div>
  );
}
