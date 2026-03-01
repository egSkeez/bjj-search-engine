"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { getBookmarkCount } from "@/lib/bookmarks";

export default function PlaylistBadge() {
  const [count, setCount] = useState(0);

  useEffect(() => {
    setCount(getBookmarkCount());
    const handler = () => setCount(getBookmarkCount());
    window.addEventListener("bookmarks-changed", handler);
    return () => window.removeEventListener("bookmarks-changed", handler);
  }, []);

  return (
    <Link href="/playlist" className="relative text-sm text-gray-400 hover:text-white transition-colors flex items-center gap-1">
      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 5a2 2 0 012-2h10a2 2 0 012 2v16l-7-3.5L5 21V5z" />
      </svg>
      Playlist
      {count > 0 && (
        <span className="absolute -top-1.5 -right-2.5 min-w-[18px] h-[18px] flex items-center justify-center text-[10px] font-bold bg-amber-500 text-black rounded-full px-1">
          {count > 99 ? "99+" : count}
        </span>
      )}
    </Link>
  );
}
