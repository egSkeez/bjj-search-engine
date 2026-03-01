import type { ChunkResult } from "./api";

const STORAGE_KEY = "bjj_bookmarks";

export interface Bookmark {
  chunk: ChunkResult;
  savedAt: string;
  note?: string;
}

function load(): Bookmark[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function save(bookmarks: Bookmark[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(bookmarks));
}

export function getBookmarks(): Bookmark[] {
  return load();
}

export function isBookmarked(chunkId: string): boolean {
  return load().some((b) => b.chunk.id === chunkId);
}

export function addBookmark(chunk: ChunkResult, note?: string): Bookmark[] {
  const bookmarks = load();
  if (bookmarks.some((b) => b.chunk.id === chunk.id)) return bookmarks;
  const updated = [{ chunk, savedAt: new Date().toISOString(), note }, ...bookmarks];
  save(updated);
  window.dispatchEvent(new Event("bookmarks-changed"));
  return updated;
}

export function removeBookmark(chunkId: string): Bookmark[] {
  const bookmarks = load().filter((b) => b.chunk.id !== chunkId);
  save(bookmarks);
  window.dispatchEvent(new Event("bookmarks-changed"));
  return bookmarks;
}

export function toggleBookmark(chunk: ChunkResult): boolean {
  if (isBookmarked(chunk.id)) {
    removeBookmark(chunk.id);
    return false;
  }
  addBookmark(chunk);
  return true;
}

export function clearBookmarks(): Bookmark[] {
  save([]);
  window.dispatchEvent(new Event("bookmarks-changed"));
  return [];
}

export function getBookmarkCount(): number {
  return load().length;
}
