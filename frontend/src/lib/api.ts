const API_BASE = process.env.NEXT_PUBLIC_API_URL || "";

export interface ChunkResult {
  id: string;
  volume_id: string;
  start_time: number;
  end_time: number;
  text: string;
  position: string | null;
  technique: string | null;
  technique_type: string | null;
  aliases: string[] | null;
  description: string | null;
  key_points: string[] | null;
  chunk_type: string;
  created_at: string;
  dvd_title: string | null;
  volume_name: string | null;
  instructor: string | null;
}

export interface SearchResult {
  chunk: ChunkResult;
  score: number;
}

export interface SearchResponse {
  query: string;
  results: SearchResult[];
  total: number;
}

export interface DVD {
  id: string;
  title: string;
  instructor: string | null;
  created_at: string;
  volume_count: number;
}

export interface IngestJob {
  id: string;
  dvd_title: string;
  volume_name: string;
  status: string;
  progress: number;
  error_message: string | null;
  created_at: string;
  updated_at: string;
}

export interface BrowseResponse {
  position: string | null;
  technique_type: string | null;
  results: ChunkResult[];
  total: number;
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, init);
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

export type SearchMode = "granular" | "semantic";

export async function search(
  q: string,
  opts?: { position?: string; type?: string; mode?: SearchMode; limit?: number; offset?: number }
): Promise<SearchResponse> {
  const params = new URLSearchParams({ q });
  if (opts?.position) params.set("position", opts.position);
  if (opts?.type) params.set("type", opts.type);
  if (opts?.mode) params.set("mode", opts.mode);
  if (opts?.limit) params.set("limit", String(opts.limit));
  if (opts?.offset) params.set("offset", String(opts.offset));
  return apiFetch(`/api/search?${params}`);
}

export async function browse(opts?: {
  position?: string;
  type?: string;
  limit?: number;
  offset?: number;
}): Promise<BrowseResponse> {
  const params = new URLSearchParams();
  if (opts?.position) params.set("position", opts.position);
  if (opts?.type) params.set("type", opts.type);
  if (opts?.limit) params.set("limit", String(opts.limit));
  if (opts?.offset) params.set("offset", String(opts.offset));
  return apiFetch(`/api/browse?${params}`);
}

export async function listDVDs(): Promise<DVD[]> {
  return apiFetch("/api/dvds");
}

export async function getDVD(id: string): Promise<DVD> {
  return apiFetch(`/api/dvds/${id}`);
}

export async function getDVDChunks(dvdId: string): Promise<ChunkResult[]> {
  return apiFetch(`/api/dvds/${dvdId}/chunks`);
}

export async function getPositions(): Promise<string[]> {
  return apiFetch("/api/positions");
}

export async function getTechniqueTypes(): Promise<string[]> {
  return apiFetch("/api/technique-types");
}

export async function getIngestJobs(): Promise<IngestJob[]> {
  return apiFetch("/api/ingest");
}

export async function getIngestJobStatus(jobId: string): Promise<IngestJob> {
  return apiFetch(`/api/ingest/${jobId}/status`);
}

export async function createIngestJob(
  file: File,
  dvdTitle: string,
  volumeName: string,
  instructor?: string
): Promise<IngestJob> {
  const form = new FormData();
  form.append("file", file);
  form.append("dvd_title", dvdTitle);
  form.append("volume_name", volumeName);
  if (instructor) form.append("instructor", instructor);
  return apiFetch("/api/ingest", { method: "POST", body: form });
}

export function formatTimestamp(seconds: number): string {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  if (h > 0) {
    return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
  }
  return `${m}:${String(s).padStart(2, "0")}`;
}
