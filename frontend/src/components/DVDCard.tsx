import Link from "next/link";
import type { DVD } from "@/lib/api";

interface DVDCardProps {
  dvd: DVD;
}

export default function DVDCard({ dvd }: DVDCardProps) {
  return (
    <Link href={`/library/${dvd.id}`}>
      <div className="bg-gray-900 border border-gray-800 rounded-lg p-5 hover:border-bjj-700 hover:bg-gray-900/80 transition-all cursor-pointer group">
        <h3 className="text-white font-semibold text-lg group-hover:text-bjj-400 transition-colors">
          {dvd.title}
        </h3>
        {dvd.instructor && (
          <p className="text-gray-400 text-sm mt-1">{dvd.instructor}</p>
        )}
        <div className="flex items-center gap-4 mt-4 text-xs text-gray-500">
          <span>
            {dvd.volume_count} volume{dvd.volume_count !== 1 ? "s" : ""}
          </span>
          <span>
            Added {new Date(dvd.created_at).toLocaleDateString()}
          </span>
        </div>
      </div>
    </Link>
  );
}
