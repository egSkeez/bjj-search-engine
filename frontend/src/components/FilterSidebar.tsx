"use client";

import { useEffect, useState } from "react";
import { getPositions, getTechniqueTypes } from "@/lib/api";

interface FilterSidebarProps {
  selectedPosition: string | null;
  selectedType: string | null;
  onPositionChange: (position: string | null) => void;
  onTypeChange: (type: string | null) => void;
}

export default function FilterSidebar({
  selectedPosition,
  selectedType,
  onPositionChange,
  onTypeChange,
}: FilterSidebarProps) {
  const [positions, setPositions] = useState<string[]>([]);
  const [types, setTypes] = useState<string[]>([]);

  useEffect(() => {
    getPositions().then(setPositions).catch(() => {});
    getTechniqueTypes().then(setTypes).catch(() => {});
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-3">
          Position
        </h3>
        <select
          value={selectedPosition || ""}
          onChange={(e) => onPositionChange(e.target.value || null)}
          className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-300 focus:outline-none focus:ring-2 focus:ring-bjj-500"
        >
          <option value="">All positions</option>
          {positions.map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>
      </div>

      <div>
        <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-3">
          Technique Type
        </h3>
        <div className="space-y-2">
          {types.map((t) => (
            <label key={t} className="flex items-center gap-2 cursor-pointer group">
              <input
                type="checkbox"
                checked={selectedType === t}
                onChange={() => onTypeChange(selectedType === t ? null : t)}
                className="rounded border-gray-600 bg-gray-800 text-bjj-500 focus:ring-bjj-500 focus:ring-offset-0"
              />
              <span className="text-sm text-gray-400 group-hover:text-gray-200 capitalize transition-colors">
                {t}
              </span>
            </label>
          ))}
        </div>
        {selectedType && (
          <button
            onClick={() => onTypeChange(null)}
            className="mt-2 text-xs text-bjj-500 hover:text-bjj-400"
          >
            Clear filter
          </button>
        )}
      </div>
    </div>
  );
}
