"use client";

import { useEffect, useState } from "react";
import {
  getPositions,
  getTechniqueTypes,
  getInstructors,
  getDVDsList,
  type DVDListItem,
} from "@/lib/api";

interface FilterSidebarProps {
  selectedPosition: string | null;
  selectedType: string | null;
  selectedInstructor: string | null;
  selectedDvdId: string | null;
  hideConcepts: boolean;
  onPositionChange: (position: string | null) => void;
  onTypeChange: (type: string | null) => void;
  onInstructorChange: (instructor: string | null) => void;
  onDvdIdChange: (dvdId: string | null) => void;
  onHideConceptsChange: (hide: boolean) => void;
}

export default function FilterSidebar({
  selectedPosition,
  selectedType,
  selectedInstructor,
  selectedDvdId,
  hideConcepts,
  onPositionChange,
  onTypeChange,
  onInstructorChange,
  onDvdIdChange,
  onHideConceptsChange,
}: FilterSidebarProps) {
  const [positions, setPositions] = useState<string[]>([]);
  const [types, setTypes] = useState<string[]>([]);
  const [instructors, setInstructors] = useState<string[]>([]);
  const [dvds, setDvds] = useState<DVDListItem[]>([]);

  useEffect(() => {
    getPositions().then(setPositions).catch(() => {});
    getTechniqueTypes().then(setTypes).catch(() => {});
    getInstructors().then(setInstructors).catch(() => {});
    getDVDsList().then(setDvds).catch(() => {});
  }, []);

  const filteredDvds = selectedInstructor
    ? dvds.filter(
        (d) => d.instructor?.toLowerCase() === selectedInstructor.toLowerCase()
      )
    : dvds;

  const hasAnyFilter =
    selectedPosition || selectedType || selectedInstructor || selectedDvdId || hideConcepts;

  return (
    <div className="space-y-5">
      {/* Instructor */}
      <div>
        <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-2">
          Instructor
        </h3>
        <select
          value={selectedInstructor || ""}
          onChange={(e) => {
            onInstructorChange(e.target.value || null);
            if (!e.target.value) onDvdIdChange(null);
          }}
          className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-300 focus:outline-none focus:ring-2 focus:ring-bjj-500"
        >
          <option value="">All instructors</option>
          {instructors.map((i) => (
            <option key={i} value={i}>
              {i}
            </option>
          ))}
        </select>
      </div>

      {/* DVD / Series */}
      <div>
        <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-2">
          DVD / Series
        </h3>
        <select
          value={selectedDvdId || ""}
          onChange={(e) => onDvdIdChange(e.target.value || null)}
          className="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-300 focus:outline-none focus:ring-2 focus:ring-bjj-500"
        >
          <option value="">All DVDs</option>
          {filteredDvds.map((d) => (
            <option key={d.id} value={d.id}>
              {d.title}
            </option>
          ))}
        </select>
      </div>

      {/* Position */}
      <div>
        <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-2">
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

      {/* Technique Type */}
      <div>
        <h3 className="text-sm font-semibold text-gray-300 uppercase tracking-wider mb-2">
          Category
        </h3>
        <div className="space-y-1.5">
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
      </div>

      {/* Hide Concepts Toggle */}
      <div className="pt-2 border-t border-gray-800">
        <label className="flex items-center gap-2.5 cursor-pointer group">
          <input
            type="checkbox"
            checked={hideConcepts}
            onChange={(e) => onHideConceptsChange(e.target.checked)}
            className="rounded border-gray-600 bg-gray-800 text-amber-500 focus:ring-amber-500 focus:ring-offset-0"
          />
          <div>
            <span className="text-sm text-gray-300 group-hover:text-white transition-colors">
              Hide concepts
            </span>
            <p className="text-xs text-gray-600">Only show technique demonstrations</p>
          </div>
        </label>
      </div>

      {/* Clear All */}
      {hasAnyFilter && (
        <button
          onClick={() => {
            onPositionChange(null);
            onTypeChange(null);
            onInstructorChange(null);
            onDvdIdChange(null);
            onHideConceptsChange(false);
          }}
          className="w-full text-xs text-bjj-500 hover:text-bjj-400 py-2 border border-gray-800 rounded-lg hover:border-gray-700 transition-colors"
        >
          Clear all filters
        </button>
      )}
    </div>
  );
}
