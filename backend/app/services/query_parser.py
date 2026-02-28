"""Parse natural language BJJ queries into structured search intent.

"armbar from the back" → technique=armbar, position=back control, type=submission
"how to escape mount"  → technique=escape, position=mount, type=escape
"darce choke setup"    → technique=darce choke, type=submission

Uses the canonical taxonomy for normalization so queries always match
the fixed categories, positions, and technique groups stored in the DB.
"""

import logging
import re
from dataclasses import dataclass, field

from app.services.taxonomy import (
    CATEGORIES,
    POSITIONS,
    normalize_category,
    normalize_position,
    normalize_technique,
)

logger = logging.getLogger(__name__)

# Words that signal a technique type in the query
TYPE_SIGNALS: dict[str, str] = {
    "submission": "submission", "sub": "submission", "finish": "submission",
    "choke": "submission", "lock": "submission", "strangle": "submission",
    "sweep": "sweep", "reversal": "sweep",
    "pass": "guard pass", "passing": "guard pass", "guard pass": "guard pass",
    "escape": "escape", "escaping": "escape",
    "defend": "control", "defense": "control", "defensive": "control",
    "takedown": "takedown", "throw": "takedown",
    "control": "control", "pin": "control", "pinning": "control",
    "concept": "concept", "principle": "concept", "theory": "concept",
    "counter": "counter",
    "retention": "guard retention", "retain": "guard retention",
    "guard retention": "guard retention",
}

_FILLER = {
    "from", "in", "on", "at", "to", "the", "a", "an", "with", "for",
    "how", "do", "does", "you", "i", "when", "while", "against",
    "using", "into", "my", "your", "get", "getting", "going",
    "what", "is", "are", "best", "way", "ways", "show", "me",
}

# Position keywords users type → canonical position (+ variants for fuzzy matching)
POSITION_ALIASES: dict[str, list[str]] = {
    "back": ["back control"],
    "back control": ["back control"],
    "back mount": ["back control"],
    "mount": ["mount"],
    "side control": ["side control"],
    "half guard": ["half guard"],
    "closed guard": ["closed guard"],
    "open guard": ["open guard"],
    "butterfly": ["butterfly guard"],
    "butterfly guard": ["butterfly guard"],
    "deep half": ["deep half guard"],
    "deep half guard": ["deep half guard"],
    "de la riva": ["de la riva guard"],
    "dlr": ["de la riva guard"],
    "reverse de la riva": ["reverse de la riva guard"],
    "rdlr": ["reverse de la riva guard"],
    "x guard": ["x guard"],
    "single leg x": ["single leg x"],
    "slx": ["single leg x"],
    "spider guard": ["spider guard"],
    "lasso": ["lasso guard"],
    "rubber guard": ["rubber guard"],
    "octopus guard": ["octopus guard"],
    "z guard": ["z guard"],
    "knee shield": ["z guard"],
    "turtle": ["turtle"],
    "half": ["half guard"],
    "guard": ["closed guard", "open guard", "half guard", "butterfly guard"],
    "standing": ["standing"],
    "top": ["top position"],
    "bottom": ["bottom position"],
    "crucifix": ["crucifix"],
    "truck": ["truck"],
    "front headlock": ["front headlock"],
    "north south": ["north south"],
    "north-south": ["north south"],
    "leg entanglement": ["leg entanglement"],
    "ashi garami": ["leg entanglement"],
    "50/50": ["50/50"],
    "fifty fifty": ["50/50"],
    "seated": ["seated guard"],
    "seated guard": ["seated guard"],
}


@dataclass
class ParsedQuery:
    """Structured search intent extracted from a natural language query."""
    technique: str | None = None
    position: str | None = None
    position_variants: list[str] = field(default_factory=list)
    technique_type: str | None = None
    raw_query: str = ""
    residual: str = ""
    is_structured: bool = False

    def __repr__(self):
        parts = []
        if self.technique:
            parts.append(f"technique={self.technique!r}")
        if self.position:
            parts.append(f"position={self.position!r}")
        if self.technique_type:
            parts.append(f"type={self.technique_type!r}")
        if self.residual:
            parts.append(f"residual={self.residual!r}")
        return f"ParsedQuery({', '.join(parts)})"


def parse_query(query: str) -> ParsedQuery:
    """Parse a natural language BJJ query into structured intent."""
    result = ParsedQuery(raw_query=query)
    q = query.lower().strip()

    # --- 1. Extract technique type signals ---
    q_words = q.split()
    detected_type = None
    type_words_found: set[str] = set()

    # Try multi-word type signals first
    for signal, cat in TYPE_SIGNALS.items():
        if " " in signal and signal in q:
            detected_type = cat
            type_words_found.update(signal.split())

    if not detected_type:
        for word in q_words:
            if word in TYPE_SIGNALS:
                detected_type = TYPE_SIGNALS[word]
                type_words_found.add(word)

    if detected_type:
        result.technique_type = detected_type

    # --- 2. Extract position (longest match first) ---
    position_found = None
    position_span = ""
    sorted_positions = sorted(POSITION_ALIASES.keys(), key=len, reverse=True)
    for pos_key in sorted_positions:
        if pos_key in q:
            position_found = pos_key
            position_span = pos_key
            result.position = pos_key
            result.position_variants = POSITION_ALIASES[pos_key]
            break

    # --- 3. Extract technique name ---
    remaining = q
    if position_span:
        remaining = remaining.replace(position_span, " ")
    for tw in type_words_found:
        remaining = re.sub(rf'\b{re.escape(tw)}\b', ' ', remaining)

    tech_words = [w for w in remaining.split() if w not in _FILLER and len(w) > 1]
    technique_candidate = " ".join(tech_words).strip()

    if technique_candidate:
        # Normalize through the taxonomy so "d'arce" → "darce choke" etc.
        normalized = normalize_technique(technique_candidate)
        result.technique = normalized if normalized else technique_candidate

    result.residual = technique_candidate if not position_found and not detected_type else ""
    result.is_structured = bool(result.technique or result.position)

    logger.debug("Parsed %r → %s", query, result)
    return result
