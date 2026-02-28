"""Parse natural language BJJ queries into structured search intent.

"armbar from the back" → technique=armbar, position=back mount, type=submission
"how to escape mount"  → technique=escape, position=mount, type=escape
"darce choke setup"    → technique=darce choke, type=setup

Uses a vocabulary built from actual data in the DB (positions, techniques,
aliases, types) so it adapts as more content is ingested.
"""

import logging
import re
from dataclasses import dataclass, field
from functools import lru_cache

logger = logging.getLogger(__name__)

# technique_type values from the tagger
TECHNIQUE_TYPES = {
    "submission", "sweep", "pass", "escape", "takedown",
    "transition", "control", "defense", "setup", "concept",
}

# Words that signal a technique type in the query
TYPE_SIGNALS: dict[str, str] = {
    "submission": "submission", "sub": "submission", "finish": "submission",
    "choke": "submission", "lock": "submission", "strangle": "submission",
    "sweep": "sweep", "reversal": "sweep",
    "pass": "pass", "passing": "pass",
    "escape": "escape", "escaping": "escape", "defend": "defense",
    "defense": "defense", "defensive": "defense",
    "takedown": "takedown", "throw": "takedown",
    "transition": "transition", "chain": "transition",
    "control": "control", "pin": "control", "pinning": "control",
    "setup": "setup", "entry": "setup", "entries": "setup",
    "concept": "concept", "principle": "concept", "theory": "concept",
}

# Prepositions and connectors to strip when extracting the core technique
_FILLER = {
    "from", "in", "on", "at", "to", "the", "a", "an", "with", "for",
    "how", "do", "does", "you", "i", "when", "while", "against",
    "using", "into", "my", "your", "get", "getting", "going",
    "what", "is", "are", "best", "way", "ways", "show", "me",
}

# Common position aliases that users type vs what's stored in the DB
POSITION_ALIASES: dict[str, list[str]] = {
    "back": ["back mount", "back control", "rear mount"],
    "back mount": ["back mount", "back control", "rear mount"],
    "mount": ["mount"],
    "side control": ["side control", "cross body ride", "cross side"],
    "half guard": ["half guard", "deep half"],
    "closed guard": ["closed guard"],
    "open guard": ["open guard", "butterfly guard", "seated guard"],
    "butterfly": ["butterfly guard"],
    "turtle": ["turtle"],
    "half": ["half guard"],
    "guard": ["guard", "closed guard", "open guard", "half guard", "butterfly guard"],
    "standing": ["standing"],
    "top": ["top position", "top control", "top pinning position"],
    "bottom": ["bottom position", "bottom side control"],
    "crucifix": ["back crucifix"],
    "front headlock": ["front headlock"],
    "north south": ["north-south"],
    "north-south": ["north-south"],
}


@dataclass
class ParsedQuery:
    """Structured search intent extracted from a natural language query."""
    technique: str | None = None
    position: str | None = None
    position_variants: list[str] = field(default_factory=list)
    technique_type: str | None = None
    raw_query: str = ""
    residual: str = ""  # leftover terms not matched to any field
    is_structured: bool = False  # True if we found at least technique OR position

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
    for word in q_words:
        if word in TYPE_SIGNALS:
            detected_type = TYPE_SIGNALS[word]
            type_words_found.add(word)

    if detected_type:
        result.technique_type = detected_type

    # --- 2. Extract position ---
    # Try longest-match first (e.g. "side control" before "side")
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
    # Remove position and type words, then what's left is the technique
    remaining = q
    if position_span:
        remaining = remaining.replace(position_span, " ")
    for tw in type_words_found:
        remaining = re.sub(rf'\b{re.escape(tw)}\b', ' ', remaining)

    # Remove filler words
    tech_words = [w for w in remaining.split() if w not in _FILLER and len(w) > 1]
    technique_candidate = " ".join(tech_words).strip()

    if technique_candidate:
        result.technique = technique_candidate

    # Whatever we couldn't parse
    result.residual = technique_candidate if not position_found and not detected_type else ""

    result.is_structured = bool(result.technique or result.position)

    logger.debug("Parsed %r → %s", query, result)
    return result
