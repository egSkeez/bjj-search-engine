"""Canonical BJJ taxonomy — fixed categories, positions, and technique groups.

Every chunk gets normalized to these values. The LLM can output free-form text,
but the normalizer maps it to the closest canonical value before storage.
"""

# ─── Categories (technique_type) ─────────────────────────────────────────────
# These are the ONLY valid values for technique_type.
CATEGORIES = [
    "submission",       # goal: break/choke opponent to get a tap
    "sweep",            # goal: reverse position to an advantageous one
    "guard pass",       # goal: pass opponent's guard
    "guard retention",  # goal: prevent opponent from passing your guard
    "escape",           # goal: get out of a bad position or submission hold
    "takedown",         # goal: bring standing opponent to the mat
    "counter",          # goal: go from being submitted to submitting
    "control",          # positional dominance — pins, grips, pressure
    "concept",          # coach explaining theory/principles without demo
]

# Map every known variant (lowercase) to its canonical category
_CATEGORY_MAP: dict[str, str] = {}

_CATEGORY_ALIASES = {
    "submission": [
        "submission", "sub", "choke", "strangle", "joint lock", "lock",
        "armlock", "leglock", "leg lock", "arm lock", "neck crank",
        "submission setup", "setup, submission", "submission, transition",
        "submission, control, setup", "attack", "re-attack",
    ],
    "sweep": [
        "sweep", "reversal", "sweep, transition", "sweep, defense",
        "sweep, submission", "sweep, transition, control",
        "sweep, transition, control, defense",
    ],
    "guard pass": [
        "pass", "guard pass", "passing", "guard passing", "pass setup",
        "pass, control", "pass, transition", "pass, control, concept",
        "pass, setup, concept", "open guard passing",
        "setup, pass, submission, control",
    ],
    "guard retention": [
        "guard retention", "retention", "guard recovery", "re-guard",
        "reguard", "guard break",
    ],
    "escape": [
        "escape", "escape, transition", "escape, defense",
        "escape, counter, concept, transition", "escape, sweep",
        "escape, pass", "escape, defense, concept",
        "escape, defense, control, sweep, concept",
        "escape, sweep, transition, control",
    ],
    "takedown": [
        "takedown", "takedown defense", "takedown, submission setup",
        "wrestling", "throw",
    ],
    "counter": [
        "counter", "counter, escape, pass",
        "defense, submission, counter",
    ],
    "control": [
        "control", "setup", "setup, control", "control, setup, concept",
        "control, transition, setup", "breakdown", "breakdown, transition",
        "back take", "entry", "recovery", "movement", "grip break",
        "defense", "defense, transition", "defense, transition, control",
        "defense, transition, submission", "defense, control, pass",
        "defense, escape, concept", "defense, sweep, transition",
        "series", "drill",
    ],
    "concept": [
        "concept", "theory", "n/a", "n/a - no instructional content",
        "n/a - blank audio", "n/a - no transcript content",
        "no instructional content", "unidentified", "unknown", "other",
        "transition", "transition, concept", "transition, submission",
        "transition, setup", "transition, control",
        "setup, transition, concept", "setup, sweep",
    ],
}

for canonical, aliases in _CATEGORY_ALIASES.items():
    for alias in aliases:
        _CATEGORY_MAP[alias.lower().strip()] = canonical


# ─── Positions ───────────────────────────────────────────────────────────────
# Canonical positions — the ONLY valid values for position.
POSITIONS = [
    "closed guard",
    "open guard",
    "half guard",
    "butterfly guard",
    "deep half guard",
    "de la riva guard",
    "reverse de la riva guard",
    "x guard",
    "single leg x",
    "spider guard",
    "lasso guard",
    "rubber guard",
    "octopus guard",
    "z guard",
    "mount",
    "side control",
    "back control",
    "north south",
    "turtle",
    "front headlock",
    "leg entanglement",
    "standing",
    "seated guard",
    "top position",
    "bottom position",
    "crucifix",
    "truck",
    "50/50",
]

# Map every known variant (lowercase) to its canonical position
_POSITION_MAP: dict[str, str] = {}

_POSITION_ALIASES = {
    "closed guard": [
        "closed guard", "full guard", "guard (closed)",
    ],
    "open guard": [
        "open guard", "open guard (seated)", "open guard (supine)",
        "open guard passing", "guard", "supine guard", "supine open guard",
        "seated open guard",
    ],
    "half guard": [
        "half guard", "half guard (top)", "half guard (bottom)",
        "half guard bottom", "bottom half guard", "top half guard",
        "half butterfly guard", "butterfly half guard",
        "knee shield half guard", "half guard passing",
    ],
    "butterfly guard": [
        "butterfly guard", "butterfly",
    ],
    "deep half guard": [
        "deep half guard", "deep half",
    ],
    "de la riva guard": [
        "de la riva guard", "de la riva", "dlr guard", "dlr",
    ],
    "reverse de la riva guard": [
        "reverse de la riva guard", "reverse de la riva", "rdlr",
    ],
    "x guard": [
        "x guard", "x-guard",
    ],
    "single leg x": [
        "single leg x", "single leg x guard", "slx", "ashi garami",
        "outside ashi", "inside ashi",
    ],
    "octopus guard": [
        "octopus guard", "octopus",
    ],
    "z guard": [
        "z guard", "z-guard", "knee shield",
    ],
    "mount": [
        "mount", "mount (bottom)", "bottom mount", "mounted",
        "mount (top)", "top mount", "s-mount", "s mount",
    ],
    "side control": [
        "side control", "side control (bottom)", "side control bottom",
        "bottom side control", "side mount", "kesa gatame",
        "100 kilos", "hundred kilos",
    ],
    "back control": [
        "back control", "back mount", "rear mount", "back",
        "back take", "rear naked",
    ],
    "north south": [
        "north south", "north-south", "n/s",
    ],
    "turtle": [
        "turtle", "turtle position", "turtle (top)", "turtle (bottom)",
    ],
    "front headlock": [
        "front headlock", "front head lock", "front headlock position",
        "guillotine position",
    ],
    "leg entanglement": [
        "leg entanglement", "ashi garami (leg entanglement)",
        "leg lock", "leg locks", "leglocks", "leglock",
        "heel hook position", "saddle", "inside sankaku",
        "outside sankaku", "50/50 leg entanglement",
        "cross ashi", "outside ashi garami", "inside ashi garami",
    ],
    "standing": [
        "standing", "stand up", "standing position",
        "wrestling", "clinch",
    ],
    "seated guard": [
        "seated guard", "seated", "sitting guard",
        "seated open guard",
    ],
    "top position": [
        "top position", "top", "guard passing", "passing",
        "guard passing (general)",
    ],
    "bottom position": [
        "bottom position", "bottom", "guard retention",
    ],
    "crucifix": [
        "crucifix", "crucifix position",
    ],
    "truck": [
        "truck", "truck position",
    ],
    "50/50": [
        "50/50", "fifty fifty", "fifty-fifty",
    ],
}

for canonical, aliases in _POSITION_ALIASES.items():
    for alias in aliases:
        _POSITION_MAP[alias.lower().strip()] = canonical

# Technique-names that the LLM sometimes puts in the position field.
# Map them to the position where the technique is typically performed.
_TECHNIQUE_AS_POSITION: dict[str, str] = {}
_TECHNIQUE_POSITION_FALLBACKS = {
    "closed guard": [
        "triangle choke", "triangle", "triangle setup", "triangle defense",
        "triangle choke defense", "triangle choke position", "triangle choke setup",
        "triangle control", "triangle position", "front triangle",
        "front triangle setup", "inside triangle choke", "side triangle",
        "rear triangle", "rear triangle setup", "reverse triangle",
        "armbar", "arm bar", "armbar position", "armbar setup",
        "armbar finish", "armbar finishing position", "armbar defense",
        "armbar control", "armbar attack", "armbar application",
        "arm bar position", "arm bar setup", "arm bar finish",
        "armbar (juji gatame)", "arm bar (jujigatame)",
        "armbar position (juji gatame)", "armbar finish position",
        "armlock", "armlock setup", "armlock control",
        "armlock setup (general)", "armlock (general)",
        "belly down armbar",
        "kimura control", "kimura control position",
        "guillotine defense", "guillotine choke",
        "scissor sweep",
    ],
    "top position": [
        "knee on belly", "neon belly", "knee on belly position",
        "body lock pass", "body lock", "side body lock",
        "leg drag", "stack pass position",
        "cross body ride", "pin escapes", "pin escapes (general)",
        "pinning positions", "pinned positions", "pinned position",
    ],
    "standing": [
        "single leg defense", "single leg takedown defense",
        "sprawl", "sprawl defense",
    ],
    "side control": [
        "head and arm control",
    ],
    "leg entanglement": [
        "ashigurami", "straight ashigurami", "reverse ashigurami",
        "yoko senkaku",
    ],
    "": [
        "general", "general concept", "general strategy",
        "general grappling", "general escapes",
        "general escape principles", "various", "various positions",
        "unspecified", "grip fighting", "scramble",
        "short offense", "leg ride",
    ],
}
for canonical, aliases in _TECHNIQUE_POSITION_FALLBACKS.items():
    for alias in aliases:
        _TECHNIQUE_AS_POSITION[alias.lower().strip()] = canonical


# ─── Technique group normalization ───────────────────────────────────────────
# Groups related techniques under one canonical name.
# "darce choke", "d'arce choke", "Darce Choke" → "darce choke"
_TECHNIQUE_GROUP_MAP: dict[str, str] = {}

_TECHNIQUE_GROUPS = {
    "rear naked choke": [
        "rear naked choke", "rnc", "rear naked strangle",
        "mata leao", "renegade strangle", "lion killer",
    ],
    "triangle choke": [
        "triangle choke", "triangle", "sankaku jime",
        "mounted triangle", "mounted triangle choke",
        "reverse triangle", "front triangle",
    ],
    "armbar": [
        "armbar", "arm bar", "juji gatame", "jujigatami",
        "straight armbar", "belly down armbar",
    ],
    "kimura": [
        "kimura", "double wristlock", "kimura lock",
        "kimura grip", "chicken wing",
    ],
    "americana": [
        "americana", "ude garami", "keylock", "key lock",
        "american lock", "americano",
    ],
    "guillotine": [
        "guillotine", "guillotine choke", "standing guillotine",
        "arm-in guillotine", "high elbow guillotine",
        "marcelotine",
    ],
    "darce choke": [
        "darce choke", "d'arce choke", "darce", "d'arce",
        "brabo choke", "no-arm darce",
    ],
    "anaconda choke": [
        "anaconda choke", "anaconda", "gator roll",
    ],
    "arm triangle": [
        "arm triangle", "arm triangle choke", "kata gatame",
        "head and arm choke", "head and arm",
    ],
    "ezekiel choke": [
        "ezekiel choke", "ezekiel", "sode guruma jime",
    ],
    "north south choke": [
        "north south choke", "north-south choke", "ns choke",
    ],
    "heel hook": [
        "heel hook", "inside heel hook", "outside heel hook",
        "leg lock", "leglock", "leg attack",
    ],
    "knee bar": [
        "knee bar", "kneebar", "knee lock",
    ],
    "toe hold": [
        "toe hold", "toehold",
    ],
    "achilles lock": [
        "achilles lock", "straight ankle lock", "ankle lock",
        "straight foot lock",
    ],
    "calf slicer": [
        "calf slicer", "calf crush", "calf lock",
    ],
    "knee cut pass": [
        "knee cut pass", "knee cut", "knee slice", "knee slide",
        "knee slice pass", "knee slide pass",
    ],
    "leg drag pass": [
        "leg drag pass", "leg drag", "leg drags",
    ],
    "body lock pass": [
        "body lock pass", "body lock", "bodylock pass",
    ],
    "toreando pass": [
        "toreando pass", "toreando", "toreando passing",
        "bull fighter pass", "bullfighter",
    ],
    "hip switch pass": [
        "hip switch pass", "hip switch",
    ],
    "over under pass": [
        "over under pass", "over-under pass", "over under",
    ],
    "x pass": [
        "x pass", "x-pass",
    ],
    "arm drag": [
        "arm drag", "arm drags", "two on one",
    ],
    "scissor sweep": [
        "scissor sweep", "scissor",
    ],
    "hip bump sweep": [
        "hip bump sweep", "hip bump",
    ],
    "flower sweep": [
        "flower sweep", "pendulum sweep",
    ],
    "sumi gaeshi": [
        "sumi gaeshi", "yoko sumi gaeshi", "sacrifice throw",
    ],
    "tomoe nage": [
        "tomoe nage", "circle throw",
    ],
    "single leg": [
        "single leg", "single leg takedown",
    ],
    "double leg": [
        "double leg", "double leg takedown",
    ],
    "elbow escape": [
        "elbow escape", "shrimp escape", "hip escape",
    ],
    "bridge escape": [
        "bridge escape", "upa", "upa escape", "trap and roll",
    ],
    "straitjacket": [
        "straitjacket", "straitjacket system", "straitjacket control",
        "straight jacket",
    ],
    "power ride": [
        "power ride", "power ride system",
    ],
}

for canonical, aliases in _TECHNIQUE_GROUPS.items():
    for alias in aliases:
        _TECHNIQUE_GROUP_MAP[alias.lower().strip()] = canonical


# ─── Public API ──────────────────────────────────────────────────────────────

def normalize_category(raw: str | None) -> str:
    """Map a raw technique_type string to a canonical category."""
    if not raw:
        return "concept"
    key = raw.lower().strip()
    if key in _CATEGORY_MAP:
        return _CATEGORY_MAP[key]
    # Fuzzy: check if any canonical category is a substring
    for cat in CATEGORIES:
        if cat in key:
            return cat
    return "concept"


def normalize_position(raw: str | None) -> str:
    """Map a raw position string to a canonical position."""
    if not raw:
        return ""
    key = raw.lower().strip()
    if key in ("unidentified", "n/a", "none", "unknown", ""):
        return ""
    if key in _POSITION_MAP:
        return _POSITION_MAP[key]
    # Check technique-as-position fallback (e.g. "triangle choke" → "closed guard")
    if key in _TECHNIQUE_AS_POSITION:
        return _TECHNIQUE_AS_POSITION[key]
    # Fuzzy: find best substring match against position aliases
    best = ""
    best_len = 0
    for alias, canonical in _POSITION_MAP.items():
        if alias in key and len(alias) > best_len:
            best = canonical
            best_len = len(alias)
    if best:
        return best
    # Check if any canonical position name appears in the raw string
    for pos in POSITIONS:
        if pos in key:
            return pos
    # Check technique-as-position substring fallback
    for alias, canonical in _TECHNIQUE_AS_POSITION.items():
        if len(alias) > 3 and alias in key:
            return canonical
    return ""


def normalize_technique(raw: str | None) -> str:
    """Map a raw technique name to its canonical group, or lowercase it."""
    if not raw:
        return ""
    key = raw.lower().strip()
    if key in ("unidentified", "n/a", "none", "unknown", ""):
        return ""
    if key in _TECHNIQUE_GROUP_MAP:
        return _TECHNIQUE_GROUP_MAP[key]
    # Check if any group name appears as a substring
    for alias, canonical in _TECHNIQUE_GROUP_MAP.items():
        if len(alias) > 3 and alias in key:
            return canonical
    # Not in a known group — just lowercase and clean up
    return key


def normalize_chunk(chunk: dict) -> dict:
    """Normalize all taxonomy fields on a chunk dict in-place."""
    chunk["technique_type"] = normalize_category(chunk.get("technique_type"))
    chunk["position"] = normalize_position(chunk.get("position"))
    chunk["technique"] = normalize_technique(chunk.get("technique"))
    return chunk
