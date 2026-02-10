"""
TOON - Token-Oriented Object Notation
Compact serialization format that reduces token count ~50-60% vs JSON.

Format:
    name[count]{key1,key2,key3}:
    value1,value2,value3
    value4,value5,value6

Ported from TypeScript: lib/research/toon.ts
"""


def _escape(value: str) -> str:
    """Escape commas and newlines in values."""
    if value is None:
        return ""
    s = str(value)
    s = s.replace("\\", "\\\\")
    s = s.replace(",", "\\,")
    s = s.replace("\n", "\\n")
    return s


def _unescape(value: str) -> str:
    """Unescape TOON values."""
    s = value
    s = s.replace("\\n", "\n")
    s = s.replace("\\,", ",")
    s = s.replace("\\\\", "\\")
    return s


def serialize(name: str, items: list[dict], keys: list[str] | None = None) -> str:
    """
    Serialize a list of dicts to TOON format.
    
    Args:
        name: Collection name (e.g. 'anime', 'characters')
        items: List of dicts to serialize
        keys: Optional key whitelist. If None, auto-detect from first item.
    
    Returns:
        TOON-formatted string
    """
    if not items:
        return f"{name}[0]{{}}"

    if keys is None:
        keys = list(items[0].keys())

    lines = [f"{name}[{len(items)}]{{{','.join(keys)}}}:"]
    for item in items:
        values = []
        for key in keys:
            val = item.get(key)
            if val is None:
                values.append("")
            elif isinstance(val, bool):
                values.append("1" if val else "0")
            elif isinstance(val, (list, dict)):
                import json
                values.append(_escape(json.dumps(val, ensure_ascii=False)))
            else:
                values.append(_escape(str(val)))
        lines.append(",".join(values))

    return "\n".join(lines)


def deserialize(toon_str: str) -> tuple[str, list[dict]]:
    """
    Deserialize TOON format back to (name, list of dicts).
    
    Returns:
        Tuple of (collection_name, list_of_dicts)
    """
    lines = toon_str.strip().split("\n")
    if not lines:
        return "", []

    import re
    header = lines[0]
    match = re.match(r'^(\w+)\[(\d+)\]\{([^}]*)\}:?$', header)
    if not match:
        raise ValueError(f"Invalid TOON header: {header}")

    name = match.group(1)
    count = int(match.group(2))
    keys = match.group(3).split(",") if match.group(3) else []

    if count == 0:
        return name, []

    items = []
    for line in lines[1:]:
        if not line.strip():
            continue
        # Split on unescaped commas
        values = []
        current = []
        i = 0
        while i < len(line):
            if line[i] == '\\' and i + 1 < len(line):
                current.append(line[i:i+2])
                i += 2
            elif line[i] == ',':
                values.append(_unescape("".join(current)))
                current = []
                i += 1
            else:
                current.append(line[i])
                i += 1
        values.append(_unescape("".join(current)))

        item = {}
        for j, key in enumerate(keys):
            item[key] = values[j] if j < len(values) else ""
        items.append(item)

    return name, items


def save(filepath: str, name: str, items: list[dict], keys: list[str] | None = None):
    """Serialize and save to file."""
    import os
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(serialize(name, items, keys))


def load(filepath: str) -> tuple[str, list[dict]]:
    """Load and deserialize from file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return deserialize(f.read())
