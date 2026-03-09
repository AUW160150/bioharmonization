"""
JSON parser — for structured records from electronic health systems.
Passes through as-is; harmonization agent handles field mapping.
"""

import json
import os


def parse(filepath: str) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    return {
        "source_file": os.path.basename(filepath),
        "source_format": "json",
        "data": data,
        "top_level_keys": list(data.keys()) if isinstance(data, dict) else None,
        "record_count": len(data) if isinstance(data, list) else 1,
    }
