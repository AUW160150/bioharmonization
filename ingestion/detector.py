"""
Format detector — inspects file extension and content to determine parser type.
"""

import os


EXTENSION_MAP = {
    ".txt":  "text",
    ".pdf":  "text",   # treat PDF-extracted text the same way
    ".csv":  "csv",
    ".json": "json",
    ".vcf":  "vcf",
}


def detect_format(filepath: str) -> str:
    """Return format string for a given file path."""
    ext = os.path.splitext(filepath)[1].lower()
    fmt = EXTENSION_MAP.get(ext)
    if fmt is None:
        raise ValueError(f"Unsupported file format: {ext} ({filepath})")
    return fmt


def detect_patient_id(filename: str):
    """Extract patient ID (e.g. P001) from a filename."""
    import re
    match = re.search(r"(P\d{3})", filename)
    return match.group(1) if match else None
