"""
Text / PDF-text parser.
Reads clinical notes (Bengali, Hindi, English) and returns raw structured dict.
No translation or extraction here — that is the harmonization agent's job.
"""

import os


def parse(filepath: str) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        raw_text = f.read()

    filename = os.path.basename(filepath)

    # Detect language tag from filename convention: clinical_note_PXXX_<lang>.txt
    lang_hint = "unknown"
    if "_bengali" in filename:
        lang_hint = "bengali"
    elif "_hindi" in filename:
        lang_hint = "hindi"
    elif "_english" in filename:
        lang_hint = "english"

    return {
        "source_file": filename,
        "source_format": "text",
        "language_hint": lang_hint,
        "raw_text": raw_text,
        "char_count": len(raw_text),
    }
