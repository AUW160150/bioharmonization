"""
BioHarmonize — Ingestion Agent
Scans the data directory, groups files by patient ID, routes each file
to the correct parser, and returns one raw record dict per patient.
"""

import os
from collections import defaultdict

from ingestion.detector import detect_format, detect_patient_id
from ingestion.parsers import text_parser, csv_parser, vcf_parser, json_parser

PARSER_MAP = {
    "text": text_parser,
    "csv":  csv_parser,
    "vcf":  vcf_parser,
    "json": json_parser,
}


def ingest_directory(data_dir: str, verbose: bool = False) -> dict:
    """
    Scan data_dir for all supported files, group by patient ID,
    and return a dict of {patient_id: raw_record}.
    """
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    # Group files by patient ID
    patient_files: dict[str, list[str]] = defaultdict(list)
    skipped = []

    for fname in sorted(os.listdir(data_dir)):
        fpath = os.path.join(data_dir, fname)
        if not os.path.isfile(fpath):
            continue
        if fname.endswith(".py"):   # skip generator script
            continue

        pid = detect_patient_id(fname)
        if pid is None:
            skipped.append(fname)
            continue
        patient_files[pid].append(fpath)

    if verbose and skipped:
        print(f"  [Ingestion] Skipped (no patient ID): {skipped}")

    # Parse each file and assemble per-patient record
    patient_records: dict[str, dict] = {}

    for pid in sorted(patient_files.keys()):
        record = {
            "patient_id": pid,
            "sources": [],
        }

        for fpath in sorted(patient_files[pid]):
            fname = os.path.basename(fpath)
            try:
                fmt = detect_format(fpath)
                parser = PARSER_MAP[fmt]
                parsed = parser.parse(fpath)
                parsed["_format"] = fmt
                record["sources"].append(parsed)

                if verbose:
                    print(f"  [Ingestion] {pid} | {fmt:5s} | {fname}")

            except Exception as e:
                record["sources"].append({
                    "source_file": fname,
                    "_format": "error",
                    "error": str(e),
                })
                if verbose:
                    print(f"  [Ingestion] ERROR {pid} | {fname} — {e}")

        patient_records[pid] = record

    return patient_records


def summarize(patient_records: dict) -> None:
    """Print a summary table of ingested records."""
    print(f"\n{'─'*60}")
    print(f"  Ingestion Agent — Summary")
    print(f"{'─'*60}")
    print(f"  {'Patient':<10} {'Files':>6}  {'Formats'}")
    print(f"{'─'*60}")
    for pid, record in sorted(patient_records.items()):
        formats = [s.get("_format", "?") for s in record["sources"]]
        fmt_str = ", ".join(sorted(set(formats)))
        print(f"  {pid:<10} {len(formats):>6}  {fmt_str}")
    print(f"{'─'*60}")
    print(f"  Total patients: {len(patient_records)}")
    print(f"{'─'*60}\n")


if __name__ == "__main__":
    import json

    DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "synthetic")
    DATA_DIR = os.path.abspath(DATA_DIR)

    print("BioHarmonize — Ingestion Agent")
    print("=" * 45)

    records = ingest_directory(DATA_DIR, verbose=True)
    summarize(records)

    # Spot-check: print P001 record structure
    p001 = records.get("P001", {})
    print("Sample output — P001 sources:")
    for src in p001.get("sources", []):
        print(f"  [{src['_format']}] {src['source_file']}")
        if src["_format"] == "text":
            print(f"    language_hint : {src['language_hint']}")
            print(f"    char_count    : {src['char_count']}")
        elif src["_format"] == "csv":
            print(f"    headers       : {src['headers'][:3]} ...")
            print(f"    row_count     : {src['row_count']}")
        elif src["_format"] == "vcf":
            print(f"    variant_count : {src['variant_count']}")
            print(f"    sample_id     : {src['sample_id']}")
    print()
    print("✓ Phase 3 complete — raw records ready for harmonization agent.")
