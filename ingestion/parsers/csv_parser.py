"""
CSV parser for lab result files.
Handles non-standard (Bengali/Hindi/mixed) column headers.
Preserves original column names — harmonization agent maps them later.
"""

import csv
import os


def parse(filepath: str) -> dict:
    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        raw_rows = list(reader)

    if not raw_rows:
        return {"source_file": os.path.basename(filepath), "source_format": "csv", "headers": [], "records": [], "reference_ranges": {}}

    headers = raw_rows[0]

    # Separate data rows from reference range rows (marked with [REF_RANGE])
    data_rows = []
    ref_row = None
    for row in raw_rows[1:]:
        if row and str(row[0]).startswith("[REF_RANGE]"):
            ref_row = row
        else:
            data_rows.append(row)

    # Build records as list of dicts preserving original column names
    records = []
    for row in data_rows:
        record = {}
        for i, col in enumerate(headers):
            record[col] = row[i] if i < len(row) else None
        records.append(record)

    # Build reference range lookup {column_name: ref_range_string}
    reference_ranges = {}
    if ref_row:
        for i, col in enumerate(headers):
            if i > 0 and i < len(ref_row) and ref_row[i]:
                reference_ranges[col] = ref_row[i]

    return {
        "source_file": os.path.basename(filepath),
        "source_format": "csv",
        "headers": headers,
        "records": records,
        "reference_ranges": reference_ranges,
        "row_count": len(records),
    }
