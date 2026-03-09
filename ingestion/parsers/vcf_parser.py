"""
VCF parser for genomic variant files.
Parses VCFv4.x format — extracts variants and INFO fields.
"""

import os


def parse(filepath: str) -> dict:
    meta = []
    header = None
    variants = []
    sample_id = None

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("##"):
                meta.append(line)
            elif line.startswith("#CHROM"):
                header = line.lstrip("#").split("\t")
                # Last column is sample ID
                if len(header) > 9:
                    sample_id = header[-1]
            elif line and header:
                parts = line.split("\t")
                if len(parts) < 8:
                    continue

                # Parse INFO field into dict
                info_dict = {}
                for item in parts[7].split(";"):
                    if "=" in item:
                        k, v = item.split("=", 1)
                        info_dict[k] = v
                    else:
                        info_dict[item] = True

                variants.append({
                    "chrom":     parts[0],
                    "pos":       parts[1],
                    "rsid":      parts[2],
                    "ref":       parts[3],
                    "alt":       parts[4],
                    "filter":    parts[6],
                    "info":      info_dict,
                    "genotype":  parts[9] if len(parts) > 9 else None,
                })

    return {
        "source_file": os.path.basename(filepath),
        "source_format": "vcf",
        "sample_id": sample_id,
        "meta_lines": len(meta),
        "variants": variants,
        "variant_count": len(variants),
    }
