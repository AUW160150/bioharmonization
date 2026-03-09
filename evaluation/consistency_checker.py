"""
BioHarmonize Self-Consistency & Cross-Lingual Checker
=======================================================
Checks that:
1. The same concept expressed in different languages maps to the same ICD-10/OMOP code
2. Repeated extraction of the same term produces consistent results
"""

import json
from pathlib import Path
from itertools import combinations

SCRIPT_DIR = Path(__file__).parent
GLOSSARY_FILE = SCRIPT_DIR / "glossary.json"

# Simulate 3 independent extraction runs per concept
# Each run may produce slightly different confidence but should give same codes
MULTI_RUN_EXTRACTIONS = {
    "C001": [
        {"lang":"bn", "run":1, "pred_icd":"E11",    "pred_omop":"201826",  "conf":0.94},
        {"lang":"bn", "run":2, "pred_icd":"E11",    "pred_omop":"201826",  "conf":0.93},
        {"lang":"bn", "run":3, "pred_icd":"E11",    "pred_omop":"201826",  "conf":0.95},
        {"lang":"hi", "run":1, "pred_icd":"E11",    "pred_omop":"201826",  "conf":0.92},
        {"lang":"hi", "run":2, "pred_icd":"E11",    "pred_omop":"201826",  "conf":0.91},
        {"lang":"hi", "run":3, "pred_icd":"E11",    "pred_omop":"201826",  "conf":0.93},
    ],
    "C002": [
        {"lang":"bn", "run":1, "pred_icd":"I10",    "pred_omop":"316866",  "conf":0.91},
        {"lang":"bn", "run":2, "pred_icd":"I10",    "pred_omop":"316866",  "conf":0.90},
        {"lang":"bn", "run":3, "pred_icd":"I10",    "pred_omop":"316866",  "conf":0.92},
        {"lang":"hi", "run":1, "pred_icd":"I10",    "pred_omop":"316866",  "conf":0.90},
        {"lang":"hi", "run":2, "pred_icd":"I10",    "pred_omop":"316866",  "conf":0.89},
        {"lang":"hi", "run":3, "pred_icd":"I10",    "pred_omop":"316866",  "conf":0.91},
    ],
    "C003": [
        {"lang":"bn", "run":1, "pred_icd":"N18.3",  "pred_omop":"46271022","conf":0.85},
        {"lang":"bn", "run":2, "pred_icd":"N18.3",  "pred_omop":"46271022","conf":0.84},
        {"lang":"bn", "run":3, "pred_icd":"N18.31", "pred_omop":"46271022","conf":0.86},  # More specific
        {"lang":"hi", "run":1, "pred_icd":"N18.3",  "pred_omop":"46271022","conf":0.84},
        {"lang":"hi", "run":2, "pred_icd":"N18.3",  "pred_omop":"46271022","conf":0.85},
        {"lang":"hi", "run":3, "pred_icd":"N18.3",  "pred_omop":"46271022","conf":0.83},
    ],
    "C004": [
        {"lang":"bn", "run":1, "pred_icd":"E78.49", "pred_omop":"432867",  "conf":0.88},
        {"lang":"bn", "run":2, "pred_icd":"E78.49", "pred_omop":"432867",  "conf":0.87},
        {"lang":"bn", "run":3, "pred_icd":"E78.49", "pred_omop":"432867",  "conf":0.89},
        {"lang":"hi", "run":1, "pred_icd":"E78.49", "pred_omop":"432867",  "conf":0.86},
        {"lang":"hi", "run":2, "pred_icd":"E78.5",  "pred_omop":"432867",  "conf":0.82},  # 1 inconsistency
        {"lang":"hi", "run":3, "pred_icd":"E78.49", "pred_omop":"432867",  "conf":0.88},
    ],
    "C005": [
        {"lang":"bn", "run":1, "pred_icd":None,     "pred_omop":"3004410", "conf":0.89},
        {"lang":"bn", "run":2, "pred_icd":None,     "pred_omop":"3004410", "conf":0.88},
        {"lang":"bn", "run":3, "pred_icd":None,     "pred_omop":"3004410", "conf":0.90},
        {"lang":"hi", "run":1, "pred_icd":None,     "pred_omop":"3004410", "conf":0.91},
        {"lang":"hi", "run":2, "pred_icd":None,     "pred_omop":"3004410", "conf":0.90},
        {"lang":"hi", "run":3, "pred_icd":None,     "pred_omop":"3004410", "conf":0.91},
    ],
}

def check_consistency() -> dict:
    results = {}

    for concept_id, runs in MULTI_RUN_EXTRACTIONS.items():
        by_run  = {}
        by_lang = {}
        for r in runs:
            key = r["run"]
            by_run.setdefault(key, []).append(r)
            by_lang.setdefault(r["lang"], []).append(r)

        # Self-consistency: do all runs for same lang agree on ICD?
        self_consistent = 0
        self_total = 0
        for lang, lang_runs in by_lang.items():
            icds = [r["pred_icd"] for r in lang_runs]
            # Check majority agreement (allow 1 more-specific variant)
            base_icds = [i[:4] if i else None for i in icds]  # Truncate to 4 chars
            most_common = max(set(base_icds), key=base_icds.count)
            consistent = sum(1 for i in base_icds if i == most_common)
            self_consistent += consistent
            self_total += len(lang_runs)

        # Cross-lingual: do Bengali and Hindi map to same OMOP?
        bn_omops = {r["pred_omop"] for r in by_lang.get("bn", [])}
        hi_omops = {r["pred_omop"] for r in by_lang.get("hi", [])}
        cross_lingual_match = bool(bn_omops & hi_omops)

        results[concept_id] = {
            "self_consistency_rate": round(self_consistent / self_total, 4) if self_total else 0,
            "cross_lingual_omop_match": cross_lingual_match,
            "total_runs": len(runs),
        }

    # Aggregate
    avg_self  = sum(v["self_consistency_rate"] for v in results.values()) / len(results)
    cross_ok  = sum(1 for v in results.values() if v["cross_lingual_omop_match"])
    cross_pct = cross_ok / len(results)

    summary = {
        "by_concept": results,
        "summary": {
            "avg_self_consistency": round(avg_self, 4),
            "cross_lingual_match_rate": round(cross_pct, 4),
            "concepts_evaluated": len(results),
        }
    }
    return summary

if __name__ == "__main__":
    result = check_consistency()
    print(json.dumps(result, indent=2))
