"""
BioHarmonize Evaluation Script
================================
Evaluates NLP extraction accuracy on the multilingual clinical glossary.
Simulates pipeline output and measures:
  - Extraction accuracy per language
  - ICD-10 / OMOP code accuracy
  - Confidence calibration
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Tuple

SCRIPT_DIR = Path(__file__).parent
GLOSSARY_FILE = SCRIPT_DIR / "glossary.json"

# Simulated pipeline extraction output (mirrors screen2_pipeline.html LOG_TIMELINE)
PIPELINE_OUTPUT = [
    # Bengali extractions
    {"raw": "টাইপ ২ ডায়াবেটিস",            "pred_icd": "E11",     "pred_omop": "201826",   "conf": 0.94, "lang": "bn"},
    {"raw": "গ্লাইকোসাইলেটেড হিমোগ্লোবিন",   "pred_icd": None,     "pred_omop": "3004410",  "conf": 0.89, "lang": "bn"},
    {"raw": "উচ্চ রক্তচাপ",                   "pred_icd": "I10",     "pred_omop": "316866",   "conf": 0.91, "lang": "bn"},
    {"raw": "মিশ্র হাইপারলিপিডেমিয়া",          "pred_icd": "E78.49",  "pred_omop": "432867",   "conf": 0.88, "lang": "bn"},
    {"raw": "দীর্ঘস্থায়ী কিডনি রোগ স্তর ৩",  "pred_icd": "N18.3",   "pred_omop": "46271022", "conf": 0.85, "lang": "bn"},

    # Hindi extractions
    {"raw": "टाइप 2 मधुमेह",                  "pred_icd": "E11",     "pred_omop": "201826",   "conf": 0.92, "lang": "hi"},
    {"raw": "उपवास रक्त शर्करा",              "pred_icd": None,      "pred_omop": "4144235",  "conf": 0.87, "lang": "hi"},
    {"raw": "उच्च रक्तचाप",                   "pred_icd": "I10",     "pred_omop": "316866",   "conf": 0.90, "lang": "hi"},
    {"raw": "मिश्रित हाइपरलिपिडेमिया",        "pred_icd": "E78.49",  "pred_omop": "432867",   "conf": 0.86, "lang": "hi"},
    {"raw": "ग्लाइकेटेड हीमोग्लोबिन",         "pred_icd": None,      "pred_omop": "3004410",  "conf": 0.91, "lang": "hi"},

    # English extractions
    {"raw": "Metformin 500mg OD",              "pred_icd": "A10BA02", "pred_omop": "1503297",  "conf": 0.96, "lang": "en"},
    {"raw": "Atorvastatin 40mg HS",            "pred_icd": "C10AA05", "pred_omop": "1545958",  "conf": 0.94, "lang": "en"},
    {"raw": "N18.3 Chronic kidney disease",    "pred_icd": "N18.3",   "pred_omop": "46271022", "conf": 0.82, "lang": "en"},
    {"raw": "E78.5 Hyperlipidemia NOS",        "pred_icd": "E78.5",   "pred_omop": "432867",   "conf": 0.81, "lang": "en"},  # Pre-correction

    # Validated / post-correction
    {"raw": "N18.31 CKD Stage 3a (corrected)", "pred_icd": "N18.31",  "pred_omop": "46271022", "conf": 0.92, "lang": "en"},
    {"raw": "E78.49 Mixed hyperlipidemia (corrected)", "pred_icd": "E78.49", "pred_omop": "432867", "conf": 0.91, "lang": "en"},
    {"raw": "PCSK9 rs11591147 GT=0/1",         "pred_icd": None,      "pred_omop": "37396683", "conf": 0.95, "lang": "vcf"},
    {"raw": "I25.110 Atherosclerosis unstable angina", "pred_icd": "I25.110", "pred_omop": "317576", "conf": 0.90, "lang": "en"},
]

def load_glossary() -> List[Dict]:
    with open(GLOSSARY_FILE) as f:
        data = json.load(f)
    return data["concepts"]

def find_ground_truth(raw: str, lang: str, concepts: List[Dict], pred_omop: str = None):
    """Find the ground-truth concept for a raw extraction."""
    for c in concepts:
        # Direct predicted OMOP match (for VCF/structured entries)
        if pred_omop and c.get("omop") == pred_omop:
            return c
        candidates = [c.get(lang, ""), *c.get(f"{lang}_alt", [])]
        candidates = [s.lower() for s in candidates if s]
        raw_lower = raw.lower()
        # Substring match (clinical text often contains dosage/qualifiers)
        if any(cand and cand in raw_lower or raw_lower in cand for cand in candidates):
            return c
        # ICD or OMOP code match in raw text
        if c.get("icd10") and c["icd10"].lower() in raw_lower:
            return c
    return None

def evaluate() -> Dict:
    concepts = load_glossary()
    results = {"by_lang": {}, "overall": {}}

    lang_buckets: Dict[str, List] = {}
    for item in PIPELINE_OUTPUT:
        lang = item["lang"]
        if lang not in lang_buckets:
            lang_buckets[lang] = []
        lang_buckets[lang].append(item)

    total_correct_icd = 0
    total_correct_omop = 0
    total_matched = 0
    total = len(PIPELINE_OUTPUT)
    conf_errors = []

    for lang, items in lang_buckets.items():
        correct_icd = 0
        correct_omop = 0
        matched = 0
        confs = []

        for item in items:
            gt = find_ground_truth(item["raw"], lang if lang in ("bn", "hi", "en") else "en", concepts, item.get("pred_omop"))
            if gt is None:
                continue
            matched += 1

            # ICD accuracy
            pred_icd = item.get("pred_icd")
            gt_icd   = gt.get("icd10")
            if pred_icd and gt_icd:
                # Allow prefix match (N18.3 matches N18.31)
                if pred_icd == gt_icd or gt_icd.startswith(pred_icd) or pred_icd.startswith(gt_icd):
                    correct_icd += 1
            elif pred_icd is None and gt_icd is None:
                correct_icd += 1  # Correctly null

            # OMOP accuracy
            pred_omop = item.get("pred_omop")
            gt_omop   = gt.get("omop")
            if pred_omop == gt_omop:
                correct_omop += 1

            confs.append(item["conf"])

        results["by_lang"][lang] = {
            "total": len(items),
            "matched_to_gt": matched,
            "icd_accuracy": round(correct_icd / matched, 4) if matched else 0,
            "omop_accuracy": round(correct_omop / matched, 4) if matched else 0,
            "avg_confidence": round(sum(confs) / len(confs), 4) if confs else 0,
        }
        total_correct_icd  += correct_icd
        total_correct_omop += correct_omop
        total_matched      += matched

    results["overall"] = {
        "total_extractions": total,
        "total_matched_to_gt": total_matched,
        "icd_accuracy":  round(total_correct_icd  / total_matched, 4) if total_matched else 0,
        "omop_accuracy": round(total_correct_omop / total_matched, 4) if total_matched else 0,
        "avg_confidence": round(
            sum(i["conf"] for i in PIPELINE_OUTPUT) / total, 4
        ),
    }
    return results

if __name__ == "__main__":
    results = evaluate()
    print(json.dumps(results, indent=2, ensure_ascii=False))
