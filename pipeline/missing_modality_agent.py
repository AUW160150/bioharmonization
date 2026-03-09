"""
BioHarmonize — Missing Modality Detection Agent

Runs after ingestion, before harmonization.
For each patient, detects which of the three clinical modalities are present
and uses Claude to assess the impact of any gaps — without dropping any patient.

Three modalities:
  clinical_note     (.txt)  — narrative, diagnoses, medications, vitals
  lab_results       (.csv)  — quantitative biomarkers
  genomic_variants  (.vcf)  — pharmacogenomic + risk variant data

Output per patient:
  modality_status:     {modality: {present, impact, what_can_be_inferred, what_is_lost}}
  completeness_score:  0.0–1.0 (rule-based weighted, refined by Claude)
  overall_recommendation: what the harmonization agent should do differently
"""

import os
import json
import datetime
import anthropic

MODEL = "claude-sonnet-4-6"

# Weights for completeness score (must sum to 1.0)
MODALITY_WEIGHTS = {
    "clinical_note":    0.40,
    "lab_results":      0.40,
    "genomic_variants": 0.20,
}

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a clinical data quality specialist for South Asian multi-modal patient datasets.

Your job is to assess the impact of missing clinical modalities for a given patient
and suggest what can still be inferred from the data that IS present.

The three modalities are:
  1. clinical_note     — physician narrative: diagnoses, medications, vitals, history
  2. lab_results       — quantitative lab values: glucose, HbA1c, lipids, renal function
  3. genomic_variants  — VCF data: pharmacogenomic and risk variants (e.g. TCF7L2, PCSK9)

For each MISSING modality, assess:
  - impact: "low" | "medium" | "high"
      low    = data gap has minimal effect on downstream OMOP mapping
      medium = some important fields will have reduced confidence
      high   = core clinical picture is significantly incomplete
  - what_can_be_inferred: specific, concrete statements about what partial evidence remains
  - what_is_lost: what clinical insight is permanently absent without this modality
  - compensating_evidence: specific fields in present modalities that partially offset the gap

Be specific — cite actual biomarkers, values, or variant names where relevant.
Never be vague. A good answer says "HbA1c of 9.8% confirms T2DM without requiring a clinical note" not "some lab values may help".

Return ONLY valid JSON — no prose outside the JSON.

Output schema:
{
  "patient_id": "string",
  "completeness_score": float (0.0-1.0, your refined assessment),
  "modality_assessments": {
    "<modality_name>": {
      "present": true/false,
      "impact": "low"|"medium"|"high"|null,
      "what_can_be_inferred": "string or null",
      "what_is_lost": "string or null",
      "compensating_evidence": "string or null"
    }
  },
  "overall_recommendation": "1-2 sentences: how should the harmonization agent handle this record?",
  "harmonization_flags": ["list of specific flags to attach to harmonized entities"]
}"""

# ---------------------------------------------------------------------------
# Modality detection
# ---------------------------------------------------------------------------

def detect_modalities(patient_record: dict) -> dict:
    """Inspect ingested sources and return present/absent per modality."""
    formats_present = {s.get("_format") for s in patient_record.get("sources", [])}
    return {
        "clinical_note":    "text" in formats_present,
        "lab_results":      "csv"  in formats_present,
        "genomic_variants": "vcf"  in formats_present,
    }


def base_completeness_score(present: dict) -> float:
    return round(sum(MODALITY_WEIGHTS[m] for m, p in present.items() if p), 2)


def _summarise_available_data(patient_record: dict) -> str:
    """Build a brief summary of what data IS present, for Claude's context."""
    lines = []
    for src in patient_record.get("sources", []):
        fmt = src.get("_format")
        if fmt == "text":
            text = src.get("raw_text", "")
            lines.append(f"CLINICAL NOTE ({src.get('language_hint', 'unknown')} language, {src.get('char_count', 0)} chars):")
            lines.append(text[:600] + ("…" if len(text) > 600 else ""))
        elif fmt == "csv":
            headers = src.get("headers", [])
            records = src.get("records", [])
            ref     = src.get("reference_ranges", {})
            lines.append(f"LAB RESULTS ({src.get('source_file', '')}):")
            if records:
                rec = records[0]
                for col in headers[:12]:  # first 12 columns
                    val    = rec.get(col, "")
                    refval = ref.get(col, "")
                    lines.append(f"  {col}: {val}" + (f"  [ref: {refval}]" if refval else ""))
        elif fmt == "vcf":
            variants = src.get("variants", [])
            lines.append(f"GENOMIC VARIANTS ({src.get('variant_count', 0)} variants):")
            for v in variants:
                info = v.get("info", {})
                lines.append(f"  {v.get('rsid')} {info.get('GENE','')} GT={v.get('genotype','')} PHENOTYPE={info.get('PHENOTYPE','')}")
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Claude assessment call
# ---------------------------------------------------------------------------

def _assess_with_claude(patient_id: str, present: dict, available_summary: str) -> dict:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY not set")

    client = anthropic.Anthropic(api_key=api_key)

    missing = [m for m, p in present.items() if not p]

    user_prompt = f"""Patient ID: {patient_id}

MODALITY STATUS:
  clinical_note    : {"PRESENT" if present["clinical_note"]    else "ABSENT"}
  lab_results      : {"PRESENT" if present["lab_results"]      else "ABSENT"}
  genomic_variants : {"PRESENT" if present["genomic_variants"] else "ABSENT"}

MISSING MODALITIES: {", ".join(missing)}

DATA AVAILABLE:
{available_summary}

Assess the impact of each missing modality and what can be inferred from present data.
Return only the JSON object."""

    with client.messages.stream(
        model=MODEL,
        max_tokens=3000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    ) as stream:
        response = stream.get_final_message()

    raw = next((b.text for b in response.content if b.type == "text"), "")

    # Strip markdown fences if present
    text = raw.strip()
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
        if text.startswith("json"):
            text = text[4:]
    text = text.strip()

    result = json.loads(text)
    result["_tokens"] = {
        "input":  response.usage.input_tokens,
        "output": response.usage.output_tokens,
    }
    return result

# ---------------------------------------------------------------------------
# Main agent function
# ---------------------------------------------------------------------------

def assess_patient(patient_record: dict, verbose: bool = False) -> dict:
    """
    Assess modality completeness for a single patient.
    Enriches the patient_record in-place and returns it.
    Always returns the patient — never drops incomplete records.
    """
    pid     = patient_record.get("patient_id", "UNKNOWN")
    present = detect_modalities(patient_record)
    score   = base_completeness_score(present)
    missing = [m for m, p in present.items() if not p]

    if verbose:
        status_str = " | ".join(
            f"{'✓' if p else '✗'} {m}" for m, p in present.items()
        )
        print(f"  [Modality]  {pid} — {status_str}  →  score={score:.2f}")

    if not missing:
        # All modalities present — no Claude call needed
        assessment = {
            "patient_id":          pid,
            "completeness_score":  1.0,
            "modality_assessments": {
                m: {"present": True, "impact": None,
                    "what_can_be_inferred": None, "what_is_lost": None,
                    "compensating_evidence": None}
                for m in MODALITY_WEIGHTS
            },
            "overall_recommendation": "All modalities present. Proceed with full harmonization.",
            "harmonization_flags":    [],
            "_source": "rule_based",
        }
    else:
        if verbose:
            print(f"  [Modality]  {pid} — calling Claude for gap assessment ({len(missing)} missing)...")
        available_summary = _summarise_available_data(patient_record)
        try:
            assessment = _assess_with_claude(pid, present, available_summary)
            assessment["_source"] = "claude"
            # Ensure present modalities are marked correctly
            for m, p in present.items():
                if m in assessment.get("modality_assessments", {}):
                    assessment["modality_assessments"][m]["present"] = p
                else:
                    assessment.setdefault("modality_assessments", {})[m] = {
                        "present": p, "impact": None,
                        "what_can_be_inferred": None, "what_is_lost": None,
                        "compensating_evidence": None,
                    }
        except Exception as e:
            # Graceful fallback — never drop the patient
            assessment = {
                "patient_id":         pid,
                "completeness_score": score,
                "modality_assessments": {
                    m: {"present": p, "impact": "high" if not p else None,
                        "what_can_be_inferred": None, "what_is_lost": None,
                        "compensating_evidence": None}
                    for m, p in present.items()
                },
                "overall_recommendation": f"Claude assessment failed ({e}). Using rule-based score.",
                "harmonization_flags": [f"missing_{m}" for m in missing],
                "_source": "fallback",
                "_error":  str(e),
            }

    assessment["patient_id"]        = pid
    assessment["missing_modalities"] = missing
    assessment["present_modalities"] = [m for m, p in present.items() if p]

    # Attach to patient record
    patient_record["modality_assessment"] = assessment
    return patient_record


def assess_all_patients(patient_records: dict, verbose: bool = True) -> dict:
    """Run modality assessment on all patients."""
    print(f"\n  Assessing {len(patient_records)} patients for modality completeness...")

    incomplete = 0
    for pid in sorted(patient_records.keys()):
        assess_patient(patient_records[pid], verbose=verbose)
        if patient_records[pid]["modality_assessment"]["missing_modalities"]:
            incomplete += 1

    scores = [
        patient_records[pid]["modality_assessment"].get("completeness_score", 0)
        for pid in patient_records
    ]
    avg_score = sum(scores) / len(scores) if scores else 0

    print(f"  Modality check complete — {incomplete}/{len(patient_records)} patients incomplete")
    print(f"  Average completeness score: {avg_score:.2f}")

    return patient_records


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from ingestion.ingestion_agent import ingest_directory

    DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "synthetic"))
    records  = ingest_directory(DATA_DIR, verbose=False)
    records  = assess_all_patients(records, verbose=True)

    print("\n=== Completeness Scores ===")
    for pid in sorted(records):
        ma = records[pid]["modality_assessment"]
        score   = ma.get("completeness_score", 0)
        missing = ma.get("missing_modalities", [])
        print(f"  {pid}  score={score:.2f}  missing={missing or 'none'}")
        if missing:
            print(f"       → {ma.get('overall_recommendation', '')[:100]}")
