"""
BioHarmonize — Semantic Harmonization Agent (Claude-powered)

For each patient record (from ingestion agent) this agent:
  1. Detects language (Bengali, Hindi, English)
  2. Extracts clinical entities: diagnoses, medications, lab values, vitals, demographics
  3. Maps each entity to ICD-10 code and OMOP concept ID
  4. Returns confidence score + reasoning per entity
  5. Flags uncertain mappings rather than guessing
  6. Writes full provenance JSON per patient to /output/provenance/
"""

import os
import json
import datetime
import anthropic

from harmonization.omop_reference import build_reference_block

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MODEL         = "claude-opus-4-6"
MAX_TOKENS    = 32000
OUTPUT_DIR    = os.path.join(os.path.dirname(__file__), "..", "output")
PROVENANCE_DIR = os.path.join(OUTPUT_DIR, "provenance")

os.makedirs(PROVENANCE_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are BioHarmonize, a specialist clinical NLP agent for South Asian multilingual medical data standardization.

Your task is to analyze raw patient records that may contain clinical text in Bengali (বাংলা), Hindi (हिन्दी), or English — often mixed — along with non-standard lab result tables and genomic variant data.

You must extract ALL clinical entities and map them to international standards (ICD-10, OMOP CDM).

{reference_block}

=== OUTPUT RULES (strictly enforced) ===
1. Return ONLY valid JSON. No prose, no markdown, no explanation outside the JSON.
2. Every extracted entity must have ALL of these fields:
   - "original_text": exact text as found in the source (preserve original script — Bengali/Hindi/English)
   - "language": one of "bengali", "hindi", "english", "mixed"
   - "standardized_english_term": canonical English medical term
   - "icd10_code": ICD-10 code string, or null if not applicable
   - "omop_concept_id": OMOP concept ID string from the reference table, or null if measurement/not in table
   - "confidence": float 0.0–1.0 (be honest — flag uncertainty rather than guess)
   - "reasoning": 1–2 sentences explaining the translation + mapping decision
   - "flag": one of null | "low_confidence" | "needs_review" | "uncertain_mapping" | "no_standard_code"

3. Confidence thresholds:
   - 0.9–1.0: Direct match, unambiguous
   - 0.7–0.89: High confidence, minor ambiguity
   - 0.5–0.69: Moderate — FLAG as "low_confidence"
   - <0.5: Uncertain — FLAG as "uncertain_mapping", do NOT invent codes

4. For measurements/lab values:
   - Extract the numeric value AND unit separately
   - Note whether value is within or outside the local reference range provided
   - Use OMOP measurement concept IDs (not condition IDs) from the reference table

5. For medications:
   - Extract drug name, dose, frequency if present
   - Standardize to generic name (not brand name)

6. For variants (VCF):
   - Extract rsID, gene, genotype, clinical significance
   - Map phenotype to relevant OMOP condition concept ID

7. If a Bengali or Hindi term has NO clear equivalent or you are genuinely uncertain:
   - Set confidence < 0.5, flag as "uncertain_mapping"
   - Still provide your best attempt at standardized_english_term
   - Set icd10_code and omop_concept_id to null

=== OUTPUT JSON SCHEMA ===
{{
  "patient_id": "string",
  "language_detected": "bengali|hindi|english|mixed",
  "entities": {{
    "demographics": {{
      "name":   {{ ...entity_struct... }},
      "age":    {{ ...entity_struct... }},
      "sex":    {{ ...entity_struct... }}
    }},
    "diagnoses":   [ {{ ...entity_struct... }} ],
    "medications": [ {{ ...entity_struct... }} ],
    "vitals":      [ {{ ...entity_struct... }} ],
    "lab_values":  [ {{ ...entity_struct... }} ],
    "variants":    [ {{ ...entity_struct... }} ]
  }},
  "flags": ["list of any patient-level concerns"],
  "harmonization_metadata": {{
    "model": "string",
    "source_files": ["list"],
    "total_entities": integer,
    "low_confidence_count": integer,
    "uncertain_count": integer,
    "timestamp": "ISO8601"
  }}
}}
"""

USER_PROMPT_TEMPLATE = """Harmonize the following patient record. Patient ID: {patient_id}

{clinical_note_section}
{lab_section}
{vcf_section}

Extract ALL entities. Map every diagnosis, medication, vital, lab value, and variant.
Return only the JSON object — no other text.
"""


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _build_clinical_note_section(sources: list) -> str:
    notes = [s for s in sources if s.get("_format") == "text"]
    if not notes:
        return ""
    parts = ["=== CLINICAL NOTE(S) ==="]
    for note in notes:
        lang = note.get("language_hint", "unknown")
        parts.append(f"[Source: {note['source_file']} | Language hint: {lang}]")
        parts.append(note.get("raw_text", ""))
    return "\n".join(parts)


def _build_lab_section(sources: list) -> str:
    csvs = [s for s in sources if s.get("_format") == "csv"]
    if not csvs:
        return ""
    parts = ["=== LAB RESULTS (non-standard column names — map to standard terms) ==="]
    for csv_src in csvs:
        parts.append(f"[Source: {csv_src['source_file']}]")
        headers = csv_src.get("headers", [])
        records = csv_src.get("records", [])
        ref_ranges = csv_src.get("reference_ranges", {})

        if records:
            rec = records[0]
            for col in headers:
                val = rec.get(col, "")
                ref = ref_ranges.get(col, "")
                ref_str = f"  [ref: {ref}]" if ref else ""
                parts.append(f"  {col}: {val}{ref_str}")
    return "\n".join(parts)


def _build_vcf_section(sources: list) -> str:
    vcfs = [s for s in sources if s.get("_format") == "vcf"]
    if not vcfs:
        return ""
    parts = ["=== GENOMIC VARIANTS (VCF) ==="]
    for vcf in vcfs:
        parts.append(f"[Source: {vcf['source_file']} | Sample: {vcf.get('sample_id', '?')}]")
        for v in vcf.get("variants", []):
            info = v.get("info", {})
            parts.append(
                f"  rsID={v['rsid']}  GENE={info.get('GENE','?')}  "
                f"GT={v.get('genotype','?')}  "
                f"CLNSIG={info.get('CLNSIG','?')}  "
                f"AF_SAS={info.get('AF_SAS','?')}  "
                f"PHENOTYPE={info.get('PHENOTYPE','?')}"
            )
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Core harmonization call
# ---------------------------------------------------------------------------

def harmonize_patient(patient_record: dict, verbose: bool = False) -> dict:
    """
    Call Claude to harmonize a single patient record.
    Returns the structured harmonization result dict.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY not found in environment. "
            "Run: export ANTHROPIC_API_KEY='your-key'"
        )

    client = anthropic.Anthropic(api_key=api_key)

    patient_id = patient_record.get("patient_id", "UNKNOWN")
    sources    = patient_record.get("sources", [])

    # Build prompt sections
    note_section = _build_clinical_note_section(sources)
    lab_section  = _build_lab_section(sources)
    vcf_section  = _build_vcf_section(sources)

    source_files = [s.get("source_file", "?") for s in sources]

    system = SYSTEM_PROMPT.format(reference_block=build_reference_block())
    user   = USER_PROMPT_TEMPLATE.format(
        patient_id=patient_id,
        clinical_note_section=note_section,
        lab_section=lab_section,
        vcf_section=vcf_section,
    )

    if verbose:
        print(f"  [Harmonization] Calling Claude for {patient_id} ...")
        print(f"  [Harmonization] Sources: {source_files}")

    # Stream the response — avoids timeout on long inputs/outputs
    # Note: thinking disabled here so all tokens go to structured JSON output.
    # The system prompt + OMOP reference table provides sufficient grounding.
    with client.messages.stream(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system,
        messages=[{"role": "user", "content": user}],
    ) as stream:
        response = stream.get_final_message()

    # Extract text block (thinking blocks are separate)
    raw_json_text = ""
    for block in response.content:
        if block.type == "text":
            raw_json_text = block.text.strip()
            break

    # Parse JSON — strip any accidental markdown fences
    if raw_json_text.startswith("```"):
        raw_json_text = raw_json_text.split("```")[1]
        if raw_json_text.startswith("json"):
            raw_json_text = raw_json_text[4:]
    raw_json_text = raw_json_text.strip()

    try:
        result = json.loads(raw_json_text)
    except json.JSONDecodeError as e:
        # Return a structured error record rather than crashing
        result = {
            "patient_id": patient_id,
            "error": f"JSON parse failed: {e}",
            "raw_response": raw_json_text[:500],
        }

    # Inject provenance metadata
    result.setdefault("harmonization_metadata", {})
    result["harmonization_metadata"]["model"]        = MODEL
    result["harmonization_metadata"]["source_files"] = source_files
    result["harmonization_metadata"]["timestamp"]    = datetime.datetime.utcnow().isoformat() + "Z"
    result["harmonization_metadata"]["input_tokens"]  = response.usage.input_tokens
    result["harmonization_metadata"]["output_tokens"] = response.usage.output_tokens

    # Write per-patient provenance JSON
    provenance_path = os.path.join(PROVENANCE_DIR, f"provenance_{patient_id}.json")
    with open(provenance_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    if verbose:
        meta = result.get("harmonization_metadata", {})
        n_entities = meta.get("total_entities", "?")
        n_flags    = meta.get("low_confidence_count", 0) + meta.get("uncertain_count", 0)
        print(f"  [Harmonization] {patient_id} done — {n_entities} entities, {n_flags} flagged")
        print(f"  [Harmonization] Tokens: {response.usage.input_tokens} in / {response.usage.output_tokens} out")
        print(f"  [Harmonization] Provenance → {provenance_path}")

    return result


# ---------------------------------------------------------------------------
# CLI — test on P001
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    from ingestion.ingestion_agent import ingest_directory

    DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "synthetic"))

    print("BioHarmonize — Harmonization Agent")
    print("=" * 50)
    print(f"Model  : {MODEL}")
    print(f"Target : P001 (test run)")
    print()

    # Ingest all patients, then pick P001
    records = ingest_directory(DATA_DIR, verbose=False)
    p001 = records.get("P001")
    if not p001:
        print("ERROR: P001 not found in ingested records.")
        sys.exit(1)

    result = harmonize_patient(p001, verbose=True)

    print()
    print("=" * 50)
    print("RAW JSON OUTPUT — P001")
    print("=" * 50)
    print(json.dumps(result, ensure_ascii=False, indent=2))
