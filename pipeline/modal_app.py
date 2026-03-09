"""
BioHarmonize — Modal App
Defines cloud functions for parallel harmonization + validation.
All Claude API calls happen inside Modal cloud using the 'anthropic-key' secret.
"""

import modal

# ---------------------------------------------------------------------------
# App + Image
# ---------------------------------------------------------------------------
app = modal.App("bioharmonize")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("anthropic>=0.84.0")
)

MODEL = "claude-sonnet-4-6"
MAX_TOKENS_HARMONIZE = 16000
MAX_TOKENS_VALIDATE  = 8000

# ---------------------------------------------------------------------------
# OMOP reference table (inlined — Modal cloud has no local file access)
# ---------------------------------------------------------------------------
OMOP_REFERENCE = """
=== OMOP CONCEPT ID REFERENCE (use these exact IDs) ===

--- CONDITIONS ---
  Type 2 Diabetes Mellitus: omop=201826  icd10=E11.9
  Type 2 Diabetes with CKD: omop=201826  icd10=E11.65
  Hypertension: omop=320128  icd10=I10
  Coronary Artery Disease: omop=317576  icd10=I25.10
  Congestive Heart Failure: omop=316139  icd10=I50.9
  HFrEF: omop=316139  icd10=I50.20
  Myocardial Infarction: omop=4329847  icd10=I21.9
  STEMI: omop=314666  icd10=I21.3
  Mixed Dyslipidemia: omop=432867  icd10=E78.5
  Hypercholesterolaemia: omop=432867  icd10=E78.00
  Chronic Kidney Disease Stage 3a: omop=443601  icd10=N18.31
  Chronic Kidney Disease Stage 3b: omop=443601  icd10=N18.32
  Atrial Fibrillation: omop=313217  icd10=I48.91
  Stable Angina: omop=321318  icd10=I20.9
  Pre-diabetes: omop=4193704  icd10=R73.09
  Obesity: omop=433736  icd10=E66.9

--- MEDICATIONS ---
  Metformin: omop=1503297
  Atorvastatin: omop=1545958
  Rosuvastatin: omop=1510813
  Aspirin: omop=1112807
  Amlodipine: omop=1332418
  Ramipril: omop=1308216
  Glimepiride: omop=1597756
  Glyclazide: omop=1516766
  Insulin Glargine: omop=40239216
  Insulin Lispro: omop=1516023
  Empagliflozin: omop=45774751
  Sitagliptin: omop=1580747
  Furosemide: omop=956874
  Carvedilol: omop=1346823
  Bisoprolol: omop=1338005
  Spironolactone: omop=974166
  Warfarin: omop=1310149
  Clopidogrel: omop=1322184
  Telmisartan: omop=1317640
  Metoprolol Succinate: omop=1307046
  Ezetimibe: omop=1547504
  Isosorbide Mononitrate: omop=1361364
  Omeprazole: omop=923645
  Omega-3 Fatty Acids: omop=19129655

--- MEASUREMENTS ---
  HbA1c: omop=3004410  loinc=4548-4
  Fasting Blood Glucose: omop=3004501  loinc=76629-5
  LDL Cholesterol: omop=3028437  loinc=2089-1
  HDL Cholesterol: omop=3007070  loinc=2085-9
  Total Cholesterol: omop=3019900  loinc=2093-3
  Triglycerides: omop=3022192  loinc=2571-8
  Serum Creatinine: omop=3016723  loinc=2160-0
  eGFR: omop=3049187  loinc=62238-1
  Systolic Blood Pressure: omop=3004249  loinc=8480-6
  Diastolic Blood Pressure: omop=3012888  loinc=8462-4
  Body Weight: omop=3025315  loinc=29463-7
  BMI: omop=3038553  loinc=39156-5
  Heart Rate: omop=3027018  loinc=8867-4
  BNP: omop=3024929  loinc=42637-9
  Serum Potassium: omop=3023103  loinc=2823-3
  Serum Sodium: omop=3019550  loinc=2951-2
  Urine ACR: omop=3029683  loinc=9318-7
  Haemoglobin: omop=3000963  loinc=718-7
  Troponin I: omop=3016931  loinc=10839-9
  INR: omop=3023314  loinc=6301-6
  SpO2: omop=3016502  loinc=59408-5
"""

# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

HARMONIZE_SYSTEM = (
    """You are BioHarmonize, a specialist clinical NLP agent for South Asian multilingual medical data standardization.

Your task is to analyze raw patient records that may contain clinical text in Bengali (বাংলা), Hindi (हिन्दी), or English — often mixed — along with non-standard lab result tables and genomic variant data.

Extract ALL clinical entities and map them to international standards (ICD-10, OMOP CDM).

"""
    + OMOP_REFERENCE
    + """

=== OUTPUT RULES (strictly enforced) ===
1. Return ONLY valid JSON. No prose, no markdown, no explanation outside the JSON.
2. Every extracted entity must have ALL of these fields:
   - "original_text": exact text as found in the source (preserve original script)
   - "language": one of "bengali", "hindi", "english", "mixed"
   - "standardized_english_term": canonical English medical term
   - "icd10_code": ICD-10 code string, or null if not applicable
   - "omop_concept_id": OMOP concept ID string from the reference table, or null
   - "confidence": float 0.0-1.0
   - "reasoning": 1-2 sentences explaining the mapping decision
   - "flag": null | "low_confidence" | "needs_review" | "uncertain_mapping" | "no_standard_code"

3. Confidence thresholds:
   - 0.9-1.0: Direct match, unambiguous
   - 0.7-0.89: High confidence, minor ambiguity
   - 0.5-0.69: Moderate — FLAG as "low_confidence"
   - <0.5: Uncertain — FLAG as "uncertain_mapping", do NOT invent codes

4. For measurements/lab values: extract numeric value AND unit separately,
   note whether value is within or outside the local reference range provided.

5. For medications: extract drug name, dose, frequency. Standardize to generic name.

6. For variants: extract rsID, gene, genotype, clinical significance.

7. Count total_entities, low_confidence_count, uncertain_count in harmonization_metadata.

=== OUTPUT JSON SCHEMA ===
{
  "patient_id": "string",
  "language_detected": "bengali|hindi|english|mixed",
  "entities": {
    "demographics": {
      "name": { ...entity... }, "age": { ...entity... }, "sex": { ...entity... }
    },
    "diagnoses":   [ { ...entity... } ],
    "medications": [ { ...entity... } ],
    "vitals":      [ { ...entity... } ],
    "lab_values":  [ { ...entity... } ],
    "variants":    [ { ...entity... } ]
  },
  "flags": ["patient-level concerns"],
  "harmonization_metadata": {
    "total_entities": integer,
    "low_confidence_count": integer,
    "uncertain_count": integer
  }
}"""
)

VALIDATE_SYSTEM = """You are BioHarmonize Validator — a second-opinion clinical coding agent.

You will receive a list of clinical entities that were flagged during harmonization
(confidence < 0.85 or flag != null). Your job is to independently review each mapping
and either CONFIRM it, CORRECT it, or escalate as FLAGGED.

=== VALIDATION RULES ===
1. Return ONLY valid JSON. No prose outside the JSON.
2. For each entity, decide:
   - "confirmed": original mapping is correct, no changes needed
   - "corrected": original mapping has an error — provide the corrected fields
   - "flagged": genuinely ambiguous — cannot determine correct mapping with confidence

3. When correcting, provide only the fields that change plus validation_reasoning.
4. Be specific in validation_reasoning — cite the clinical or coding rule you're applying.
5. Do not invent OMOP concept IDs. If uncertain, set to null and flag.

=== OUTPUT SCHEMA ===
{
  "patient_id": "string",
  "validations": [
    {
      "category": "diagnoses|medications|vitals|lab_values|variants|demographics",
      "index_or_key": "integer index or demographic key (name/age/sex)",
      "original_text": "string",
      "validation_status": "confirmed|corrected|flagged",
      "corrected_mapping": {
        "standardized_english_term": "string or null",
        "icd10_code": "string or null",
        "omop_concept_id": "string or null",
        "confidence": float,
        "flag": null
      },
      "validation_reasoning": "1-2 sentences explaining the decision"
    }
  ],
  "validation_metadata": {
    "entities_reviewed": integer,
    "confirmed": integer,
    "corrected": integer,
    "flagged": integer
  }
}"""

# ---------------------------------------------------------------------------
# Prompt builders (module-level — available in Modal cloud container)
# ---------------------------------------------------------------------------

def _build_note_section(sources):
    notes = [s for s in sources if s.get("_format") == "text"]
    if not notes:
        return ""
    parts = ["=== CLINICAL NOTE(S) ==="]
    for note in notes:
        lang = note.get("language_hint", "unknown")
        parts.append(f"[Source: {note['source_file']} | Language hint: {lang}]")
        parts.append(note.get("raw_text", ""))
    return "\n".join(parts)


def _build_lab_section(sources):
    csvs = [s for s in sources if s.get("_format") == "csv"]
    if not csvs:
        return ""
    parts = ["=== LAB RESULTS (non-standard column names — map to standard terms) ==="]
    for csv_src in csvs:
        parts.append(f"[Source: {csv_src['source_file']}]")
        headers   = csv_src.get("headers", [])
        records   = csv_src.get("records", [])
        ref_ranges = csv_src.get("reference_ranges", {})
        if records:
            rec = records[0]
            for col in headers:
                val = rec.get(col, "")
                ref = ref_ranges.get(col, "")
                ref_str = f"  [ref: {ref}]" if ref else ""
                parts.append(f"  {col}: {val}{ref_str}")
    return "\n".join(parts)


def _build_vcf_section(sources):
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
                f"GT={v.get('genotype','?')}  CLNSIG={info.get('CLNSIG','?')}  "
                f"AF_SAS={info.get('AF_SAS','?')}  PHENOTYPE={info.get('PHENOTYPE','?')}"
            )
    return "\n".join(parts)


def _build_harmonize_user_prompt(patient_record):
    pid     = patient_record.get("patient_id", "UNKNOWN")
    sources = patient_record.get("sources", [])
    sections = filter(None, [
        _build_note_section(sources),
        _build_lab_section(sources),
        _build_vcf_section(sources),
    ])
    body = "\n\n".join(sections)
    return (
        f"Harmonize the following patient record. Patient ID: {pid}\n\n"
        f"{body}\n\n"
        "Extract ALL entities. Return only the JSON object — no other text."
    )


def _collect_entities_for_validation(harmonized):
    """Return list of (category, index_or_key, entity) for entities needing validation."""
    to_validate = []
    entities = harmonized.get("entities", {})

    # Demographics (dict of key→entity)
    for key, entity in entities.get("demographics", {}).items():
        if isinstance(entity, dict):
            if entity.get("confidence", 1.0) < 0.85 or entity.get("flag"):
                to_validate.append(("demographics", key, entity))

    # List categories
    for cat in ("diagnoses", "medications", "vitals", "lab_values", "variants"):
        for idx, entity in enumerate(entities.get(cat, [])):
            if isinstance(entity, dict):
                if entity.get("confidence", 1.0) < 0.85 or entity.get("flag"):
                    to_validate.append((cat, idx, entity))

    return to_validate


def _build_validate_user_prompt(patient_id, entities_to_validate):
    lines = [f"Patient ID: {patient_id}", "", "Entities requiring validation:"]
    for cat, idx, entity in entities_to_validate:
        lines.append(f"\n[category={cat}  index_or_key={idx}]")
        lines.append(f"  original_text         : {entity.get('original_text', '')}")
        lines.append(f"  standardized_term     : {entity.get('standardized_english_term', '')}")
        lines.append(f"  icd10_code            : {entity.get('icd10_code', 'null')}")
        lines.append(f"  omop_concept_id       : {entity.get('omop_concept_id', 'null')}")
        lines.append(f"  confidence            : {entity.get('confidence', '?')}")
        lines.append(f"  flag                  : {entity.get('flag', 'null')}")
        lines.append(f"  reasoning             : {entity.get('reasoning', '')}")
    lines.append("\nReturn only the JSON object — no other text.")
    return "\n".join(lines)


def _apply_validations(harmonized, validation_result):
    """
    Merge validation decisions into the harmonized result.
    Preserves BOTH original_mapping and corrected_mapping for full audit trail.
    """
    entities = harmonized.get("entities", {})
    validations = validation_result.get("validations", [])

    for v in validations:
        cat        = v.get("category")
        idx        = v.get("index_or_key")
        status     = v.get("validation_status", "confirmed")
        corrected  = v.get("corrected_mapping")
        reasoning  = v.get("validation_reasoning", "")

        try:
            if cat == "demographics":
                entity = entities.get("demographics", {}).get(idx)
            else:
                entity = entities.get(cat, [])[int(idx)]
        except (KeyError, IndexError, TypeError, ValueError):
            continue

        if entity is None:
            continue

        # Store original mapping snapshot
        entity["original_mapping"] = {
            "standardized_english_term": entity.get("standardized_english_term"),
            "icd10_code":               entity.get("icd10_code"),
            "omop_concept_id":          entity.get("omop_concept_id"),
            "confidence":               entity.get("confidence"),
            "flag":                     entity.get("flag"),
        }

        entity["validation_status"]    = status
        entity["validation_reasoning"] = reasoning

        if status == "corrected" and corrected:
            entity["corrected_mapping"] = corrected
            # Update live fields to corrected values for OMOP output
            for field in ("standardized_english_term", "icd10_code",
                          "omop_concept_id", "confidence", "flag"):
                if field in corrected:
                    entity[field] = corrected[field]
        else:
            entity["corrected_mapping"] = None

    # Attach validation summary
    harmonized["validation_summary"] = validation_result.get("validation_metadata", {})
    return harmonized


def _parse_json_response(raw_text):
    import json
    text = raw_text.strip()
    if text.startswith("```"):
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else text
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


# ---------------------------------------------------------------------------
# Modal cloud functions
# ---------------------------------------------------------------------------

@app.function(
    image=image,
    secrets=[modal.Secret.from_name("anthropic-key")],
    timeout=180,
)
def harmonize_patient_modal(patient_record: dict) -> dict:
    import os, json, datetime
    import anthropic

    client     = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    patient_id = patient_record.get("patient_id", "UNKNOWN")

    with client.messages.stream(
        model=MODEL,
        max_tokens=MAX_TOKENS_HARMONIZE,
        system=HARMONIZE_SYSTEM,
        messages=[{"role": "user", "content": _build_harmonize_user_prompt(patient_record)}],
    ) as stream:
        response = stream.get_final_message()

    raw = next((b.text for b in response.content if b.type == "text"), "")

    try:
        result = _parse_json_response(raw)
    except Exception as e:
        result = {"patient_id": patient_id, "error": str(e), "raw_response": raw[:500]}

    result.setdefault("harmonization_metadata", {})
    result["harmonization_metadata"]["model"]         = MODEL
    result["harmonization_metadata"]["run_location"]  = "modal_cloud"
    result["harmonization_metadata"]["input_tokens"]  = response.usage.input_tokens
    result["harmonization_metadata"]["output_tokens"] = response.usage.output_tokens
    result["harmonization_metadata"]["timestamp"]     = datetime.datetime.utcnow().isoformat() + "Z"
    return result


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("anthropic-key")],
    timeout=180,
)
def validate_patient_modal(harmonized_result: dict) -> dict:
    import os, json, datetime
    import anthropic

    patient_id         = harmonized_result.get("patient_id", "UNKNOWN")
    entities_to_review = _collect_entities_for_validation(harmonized_result)

    # Nothing to validate
    if not entities_to_review:
        harmonized_result["validation_summary"] = {
            "status": "skipped",
            "reason": "all entities confidence >= 0.85 with no flags",
            "entities_reviewed": 0,
            "confirmed": 0,
            "corrected": 0,
            "flagged": 0,
        }
        return harmonized_result

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    with client.messages.stream(
        model=MODEL,
        max_tokens=MAX_TOKENS_VALIDATE,
        system=VALIDATE_SYSTEM,
        messages=[{
            "role": "user",
            "content": _build_validate_user_prompt(patient_id, entities_to_review),
        }],
    ) as stream:
        response = stream.get_final_message()

    raw = next((b.text for b in response.content if b.type == "text"), "")

    try:
        validation_result = _parse_json_response(raw)
    except Exception as e:
        harmonized_result["validation_summary"] = {
            "status": "error",
            "error": str(e),
            "raw_response": raw[:300],
        }
        return harmonized_result

    validation_result["validation_metadata"] = validation_result.get("validation_metadata", {})
    validation_result["validation_metadata"]["model"]         = MODEL
    validation_result["validation_metadata"]["run_location"]  = "modal_cloud"
    validation_result["validation_metadata"]["input_tokens"]  = response.usage.input_tokens
    validation_result["validation_metadata"]["output_tokens"] = response.usage.output_tokens
    validation_result["validation_metadata"]["timestamp"]     = datetime.datetime.utcnow().isoformat() + "Z"

    return _apply_validations(harmonized_result, validation_result)
