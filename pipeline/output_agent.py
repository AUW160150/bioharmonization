"""
BioHarmonize — Output Agent
Reads validated harmonized records and writes OMOP-compliant Parquet tables + provenance JSON.

OMOP tables produced:
  person.parquet
  condition_occurrence.parquet
  drug_exposure.parquet
  measurement.parquet

Provenance:
  pipeline_provenance.json  — every AI decision, original + corrected, per patient
"""

import os
import json
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT_DIR     = os.path.join(os.path.dirname(__file__), "..", "output")
PROVENANCE_DIR = os.path.join(OUTPUT_DIR, "provenance")

# OMOP concept IDs for race / ethnicity (South Asian)
RACE_CONCEPT_SOUTH_ASIAN   = 44814660
ETHNICITY_NOT_HISPANIC     = 38003564

# Gender concept IDs
GENDER_MALE   = 8507
GENDER_FEMALE = 8532
GENDER_UNKNOWN = 0


def _gender_concept(sex_str):
    if not sex_str:
        return GENDER_UNKNOWN
    s = str(sex_str).lower()
    if s in ("male", "m", "पुरुष", "পুরুষ"):
        return GENDER_MALE
    if s in ("female", "f", "महिला", "মহিলা"):
        return GENDER_FEMALE
    return GENDER_UNKNOWN


def _patient_num(patient_id):
    """P001 → 1, P010 → 10"""
    try:
        return int(patient_id.replace("P", "").lstrip("0") or "0")
    except (ValueError, AttributeError):
        return 0


def _get_visit_date(harmonized):
    """Best-effort: extract visit/lab date from source metadata or use today."""
    # Try to get date from lab CSV source file name pattern (not stored, so default)
    return datetime.date(2024, 1, 15)  # representative date for synthetic data


def _best_entity(entity):
    """Return the final (post-validation) mapping fields of an entity."""
    if entity.get("validation_status") == "corrected" and entity.get("corrected_mapping"):
        c = entity["corrected_mapping"]
        return {
            "icd10_code":       c.get("icd10_code")       or entity.get("icd10_code"),
            "omop_concept_id":  c.get("omop_concept_id")  or entity.get("omop_concept_id"),
            "standard_term":    c.get("standardized_english_term") or entity.get("standardized_english_term"),
            "confidence":       c.get("confidence", entity.get("confidence", 0)),
        }
    return {
        "icd10_code":      entity.get("icd10_code"),
        "omop_concept_id": entity.get("omop_concept_id"),
        "standard_term":   entity.get("standardized_english_term"),
        "confidence":      entity.get("confidence", 0),
    }


def _safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------------------

def build_person_table(validated_records):
    rows = []
    for pid, record in sorted(validated_records.items()):
        entities    = record.get("entities", {})
        demographics = entities.get("demographics", {})

        age_entity = demographics.get("age", {})
        sex_entity = demographics.get("sex", {})

        age_text  = age_entity.get("standardized_english_term", "")
        sex_text  = sex_entity.get("standardized_english_term", "")

        try:
            age_val = int("".join(filter(str.isdigit, age_text.split()[0])))
        except (ValueError, IndexError):
            age_val = None

        year_of_birth = (2024 - age_val) if age_val else None

        rows.append({
            "person_id":                  _patient_num(pid),
            "person_source_value":        pid,
            "gender_concept_id":          _gender_concept(sex_text),
            "gender_source_value":        sex_entity.get("original_text", ""),
            "year_of_birth":              year_of_birth,
            "race_concept_id":            RACE_CONCEPT_SOUTH_ASIAN,
            "race_source_value":          "South Asian",
            "ethnicity_concept_id":       ETHNICITY_NOT_HISPANIC,
            "language_detected":          record.get("language_detected", "unknown"),
        })
    return pd.DataFrame(rows)


def build_condition_occurrence_table(validated_records):
    rows = []
    occ_id = 1
    for pid, record in sorted(validated_records.items()):
        person_id  = _patient_num(pid)
        visit_date = _get_visit_date(record)
        diagnoses  = record.get("entities", {}).get("diagnoses", [])

        for entity in diagnoses:
            best    = _best_entity(entity)
            omop_id = _safe_int(best["omop_concept_id"])
            rows.append({
                "condition_occurrence_id":     occ_id,
                "person_id":                   person_id,
                "condition_concept_id":        omop_id or 0,
                "condition_start_date":        str(visit_date),
                "condition_source_value":      entity.get("original_text", ""),
                "condition_source_concept_id": 0,
                "icd10_code":                  best["icd10_code"],
                "standardized_term":           best["standard_term"],
                "confidence":                  best["confidence"],
                "validation_status":           entity.get("validation_status", "not_reviewed"),
                "original_icd10":              entity.get("original_mapping", {}).get("icd10_code") if entity.get("original_mapping") else None,
                "original_omop":               entity.get("original_mapping", {}).get("omop_concept_id") if entity.get("original_mapping") else None,
                "flag":                        entity.get("flag"),
            })
            occ_id += 1
    return pd.DataFrame(rows)


def build_drug_exposure_table(validated_records):
    rows = []
    exp_id = 1
    for pid, record in sorted(validated_records.items()):
        person_id   = _patient_num(pid)
        visit_date  = _get_visit_date(record)
        medications = record.get("entities", {}).get("medications", [])

        for entity in medications:
            best    = _best_entity(entity)
            omop_id = _safe_int(best["omop_concept_id"])
            rows.append({
                "drug_exposure_id":              exp_id,
                "person_id":                     person_id,
                "drug_concept_id":               omop_id or 0,
                "drug_exposure_start_date":      str(visit_date),
                "drug_source_value":             entity.get("original_text", ""),
                "standardized_term":             best["standard_term"],
                "dose_value":                    entity.get("dose"),
                "dose_unit_source_value":        None,
                "sig":                           entity.get("frequency"),
                "route_source_value":            entity.get("route"),
                "confidence":                    best["confidence"],
                "validation_status":             entity.get("validation_status", "not_reviewed"),
                "flag":                          entity.get("flag"),
            })
            exp_id += 1
    return pd.DataFrame(rows)


def build_measurement_table(validated_records):
    rows = []
    meas_id = 1
    for pid, record in sorted(validated_records.items()):
        person_id  = _patient_num(pid)
        visit_date = _get_visit_date(record)

        for cat in ("lab_values", "vitals"):
            for entity in record.get("entities", {}).get(cat, []):
                best    = _best_entity(entity)
                omop_id = _safe_int(best["omop_concept_id"])
                rows.append({
                    "measurement_id":              meas_id,
                    "person_id":                   person_id,
                    "measurement_concept_id":      omop_id or 0,
                    "measurement_date":            str(visit_date),
                    "measurement_source_value":    entity.get("original_text", ""),
                    "standardized_term":           best["standard_term"],
                    "value_as_number":             _safe_float(entity.get("value")),
                    "unit_source_value":           entity.get("unit"),
                    "range_low":                   entity.get("reference_range", ""),
                    "interpretation":              entity.get("interpretation"),
                    "confidence":                  best["confidence"],
                    "validation_status":           entity.get("validation_status", "not_reviewed"),
                    "flag":                        entity.get("flag"),
                    "category":                    cat,
                })
                meas_id += 1
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Provenance writer
# ---------------------------------------------------------------------------

def write_provenance(validated_records, pipeline_meta):
    provenance = {
        "pipeline": "BioHarmonize",
        "version":  "1.0",
        "run_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "pipeline_metadata": pipeline_meta,
        "patients": {},
    }

    for pid, record in sorted(validated_records.items()):
        patient_prov = {
            "patient_id":              pid,
            "language_detected":       record.get("language_detected"),
            "modality_assessment":     record.get("modality_assessment", {}),
            "harmonization_metadata":  record.get("harmonization_metadata", {}),
            "validation_summary":      record.get("validation_summary", {}),
            "flags":                   record.get("flags", []),
            "entity_audit_trail":      {},
        }

        entities = record.get("entities", {})
        for cat in ("diagnoses", "medications", "vitals", "lab_values", "variants"):
            audit = []
            for entity in entities.get(cat, []):
                audit.append({
                    "original_text":          entity.get("original_text"),
                    "language":               entity.get("language"),
                    "original_mapping":       entity.get("original_mapping"),       # pre-validation
                    "validation_status":      entity.get("validation_status"),
                    "corrected_mapping":      entity.get("corrected_mapping"),       # post-validation
                    "validation_reasoning":   entity.get("validation_reasoning"),
                    "final_icd10":            entity.get("icd10_code"),
                    "final_omop":             entity.get("omop_concept_id"),
                    "final_confidence":       entity.get("confidence"),
                    "flag":                   entity.get("flag"),
                    "reasoning":              entity.get("reasoning"),
                })
            patient_prov["entity_audit_trail"][cat] = audit

        provenance["patients"][pid] = patient_prov

    path = os.path.join(OUTPUT_DIR, "pipeline_provenance.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(provenance, f, ensure_ascii=False, indent=2)
    return path


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def write_omop_output(validated_records, pipeline_meta=None):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pipeline_meta = pipeline_meta or {}

    print("  [Output Agent] Building OMOP tables...")
    person_df    = build_person_table(validated_records)
    condition_df = build_condition_occurrence_table(validated_records)
    drug_df      = build_drug_exposure_table(validated_records)
    measure_df   = build_measurement_table(validated_records)

    tables = {
        "person":               person_df,
        "condition_occurrence": condition_df,
        "drug_exposure":        drug_df,
        "measurement":          measure_df,
    }

    written = []
    for name, df in tables.items():
        path = os.path.join(OUTPUT_DIR, f"{name}.parquet")
        df.to_parquet(path, index=False, engine="pyarrow")
        written.append((name, path, len(df)))
        print(f"  [Output Agent] {name}.parquet — {len(df)} rows → {path}")

    prov_path = write_provenance(validated_records, pipeline_meta)
    print(f"  [Output Agent] pipeline_provenance.json → {prov_path}")

    return {
        "tables": {name: path for name, (_, path, _) in zip(tables.keys(), [(n, p, r) for n, p, r in written])},
        "provenance": prov_path,
        "row_counts": {name: rows for name, _, rows in written},
    }
