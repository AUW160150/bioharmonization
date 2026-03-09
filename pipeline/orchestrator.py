"""
BioHarmonize — Pipeline Orchestrator
Coordinates all agents: ingestion → harmonization → validation → output.
Uses Modal for parallel cloud execution when use_modal=True.
Falls back to local sequential execution when use_modal=False.
"""

import os
import sys
import json
import datetime
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from ingestion.ingestion_agent import ingest_directory
from pipeline.missing_modality_agent import assess_all_patients
from pipeline.output_agent import write_omop_output

DATA_DIR      = os.path.join(PROJECT_ROOT, "data", "synthetic")
OUTPUT_DIR    = os.path.join(PROJECT_ROOT, "output")
CACHE_DIR     = os.path.join(OUTPUT_DIR, "cache")

os.makedirs(CACHE_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Cache helpers — skip already-processed patients on rerun
# ---------------------------------------------------------------------------

def _cache_path(patient_id, stage):
    return os.path.join(CACHE_DIR, f"{stage}_{patient_id}.json")


def _load_cache(patient_id, stage):
    path = _cache_path(patient_id, stage)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def _save_cache(patient_id, stage, data):
    path = _cache_path(patient_id, stage)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Local fallback (sequential, no Modal)
# ---------------------------------------------------------------------------

def _run_harmonize_local(patient_records):
    from harmonization.harmonization_agent import harmonize_patient

    results = {}
    for pid, record in sorted(patient_records.items()):
        cached = _load_cache(pid, "harmonized")
        if cached:
            print(f"  [Harmonization] {pid} — loaded from cache")
            results[pid] = cached
            continue

        print(f"  [Harmonization] {pid} — calling Claude (local)...")
        result = harmonize_patient(record, verbose=False)
        _save_cache(pid, "harmonized", result)
        results[pid] = result
    return results


def _run_validate_local(harmonized_results):
    from pipeline.modal_app import (
        _collect_entities_for_validation,
        _build_validate_user_prompt,
        _apply_validations,
        _parse_json_response,
        VALIDATE_SYSTEM,
        MODEL,
        MAX_TOKENS_VALIDATE,
    )
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY not set")
    client = anthropic.Anthropic(api_key=api_key)

    results = {}
    for pid, record in sorted(harmonized_results.items()):
        cached = _load_cache(pid, "validated")
        if cached:
            print(f"  [Validation]    {pid} — loaded from cache")
            results[pid] = cached
            continue

        entities_to_review = _collect_entities_for_validation(record)
        if not entities_to_review:
            print(f"  [Validation]    {pid} — skipped (all confidence >= 0.85)")
            record["validation_summary"] = {"status": "skipped", "entities_reviewed": 0}
            _save_cache(pid, "validated", record)
            results[pid] = record
            continue

        print(f"  [Validation]    {pid} — reviewing {len(entities_to_review)} entities (local)...")
        with client.messages.stream(
            model=MODEL,
            max_tokens=MAX_TOKENS_VALIDATE,
            system=VALIDATE_SYSTEM,
            messages=[{"role": "user", "content": _build_validate_user_prompt(pid, entities_to_review)}],
        ) as stream:
            response = stream.get_final_message()

        raw = next((b.text for b in response.content if b.type == "text"), "")
        try:
            validation_result = _parse_json_response(raw)
        except Exception as e:
            print(f"  [Validation]    {pid} — JSON parse error: {e}")
            record["validation_summary"] = {"status": "error", "error": str(e)}
            results[pid] = record
            continue

        validated = _apply_validations(record, validation_result)
        _save_cache(pid, "validated", validated)
        results[pid] = validated

    return results


# ---------------------------------------------------------------------------
# Modal execution (parallel)
# ---------------------------------------------------------------------------

def _run_harmonize_modal(patient_records):
    from pipeline.modal_app import harmonize_patient_modal

    sorted_pids    = sorted(patient_records.keys())
    to_run_pids    = []
    to_run_records = []
    cached_results = {}

    for pid in sorted_pids:
        cached = _load_cache(pid, "harmonized")
        if cached:
            print(f"  [Harmonization] {pid} — loaded from cache")
            cached_results[pid] = cached
        else:
            to_run_pids.append(pid)
            to_run_records.append(patient_records[pid])

    if to_run_records:
        print(f"  [Harmonization] Dispatching {len(to_run_records)} patients to Modal cloud (parallel)...")
        modal_results = list(harmonize_patient_modal.map(to_run_records))
        for pid, result in zip(to_run_pids, modal_results):
            _save_cache(pid, "harmonized", result)
            cached_results[pid] = result
            print(f"  [Harmonization] {pid} — complete ✓")

    return cached_results


def _run_validate_modal(harmonized_results):
    from pipeline.modal_app import validate_patient_modal

    sorted_pids     = sorted(harmonized_results.keys())
    to_run_pids     = []
    to_run_records  = []
    cached_results  = {}

    for pid in sorted_pids:
        cached = _load_cache(pid, "validated")
        if cached:
            print(f"  [Validation]    {pid} — loaded from cache")
            cached_results[pid] = cached
        else:
            to_run_pids.append(pid)
            to_run_records.append(harmonized_results[pid])

    if to_run_records:
        print(f"  [Validation]    Dispatching {len(to_run_records)} patients to Modal cloud (parallel)...")
        modal_results = list(validate_patient_modal.map(to_run_records))
        for pid, result in zip(to_run_pids, modal_results):
            _save_cache(pid, "validated", result)
            cached_results[pid] = result
            # Summarize validation
            summary = result.get("validation_summary", {})
            n       = summary.get("entities_reviewed", 0)
            corr    = summary.get("corrected", 0)
            status  = summary.get("status", "done")
            print(f"  [Validation]    {pid} — {status} | {n} reviewed | {corr} corrected ✓")

    return cached_results


# ---------------------------------------------------------------------------
# Main orchestration entry point
# ---------------------------------------------------------------------------

def run(use_modal=True, force_rerun=False, data_dir=None):
    """
    Full pipeline: ingest → harmonize → validate → output.

    Args:
        use_modal:   Run harmonization + validation on Modal cloud (parallel).
        force_rerun: Ignore cache and reprocess all patients.
        data_dir:    Override data directory path.
    """
    t_start = time.time()
    data_dir = data_dir or DATA_DIR

    if force_rerun:
        import shutil
        if os.path.exists(CACHE_DIR):
            shutil.rmtree(CACHE_DIR)
            os.makedirs(CACHE_DIR)
        print("  [Orchestrator] Cache cleared — full rerun")

    print()
    print("=" * 60)
    print("  BioHarmonize Pipeline")
    print(f"  Mode     : {'Modal cloud (parallel)' if use_modal else 'Local (sequential)'}")
    print(f"  Data dir : {data_dir}")
    print("=" * 60)

    # ── Step 1: Ingest ─────────────────────────────────────────────────────
    print("\n[Step 1/5] Ingestion Agent")
    patient_records = ingest_directory(data_dir, verbose=False)
    print(f"  Ingested {len(patient_records)} patients: {', '.join(sorted(patient_records.keys()))}")

    # ── Step 1.5: Missing Modality Detection ───────────────────────────────
    print("\n[Step 2/5] Missing Modality Detection Agent (Claude-powered)")
    t_mod = time.time()
    patient_records = assess_all_patients(patient_records, verbose=True)
    print(f"  Modality detection complete — {time.time() - t_mod:.1f}s")

    # ── Step 2: Harmonize ──────────────────────────────────────────────────
    print("\n[Step 3/5] Harmonization Agent (Claude-powered)")
    t2 = time.time()
    if use_modal:
        harmonized = _run_harmonize_modal(patient_records)
    else:
        harmonized = _run_harmonize_local(patient_records)
    print(f"  Harmonization complete — {time.time() - t2:.1f}s")

    # ── Step 3: Validate ───────────────────────────────────────────────────
    print("\n[Step 4/5] Validation Agent (Claude-powered)")
    t3 = time.time()
    if use_modal:
        validated = _run_validate_modal(harmonized)
    else:
        validated = _run_validate_local(harmonized)
    print(f"  Validation complete — {time.time() - t3:.1f}s")

    # Re-attach modality assessments to validated records (cache may have stripped them)
    for pid in validated:
        if "modality_assessment" not in validated[pid] and pid in patient_records:
            validated[pid]["modality_assessment"] = patient_records[pid].get("modality_assessment", {})

    # ── Step 5: Output ─────────────────────────────────────────────────────
    print("\n[Step 5/5] Output Agent — OMOP Parquet + Provenance")
    pipeline_meta = {
        "run_mode":       "modal_cloud" if use_modal else "local",
        "patients":       list(sorted(validated.keys())),
        "total_duration": None,  # filled below
    }
    output_info = write_omop_output(validated, pipeline_meta)

    total = time.time() - t_start
    pipeline_meta["total_duration"] = f"{total:.1f}s"

    # ── Summary ────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  Pipeline Complete")
    print(f"  Total time : {total:.1f}s")
    print(f"  Patients   : {len(validated)}")
    print()
    print("  OMOP Output:")
    for name, rows in output_info["row_counts"].items():
        print(f"    {name}.parquet — {rows} rows")
    print(f"\n  Provenance : {output_info['provenance']}")
    print("=" * 60)

    return validated, output_info
