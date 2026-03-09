"""
BioHarmonize — HTML Report Agent
Reads pipeline_provenance.json and generates a self-contained demo report.
"""

import json
import os
import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")


def _conf_class(conf):
    if conf is None:
        return "conf-unknown"
    if conf >= 0.85:
        return "conf-high"
    if conf >= 0.70:
        return "conf-medium"
    return "conf-low"


def _conf_label(conf):
    if conf is None:
        return "N/A"
    return f"{conf:.0%}"


def _flag_badge(flag):
    if not flag:
        return ""
    colors = {
        "low_confidence":    ("#f59e0b", "#fef3c7"),
        "needs_review":      ("#3b82f6", "#eff6ff"),
        "uncertain_mapping": ("#ef4444", "#fef2f2"),
        "no_standard_code":  ("#8b5cf6", "#f5f3ff"),
    }
    color, bg = colors.get(flag, ("#6b7280", "#f9fafb"))
    label = flag.replace("_", " ").title()
    return f'<span class="badge" style="background:{bg};color:{color};border:1px solid {color}">{label}</span>'


def _vs_badge(status):
    if not status or status == "not_reviewed":
        return ""
    styles = {
        "confirmed": ("✓ Confirmed", "#16a34a", "#f0fdf4"),
        "corrected":  ("✎ Corrected", "#b45309", "#fffbeb"),
        "flagged":    ("⚑ Flagged",   "#dc2626", "#fef2f2"),
        "skipped":    ("— Skipped",   "#6b7280", "#f9fafb"),
    }
    label, color, bg = styles.get(status, (status, "#6b7280", "#f9fafb"))
    return f'<span class="badge" style="background:{bg};color:{color};border:1px solid {color};font-weight:600">{label}</span>'


def _entity_rows(entities, cat):
    if not entities:
        return '<tr><td colspan="7" style="color:#9ca3af;text-align:center;padding:12px">No entities</td></tr>'

    rows = []
    for e in entities:
        orig_text  = (e.get("original_text") or "")[:70]
        std_term   = e.get("standardized_english_term") or e.get("standard_term") or "—"
        icd10      = e.get("icd10_code") or "—"
        omop       = e.get("omop_concept_id") or "—"
        conf       = e.get("confidence")
        flag       = e.get("flag")
        vs         = e.get("validation_status", "")
        lang       = e.get("language", "")

        cc = _conf_class(conf)
        cl = _conf_label(conf)

        # Build correction row if corrected
        corr_row = ""
        if vs == "corrected":
            corr = e.get("corrected_mapping") or {}
            orig_map = e.get("original_mapping") or {}
            reasoning = e.get("validation_reasoning", "")
            corr_icd  = corr.get("icd10_code") or "—"
            corr_omop = corr.get("omop_concept_id") or "—"
            corr_term = corr.get("standardized_english_term") or std_term
            corr_conf = corr.get("confidence")
            orig_icd  = orig_map.get("icd10_code") or icd10
            corr_row = f"""
        <tr class="correction-row">
          <td colspan="7">
            <div class="correction-block">
              <div class="correction-header">✎ Validation Agent Correction</div>
              <div class="correction-grid">
                <div class="correction-col">
                  <div class="correction-label">Before</div>
                  <div class="correction-val old-val">{orig_icd} · {orig_map.get("omop_concept_id") or omop}</div>
                  <div style="font-size:11px;color:#9ca3af">conf: {_conf_label(orig_map.get("confidence", conf))}</div>
                </div>
                <div class="correction-arrow">→</div>
                <div class="correction-col">
                  <div class="correction-label">After</div>
                  <div class="correction-val new-val">{corr_icd} · {corr_omop}</div>
                  <div style="font-size:11px;color:#9ca3af">conf: {_conf_label(corr_conf)}</div>
                </div>
                <div class="correction-col reasoning-col">
                  <div class="correction-label">Validation Reasoning</div>
                  <div style="font-size:12px;color:#374151">{reasoning[:200]}{"…" if len(reasoning) > 200 else ""}</div>
                </div>
              </div>
            </div>
          </td>
        </tr>"""

        lang_badge = f'<span class="lang-pill lang-{lang}">{lang}</span>' if lang else ""

        rows.append(f"""
        <tr class="entity-row {'corrected-entity' if vs == 'corrected' else ''}">
          <td class="original-text" title="{e.get('original_text', '')}">{orig_text}{"…" if len(e.get("original_text","")) > 70 else ""} {lang_badge}</td>
          <td>{std_term[:50]}{"…" if len(std_term) > 50 else ""}</td>
          <td><code>{icd10}</code></td>
          <td><code>{omop}</code></td>
          <td><span class="conf-pill {cc}">{cl}</span></td>
          <td>{_flag_badge(flag)}</td>
          <td>{_vs_badge(vs)}</td>
        </tr>{corr_row}""")

    return "\n".join(rows)


def _completeness_badge(score):
    if score is None:
        return ""
    pct = int(score * 100)
    if score >= 0.95:
        color, bg = "#16a34a", "#dcfce7"
    elif score >= 0.70:
        color, bg = "#b45309", "#fef3c7"
    else:
        color, bg = "#dc2626", "#fee2e2"
    return (f'<span class="badge" style="background:{bg};color:{color};'
            f'border:1px solid {color};font-weight:700;font-size:11px">'
            f'◉ {pct}% complete</span>')


def _modality_status_block(ma):
    if not ma:
        return ""
    assessments = ma.get("modality_assessments", {})
    modality_icons = {
        "clinical_note":   ("📋", "Clinical Note"),
        "lab_results":     ("🧪", "Lab Results"),
        "genomic_variants":("🧬", "Genomic Variants"),
    }
    pills = []
    for key, (icon, label) in modality_icons.items():
        info    = assessments.get(key, {})
        present = info.get("present", True)
        impact  = info.get("impact")
        if present:
            pills.append(f'<span class="mod-pill mod-present">{icon} {label} ✓</span>')
        else:
            impact_color = {"high": "#dc2626", "medium": "#b45309", "low": "#16a34a"}.get(impact, "#6b7280")
            pills.append(f'<span class="mod-pill mod-absent" style="border-color:{impact_color};color:{impact_color}">'
                         f'{icon} {label} ✗ <span style="font-size:9px">({impact or "?"})</span></span>')

    missing = ma.get("missing_modalities", [])
    if not missing:
        return f'<div class="mod-row">{"".join(pills)}</div>'

    # Build per-missing-modality detail
    detail_rows = []
    for m in missing:
        info = assessments.get(m, {})
        inferred = info.get("what_can_be_inferred") or "—"
        lost     = info.get("what_is_lost") or "—"
        comp     = info.get("compensating_evidence") or "—"
        label    = modality_icons.get(m, ("?", m))[1]
        detail_rows.append(f"""
            <tr>
              <td style="font-weight:600;color:#374151;white-space:nowrap">{label}</td>
              <td style="color:#16a34a">{inferred[:120]}{"…" if len(inferred) > 120 else ""}</td>
              <td style="color:#dc2626">{lost[:100]}{"…" if len(lost) > 100 else ""}</td>
              <td style="color:#2563eb">{comp[:100]}{"…" if len(comp) > 100 else ""}</td>
            </tr>""")

    rec = ma.get("overall_recommendation", "")
    flags_html = ""
    hflags = ma.get("harmonization_flags", [])
    if hflags:
        flags_html = " ".join(f'<code style="background:#fef3c7;padding:1px 5px;border-radius:4px;font-size:10px">{f}</code>' for f in hflags)

    return f"""
    <div class="mod-row">{"".join(pills)}</div>
    <div class="mod-detail">
      <table class="mod-table">
        <thead><tr>
          <th>Missing</th><th>What can be inferred</th>
          <th>What is lost</th><th>Compensating evidence</th>
        </tr></thead>
        <tbody>{"".join(detail_rows)}</tbody>
      </table>
      {f'<div class="mod-rec">💡 {rec}</div>' if rec else ""}
      {f'<div style="margin-top:6px">Harmonization flags: {flags_html}</div>' if flags_html else ""}
    </div>"""


def _patient_section(pid, patient, idx):
    lang        = patient.get("language_detected", "unknown")
    hm          = patient.get("harmonization_metadata", {})
    vs          = patient.get("validation_summary", {})
    flags       = patient.get("flags", [])
    audit       = patient.get("entity_audit_trail", {})
    ma          = patient.get("modality_assessment", {})
    comp_score  = ma.get("completeness_score")
    missing_mod = ma.get("missing_modalities", [])

    model       = hm.get("model", "—")
    tok_in      = hm.get("input_tokens", "—")
    tok_out     = hm.get("output_tokens", "—")
    n_entities  = hm.get("total_entities", "—")
    n_reviewed  = vs.get("entities_reviewed", 0) if isinstance(vs, dict) else 0
    n_corrected = vs.get("corrected", 0) if isinstance(vs, dict) else 0
    n_flagged   = vs.get("flagged", 0) if isinstance(vs, dict) else 0
    v_status    = vs.get("status", "done") if isinstance(vs, dict) else "—"

    lang_map = {
        "hindi":   ("Hindi", "#4f46e5", "#eef2ff"),
        "bengali": ("Bengali", "#0891b2", "#ecfeff"),
        "english": ("English", "#16a34a", "#f0fdf4"),
        "mixed":   ("Mixed", "#7c3aed", "#f5f3ff"),
    }
    lang_label, lang_color, lang_bg = lang_map.get(lang, (lang.title(), "#6b7280", "#f9fafb"))

    flag_items = "".join(f'<li>{f}</li>' for f in flags[:6])
    more = f'<li style="color:#9ca3af">+{len(flags)-6} more…</li>' if len(flags) > 6 else ""

    # Build entity tabs
    cat_labels = {
        "diagnoses":   ("🩺", "Diagnoses"),
        "medications": ("💊", "Medications"),
        "vitals":      ("📊", "Vitals"),
        "lab_values":  ("🧪", "Lab Values"),
        "variants":    ("🧬", "Variants"),
    }

    tab_headers = []
    tab_panels  = []
    for cat, (icon, label) in cat_labels.items():
        entities = audit.get(cat, [])
        count    = len(entities)
        corr_ct  = sum(1 for e in entities if e.get("validation_status") == "corrected")
        corr_ind = f' <span class="corr-indicator">{corr_ct}✎</span>' if corr_ct else ""
        tab_id   = f"tab-{pid}-{cat}"

        tab_headers.append(
            f'<button class="tab-btn" onclick="switchTab(\'{pid}\',\'{cat}\')" id="tbtn-{pid}-{cat}">'
            f'{icon} {label} <span class="tab-count">{count}</span>{corr_ind}</button>'
        )
        tab_panels.append(f"""
        <div class="tab-panel" id="{tab_id}" style="display:none">
          <table class="entity-table">
            <thead>
              <tr>
                <th>Original Text</th>
                <th>Standardized Term</th>
                <th>ICD-10</th>
                <th>OMOP</th>
                <th>Confidence</th>
                <th>Flag</th>
                <th>Validation</th>
              </tr>
            </thead>
            <tbody>
              {_entity_rows(entities, cat)}
            </tbody>
          </table>
        </div>""")

    tab_html = "\n".join(tab_headers)
    panel_html = "\n".join(tab_panels)

    return f"""
  <div class="patient-card" id="patient-{pid}">
    <div class="patient-header" onclick="togglePatient('{pid}')">
      <div class="patient-title">
        <span class="patient-id">{pid}</span>
        <span class="lang-pill lang-{lang}" style="background:{lang_bg};color:{lang_color}">{lang_label}</span>
        {_completeness_badge(comp_score)}
        {"".join([f'<span class="corr-badge">✎ {n_corrected} corrected</span>' if n_corrected else ""])}
        {"".join([f'<span class="badge" style="background:#fef2f2;color:#dc2626;border:1px solid #dc2626;font-size:10px">⚠ {len(missing_mod)} missing modalities</span>' if missing_mod else ""])}
      </div>
      <div class="patient-meta">
        <span title="Entities mapped">{n_entities} entities</span>
        <span title="Reviewed by Validation Agent">{n_reviewed} reviewed</span>
        <span title="Tokens used">🔑 {tok_in}↑ {tok_out}↓</span>
        <span class="chevron" id="chev-{pid}">▼</span>
      </div>
    </div>

    <div class="patient-body" id="body-{pid}" style="display:none">
      <div class="modality-section">
        <div class="summary-block-title" style="padding:10px 20px 4px">Data Completeness</div>
        <div style="padding:0 20px 12px">{_modality_status_block(ma)}</div>
      </div>

      <div class="patient-summary-row">
        <div class="summary-block">
          <div class="summary-block-title">Clinical Flags</div>
          <ul class="flag-list">{flag_items}{more}</ul>
        </div>
        <div class="summary-block">
          <div class="summary-block-title">Pipeline Metadata</div>
          <table class="meta-table">
            <tr><td>Model</td><td><code>{model}</code></td></tr>
            <tr><td>Language</td><td>{lang_label}</td></tr>
            <tr><td>Validation</td><td>{v_status} · {n_corrected} corrected · {n_flagged} flagged</td></tr>
            <tr><td>Tokens</td><td>{tok_in} in / {tok_out} out</td></tr>
          </table>
        </div>
      </div>

      <div class="tabs-container">
        <div class="tab-bar">{tab_html}</div>
        {panel_html}
      </div>
    </div>
  </div>"""


def generate_report(provenance_path=None, output_path=None):
    provenance_path = provenance_path or os.path.join(OUTPUT_DIR, "pipeline_provenance.json")
    output_path     = output_path     or os.path.join(OUTPUT_DIR, "report.html")

    with open(provenance_path, "r", encoding="utf-8") as f:
        prov = json.load(f)

    patients   = prov.get("patients", {})
    n_patients = len(patients)

    # Aggregate stats
    total_entities    = 0
    total_corrected   = 0
    total_flagged     = 0
    total_reviewed    = 0
    incomplete_count  = 0
    langs             = {}

    for pid, patient in patients.items():
        hm = patient.get("harmonization_metadata", {})
        vs = patient.get("validation_summary", {})
        ma = patient.get("modality_assessment", {})
        total_entities  += hm.get("total_entities", 0) or 0
        total_corrected += vs.get("corrected", 0)         if isinstance(vs, dict) else 0
        total_flagged   += vs.get("flagged", 0)           if isinstance(vs, dict) else 0
        total_reviewed  += vs.get("entities_reviewed", 0) if isinstance(vs, dict) else 0
        if ma.get("missing_modalities"):
            incomplete_count += 1
        lang = patient.get("language_detected", "unknown")
        langs[lang] = langs.get(lang, 0) + 1

    lang_breakdown = " · ".join(f"{v} {k.title()}" for k, v in sorted(langs.items()))
    run_ts = prov.get("run_timestamp", datetime.datetime.utcnow().isoformat() + "Z")

    patient_sections = "\n".join(
        _patient_section(pid, patient, idx)
        for idx, (pid, patient) in enumerate(sorted(patients.items()))
    )

    first_pid = sorted(patients.keys())[0] if patients else ""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BioHarmonize — Pipeline Report</title>
<style>
  :root {{
    --green:  #16a34a; --green-bg:  #f0fdf4;
    --yellow: #b45309; --yellow-bg: #fffbeb;
    --red:    #dc2626; --red-bg:    #fef2f2;
    --blue:   #1d4ed8; --blue-bg:   #eff6ff;
    --gray:   #6b7280; --gray-bg:   #f9fafb;
    --border: #e5e7eb;
    --radius: 10px;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
          background: #f3f4f6; color: #111827; font-size: 14px; }}

  /* ── Header ── */
  .header {{ background: linear-gradient(135deg, #1e1b4b 0%, #312e81 50%, #1e40af 100%);
             color: #fff; padding: 32px 40px; }}
  .header h1 {{ font-size: 28px; font-weight: 700; letter-spacing: -0.5px; }}
  .header h1 span {{ color: #a5b4fc; }}
  .header-sub {{ margin-top: 6px; color: #c7d2fe; font-size: 13px; }}
  .run-info {{ margin-top: 12px; font-size: 12px; color: #818cf8; }}

  /* ── Stat cards ── */
  .stats-row {{ display: flex; gap: 16px; padding: 24px 40px; flex-wrap: wrap; }}
  .stat-card {{ background: #fff; border: 1px solid var(--border); border-radius: var(--radius);
                padding: 20px 24px; flex: 1; min-width: 140px; box-shadow: 0 1px 3px rgba(0,0,0,.06); }}
  .stat-number {{ font-size: 36px; font-weight: 700; line-height: 1; }}
  .stat-label  {{ font-size: 12px; color: var(--gray); margin-top: 4px; text-transform: uppercase; letter-spacing: .5px; }}
  .stat-sub    {{ font-size: 11px; color: #9ca3af; margin-top: 6px; }}
  .stat-green  {{ color: var(--green); }}
  .stat-yellow {{ color: var(--yellow); }}
  .stat-blue   {{ color: var(--blue); }}
  .stat-purple {{ color: #7c3aed; }}

  /* ── Legend ── */
  .legend {{ display: flex; gap: 20px; padding: 0 40px 20px; align-items: center; flex-wrap: wrap; }}
  .legend-title {{ font-size: 12px; color: var(--gray); font-weight: 600; text-transform: uppercase; letter-spacing: .5px; }}
  .legend-item  {{ display: flex; align-items: center; gap: 6px; font-size: 12px; color: #374151; }}

  /* ── Patients ── */
  .patients-container {{ padding: 0 40px 40px; display: flex; flex-direction: column; gap: 12px; }}

  .patient-card {{ background: #fff; border: 1px solid var(--border); border-radius: var(--radius);
                   box-shadow: 0 1px 3px rgba(0,0,0,.06); overflow: hidden; }}
  .patient-card.has-corrections {{ border-left: 4px solid var(--yellow); }}

  .patient-header {{ display: flex; justify-content: space-between; align-items: center;
                     padding: 16px 20px; cursor: pointer; user-select: none;
                     transition: background .15s; }}
  .patient-header:hover {{ background: #fafafa; }}
  .patient-title {{ display: flex; align-items: center; gap: 10px; }}
  .patient-id    {{ font-size: 16px; font-weight: 700; color: #111827; }}
  .patient-meta  {{ display: flex; align-items: center; gap: 16px; color: var(--gray); font-size: 12px; }}
  .chevron       {{ font-size: 11px; color: #9ca3af; transition: transform .2s; }}
  .chevron.open  {{ transform: rotate(180deg); }}

  .corr-badge {{ background: #fef3c7; color: #92400e; border: 1px solid #fbbf24;
                 font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 20px; }}

  /* ── Patient body ── */
  .patient-body {{ border-top: 1px solid var(--border); }}
  .patient-summary-row {{ display: flex; gap: 0; border-bottom: 1px solid var(--border); }}
  .summary-block {{ flex: 1; padding: 16px 20px; }}
  .summary-block + .summary-block {{ border-left: 1px solid var(--border); }}
  .summary-block-title {{ font-size: 11px; font-weight: 600; color: var(--gray);
                          text-transform: uppercase; letter-spacing: .5px; margin-bottom: 8px; }}

  .flag-list {{ list-style: none; display: flex; flex-direction: column; gap: 4px; }}
  .flag-list li {{ font-size: 12px; color: #374151; padding: 3px 0;
                   border-left: 3px solid #fbbf24; padding-left: 8px; }}

  .meta-table {{ width: 100%; border-collapse: collapse; }}
  .meta-table td {{ padding: 4px 8px; font-size: 12px; color: #374151; vertical-align: top; }}
  .meta-table td:first-child {{ color: var(--gray); width: 90px; white-space: nowrap; }}

  /* ── Tabs ── */
  .tabs-container {{ padding: 0; }}
  .tab-bar {{ display: flex; gap: 2px; padding: 12px 16px 0; border-bottom: 1px solid var(--border);
              background: #f9fafb; overflow-x: auto; }}
  .tab-btn {{ background: none; border: none; border-bottom: 3px solid transparent;
              padding: 8px 14px; font-size: 13px; cursor: pointer; color: var(--gray);
              white-space: nowrap; transition: all .15s; border-radius: 6px 6px 0 0; }}
  .tab-btn:hover {{ color: #1d4ed8; background: #eff6ff; }}
  .tab-btn.active {{ color: #1d4ed8; border-bottom-color: #1d4ed8; background: #fff; font-weight: 600; }}
  .tab-count {{ background: #e5e7eb; color: #374151; font-size: 10px; font-weight: 700;
                padding: 1px 5px; border-radius: 10px; margin-left: 4px; }}
  .corr-indicator {{ color: var(--yellow); font-size: 10px; font-weight: 700; margin-left: 2px; }}
  .tab-panel {{ padding: 12px 16px 16px; overflow-x: auto; }}

  /* ── Entity table ── */
  .entity-table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
  .entity-table th {{ background: #f9fafb; color: var(--gray); font-weight: 600; font-size: 11px;
                      text-transform: uppercase; letter-spacing: .4px;
                      padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border); }}
  .entity-table td {{ padding: 8px 10px; border-bottom: 1px solid #f3f4f6; vertical-align: top; }}
  .entity-row:hover td {{ background: #fafafa; }}
  .corrected-entity > td {{ background: #fffbeb !important; }}
  code {{ background: #f3f4f6; padding: 1px 5px; border-radius: 4px; font-size: 11px; }}

  .original-text {{ max-width: 220px; color: #374151; font-family: monospace; font-size: 11px; }}
  .lang-pill {{ font-size: 10px; font-weight: 600; padding: 1px 6px; border-radius: 10px;
                display: inline-block; text-transform: uppercase; letter-spacing: .5px; }}
  .lang-hindi   {{ background: #eef2ff; color: #4f46e5; }}
  .lang-bengali {{ background: #ecfeff; color: #0891b2; }}
  .lang-english {{ background: #f0fdf4; color: #16a34a; }}
  .lang-mixed   {{ background: #f5f3ff; color: #7c3aed; }}
  .lang-unknown {{ background: #f9fafb; color: #6b7280; }}

  /* ── Confidence pills ── */
  .conf-pill {{ font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 20px;
                display: inline-block; }}
  .conf-high    {{ background: #dcfce7; color: #166534; }}
  .conf-medium  {{ background: #fef9c3; color: #713f12; }}
  .conf-low     {{ background: #fee2e2; color: #991b1b; }}
  .conf-unknown {{ background: #f3f4f6; color: #6b7280; }}

  /* ── Badge ── */
  .badge {{ font-size: 10px; padding: 2px 7px; border-radius: 20px; display: inline-block;
            font-weight: 500; white-space: nowrap; }}

  /* ── Correction block ── */
  .correction-row td {{ padding: 0 10px 10px; }}
  .correction-block {{ background: #fffbeb; border: 1px solid #fbbf24; border-radius: 8px; padding: 12px 16px; }}
  .correction-header {{ font-size: 11px; font-weight: 700; color: #92400e;
                        text-transform: uppercase; letter-spacing: .5px; margin-bottom: 10px; }}
  .correction-grid {{ display: flex; gap: 16px; align-items: flex-start; flex-wrap: wrap; }}
  .correction-col {{ flex: 1; min-width: 120px; }}
  .reasoning-col  {{ flex: 3; min-width: 200px; }}
  .correction-label {{ font-size: 10px; font-weight: 600; color: var(--gray);
                       text-transform: uppercase; letter-spacing: .4px; margin-bottom: 4px; }}
  .correction-val {{ font-size: 13px; font-weight: 700; padding: 4px 0; }}
  .old-val {{ color: #dc2626; text-decoration: line-through; }}
  .new-val {{ color: #16a34a; }}
  .correction-arrow {{ font-size: 20px; color: #9ca3af; padding-top: 18px; }}

  /* ── Misc ── */
  .section-title {{ padding: 0 40px 12px; font-size: 11px; font-weight: 600; color: var(--gray);
                    text-transform: uppercase; letter-spacing: .5px; }}
  /* ── Modality section ── */
  .modality-section {{ border-bottom: 1px solid var(--border); background: #fafafa; }}
  .mod-row {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }}
  .mod-pill {{ font-size: 11px; font-weight: 600; padding: 3px 10px; border-radius: 20px;
               border: 1.5px solid; display: inline-block; }}
  .mod-present {{ background: #f0fdf4; color: #16a34a; border-color: #86efac; }}
  .mod-absent  {{ background: #fff; }}
  .mod-detail {{ background: #fffbeb; border: 1px solid #fbbf24; border-radius: 8px;
                 padding: 10px 14px; margin-top: 4px; }}
  .mod-table {{ width: 100%; border-collapse: collapse; font-size: 11px; }}
  .mod-table th {{ color: var(--gray); font-weight: 600; text-transform: uppercase;
                   font-size: 10px; letter-spacing: .4px; padding: 4px 8px;
                   border-bottom: 1px solid var(--border); text-align: left; }}
  .mod-table td {{ padding: 5px 8px; border-bottom: 1px solid #f3f4f6; vertical-align: top; }}
  .mod-rec {{ margin-top: 8px; font-size: 12px; color: #374151;
              background: #eff6ff; border-left: 3px solid #3b82f6; padding: 6px 10px;
              border-radius: 0 4px 4px 0; }}

  @media (max-width: 768px) {{
    .stats-row, .patients-container {{ padding: 16px; }}
    .legend {{ padding: 0 16px 16px; }}
    .correction-grid {{ flex-direction: column; }}
  }}
</style>
</head>
<body>

<div class="header">
  <h1>Bio<span>Harmonize</span></h1>
  <div class="header-sub">Agentic South Asian Clinical Data Standardization Pipeline · OMOP CDM Output</div>
  <div class="run-info">Run: {run_ts} · Languages: {lang_breakdown}</div>
</div>

<div class="stats-row">
  <div class="stat-card">
    <div class="stat-number stat-blue">{n_patients}</div>
    <div class="stat-label">Patients Processed</div>
    <div class="stat-sub">{lang_breakdown}</div>
  </div>
  <div class="stat-card">
    <div class="stat-number stat-green">{total_entities}</div>
    <div class="stat-label">Entities Mapped</div>
    <div class="stat-sub">ICD-10 + OMOP CDM</div>
  </div>
  <div class="stat-card">
    <div class="stat-number stat-purple">{total_reviewed}</div>
    <div class="stat-label">Entities Validated</div>
    <div class="stat-sub">By Validation Agent</div>
  </div>
  <div class="stat-card">
    <div class="stat-number stat-yellow">{total_corrected}</div>
    <div class="stat-label">Corrections Made</div>
    <div class="stat-sub">Full audit trail preserved</div>
  </div>
  <div class="stat-card">
    <div class="stat-number" style="color:#dc2626">{total_flagged}</div>
    <div class="stat-label">Flagged for Review</div>
    <div class="stat-sub">Requires human sign-off</div>
  </div>
  <div class="stat-card">
    <div class="stat-number" style="color:#7c3aed">{incomplete_count}<span style="font-size:18px;color:#9ca3af">/{n_patients}</span></div>
    <div class="stat-label">Incomplete Records</div>
    <div class="stat-sub">Missing ≥1 modality — still processed</div>
  </div>
</div>

<div class="legend">
  <span class="legend-title">Confidence</span>
  <div class="legend-item"><span class="conf-pill conf-high">≥85%</span> High — auto-accepted</div>
  <div class="legend-item"><span class="conf-pill conf-medium">70–84%</span> Medium — validated</div>
  <div class="legend-item"><span class="conf-pill conf-low">&lt;70%</span> Low — flagged for review</div>
  <span style="margin-left:16px" class="legend-title">Validation</span>
  <div class="legend-item">{_vs_badge("confirmed")} Original mapping correct</div>
  <div class="legend-item">{_vs_badge("corrected")} Agent updated the mapping</div>
  <div class="legend-item">{_vs_badge("flagged")} Needs human review</div>
</div>

<div class="section-title">Patient Records — Click to expand</div>

<div class="patients-container">
{patient_sections}
</div>

<script>
function togglePatient(pid) {{
  const body = document.getElementById('body-' + pid);
  const chev = document.getElementById('chev-' + pid);
  const isOpen = body.style.display !== 'none';
  body.style.display = isOpen ? 'none' : 'block';
  chev.classList.toggle('open', !isOpen);
  if (!isOpen) {{
    // Activate first tab
    const firstCat = ['diagnoses','medications','vitals','lab_values','variants'];
    for (const cat of firstCat) {{
      const panel = document.getElementById('tab-' + pid + '-' + cat);
      const btn   = document.getElementById('tbtn-' + pid + '-' + cat);
      if (panel) {{ panel.style.display = 'block'; btn.classList.add('active'); break; }}
    }}
  }}
}}

function switchTab(pid, cat) {{
  const cats = ['diagnoses','medications','vitals','lab_values','variants'];
  cats.forEach(c => {{
    const panel = document.getElementById('tab-' + pid + '-' + c);
    const btn   = document.getElementById('tbtn-' + pid + '-' + c);
    if (panel) {{ panel.style.display = 'none'; btn.classList.remove('active'); }}
  }});
  const active = document.getElementById('tab-' + pid + '-' + cat);
  const abtn   = document.getElementById('tbtn-' + pid + '-' + cat);
  if (active) {{ active.style.display = 'block'; abtn.classList.add('active'); }}
}}

// Auto-open first patient on load
window.onload = () => togglePatient('{first_pid}');
</script>
</body>
</html>"""

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    return output_path


if __name__ == "__main__":
    path = generate_report()
    print(f"Report generated → {path}")
