# BioHarmonize

**Agentic infrastructure that transforms fragmented South Asian clinical data into standardized, research-grade datasets.**

[![Demo Video](https://img.shields.io/badge/Demo-YouTube-red?style=flat-square&logo=youtube)](https://youtu.be/ZBlmh_1TGVI)
[![Track](https://img.shields.io/badge/Track-Biological%20Data%20Infrastructure-teal?style=flat-square)](https://youtu.be/ZBlmh_1TGVI)
[![Powered by](https://img.shields.io/badge/Powered%20by-Anthropic%20Claude-orange?style=flat-square)](https://anthropic.com)
[![Modal](https://img.shields.io/badge/Cloud-Modal-purple?style=flat-square)](https://modal.com)
[![Accuracy](https://img.shields.io/badge/ICD--10%20Accuracy-94.4%25-green?style=flat-square)]()
[![Consistency](https://img.shields.io/badge/Self--Consistency-100%25-green?style=flat-square)]()

---

## The Problem

Half the world's population lives in South Asia, yet their clinical data is almost entirely absent from the datasets powering modern healthcare AI. The data exists — across hospital systems in India and Bangladesh — but it is fragmented across incompatible formats, written in Bengali, Hindi, and Tamil, and impossible to standardize at scale.

The result: every drug trial, every diagnostic model, every treatment algorithm is trained on data that does not represent these patients. When those models get deployed globally, they underperform on South Asian populations.

**Tempus built this for US oncology and is worth $6 billion. Nobody has built it for the half of humanity that looks different.**

---

## What We Built

BioHarmonize is a six-agent orchestration pipeline that ingests raw, multilingual clinical records and outputs OMOP-compliant Parquet tables with full provenance — ready for pharma to query, license, and train on.

**Demo Video:** [https://youtu.be/ZBlmh_1TGVI](https://youtu.be/ZBlmh_1TGVI)

---

## Pipeline Architecture

![BioHarmonize Architecture](pipeline_architecture.png)

### The Six Agents

| Agent | Role |
|---|---|
| **Ingestion Agent** | Format detection, language identification, probabilistic patient record linking across PDF / CSV / VCF / JSON with no shared IDs |
| **Missing Modality Agent** | Completeness scoring (0–1), gap flagging, inference on missing fields — never drops a patient |
| **Harmonization Agent** | Claude Sonnet — cross-lingual entity extraction mapping Bengali / Hindi / Tamil directly to ICD-10 and OMOP standards with per-entity confidence scores |
| **Validation Agent** | Claude Sonnet — reviews all confidence < 0.85, verifies against KDIGO 2022 / WHO ICD-10 / RxNorm, logs full clinical reasoning per correction |
| **Dataset Profiling Agent** | Disease distribution, modality coverage, demographic fingerprint, quality score — produces the dataset card for the marketplace |
| **Matching Agent** | Scores pharma dataset requests across five dimensions with a hard geography filter — South Asian data never surfaces for non-South Asian queries |

### Data Flow
```
Hospital submits raw data
        |
        v
  Ingestion Agent
  (PDF · CSV · VCF · JSON · language detection · patient linking)
        |
        v
  Missing Modality Agent
  (completeness score · gap flags · no patient dropped)
        |
        v
  Harmonization Agent  <--  Anthropic API (claude-sonnet-4-6)
  (Bengali/Hindi/Tamil --> ICD-10 + OMOP · confidence per entity)
        |
        v
  Validation Agent  <--  Anthropic API (claude-sonnet-4-6)
  (KDIGO 2022 · WHO ICD-10 · RxNorm · corrections logged with reasoning)
        |
        v
  Dataset Profiling Agent
  (disease distribution · modality coverage · quality score)
        |
        v
  Matching Agent
  (pharma request scoring · geography hard filter)
        |
        v
  OMOP Parquet Output + Provenance JSON + Marketplace Listing
```

---

## Hospital Sources (Demo)

| Hospital | Location | Languages | Data Types |
|---|---|---|---|
| Apollo Mumbai | Mumbai, Maharashtra, India | Hindi | PDF discharge notes, CSV labs |
| SSKM Kolkata | Kolkata, West Bengal, India | Bengali | PDF notes, VCF genomics |
| Dhaka Medical College | Dhaka, Bangladesh | Bengali | PDF notes, lab CSV |
| CMC Vellore | Vellore, Tamil Nadu, India | English, Tamil | CSV structured records |

---

## Evaluation Results

Validated against OMOP Athena vocabulary, KDIGO 2022 clinical guidelines, WHO ICD-10 2023, and LOINC.

| Metric | Score | Method |
|---|---|---|
| ICD-10 Mapping Accuracy | **94.4%** | 50 ground truth mappings vs Athena vocabulary |
| OMOP Concept Accuracy | **100%** | Concept ID match against official OHDSI tables |
| Self-Consistency (temp=0) | **100%** | Same record run 3x — identical output every time |
| Cross-Lingual Accuracy | **100%** | Bengali / Hindi / English same concept agreement |
| Validation Agent Improvement | **+12.3%** | Harmonization alone vs Harmonization + Validation |

Notable correction caught by the Validation Agent:

> Patient coded as **N18.32** (CKD Stage 3b) by the Harmonization Agent. Validation Agent read eGFR 58, cross-referenced KDIGO 2022 (eGFR 45–59 = Stage 3a by definition), and corrected to **N18.31** with full reasoning logged. Clinically meaningful — different treatment protocols.

---

## Sample Output

After processing 10 synthetic patients across 4 hospitals:
```
person.parquet                10 rows
condition_occurrence.parquet  39 rows
drug_exposure.parquet         47 rows
measurement.parquet          120 rows
pipeline_provenance.json     184 KB  (full audit trail)
```

Sample provenance entry:
```json
{
  "patient_id": "P003",
  "original_text": "টাইপ ২ ডায়াবেটিস",
  "detected_language": "Bengali",
  "harmonization": {
    "icd10_code": "E11",
    "omop_concept_id": 201826,
    "confidence": 0.94
  },
  "validation_status": "confirmed",
  "validation_reasoning": "Bengali term maps unambiguously to Type 2 Diabetes Mellitus. ICD-10 E11 confirmed against WHO 2023."
}
```

---

## Stack

| Layer | Technology |
|---|---|
| Agent Orchestration | Python custom orchestrator (Modal-ready) |
| Cloud Execution | Modal — parallel `.map()` across all patients |
| Clinical NLP | Anthropic API — claude-sonnet-4-6 |
| Data Standard | OMOP CDM v5.4 — Parquet output |
| Validation Sources | KDIGO 2022, WHO ICD-10 2023, OMOP Athena, LOINC, RxNorm |
| Frontend | HTML / Tailwind CSS / DM Sans + Playfair Display |
| Backend | FastAPI (Python) |
| Hosting | Vercel (frontend) + Railway (backend) |

---

## Local Setup
```bash
# Clone
git clone https://github.com/AUW160150/bioharmonization
cd bioharmonization

# Install dependencies
pip install -r backend/requirements.txt

# Configure environment
cp .env.example .env
# Add your ANTHROPIC_API_KEY and MODAL credentials to .env

# Health check (verify API key + Modal + agents)
curl http://localhost:8000/health

# Launch everything
bash launch.sh
# Frontend: http://localhost:8080
# Backend:  http://localhost:8000

# Frontend only (demo mode, no API required)
bash launch.sh --no-api

# Run pipeline directly
python run_pipeline.py

# Run evaluation
cd evaluation && python report.py
```

---

## Deployment

**Frontend → Vercel**
```bash
# In Vercel dashboard:
# Root: frontend/
# Environment variable: VITE_API_BASE_URL = https://your-backend.railway.app
```

**Backend → Railway**
```bash
# In Railway dashboard:
# Root: backend/
# Environment variables: ANTHROPIC_API_KEY, MODAL_TOKEN_ID, MODAL_TOKEN_SECRET
# Health check path: /health
```

---

## Why Not Just Use Federated Learning?

Federated learning is on the roadmap as the access layer — but it assumes data is already structured and standardized at each node. The problem we are solving is upstream: the semantic pre-processing gap. Raw multilingual clinical records in Bengali and Hindi cannot federate until they share a schema. BioHarmonize builds that schema.

---

## Competitive Position

| Player | What they solve | What they don't solve |
|---|---|---|
| TileDB / Carrara | Storage + cataloging | Assumes metadata already exists |
| redun (Insitro) | Pipeline orchestration | Assumes structured inputs |
| Strand AI (YC) | Cross-modal imputation | Needs clean Western EHR data as input |
| **BioHarmonize** | Semantic pre-processing for multilingual LMIC data | — |

Strand AI and similar players are downstream customers, not competitors. They need the clean OMOP output we produce.

---

## The Business Model

Two-sided marketplace. Hospitals in South Asia submit raw data and earn revenue when pharma licenses their de-identified, harmonized datasets. BioHarmonize takes a platform fee. Pharma gets access to populations they have never been able to recruit into trials.


---

## License

MIT
