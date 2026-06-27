# BioGrid — BioHarmonize on SuperPlane

BioGrid recycles the existing **BioHarmonize** project (FastAPI backend + static
frontend, already built in this repo) and runs it as a governed **SuperPlane**
workflow, hosted on **Render**, connected via **GitHub**.

The pitch: a non-technical operator triggers a clinical data-harmonization run
from a **visual canvas**, every stage is **gated before the next**, and the run
ships a reviewable artifact — a **GitHub PR with a live Render preview link**.

## How it maps to the hackathon requirements
| Requirement | BioGrid |
| --- | --- |
| Built on SuperPlane Cloud | The orchestration is a SuperPlane canvas (`workflow.yaml`) |
| LLM agents do the heavy lifting | Harmonize + Validate stages use native SuperPlane LLM (Claude) components |
| Each stage validates the previous | Every stage has a `gate:` — empty input, processing complete, confidence threshold, OMOP completeness ≥ 0.85 |
| Working output to a preview env | Native Render component provisions a preview; URL posted back to the PR |
| Link on the PR | `github.create_comment` posts the live preview URL |

## Architecture
```
operator / git push ─▶ Ingest ─▶ Harmonize (Claude) ─▶ Validate (gated) ─▶ Publish OMOP
                                                                              │
                                                              GitHub PR ◀─────┘
                                                                  │
                                                       Render preview ─▶ comment URL on PR
```
The **confidence-gated validation** stage is the differentiator — ported from
`pipeline/orchestrator.py` (`_collect_entities_for_validation`): only low-confidence
entities get a second Claude review. That's what makes the factory *trustworthy*,
not just automated.

## The app being deployed
- Backend: `api.py` (FastAPI) — `/api/pipeline/run`, `/api/pipeline/status`, `/api/results`, `/api/search`, `/api/health`
- Frontend: `frontend/screen0..4` — served from the same Render origin as the API
- Safe demo path: if no `ANTHROPIC_API_KEY` is set, the pipeline runs a simulated
  progress walkthrough (clean for a live demo, no keys required)

See [`DEPLOY.md`](./DEPLOY.md) for the step-by-step setup and [`workflow.yaml`](./workflow.yaml)
for the canvas blueprint.

## Demo script (90 seconds)
1. Open the deployed Render URL → landing screen.
2. Hospital flow: Landing → 1A → **Pipeline** (watch the gated stages animate) → Results → Earnings.
3. Switch to the SuperPlane canvas → show the same stages running as a governed workflow.
4. Show the GitHub PR the run opened, click the **Render preview link** in the PR comment.
