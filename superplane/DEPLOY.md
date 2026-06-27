# BioGrid — Deploy & Wire-Up Guide

Goal: existing repo → live on Render → orchestrated by a SuperPlane Cloud canvas →
demoable PR with a preview link. Do the steps in order; each is independently testable.

## 0. Prereqs
- This repo pushed to GitHub: `https://github.com/AUW160150/bioharmonization` (already set).
- Render account. SuperPlane Cloud access (invite link you have).

## 1. Deploy the app on Render (do this FIRST — de-risks everything)
1. Render Dashboard → **New + → Blueprint**.
2. Connect the GitHub repo `AUW160150/bioharmonization`.
3. Render reads [`render.yaml`](../render.yaml) and proposes the `biogrid` web service.
4. Click **Apply**. First build installs `requirements.txt` and starts uvicorn.
5. When live, open the service URL:
   - `/api/health` → `{"status":"ok"}`
   - `/` → BioGrid landing screen (frontend served from the same origin)

✅ Checkpoint: the whole app is live on one Render URL, no API keys needed
(the pipeline uses its built-in simulated walkthrough).

## 2. Connect GitHub → SuperPlane
1. In SuperPlane Cloud, create a new canvas named **biogrid**.
2. Add the **GitHub integration** and authorize the `bioharmonization` repo.
   SuperPlane auto-creates the webhook.
3. Add the **Render integration** with your Render API key (Bearer).

## 3. Build the canvas (from `workflow.yaml`)
Recreate the graph visually using [`workflow.yaml`](./workflow.yaml) as the blueprint:
- Triggers: `manual`, `github_push`, `github_pr`
- Stages: ingest → harmonize → validate → publish (each with its `gate:`)
- Deploy chain: `open_pull_request` → Render `on_deploy` → `create_comment` (preview URL)

Tip for the pitch: keep the stage labels human-readable ("Confidence-gated validation")
so a non-technical operator can follow the run on the canvas console.

## 4. Produce the demo PR
- Trigger a run (manual button or a push). The `publish` stage opens a PR;
  the Render stage posts the live preview URL as a PR comment.
- That PR + preview link is your on-stage artifact.

## 5. Fallbacks (keep the demo bulletproof)
- SuperPlane Cloud Beta flaky? Self-host: `docker run --rm -p 3000:3000 ghcr.io/superplanehq/superplane-demo:stable`
- Render preview slow? The main Render service URL is always live as a backup demo.
- No internet for the canvas? The deployed Render app alone tells the full BioHarmonize story.

## Local smoke test (before pushing)
```bash
pip install -r requirements.txt
uvicorn api:app --host 0.0.0.0 --port 8000
# open http://localhost:8000/  → landing screen, same-origin /api calls
```
