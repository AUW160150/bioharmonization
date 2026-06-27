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

## 3. Import the canvas (real file: `canvas.yaml`)
[`canvas.yaml`](./canvas.yaml) is a real SuperPlane canvas (`apiVersion: v1`, `kind: Canvas`)
using actual component names (`github.onPush`, `http`, `if`, `claude.textPrompt`,
`github.createPullRequest`, `render.deploy`). [`workflow.yaml`](./workflow.yaml) is the
human-readable design notes; `canvas.yaml` is the thing you apply.

Apply flow (a canvas `update` needs the canvas UUID, so create it first):
```bash
# a) create an empty canvas in SuperPlane Cloud (UI or API) → copy its id
# b) paste that id into canvas.yaml  →  metadata.id: "<uuid>"
# c) create three org-level integrations named EXACTLY:
#      biogrid-github   (GitHub, authorize AUW160150/bioharmonization)
#      biogrid-render   (Render API key)
#      biogrid-claude   (Anthropic API key)
# d) apply:
superplane apps canvas update -f superplane/canvas.yaml
```
Notes:
- Component strings must match SuperPlane's registry exactly (already correct in the file).
- `position.y` is intentionally quoted (`"y"`) — required by the YAML parser.
- A couple of `configuration` keys (http `body`, claude model id) may need a small tweak in
  the canvas UI to match your integrations; the node graph + wiring is the hard part and it's done.

Pitch tip: the node labels are human-readable ("Wait for pipeline", "Completed?", "Summarize
dataset (Claude)") so a non-technical operator can follow the run live on the canvas console.

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
