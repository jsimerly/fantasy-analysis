# orchestration — the daily pipeline DAG

Full detail is in **[README.md](README.md)** — read it (it has the DAG diagram + dependency rationale).
Quick version:

- The daily pipeline is **one Cloud Workflow** ([pipeline.yaml](pipeline.yaml)) fired by **one** Cloud
  Scheduler job (10:00 UTC) — replacing the old 14 fixed-time per-job triggers that could drift.
- Order: **bronze (parallel)** → **silver T1** (dims + staging-asset-alignment) → **T2**
  (dim-franchise-meta, fact-asset-values) → **T3** (fact-roster-membership). Edges exist because each
  tier reads the previous tier's output.
- **Best-effort:** a single job failing is recorded but doesn't abort the run; all failures are
  collected and the workflow fails at the very end (so a bad run still shows red).
- **Not in the daily DAG** (run manually): the `*-full`/backfill jobs and
  `sleeper-commissioner-adjustments`. **In** the DAG despite living in `historical/`:
  `sleeper-drafts-overview` (the pick lifecycle keys off draft status).
- **Deploy:** push to `main` under `orchestration/**` (`.github/workflows/deploy-orchestration.yaml`).
