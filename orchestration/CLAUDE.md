# orchestration — the daily pipeline DAG

Full detail is in **[README.md](README.md)** (the DAG diagram, the dependency rationale, the deploy).
[pipeline.yaml](pipeline.yaml) is the source of truth for what runs in which tier; do not copy its
job lists into other docs.

- The daily pipeline is **one Cloud Workflow** (`fantasy-pipeline`, [pipeline.yaml](pipeline.yaml))
  fired by **one** Cloud Scheduler job (`fantasy-pipeline-daily`, 10:00 UTC). The 14 legacy per-job
  triggers it replaced were deleted on 2026-10-10; the only other schedulers are the FantasyPros live
  job, the ML weekly refresh and the yearly college workflow.
- Order: **bronze (parallel: every incremental ingestion, the provider projections, the seasonal
  nflverse reconcile)** → **silver T1** (dims, staging-asset-alignment, the season-level facts that
  read bronze only) → **T2** (facts that read T1: franchise meta, asset values, league events, player
  weeks) → **T3** (the roster ledger, player seasons, weekly status) → **data quality**
  (`silver-data-quality`, the last step; failures are recorded, warnings tolerated). Edges exist
  because each tier reads the previous tier's output.
- **Best-effort:** a single job failing is recorded but doesn't abort the run; all failures are
  collected and the workflow fails at the very end (so a bad run still shows red).
- **Not in the daily DAG** (run by hand, on the owner's go): the `*-full` / `*-yearly` backfills
  (`adp-yearly`, the FFToday scrape, the college backfill) and `sleeper-commissioner-adjustments`.
  **In** the DAG despite living in `historical/`: `sleeper-drafts-overview` (the pick lifecycle keys
  off draft status).
- **Yearly:** [college.yaml](college.yaml) refreshes the college data each February
  (`fantasy-college-yearly`).
- **Deploy:** push to `main` under `orchestration/**` (`.github/workflows/deploy-orchestration.yaml`).
  Jobs themselves deploy from their own project's workflow; the DAG only references them by name, so a
  renamed job must be renamed here too (the drift trap).
