# historical — Sleeper backfills

Backfills that bootstrap bronze with deep history the daily incrementals don't reach (full
transaction logs, lineage, drafts, off-platform trades). Mostly **run manually** — when first
populating bronze or when inputs change — and excluded from the daily DAG, with **one exception**.

| Script | Cloud Run job | Run cadence |
| --- | --- | --- |
| `league_lineage_ingestion.py` | `sleeper-league-lineage` | manual — walk every league back via `previous_league_id` |
| `league_transactions_ingestion.py` | `sleeper-league-transactions` | manual — full GraphQL transaction history (needs `AUTH_TOKEN`) |
| `league_draft_picks_ingestion.py` | `sleeper-draft-picks` | manual — player picks + traded picks per draft |
| `league_drafts_ingestion.py` | `sleeper-drafts-overview` | **DAILY** (in the DAG) — drafts + draft order |
| `commissioner_adjustments.py` | `sleeper-commissioner-adjustments` | manual — only when you edit the hand-coded list |

> `sleeper-drafts-overview` runs daily even though it lives in `historical/`, because
> `fact-roster-membership` keys the pick lifecycle off draft `status`/`rounds` and must stay current.

## Gotchas
- **GraphQL auth.** `league_transactions_ingestion` hits `sleeper.com/graphql` and needs a Bearer
  token via the `AUTH_TOKEN` env var. Programmatic login is captcha-blocked (`_utils.sleeper_login`),
  so the token is grabbed by hand from a browser session.
- **Draft-pick string parsing (the from/to swap bug).** Sleeper's GraphQL pick string is
  `"<orig_roster>,<season>,<round>,<NEW_OWNER>,<PREV_OWNER>"` — the **4th field is the new owner**
  (verified against the `traded_picks` net state). The script emits `owner_id`/`previous_owner_id` to
  match the daily REST feed. A prior version mislabeled these `from_team_id`/`to_team_id` and swapped
  them, which broke pick-ownership reconstruction — don't reintroduce that.
- **`commissioner_overrides`** is a hand-maintained static list of off-platform pick trades; here
  `to_team_id` = the **new** owner (correct, opposite of the old buggy GraphQL labeling). Edit the
  list in the file, then re-run the job.
- **Dedup on read.** `transactions/full_load` was written as two overlapping dumps (dedup on
  `transaction_id`), and `drafts/drafts` lists each `draft_id` twice (dedup on `draft_id`).
- **Direct-key fragility.** `league_transactions_ingestion` still flattens with direct `txn['key']`
  access (the historical sibling of the daily flattener hardened in PR #18). It's a manual backfill so
  the risk is low, but if you re-run it across many leagues, harden it with `.get()` first.
