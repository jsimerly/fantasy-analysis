# Model backlog

Ordered by expected gain per effort. Every item is accepted or rejected by the ablation harness
(item 1), scored on rank correlation with realized 3-year PAR among market-priced players plus
top-decile precision. Evidence below comes from the 2026-10-01 probes (OLS lift over a base of
games / ppg / fpts / age / experience / touches, fantasy-relevant players, 2013–2024).

0. **PAR exploration (owner, first).** TBD — the owner has one PAR question to settle before the
   items below start.

1. **Ablation harness — DONE 2026-10-01.** `src/feature_groups.py` (named groups: base, career,
   injury, role, trend, situation) + `src/experiments.py` + `scripts/run_experiment.py`; results
   ledger in the ML bucket, `--leaderboard` compares like for like. Every item below is accepted
   only if it wins there. Items 2–4 are now one-line experiments (`--groups base,career,role`).

2. **Role features from data already in the lake** (the Anthony Richardson signal).
   - Season-end depth slot and depth movement (QB listed 2nd at season end kept 32 % of points;
     moved down: QB 0.40 / WR 0.59 / TE 0.68 vs starters 0.86 / 0.76 / 0.75; RB moving *up*
     mid-season does not carry over, 0.55 — fill-ins). Lift: QB +0.011, TE +0.007, RB +0.006,
     WR +0.003 R².
   - Within-season: QB dropped a slot in the last 3 weeks → ROS ppg −2.6 (W8) / −4.0 (W12) vs
     expected and half the remaining games. Up-moves add ~nothing beyond to-date production.
   - Cause of a season-ending absence from `rosters_weekly.status` + snap counts: benched (ACT,
     no snaps) QB kept 52 % of points (−21 pts next year); injured reserve bounced back (−2).
   - Prerequisite: a silver `fact_depth_chart_weekly` that normalizes the two depth-chart eras
     (old weekly `depth_team`, 2001–2024; daily snapshots with `pos_rank` and no season/week
     from 2025 → map `dt` to season/week via schedules, last snapshot before kickoff).

3. **Decompose the games target by cause.** Train availability as injury-missed vs role-missed
   instead of one blended number: games missed with no "Out" listing kept 63 % of points next
   year, soft-tissue 94 %, other injuries 82 % — the model currently averages opposite signals.

4. **Injury features from `bronze/nflverse/injuries`** (2009+, in the lake since 2026-10-01).
   Concussion Out → −14 pts next year; soft-tissue recurrence 16 % vs 8 % base; RB ppg ratio
   after knee/Achilles 0.82 vs 0.87, after soft tissue 0.76; WR knee 0.84 vs 0.89; QB no drag.
   Aggregate lift small (+0.004 games, +0.003 fpts R²). IR/PUP players are not on the report:
   use `rosters_weekly.status` for those.

5. **Opportunity vs efficiency split (per-game stats, not just points).** Project targets and
   carries per game and points per opportunity separately; opportunity persists, efficiency
   regresses. In-season, weight stable stats early (15 targets in week 1 is signal; one 2-yard
   TD is noise). Volume is already an *input* (targets, carries, target share, WOPR, touches,
   yards per touch) but never a projected quantity. Routes run (FTN charting) and air yards /
   expected points (next-gen, play-by-play) are in the lake and can enter here.

6. **Player-specific uncertainty.** Replace the single per-horizon sigma with a quantile model so
   young, high-variance players get their own upside in the expected-excess calculation.

7. **Team commitment.** Draft capital spent at the player's position and contract guarantees
   (nflverse contracts, not yet ingested) as role-security signals, especially for QBs.

8. **College production / draft-year inputs** for rookies and players without a complete-season
   row (nflverse draft_picks has cfb ids; CFBD for stats).

Context: next-season variance splits roughly a quarter to a third availability, about half
per-game rate, the rest covariance — rate projection is the bigger lever, availability second.

## Added 2026-10-01 (after the Washington / Watson / Walker review)

9. **Games-model calibration by position × age × horizon.** For RBs aged 24–26 coming off a
   15+ game, 200+ touch season, the career model projects 12.8 / 9.5 / 6.8 games at 1 / 2 / 3
   years out; the same profile in 2012–2022 actually averaged 12.0 / 10.6 / 9.8 (median 14 /
   14 / 13). Three games low at year 3 compounds through the discounting and is a candidate
   reason RBs look "overvalued" on the market. Check every position; fix with a monotone or
   calibrated availability model if it holds.

10. **Prior-season trend inputs for the preseason model.** Season totals hide late-season
    role changes: Watson went from GB WR7 to WR1 between weeks 1 and 12 of 2025, Washington from
    JAX WR4 to WR2/3 at week 9. Add second-half vs first-half targets/touches per game and the
    depth-chart movement from `fact_depth_chart_week` as T−1 features.

11. **Depth chart entering week 1 as a feature.** Walker was KC's RB1 on the week-1 chart after
    the move from Seattle's two-back split; the preseason projection could not see it, the
    in-season model only after three games. Same source table.

Done 2026-10-01: the table now runs off the in-season model (ROS at full weight, next season at
1 − rate, this season's games included, rookies in). The three cases moved from IV ranks
126 / 85 / 116 preseason to 50 / 33 / 68 in-season against market ranks 52 / 16 / 32.

12. **Situation-change flags as direction features, not a volatility rating.** Tested 2013–2024
    (relevant players who played the next season): a new team shifts next-season ppg −1.0
    relative to stayers, a week-1 depth demotion −1.7, a promotion +0.9, a new starting QB
    nothing; the spread of outcomes (std ≈ 3.5 ppg) does not widen with any of them, nor with
    the count of changes. So they belong in the ppg / games models (direction), not in sigma.
    Teammate turnover (top-3 target earners) is untested. Caveat: availability effects not
    tested (conditioned on playing ≥ 6 games).

Recency check (2012–2024 snapshots): realized ROS ppg ≈ 0.39·to-date + 0.46·prior at week 3,
0.59 / 0.27 at week 9, 0.66 / 0.20 at week 13; last-3 adds ≤ 0.17 beyond to-date. The in-season
model's own weights are 0.49 / 0.37 at W3 and 0.62 / 0.23 at W9 — already about right.
