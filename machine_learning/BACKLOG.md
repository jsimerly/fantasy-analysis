# Model backlog

Ordered by expected gain per effort. Every item is accepted or rejected by the ablation harness
(item 1), scored on rank correlation with realized 3-year PAR among market-priced players plus
top-decile precision. Evidence below comes from the 2026-10-01 probes (OLS lift over a base of
games / ppg / fpts / age / experience / touches, fantasy-relevant players, 2013–2024).

0. **PAR exploration (owner, first).** TBD — the owner has one PAR question to settle before the
   items below start.

1. **Ablation harness.** Walk-forward evaluator with feature groups toggled; one command, one
   table. Needed because every lift below is small in aggregate (+0.003 to +0.011 R²) and
   concentrated in tails, so nothing should be added on a hunch.

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
