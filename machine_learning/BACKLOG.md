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

13. **Position-level calibration of value.** First-3-span IV shares (QB 33 / RB 26 / TE 12 / WR 29 %)
    vs realized 3-year PAR shares on priced players 2017–2022 (QB 26 / RB 23 / TE 13 / WR 38 %); the
    market's shares then matched realized (QB 27 / WR 38). Candidate causes: QB sigma (5.2) doubles
    WR's (2.5) and inflates expected-excess upside; WR shrinkage; QB games. Test as a value
    variant in the harness (cross-position rank agreement + per-position share error).

14. **WAR acceptance test (needs data).** Which definition (PAR vs WAR, share vs fill replacement)
    best predicts team weekly wins from roster strength. Standings exist from 2025-10 and roster
    snapshots from 2025-10-16 only (~140 team-weeks); in-season projections are persisted only for
    2026 week 3. Schedule the in-season run weekly, then run the test on a season of data.

Done 2026-10-01 (late): IV v2 = WAR (`src/league.py`, `src/lineup.py`, `src/war.py`,
`scripts/build_war.py`): explicit league fill replaces the flex-share line (RB 10.9 → 9.9 ppg),
league win curve turns PAR into wins, per-roster marginal WAR + outside targets, any league via
JSON. Rank-match fair value on the table removed the curve artifact that made every top asset
look rich (top-24 mean mispricing +12 % → +7 %, the 9990+ assets to 0 %).

15. **Price draft picks into WAR.** A pick is a claim on the player taken at that slot: value =
    E[WAR of the player drafted there | slot, class] discounted to the draft date, plus the
    option value of the slot's distribution. Build from history: the league's own rookie drafts
    (bronze sleeper drafts/draft_picks, 2021+) and NFL draft capital -> realized WAR of those
    players by slot; current picks come from `fact_pick_values` (standings-projected slot tier).
    Separate model from the player projection; likely a compound (slot -> expected rookie
    profile -> WAR).

16. **Rookie premium: market, not model (tested 2026-10-01).** Out of sample (in-season model fit
    as of T-1; cohorts 2022-2024, weeks 3/6/9, KTC-priced players): the market's rookies finished
    13.7 ranks WORSE than it ranked them (vets 8.9 ranks better); the model's rookie ranks were
    off by 2.4. Realized minus projected next-season points: rookies +20, vets +34 -- the model
    is not under-projecting rookies relative to veterans. Rookies the market had in its top 40 at
    week 3 (n=19): market rank 25, model rank 31, realized 63. So "every rookie looks rich" is
    mostly the market's rookie premium; the model's rookie prior is close to unbiased. Open
    question worth a test: whether the premium is rational as resale value (rookies hold price
    for a year even when they underperform), which an intrinsic measure will never show.

17. **Rookie evaluation: college production + experience-scaled draft capital.** Draft capital's
    pull on next-season points fades with experience exactly as the owner guessed (Spearman of
    draft pick vs next-season points, 2010+): rookie year −0.52, year 2 −0.46, year 3 −0.45,
    years 4–6 −0.38, years 7–10 −0.28, year 11+ −0.27; after controlling for this season's ppg the
    residual pull is −0.19 for rookies vs −0.09 for veterans. Draft round/pick and experience are
    already base features, so the trees can learn the interaction, but rookie rows are few
    (~70 a year) and the in-season model sees a rookie with only his draft slot and a few games.
    Plan: (a) a `rookie` feature group: draft pick × (experience == 0) and × (experience ≤ 2),
    NFL draft capital tiers; (b) college production via nflverse draft_picks (cfb ids) + CFBD
    (receiving/rushing yards per team play, breakout age, final-season dominator) — a DE
    ingestion first; (c) an explicit rookie prior in the in-season model (draft slot → expected
    rookie-year ppg curve by position) that the to-date games update. Accept only through the
    harness (rank agreement) and the rookie residual test (BACKLOG 16).

    Tested 2026-10-01: the explicit interaction group (`rookie`: pick x rookie, pick x young,
    pick / (1 + exp), round x rookie) scores 0.669 vs 0.671 for the current set in the season-level
    harness — the trees already had draft pick and experience and were using them. The remaining
    lead is inputs the model does not have (college production) and the in-season rookie prior,
    not more combinations of what it has.

Item 9, refined (2026-10-01, Bijan check): the games model is too low for prime RBs at 2–3 years
out, but the ppg decay is too mild, and the PAR path nets out close to history. Elite young RBs
(28 seasons 2006–2022: age 23–25, ≥ 17 ppg, ≥ 14 games) averaged 74 / 65 / 48 / 37 / 21 PAR over
the next five seasons (0.77 / 0.67 / 0.50 / 0.38 / 0.22 wins), 2.9 wins over ten years
undiscounted, 2.0 at 20 %; only 46 % were still a 15-ppg player a year later, 29 % three years
later, none seven. The model gives Bijan 4.0 undiscounted / 3.4 at 20 % (full-season scaled), i.e.
above the base rate. Calibrate games and ppg decay JOINTLY by position × age × horizon so the
pieces are right, not just the total.

18. **The career model shrinks good players too much (tested 2026-10-01, out of sample, cohorts
    2017–22, all positions).** Realized minus projected season points by the player's prior-season
    position tier: top 5 +13 / +7 / +22 at 1 / 2 / 3 years out; tier 6–12 +11 / +18 / +15; tier
    13–24 +5 / 0 / −1; lower tiers ≈ 0. WRs ranked 6–12 are projected ~30 points (18 %) low at
    every horizon. This is the across-the-board bias behind "our model is cold on established
    stars"; the market's top-tier ranks are not systematically wrong in the same way. Fix
    candidates, through the harness: less regularisation / more depth for the ppg model, or a
    walk-forward post-hoc calibration of ppg_hat by position and prior tier. Note the down-year
    case is NOT part of it: former top-12 players coming off a season ≤ 70 % of their best
    (n = 50) realized 10.2 ppg vs 9.9 projected next year and only 20 % got back to ≥ 90 % of
    their prior rate, so the model's cold read on a Jefferson-type season is calibrated.

## Objective, restated (2026-10-01, final)
Intrinsic value = projected wins above replacement. Validation is by time: each season 2015–2022 is
a test set, the model is trained on earlier seasons only, and projected WAR is scored against the
WAR those players actually delivered over the following years, in the owner's league's units.
Primary metrics (market-free): rank agreement of projected vs realized WAR among the top 150 by
projected WAR (`spearman_war_top`) and among everyone (`spearman_war_all`); mean error and bias in
wins (`mae_war_*`, `bias_war_*`, `bias_war_top12`); top-decile hit rate. The leaderboard sorts by
`spearman_war_top`. Market columns (KTC's agreement on priced players, edge corr/spread) are
context only: what a manager would have done without a model, and whether our disagreement with
the market predicted its error. Agreement with KTC itself is not tracked as a goal.

## Rounds 2–4 (2026-10-01): what was tested and why nothing was adopted
Primary metric: rank agreement of projected WAR with realized WAR among the top 150 by projected
WAR, cohorts 2015–2022 (market-free). Current model 0.580.
- calibrated (ppg+games lines on projection): 0.573; top-12 bias worse (+8 → +17 pts) — the games
  line through a 0-or-14 target drags starters down. Rejected.
- calibrated_ppg: 0.573; bias +8 → +11. The line conditions on the projection, where the model is
  slightly over-confident at its own top, not on prior tier. Rejected.
- deep trees (depth 6, mcw 1): ordering 0.654 vs 0.671 on the priced set. Rejected.
- quantile sigma (per-player spread): 0.571; no bias change. Rejected for now (item 6 stays open;
  the spread model may matter more for the roster/title-equity layer than for ordering).
- tier_calibrated (holdout residual per position × prior tier): 0.570; h1 top-12 bias +8 → +6 but
  h3 +13 → +16. Rejected.
- position_scale (holdout realized/projected PAR share per position): 0.580 (tie), edge 0.306 vs
  0.299, BUT the scales are regime-dependent: the 2023–25 holdout says RB ×1.40 / QB ×0.82 /
  WR ×0.91, which pushes the WR share further from the 2017–22 realized share (38 %), not toward
  it. Not adopted; item 13 needs a longer window and a stability test before it is a calibration.
Net: the current model stands; the top-tier shrinkage (item 18) is real but none of the post-hoc
fixes improved the held-out ordering. Next candidates are inputs, not corrections: opportunity /
efficiency split (5), college production (17), and the in-season model's own tier bias.
