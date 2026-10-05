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
    **Tested 2026-10-01 (in-season model):** `inseason.depth_features` adds the depth rank entering
    the snapshot week, its three-week change and the starter flag (86 % of snapshots listed);
    `backtest_inseason.py --depth`. Cohorts 2021–24: ROS rank agreement 0.784 → 0.786 (W3),
    0.772 → 0.770 (W6); next-season +0.001–0.003; targeted (to-date ppg ≥ 6): starters unchanged,
    backup QBs (n = 45) ROS ppg MAE 5.02 → 4.91 and games bias +0.26 → −0.01, backup RBs unchanged.
    Neutral: the to-date usage features already carry the role. Kept opt-in, not production.
    The Brissett / Watson / Mariota projections (8–11 ROS games) therefore reflect that they have
    been playing in 2026; the market's discount on them is about who starts next, which no
    feature in the lake sees yet (item 7, team commitment).

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
    **Tested 2026-10-01 (weekly line, cohorts 2015–2022, projected vs realized 3-year WAR on every
    projected player; the ledger now records per-position shares and `share_abs_err`).** Projected
    shares QB 28.9 / RB 30.1 / TE 10.0 / WR 31.0 % vs realized QB 27.0 / RB 30.0 / TE 11.1 / WR 31.9 %.
    The QB tilt is 2 points, not the 7 the priced-player IV comparison suggested. A fixed QB ×0.8
    (`--fixed-scale QB=0.8`) overshoots (24.5 % vs 27.0, share error 0.092 → 0.101) while nudging
    the top-150 rank agreement 0.603 → 0.613; not adopted. Player level (cohorts 2017–22, players
    projected > 0.3 WAR): realized / projected is QB 1.16, RB 1.26, TE 1.33, WR 1.44, so the model
    under-projects good players everywhere and least for QBs, which is the whole cross-position
    tilt: item 18 (shrinkage), worst for WRs. By QB age: <26 1.33, 26–29 1.04, 30–33 1.09, 34+ 0.91;
    by tier: QB1–12 1.29, QB13–24 1.02, QB25+ 1.39. So the "every QB is cheap vs KTC" page pattern
    (31 of 39 priced QBs, mean age 30.6 vs 25.4 for the fairly-priced ones) is a disagreement with
    the market's age curve, and the next three years of wins side with the model on 26–33-year-olds
    and slightly with the market on 34+. What the model cannot see: resale value and retirement
    beyond the 3-year window (its years-3+ tail is 30–35 % of an old QB's value and is unverified),
    and, in-season, depth charts (backups projected 8–11 ROS games: Brissett, Mariota, Lock,
    Watson) — item 11 for the in-season model. No manual position weights exist anywhere in
    production; values are the tree models' points and games projections through the league's
    lineup, replacement line and win curve.

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
    **Done 2026-10-01 (first cut, `src/picks.py`, Rosters tab "Draft picks in wins").** Two
    steps with real samples: (1) realized WAR by NFL draft pick for every drafted QB/RB/WR/TE of
    the 2010–16 classes (539 players, ten seasons, never-played = 0, weekly line of each season,
    owner's curve), fitted log(wins + 0.05) = 1.00 − 0.71·log(pick): NFL pick 1 ≈ 2.7 wins
    (10-year, 20 % discounted), 6 ≈ 0.7, 12 ≈ 0.4, 24 ≈ 0.23, 48 ≈ 0.12, 96 ≈ 0.05; (2) the
    three leagues' rookie drafts (258 picks) say which NFL picks go at each slot (early 1sts
    median NFL pick 6, mid 20, late 25, 2nd round 62, 3rd 93), so a slot tier's value is the
    curve averaged over the picks actually taken there. 2027 picks (one year of discount):
    early 1st 0.80 wins, mid 0.42, late 0.21, 2nd 0.08–0.14, 3rd 0.03–0.07; per 1,000 KTC
    0.11 / 0.07 / 0.04 / 0.02–0.04 / 0.01–0.03 against 0.15–0.27 for the owner's players, i.e.
    the market pays two to four times more per expected win for picks than for players, most of
    all for late firsts and seconds. Caveats: the curve is one class era; option value (the
    chance a slot lands a pick-1 talent) is in the mean, not priced separately; the leagues'
    own 2022–23 picks delivered 1.4 three-year wins per early / mid 1st (n = 6 each). Next:
    standings-projected tier per owned pick (which slot each team's pick is likely to be) and
    picks as pieces in the trade builder.

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

## Replacement level and injuries (2026-10-01)
19. **Injury-aware replacement (tested 2026-10-01, out of sample, cohorts 2015–2022).** The question:
    if the league starts 20 QBs but starters get hurt, is the marginal available player the 22nd?
    Rather than assume a rate, `lineup.replacement_weekly` fills the league's lineups each week from
    the players who actually played (fact_player_week) and averages the leftover's season ppg over
    regular-season weeks. Injuries AND byes push the line deeper than a 10 % guess, and by different
    amounts per position (2021–25, owner's league, effective starters full-season → weekly):
    QB 20 → 25 (+23 %), RB 28 → 32.5 (+16 %), WR 31 → 39 (+25 %), TE 12 → 16 (+35 %); replacement
    ppg QB −1.6, RB −0.8, WR −0.9, TE −0.8. TE moves most because its ppg curve is flat past 12.
    Harness, projected WAR vs realized WAR (`spearman_war_top`), 2 × 2 so the yardstick is held fixed:

    | projected \ realized | share rep | weekly rep |
    |---|---|---|
    | share rep (current) | 0.580 | 0.606 |
    | weekly rep | 0.569 | 0.603 |

    Scoring both sides on the weekly rep reads 0.603 vs 0.580, but that is the yardstick moving, not
    the ranking: against either fixed realized definition the weekly-rep projection is no better
    (0.569 vs 0.580; 0.603 vs 0.606). The replacement line is a per-position constant, so it cannot
    reorder players within a position; it only shifts positions against each other (QBs gain most)
    and that shift is a wash on held-out ordering. So the harness cannot decide this one: it is a
    definition of the yardstick, not a model change, and the realistic definition is the weekly one
    (lineups are filled every week from the players who are actually available). **Adopted as the
    production line** (`build_war.py --replacement weekly`, default; the harness records the choice
    in the ledger, `--realized-replacement` for cross-checks). Effect on the 2026 week-3 table,
    owner's league: every value rises by a games-weighted constant (QB +0.30, TE +0.14, WR +0.13,
    RB +0.11 career wins on average in the top 150); position shares of top-150 WAR QB 34.7 → 36.9 %,
    RB 29.8 → 26.9 %, WR 24.7 → 25.3 %, TE 10.7 → 10.9 %; the pack moves closer to the top
    (#1 / #12 1.96 → 1.70, #12 / #48 2.44 → 2.14); top-30 reorders are QBs up 2–5 places
    (Hurts, Mahomes, Williams, Burrow, Dart) and RBs down (Jeanty 7 → 11, McCaffrey 13 → 18,
    Taylor 17 → 22, Henry 23 → 29). A player's OWN injury risk is separate and already in WAR
    through projected games; the trade builder enumerates deal players in / out by availability.
    Bench value (the 25th QB starts ~20 % of weeks in a 10-team superflex) is item 20.
20. **Depth value for benches.** With weekly availability known per position (item 19), value a
    bench player as the weeks he would actually start for THIS roster (expected starts × his edge
    over the next man), rather than 0 below replacement. Needs the roster layer, not the model.
    **Done 2026-10-01 in the roster layer, with a correction.** The page's trade builder draws every
    rostered player in or out weekly by projected availability and re-optimises the lineup, so
    depth has value. First version had no floor: a roster whose WR3–5 project below the line
    credited an incoming WR with his edge over the weak bench (Olave: +2.13 wins for a 1.00-WAR
    player, +0.46 in 2028 alone when his 9.5 ppg beat the roster's own 7–8-ppg WRs). Every slot is
    now floored at the league's replacement line (a free agent at that level is always available):
    `lineup.optimal_lineup(..., floor=rep)` adds a phantom free agent per slot, used by
    `war.team_marginal_war` / `roster_total` and by the page. Olave alone now adds 0.86 wins to
    the owner's lineup vs his 1.00 WAR (availability draws and the roster's point on the curve
    explain the rest). A roster's gain from a player is his edge over the line plus the weeks the
    roster's own depth falls below it, never a credit for a weak bench.
21. **ROS vs Career toggle on the Players page, with a redraft market for ROS.** The model is
    built for the long term but its first span is a rest-of-season projection, so the page can
    double as a redraft tool: one toggle switches every column (value, rank, fair, mispricing) to
    ROS, and the market comparison switches with it, since KTC's dynasty values lag badly for
    redraft: FantasyPros ROS rankings / projections (the fantasypros ingestion in progress), or
    KTC's redraft values if they are exposed. Career keeps the KTC dynasty comparison.
    **Done 2026-10-01 (first cut):** KTC redraft values were already in the lake
    (`fact_asset_values_daily`, market_type REDRAFT, SF and 1QB, since 2025-10-08, ~140 players a
    day). The export carries `rd_sf` / `rd_1qb` per player and the page has a View control
    (Career = multi-year value vs KTC dynasty; ROS = rest-of-season value vs KTC redraft SF) plus
    the two redraft series in the Market selector. Coverage is the top ~100 of the projected
    players; FantasyPros ROS projections (ingestion in progress) would widen it.

## Round 5 (2026-10-01): inputs and training changes on the WAR objective, weekly line
Baseline `current_weekly` 0.603 (`spearman_war_top`, cohorts 2015–2022). Feature groups re-tested
under the final objective, then two training changes aimed at item 18 (shrinkage): a residual
target (the ppg model learns the change from this season's rate, so "stays the same" is the
default and regression to the mean has to be learnt) and relevance weights (1 + ppg / 10).

| variant | top-150 | all | top decile | bias top-12 (wins) |
|---|---|---|---|---|
| current (base, career) | 0.603 | 0.623 | 0.686 | 0.302 |
| + injury | 0.589 | | 0.675 | 0.308 |
| + role | 0.604 | | 0.688 | 0.303 |
| + trend | 0.610 | | 0.688 | 0.304 |
| + situation | 0.593 | | 0.673 | 0.303 |
| + all four | 0.602 | | 0.677 | 0.311 |
| residual target | 0.611 | | 0.680 | 0.293 |
| weights (ppg) | 0.604 | | 0.684 | 0.306 |
| residual + weights | 0.599 | | 0.675 | 0.309 |

| 600 trees at 0.03 | 0.595 | 0.624 | 0.684 | 0.303 |
| depth 5, mcw 3 | 0.589 | 0.622 | 0.684 | 0.301 |
| lambda 0.1, mcw 2 | 0.593 | | 0.682 | 0.303 |
| residual + trend | 0.618 | 0.623 | 0.686 | 0.300 |
| opportunity × efficiency (item 5) | 0.589 | 0.617 | 0.651 | 0.373 |

Trend (second-half vs first-half usage, last-4 form) and the residual target are the two
positive signals; injury and situation hurt as inputs to the career model (their information
is already in games / usage, and the extra columns cost the small cohorts more than they add);
every hyperparameter move away from the defaults loses. Per-cohort results are now persisted
per run and `--paired A B` gives the mean difference, its standard error over the cohorts and
the cohorts won. Paired, residual + trend vs current: +0.015 on the top-150 agreement
(t = 1.6, 6 of 8 cohorts), −0.006 wins MAE (t = −1.4); over twelve cohorts (2011–2022)
+0.006 (t = 0.7, 7 of 12), top decile +0.007 (t = 1.1), top-12 bias −0.007 (t = −1.9).
Consistent in sign on every metric, never past the 2.4 line: **not adopted**; the production
model stays `base,career`, level target, default trees. The honest reading of round 5 is that
training-side changes are worth at most a hundredth on this objective; what is left of item 18
is inputs the model does not have.

Item 5 (opportunity × efficiency), tested: a first run scored 0.386 because a quarter of the
2000–09 rows carry no targets / attempts in the season fact (opportunities read as zero); with
the models restricted to rows with recorded opportunities it scores 0.589 vs 0.603, top decile
0.651 vs 0.686 (t = −4.1), top-12 bias worse. Projecting volume and efficiency separately and
multiplying loses to projecting the rate directly: the product compounds two errors. Rejected.

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

## Programme (2026-10-05): best model first, then rookies, then the leagues' trading
The owner's order of work. Items 22–23 come first and run together (the bake-off is scored on
the same backtest that answers "are we beating the market"); 24 after the winner is chosen; 25 last.

22. **Model bake-off: TabPFN-3.5 (GPU) against the XGBoost career model.** The harness gets a
    `--backend tabpfn` switch (`career.HorizonModels(backend=...)`): the same feature frame, the
    same per-horizon games / ppg targets, the same walk-forward cohorts and the same WAR objective,
    so the only thing that changes is the estimator (a pretrained tabular foundation model doing
    in-context regression on the ~10–15k training rows per horizon, on the owner's RTX 2060).
    Variants to score: TabPFN-3.5 default, TabPFN-3.5 with the residual target, and a blend
    (average of the two models' ppg / games projections). Accept the winner on `spearman_war_top`
    with the paired test (|t| > 2.4), as every other change. The pretrained weights need a one-time
    PriorLabs licence acceptance (`TABPFN_TOKEN` for headless runs).
    **Set-up (2026-10-05):** `career.HorizonModels(backend="xgb"|"tabpfn"|"blend")`, harness
    `--backend` / `--tabpfn-params`; tabpfn 9.1.0 + torch cu126 in the ML venv; the 3.5 weights are
    licence-gated (owner's one-time login), the v2 weights are open, so the first round is TabPFN v2.
    One fit-and-predict at the matrix's size is about a minute on the RTX 2060 (fit_preprocessors;
    the KV-cache mode overflowed 6 GB with twelve fitted models per cohort).
    **Market backtest, 3 years, cohorts 2020–22 (366 priced players), TabPFN v2 vs the trees:**
    rank agreement with realized WAR 0.709 vs 0.685 (KTC 0.678), better in every cohort (0.715 /
    0.715 / 0.699 vs 0.691 / 0.681 / 0.684), top-decile hits 0.33 / 0.42 / 0.50 vs 0.22 / 0.33 /
    0.44, edge corr 0.353 vs 0.319; swaps 58 at +0.53 wins each (67 % won, 30.9 wins in total) vs
    63 at +0.42 (68 %, 26.2). Where it differs: the market's top 24 (0.432 vs 0.321, KTC 0.358 —
    TabPFN beats the market at the top where the trees lose to it), QBs (0.653 vs 0.620, KTC
    0.665), RB buys (+1.05 per swap, 89 % won), 29–32-year-old buys (+0.71 vs +0.05); rookies still
    below the market (0.484 vs 0.462, KTC 0.514). Bias is smaller (+0.28 vs +0.31 wins), i.e. less
    shrinkage of the top (item 18). Same inputs, same targets, same cohorts: the estimator alone
    moved the market backtest by +0.024. The harness's own top-150 metric on these three cohorts is
    mixed (0.564 / 0.608 / 0.640 vs 0.580 / 0.637 / 0.591), so the acceptance rests on the full
    eight-cohort paired run (below).
    **Harness, cohorts 2015–2022, TabPFN v2 (`tabpfn_v2_weekly`) vs the trees (`current_w`), paired:**
    top-150 rank agreement 0.613 vs 0.603 (+0.010, t = 0.8, 4 of 8 cohorts) — not past the line;
    everyone-projected agreement 0.627 vs 0.623 (+0.004, t = 3.2, 7 of 8); top-12 bias 0.270 vs
    0.302 wins (less shrinkage of the top, t = 3.7, 7 of 8); wins MAE on the top 150 0.511 vs 0.518
    (t = 1.9); top-decile hits 0.677 vs 0.686 (t = −0.9); position-share error 0.104 vs 0.092
    (worse, t = −2.1). Reading: the foundation model is a slightly better ranker and clearly less
    shrunk at the top, with the same inputs and no tuning, and it beat the trees on the 2020–22
    market backtest by more than the harness shows — but the primary metric is not significant,
    so **not adopted on its own**. **Blend (`blend_v2_weekly`, mean of the trees and TabPFN v2):**
    0.605 on the top-150 metric — no better than either parent (vs the trees −0.002, t = −0.2; vs
    TabPFN −0.008, t = 0.8), everyone-projected 0.627 (= TabPFN), top-12 bias 0.287 (between the
    two), position-share error 0.097. Averaging does not add anything here, so the candidate is
    TabPFN on its own. Market backtest 2020–22 (3 yr) for the three: rank agreement 0.709 TabPFN /
    0.700 blend / 0.685 trees (KTC 0.678); swaps +0.53 (67 % won) / +0.53 (72 %) / +0.42 (68 %);
    at the market's top 24 TabPFN 0.432, blend 0.386, trees 0.321 vs KTC 0.358. Same order on
    every slice: TabPFN ≥ blend > trees. **Adopted for production on the owner's call (2026-10-05):**
    the in-season refresh's multi-year tail (`backtest_inseason.py --current --backend tabpfn
    --tabpfn-params model_version=v2 --device cuda`, via `weekly_refresh.py --backend tabpfn`) runs on
    TabPFN v2; the in-season model itself (ROS / next season) stays xgboost, untested with TabPFN.
    The paired test on the primary metric was not significant (t = 0.8), the owner accepted the
    secondary and market-backtest evidence. Operational consequence: TabPFN needs a GPU, so the
    Cloud Run weekly job (CPU, no torch in the image) keeps `xgb`; the TabPFN-backed refresh is a
    local run that writes the same ML-bucket paths, and the page header names the career model
    that produced each export (`career_backend`). Next: TabPFN-3.5 on the owner's new computer (expected 2026-10-06; the
    licence login is a one-time step there, `TABPFN_TOKEN` for headless runs);
    whichever passes the paired test on `spearman_war_top` becomes production. Both runs went
    through CUDA on the RTX 2060 (~1.5 h for TabPFN v2 alone, ~1.4 h for the blend).

23. **Model vs market backtest, 2021 to now: are we winning, and where.** KTC dynasty values are
    daily from 2020-04, so each cohort T = 2020…2025 can be scored as "the model's projection at
    season end vs KTC the following February", against what the players then delivered. Report
    per cohort and per horizon (1 / 2 / 3 years so 2023–25 count too): rank agreement with realized
    WAR for the model and for KTC on the same priced players, top-decile hit rate, and the
    disagreement test (does our gap to the market predict the market's error); broken down by
    position, age band, experience (rookies / years 1–2 / veterans) and market tier (top 24, 25–60,
    61–120, rest). Then the money question as a trade simulation: each February, swap the players
    the model calls rich for the ones it calls cheap at equal KTC, and count the realized WAR
    gained; by segment, so the answer is "we win on 27–31-year-old QBs and lose on rookies", not
    one number. Output: `scripts/market_backtest.py` + a Backtest tab on the page.
    **Results, current model (xgb, weekly line), 2026-10-05.** Three years out, cohorts 2020–22
    (366 priced players): rank agreement with realized WAR 0.685 for the model vs 0.678 for KTC —
    a tie on ordering. The disagreements are where the value is: the third of players we liked
    more than the market finished 11.9 ranks better than KTC had them, the third we liked less
    9.5 ranks worse (edge corr 0.32); the cheap third returned 0.244 realized wins per 1,000 KTC
    against 0.179 for the rich third. Swap test (63 pairs, equal KTC, ≥ 10 ranks of disagreement
    on both sides): +0.42 wins per swap over three years, 68 % of swaps won, 26 wins in total
    (the model had said +0.52). Two years out (2020–23, 586 players): 0.681 vs 0.671, 126 swaps
    at +0.29 (58 %). One year out (2020–24, 857): 0.624 vs 0.612, 214 swaps at +0.19 mean but a
    median of zero (49 % won): most one-year swaps are between players who both delivered
    nothing, so the one-year edge is a few big hits. By cohort (3-yr): 2020 the market was better
    (0.691 vs 0.724, swaps net zero, 91 priced players in KTC's first year); 2021 and 2022 ours
    (0.681 vs 0.654 with +9.3 wins over 17 swaps, 88 % won; 0.684 vs 0.665 with +16.9 over 37,
    60 %). **Where we win:** market ranks 25–60 (0.44 vs 0.32, edge 0.36) and 61–120 (tie on
    ordering, swaps +0.39); veterans of four-plus years (swaps +0.43, 69 %) and buys aged 25–28
    (+0.64, 79 %); RB buys (+0.84 per swap, 75 %) and WR buys (+0.37, 71 %); WR sells (+0.62,
    70 %) and QB sells (+0.41, 83 %). **Where the market wins:** the top 24 (0.32 vs 0.36; the
    market's top decile hit rate beat ours in 2020 and 2021, 0.44 / 0.42 vs 0.22 / 0.33, ours won
    2022 0.44 vs 0.38), rookies (0.46 vs 0.51, edge 0.07; buying the rookies we liked won 19 % of
    16 one-year swaps and 10 % of 10 two-year swaps), players under 25 (0.64 vs 0.69), QB
    ordering (0.62 vs 0.67, though QB sells paid), and TE is a wash (0.53 vs 0.55, edge 0.04).
    Reading: the model is not a better ranker than the market overall; it is a better judge of
    established players in the middle of the market, and the market knows more than we do about
    rookies and the top tier (resale value, role security: items 16, 17, 18). The one-year numbers
    say the model's edge is a multi-year edge — it is right about careers more than about next season.

24. **College and early-career players: overvalued by the market or misvalued by us.** Item 16 says
    the market's rookies finished 13.7 ranks worse than priced and the model's rookie ranks were
    off by 2.4, i.e. hypothesis A (the dynasty community overpays) on the evidence so far; but the
    model only sees draft slot and a few games, so B (we misvalue them) is untested on inputs it
    does not have. Make sure the data can answer it: (a) college production from CFBD through
    nflverse draft ids (item 17b, a DE ingestion), (b) NFL draft capital, combine and age at draft
    (nflverse draft_picks / combine, in the lake), (c) the resale-value test from item 16 (do
    rookies hold price for a year regardless of production, which would make the premium rational
    for a trader even if wrong about wins). Then re-run item 23's segments on rookies / year-2
    players with the winning model.
    **2026-10-05, two findings and the data build.** (1) "Not a single 2026 rookie is a value" is
    the games tail, not the rate: the model gives Jeremiyah Love 12.6 / 13.8 / 12.9 / 13.3 / 14.3
    ppg over five years but 11.2 / 13.3 / 7.2 / 4.7 / 3.3 games. Walk-forward on 2015–2020
    cohorts, starters under 24 actually played 13.1 / 12.0 / 11.5 / 11.1 / 10.2 games in years
    1–5 against the games model's 12.7 / 10.4 / 8.1 / 6.1 / 4.5; 24–26-year-old starters 12.4 /
    12.3 / 11.0 / 9.7 / 8.7 vs 12.4 / 9.5 / 7.0 / 5.1 / 3.5. The rookie tail is extrapolated from
    those same decaying games, and discounting compounds it, so young players carry the whole
    bias (item 9, now measured). An empirical games table (`--calibrate games_table`, mean
    realized games by position × age bucket × prior tier, learnt per cohort) replacing the games
    model scores 0.609 vs 0.603 on the top-150 metric (t = 1.4) but hurts everyone-projected
    ordering (0.616 vs 0.623, t = 3.9) and position shares. Narrower variants (3-yr harness, paired
    vs `current_w`): starters-and-mid only, fringe keeps the model (`games_table:tiers:1.0`) 0.607
    (t = 0.8), everyone 0.623 = 0.623, top-12 bias 0.296 vs 0.302 (t = 3.3), share error 0.095 vs
    0.092 (t = −2.6); a 50/50 blend of table and model for those tiers (`games_table:tiers:0.5`)
    0.608 (t = 1.0), top decile 0.688 vs 0.686 (8 of 8), top-12 bias 0.297 (t = 4.9, 8 of 8), share
    error 0.093 (t = −1.6). Market backtest 2020–22 for the blend: 0.687 vs 0.685, under-25s 0.641
    vs 0.638, rookies 0.466 vs 0.462, swaps +0.47 vs +0.42 (70 % won), 25–28 buys +0.76 vs +0.64.
    Consistent in direction, never past the line — as expected, because rank metrics at three
    years barely see a games correction whose weight is in years 3–10; the five-year harness
    (`--horizon 5`, cohorts 2015–2020) is the test that can: paired vs `current_w5` (0.601),
    the starters-and-mid table 0.614 (+0.013, t = 1.5, 4 of 6 cohorts), the blend 0.607 (t = 1.1);
    everyone-projected and top-decile a hair lower (t ≤ 1.6), wins MAE and bias unchanged — the
    top-12 under-projection at five years (0.45 wins) barely moves because the ppg tail for the
    best players is also shrunk (item 18): more games × an excess near zero is still near zero,
    so games and ppg tails need fixing together. Standing: directionally right on every test,
    never past the line; adoption is the owner's call (the measured gap itself is not in doubt).
    **Resolved 2026-10-05: it was the age-survival cap, not the games model.** A pooled model with
    years-ahead as a feature (`--stacked`) scored exactly the current 0.603 and produced the same
    tail, which pointed past the model: `AgeSurvival.cap_games` multiplied every projection by the
    population's yearly continuation odds (~0.8 at 23, fringe included), so a 23-year-old starter's
    12.6 / 11.7 / 11.0 / 10.6 / 8.6 projected games became 12.6 / 10.4 / 8.0 / 6.1 / 4.5. Without
    the cap the games model alone is right for every tier and age (starters < 24: 12.8 / 12.0 /
    11.3 / 10.5 / 9.3 vs 13.1 / 12.0 / 11.5 / 11.1 / 10.2 real; fringe matches to a tenth); only
    30+ starters need it (model 12.8 / 11.0 / 9.2 / 8.0 / 6.5 vs 12.7 / 11.0 / 9.1 / 6.5 / 4.9).
    Harness, cap only from age 30 (`--cap 30+`) vs the old cap: 3 yr 0.607 vs 0.603 (t = 1.1),
    top decile 0.692 vs 0.686, top-12 bias 0.135 vs 0.302 wins (t = 23, 8 of 8); 5 yr 0.609 vs
    0.601 (t = 1.6, 5 of 6), top decile 0.704 vs 0.690 (6 of 6), top-12 bias 0.162 vs 0.446
    (t = 21), wins MAE 0.660 vs 0.645 (t = −1.3). No cap at all is similar with a worse MAE.
    **Adopted:** the cap applies from age 30 in the career model, the in-season tail and the
    next-season games (`--cap`, default `30+`, in `backtest_inseason.py`, `build_intrinsic_value.py`,
    `weekly_refresh.py`, the harness). The games table (above) is superseded; the ppg tail for the
    best players (item 18) is now the remaining shrinkage. The rookie tail is extrapolated from
    young players' career tails, so rookies rise with this automatically.
    **Why a post-processing cap at all (owner's question):** the in-model version — the pooled model
    with years-ahead and age + years-ahead as features, no cap — learns the decline to age 32 on its
    own (30–32 starters: 12.6 / 11.3 / 9.4 / 7.5 / 6.7 games vs 12.6 / 11.5 / 9.9 / 7.9 / 6.4 real)
    but extrapolates flat past 33 (8.9 / 7.5 / 6.7 vs 8.2 / 5.1 / 3.4), because the training data
    has too few 33-year-olds who became 37; and it scores below the capped per-horizon model (3 yr
    0.593 vs 0.607, top decile 0.673 vs 0.692 with t = 2.3; 5 yr 0.604 vs 0.609, top decile 0.675
    vs 0.704, t = 2.7) while shrinking the top even less (bias 0.094 / 0.104). So the survival prior
    stays, as a prior for the ages beyond the data's reach only; a cap from 33 instead of 30 is the
    untested refinement (30+ starters uncapped: 9.2 / 8.0 / 6.5 at years 3–5 vs 9.1 / 6.5 / 4.9
    real; capped from 30: 5.6 / 3.8 / 2.5). (2) The college data build is in:
    `data_engineering/src/cfbd_ingestion` (CFBD backfill, needs the owner's free `CFBD_API_KEY`),
    `silver_fantasy.fact_college_player_season` + `dim_college_crosswalk`, and the ML `college`
    feature group (final-season dominator, yards per team play, usage and touch shares, best
    dominator, breakout age, college seasons, final team's SP+, early declaration). Once the owner
    runs the backfill and the silver job, the harness run is `--groups base,career,college` and the
    rookie residual test (item 16).

25. **The leagues' trading: who trades well, the worst trades ever, and where managers slip.**
    From `fact_transactions` (trades with both sides' assets and the date) priced three ways:
    KTC at the trade date (what the market said), the model's value at the trade date (walk-
    forward, what we would have said) and realized WAR after the trade (what actually happened,
    through the end of the window). Per manager: value given vs received on each basis, win rate,
    best and worst deals; league-wide: the worst trades of all time by realized WAR swing, the
    pattern of mistakes (in-season panic sells, paying for last month's form, pick fever, position
    bias, trading with one partner); and the counterparties each manager loses to. Output: a
    Trades tab (per-league leaderboard, trade log with the three prices, manager profiles).
    **First cut, 2026-10-05 (`analysis/trades.py`, `analysis/trade_report.py` — analysis, not a
    model, so it lives with the notebooks). Value first, per the owner: every side is priced with
    KTC's trade calculator (the combine, so lopsided packages are judged as KTC judges them) at
    the trade date N and re-priced with the same assets at N+1, N+2, N+3 and today (a pick turns
    into the rookie it became); wins delivered since are the secondary column. The page has a
    Trades tab (scorecards, log, head-to-head, patterns; sortable, filtered by league, manager,
    horizon and trade age).** Value findings (two-team trades at least a season old, all leagues):
    (a) the calculator's verdict persists: the side it favoured at N (mean +2.1k combined, 32 %
    lean) is still ahead by +0.6k at N+1 and +1.1k at N+2 and wins 63 % of trades by value at both
    horizons, but only +0.17 wins on the field (51 % won on wins); (b) **picks appreciate**: the
    side that took picks for players was called slightly short at N (−0.3k) yet is ahead +1.2k at
    N+1, +1.8k at N+2 and +1.9k today, winning 66–73 % by value, while delivering −0.22 wins and
    losing 65 % on wins — the owner's hunch (hold picks, their price rises toward the draft) holds
    in market terms, and the wins-based "pick fever" is the same trades seen from the other side;
    (c) timing is a wash by value (49–50 % won at N+1 / N+2 either way). Scorecards by value at
    N+2, Stuck: Noah Smyth +39.8k (70 % won), Spencer Carella +13.9k (61 %), the owner +8.0k
    (55 %; −15.9k at N+1 then recovering), Alex Walker −8.8k, Brayton Green −9.9k (but +28.4k at
    N+1 and the most wins delivered), Anthony Golden −13.2k, Jake Kliest −14.6k, Alex Vaught
    −12.6k. Numbers in the paragraph above are from the wins-based first pass.**
    **Picks (owner, 2026-10-05): a pick is valued as a pick at the time, never as the player taken.**
    Next year's pick is priced at the tier its original team is likely to land in:
    `analysis/pick_slots.py` builds P(Early / Mid / Late | weeks played, record fifth, points-for
    fifth) from 26,769 league-seasons of the Sleeper crawl (an empirical table, no fitting) plus a
    preseason prior from last season's finish; calibration on our leagues 59 / 72 / 81 % right
    after 4 / 8 / 12 weeks vs 32 % for Mid-for-everyone. The page's Draft slots tab shows every
    current team's outlook and the tables. Pick prices now come from every KTC source in the lake
    (the silver fact had no 2023 picks after 2022-05 and no 2025 picks after 2024-08), with a
    same-distance-to-draft fallback across seasons. **Slot curve (owner, 2026-10-05: "the 1.01 is
    worth way more than the 1.12"):** KTC's tier prices flatten the round, so the value of each
    slot comes from what the player taken there was worth on KTC a month after the draft, over
    586k crawl rookie picks: 1.01 ≈ 1.46× the round mean, 1.12 ≈ 0.80× (1.8× apart), 2.01 above a
    late 1st. A pick is priced as the round's level that day × the expected curve value over the
    team's slot distribution (twelfths), so after week 3 of 2026 a 0–3 team's 2027 1st reads 7.3k
    against 5.2k for a 3–0 team's, where the tier version had 6.7k vs 5.3k. Open: an ordered
    regression for the preseason prior (roster value + returning record); XGBoost only if it
    beats that out of sample; a layout for three-team trades. 298 completed trades,
    932 asset legs (519 players, 492 priced at the trade date; 413 picks, 297 priced, 277 resolved
    to the rookie taken), 454 trade sides at least a season old. Stuck in High School is the
    trading league (245 of the 298). Scorecards (net wins delivered since, trades ≥ 1 season old):
    Brayton Green +11.0 over 65 trades, Noah Smyth +10.7 (61 % won), Alex Walker +8.2, Spencer
    Carella +4.3 (buys picks: +14 net); Jake Kliest −2.6 on 54 trades (the volume trader, 104 wins
    in and 107 out), Anthony Golden −4.9, the owner −6.4 (30 trades, 50 % won, but one −11.2 trade:
    Henry, Hill, Engram and the pick that became Bowers out for Taylor, Ertz and a 2nd in April
    2022, 26.0k KTC given for 11.0k), Alex Piroozi −6.6, Timmy Becker −11.3 (55 % of trades lost).
    Patterns, two-team trades: (1) KTC's verdict at the time is barely a predictor: the side the
    market favoured won 52 % and lost 45 % (+0.12 wins); (2) **pick fever is real**: the side that
    took picks for players lost 60 % of the time (−0.24 wins per side) although the market called
    those trades even (+0.2k KTC to the pick side) — picks deliver fewer wins than their price, as
    item 15's curve said; (3) consolidation helps a little: the side getting the single most
    valuable asset won 52 % (+0.13 wins); (4) in-season and offseason trades have the same win
    rate (46–47 %), in-season swings are larger; (5) wins per 1,000 KTC of the players bought:
    QB 0.34, TE 0.34, RB 0.33, WR 0.28; (6) pairs to watch: Becker vs Green 0 of 6, Piroozi vs
    Kliest 1 of 4, Kliest vs Green 18 trades at −4.3. Data: the silver KTC fact lists only today's
    ~430 players, so the bronze archive (2020-04 → 2024-08, by Sleeper id) prices the rest;
    dim_players_master's gsis ids are sparse and padded, nflverse `fantasy_player_ids` is the
    crosswalk. Next: the model's walk-forward value at the trade date as the third price (who
    would have been right with the model), a Trades tab on the page, and realized wins per season
    elapsed so 2021 and 2025 trades compare.

26. **Valuing the owner's own roster: bench players and the range of outcomes.** Suspicion: the
    roster layer slightly undervalues bench players, and the cause may be that the projection is
    collapsed to one number per player per year too early. Today a player's spread (`h{k}_ppg_sigma`,
    one per position and horizon) enters only through the expected-excess-over-replacement
    integral in `value.py`; the trade builder and the roster tables then take that expectation and
    only draw availability (in / out by week), never the rate. A bench player's worth to a roster is
    an option: he pays in the states where a starter is hurt or fades AND he is good, and those
    states are correlated with his own upside (young players with wide spreads). Test, through the
    roster layer: (a) carry the full distribution (quantile models, item 6, or empirical residual
    draws by position × age × horizon) into the roster Monte Carlo so lineups are re-optimised over
    rate draws as well as availability draws; (b) compare the roster's expected wins from the
    distribution with the single-number version per bench player; (c) check against history:
    did benches with more spread (young, high-sigma players) produce more realized starts and wins
    than their point projections said, cohorts 2015–2024, using the leagues' own rosters where we
    have them. Accept if the distributional roster value predicts realized team wins better than
    the point version (item 14's acceptance test).

27. **Neural / distributional models for the things trees cannot express (owner, 2026-10-05).**
    The owner's hypothesis: features that failed as inputs to the trees (a team move, a depth-chart
    change, a new quarterback) may matter as *spread* rather than *level* — a player who moved
    teams has a wider range of next-season outcomes, and a point-regression tree can only shift his
    mean. Item 12's test (the std of outcomes does not widen with a move, conditioned on playing
    6+ games) argues against it, but that test was one slice, not a model. Two ways to let the
    model say so: (a) TabPFN regression already returns a full predictive distribution per row (its
    output is a histogram over the target), so the bake-off (item 22) can score the per-player
    spread it implies against realized residuals — if a mover's distribution is wider, the feature
    is doing exactly what the owner expects, for free; (b) a small heteroscedastic network (mean and
    log-variance heads, Gaussian likelihood, the same frame) as a third bake-off entrant, scored on
    the ordering (`spearman_war_top`) AND on calibration of the spread (coverage of 16–84 %
    intervals by position / mover / rookie). Either feeds item 26 (range of outcomes in the roster
    layer) and item 6 (player-specific uncertainty) with a spread that depends on the inputs rather
    than one sigma per position.
