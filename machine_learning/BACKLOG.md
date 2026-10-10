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
    that produced each export (`career_backend`). **The new computer arrived 2026-10-06** (RTX 5070 Ti
    16 GB, Ryzen 9 9950X3D2): torch had to be reinstalled from the CUDA 13.0 index for Blackwell, the
    licence token is cached (browser callback) and the 3.5 regressor weights downloaded; the v2
    refresh whose projection step took 1 h 55 min on the RTX 2060 ran the whole pipeline in 32 min.
    **Results 2026-10-06/07 (3-year harness, production cap `30+t`, co-primary rule):**
    `tabpfn_v2_cap30t_w` (v2, base+career) vs the trees `cap30t_w`: ordering 0.618 vs 0.605
    (t = 1.3), all-player ordering 0.628 vs 0.624 (t = 3.0), wins error 0.513 vs 0.524 (t = 2.5),
    top decile 0.677 vs 0.684 (t = 0.6) -> **ADOPT** (error gain past the line, ordering better):
    the production choice of v2 for the in-season tail is now validated under the rule.
    `tabpfn35_w` (3.5, same inputs): ordering 0.604, wins error 0.516, share error 0.121 vs the
    trees' 0.094 (t = 4.3) -> tie with the trees, and v2 beats it on both co-primaries (0.618 /
    0.513 vs 0.604 / 0.516); at ~6x the GPU time per cohort 3.5 earns nothing on base inputs. The
    5-year 3.5 run was stopped after one cohort (60 min each) to bring the feature-set runs
    forward; re-run only if the feature set wins on 3.5. Open: the feature set
    (`tabpfn35_set_w`: base, career, injury, trend, situation, rookie, college) and the weekly
    sequence group (`tabpfn35_setw_w`) on 3.5, queued; then (owner, 2026-10-07) pooled horizons
    (`--stacked`: one games and one ppg model over every horizon, years-ahead and age-at-horizon
    as inputs, ~37k rows, two fits per cohort instead of six) on v2 (`tabpfn_v2_stacked_w`), on 3.5
    base+career (`tabpfn35_stacked_w`) and on 3.5 with the feature set (`tabpfn35_set_stacked_w`),
    each paired against its per-horizon twin; on the trees the pooled model tied (item 24).
    **Feature-set verdicts (2026-10-07 00:32-02:24, co-primary rule, 3-year):** the owner's set
    (base, career, injury, trend, situation, rookie, college; 73 columns) adds nothing on either
    model: 3.5 set 0.603 / 0.515 vs 3.5 base 0.604 / 0.516 (NO), v2 set 0.619 / 0.511 vs v2 base
    0.618 / 0.513 (NO). The **residual target** (the ppg model learns the change from this
    season's rate) is the live ingredient: 3.5 set + residual 0.613 / 0.507 vs 3.5 set 0.603 /
    0.515 -> ADOPT (wins error t = 4.6, share error 0.096 vs 0.117), and vs 3.5 base -> ADOPT
    (t = 3.9). Against production v2 (0.618 / 0.513) it is short of the line: wins error t = 2.05,
    ordering 0.613 (t = -0.6), and the prior-top-12 bias is worse (0.098 vs 0.057, t = 3.7) -> NO.
    So production stays v2; the residual target on v2 (base and with the set) and on 3.5 base are
    queued (`tabpfn_v2_res_w`, `tabpfn_v2_set_res_w`, `tabpfn35_res_w`) to isolate the gain where it
    is cheap. Timing on the 5070 Ti: 3.5 with 195 columns ~3.5 min per cohort (30 min a run);
    v2 with 195 columns ~7 min per cohort (it scales its ensemble up past its 85-column
    pretraining width), so 3.5 is the faster model on wide inputs.
    **Weekly sequence group (2026-10-07 02:24-03:21, 3.5, set + weekly = 244 columns, level
    target):** vs the set 0.611 / 0.513 against 0.603 / 0.515 (ordering t = 1.8, error t = 0.8) ->
    NO, short of the line; vs 3.5 base 0.611 / 0.513 against 0.604 / 0.516 -> NO (prior-top-12
    bias better, 0.102 vs 0.115, t = 2.3). Consistent lean on ordering, nothing past 2.4. ~7 min
    per cohort. Queued: the weekly group WITH the residual target (`tabpfn35_setw_res_w`), paired
    against the best 3.5 run so far (`tabpfn35_set_res_w`) and production.
    **Pooled horizons (2026-10-07 04:00-11:00): the first variant to beat production.**
    `tabpfn35_set_stacked_w` (3.5, the owner's feature set, one games and one ppg model over all
    horizons with years-ahead and age-at-horizon as inputs, level target) vs production v2
    (`tabpfn_v2_cap30t_w`): ordering 0.622 vs 0.618 (t = 0.4), wins error 0.474 vs 0.513 (t = 7.0,
    8 of 8 cohorts), prior-top-12 bias -0.043 vs +0.057 -> **ADOPT**; vs 3.5 with the set
    per-horizon (0.603 / 0.515): ADOPT (t = 7.5). Pooled on 3.5 without the set
    (`tabpfn35_stacked_w`): the same error gain (0.474, t = 7.4 vs production) but ordering 0.607
    (t = -1.6) -> TRADE-OFF; the feature set is what keeps the ordering. Pooled on v2
    (`tabpfn_v2_stacked_w`): NO (ordering 0.606, error 0.515; v2 at 37k pooled rows is far past its
    10k pretraining, 3 h per run). Caveats: the position-share error is worse (0.126 vs 0.100,
    t = -2.2) and the prior top 12 flips from under- to slightly over-projected; both are
    calibration-sized. Timing: 55 min for the 8-cohort run with the set (two fits per cohort;
    strangely 3 h without it, the memory-saving fallback). **Before it becomes production:** (1) the
    5-year harness (`tabpfn35_set_stacked_w5` vs the trees' `cap30t_w5` and a v2 5-year baseline),
    (2) the residual target on top (`tabpfn35_set_stacked_res_w`), (3) the market backtest, (4) the
    production path needs `--groups` and `--stacked` (the career tail today assembles base+career
    only) and the band for pooled models (`_range_columns` skips them today). Runs queued after the
    final refresh.
    **Data caveat for every 2026-10-07 comparison.** The 10:00 UTC pipeline rebuilt the facts with the
    league's real fumble setting (0, not the -1 of an older season's row the legacy trigger kept
    stamping as current; see the DE commit of the same day): QB seasons rose ~4 points, RB ~1.
    Runs that started before 06:00 local (the trees `cap30t_w`, production `tabpfn_v2_cap30t_w`,
    `tabpfn35_w`, the feature-set and weekly runs) and runs after it (the pooled, position-scale and
    residual runs) are therefore NOT on the same data; the realized QB share moved from 27 % to
    30 %. Every post-rebuild candidate is re-paired against same-data baselines
    (`tabpfn_v2_cap30t_w_b`, `cap30t_w_b`) in the final chain before anything is called adopted.
    **Residual target (2026-10-07 12:00-14:34, post-rebuild data):** v2 base + residual
    (`tabpfn_v2_res_w`) 0.613 / 0.474 - the same 0.474 wins error as the pooled 3.5 candidate, in
    26 minutes on the cheap model; v2 feature set + residual 0.621 / 0.505 (the set costs v2 error);
    3.5 base + residual 0.619 / 0.507. Against the OLD-data production row these print ADOPT
    (t = 14 on error), which is exactly the contamination above: the verdicts that count are the
    same-data ones in the final chain. Open question that chain answers: how much of the 0.513 ->
    0.474 is the scoring fix (the baseline will move too) and how much is the pooled model or the
    residual target.
    **Weekly group + residual target on 3.5 (2026-10-07 14:35-15:32, post-rebuild data):**
    `tabpfn35_setw_res_w` 0.622 / 0.506; vs set + residual (pre-rebuild, 0.613 / 0.507) NO on the
    rule and cross-data anyway. On today's data the weekly columns do not move error (0.506 vs the
    3.5 base residual's 0.507) and lean on ordering only (+0.009, t = 1.6), the same lean as the
    level-target weekly run. The weekly sequence group stays unadopted; its value, if any, is in
    the in-season model (rest of season, next season), which has not been tested with it.
    **Position-share calibration re-test (2026-10-07 11:00-11:58, v2, `--position-scale` vs
    production):** wins error 0.484 vs 0.513 (t = 7.9, 8 of 8), prior-top-12 bias 0.037 vs 0.057
    (t = 3.1), ordering 0.606 vs 0.618 (t = -1.5) -> TRADE-OFF by the rule, and the one thing it was
    meant to fix got worse: share error 0.152 vs 0.100 (t = -3.5), i.e. the holdout PAR-share scale
    over-corrects on this backend. Not adopted; the pooled candidate reaches a lower error (0.474)
    without the rescaling, so the QB tilt is better addressed by the per-player band in the upside
    term (item 26) than by a post-hoc scale. Both runs went
    through CUDA on the RTX 2060 (~1.5 h for TabPFN v2 alone, ~1.4 h for the blend).
    **Same-data verdicts (2026-10-07 16:09-17:32, both baselines re-run on the rebuilt facts):** the
    rebuild barely moved v2 (`tabpfn_v2_cap30t_w_b` 0.617 / 0.515 vs the old row's 0.618 / 0.513,
    t = 0.6 / 2.9 on tiny differences) and cost the trees a little (`cap30t_w_b` 0.593 / 0.530 vs
    0.605 / 0.524), so the 0.513 -> 0.474 wins-error gain of the post-rebuild candidates is the model,
    not the scoring fix; v2 vs the trees on the same data -> ADOPT (t = +2.5 / +3.1), the production
    choice stands. Against same-data production v2 (0.617 / 0.515): **3.5 set + pooled** 0.622 /
    0.474 -> **ADOPT** (ordering t = +0.5, error t = +7.3, 8 of 8 cohorts); **v2 + residual** 0.613 /
    0.474 -> ADOPT (t = -0.4 / +13.8); v2 set + residual 0.621 / 0.505 -> ADOPT (t = +0.6 / +4.3);
    3.5 set + pooled + residual (`tabpfn35_set_stacked_res_w`, new today) 0.628 / 0.499 -> ADOPT vs
    production (t = +1.8 / +2.9) but NO vs the pooled candidate without it (error worse, t = -4.7:
    the residual target and pooling do not stack, each alone takes the error to 0.474-0.499);
    3.5 base + residual 0.619 / 0.507 -> NO; 3.5 base + pooled 0.607 / 0.474 and v2 position-scale
    0.606 / 0.484 -> TRADE-OFF (error gain, ordering dip). The two front-runners paired directly
    (3.5 set + pooled vs v2 + residual): wins error identical (0.474, t = 0.1), top-150 ordering
    +0.009 for 3.5 (t = 0.7), all-player ordering +0.005 for v2 (t = 3.1), prior-top-12 bias -0.043
    (3.5) vs +0.095 (v2), position-share error 0.126 vs 0.114 -> a tie on the rule; 3.5 set + pooled
    costs 55 min a run, v2 + residual 26. Tie-break in flight: the market backtest of both (the 3.5
    candidate first, cohorts 2020-22 vs KTC), then the 5-year harness for the 3.5 candidate against a
    v2 5-year baseline. The first final chain died two cohorts into the 5-year run when the session
    restarted (background tasks go with it); relaunched detached (~20 min per 5-year cohort).
    Harness fix (same day): `--paired` matched a run name as a prefix, so `cap30t_w` resolved to
    `cap30t_w_b`'s newest file and the first two same-data pairings compared a run with itself
    (t = inf on every row); `experiments.run_paths` now matches `<name>_<timestamp>` exactly.
    **Market backtest, same data, 2026-10-07 evening (cohorts 2020-22, 364 KTC-priced players,
    3-year realized WAR; KTC itself 0.673):** production v2 0.698, v2 + residual 0.695, 3.5 set +
    pooled 0.695: a three-way tie on whole-pool rank agreement. Where they differ: at KTC's top 24
    the 3.5 candidate is 0.406 vs v2's 0.379 (KTC 0.345), ranks 25-60 0.492 vs 0.464 (KTC 0.315),
    121+ 0.363 vs 0.311; v2 keeps 61-120 (0.567 vs 0.544, KTC 0.554). Top-decile hits by cohort
    0.56 / 0.36 / 0.50 for 3.5 vs 0.44 / 0.27 / 0.50 for v2; edge correlation 0.368 vs 0.342; bias
    +0.14 vs +0.17 wins. Swaps: v2 63 at +0.50 (73 % won, 31.3 wins), 3.5 66 at +0.46 (73 %, 30.6),
    v2 + residual 65 at +0.47 (69 %, 30.3). The residual variant is not better than production on
    any market slice, so it drops out. **Verdict:** 3.5 feature set + pooled horizons matches
    production on whole-pool ordering against the market, beats it at the top of the market where
    the value is, and carries the harness's 0.474 vs 0.515 wins error (t = 7.3) with ordering no
    worse: the production candidate, pending the owner's call and the GPU (the queue holds it until
    about midday 2026-10-08). Production switch = `weekly_refresh.py --device cuda --backend tabpfn
    --groups base,career,injury,trend,situation,rookie,college --stacked --range`. The page's
    performance tab now shows production v2's same-data backtest (2026-10-07) and drops the
    trees-era season-end card (2026-10-01) that sat above it.
    **Feature push on the 3.5 pooled candidate (2026-10-08, 02:43 on; weekly first):** set + weekly
    (195 columns) 0.618 / 0.518 vs 0.622 / 0.474 -> NO (error t = -12.6, 8 of 8 worse; all-player
    ordering 0.624 vs 0.605, t = +5.3; prior-top-12 bias -0.065, the stars pushed further under).
    That is the fourth addition in a row (team, contract, both, weekly) with the same signature:
    whole-pool ordering up ~0.02, position shares sharper, top-150 wins error up ~0.04, the top
    12 under-projected more. The set itself (42 -> 73 columns) did none of that, so the suspect is
    the width of the table past ~80 columns acting on 3.5's column embedding, not the columns'
    content. **Control queued behind the queue** (`feature_groups` "noise": 15 seeded Gaussian
    columns, no information): `tabpfn35_set_noise_stacked_w` (set + noise vs the candidate) and
    `tabpfn35_noise_stacked_w` (base + noise vs 3.5 pooled base). If noise costs the same 0.04,
    every wide-table result above is a width artefact and the fix is in the model's preprocessing
    (fewer, denser columns; or TabPFN's own feature subsampling), not in the features. Set + role
    (8 depth-chart columns, 81 in all; 04:10-05:06): 0.622 / 0.510 -> NO, the fifth repeat
    (all-player 0.626, t = +6.8; error t = -12.3, 8 of 8), from the smallest addition yet, which
    points harder at width than at content. The drop-one ablation follows in the queue, then the
    five-year pair, then the noise control.
    **CORRECTION (2026-10-08 06:30): the 0.474 was a measurement regime, not a model.** Set minus
    injury (64 columns, narrower than the candidate) scored 0.626 / 0.515 too, which killed the
    width story and sent me back to the ledger. Every run's all-player ordering tells the regimes
    apart: the four runs at 0.474-0.484 (`tabpfn35_stacked_w`, `tabpfn35_set_stacked_w`,
    `tabpfn_v2_posscale_w`, `tabpfn_v2_res_w`; all-player ordering 0.605-0.61) ran between the
    10:00 UTC fact rebuild and the lineup-selection fix of 12:04 local (commit 228b5e8; the
    v2-residual process started before the fix and imported the old module), i.e. with the OLD
    settings-row choice, which handed `replacement.league_lineup` a different lineup, hence other
    replacement levels, hence another WAR scale for every player; a scale change moves the wins
    error of every model by the same amount and leaves rank agreement almost alone. Every run
    before (pre-rebuild data, old lineup) and after (corrected lineup; all-player ordering
    0.62-0.63) sits at 0.50-0.53. The lake's `fact_player_season` was written once, at 10:18 UTC
    on 10-07, and has not changed since, so the data is not the confounder; the lineup is.
    **Consequences.** (1) Every pairing of a 0.474 run against anything else is void: the "3.5
    set + pooled ADOPTS over production (error t = 7.3)" and the "v2 + residual ADOPTS (t = 13.8)"
    claims are withdrawn, and so is the production recommendation that rested on them. (2) The
    five "additions" tonight (team, contract, both, weekly, role) and the drop-injury ablation
    were all compared against a 0.474 run: their "0.04 worse" is the regime, so none of them is
    proven worse (or better); they are re-paired against a fresh candidate run below. The width
    hypothesis and the noise control are withdrawn. (3) The market backtests (all three on the
    corrected lineup) stand: a three-way tie overall, 3.5 better at the top. (4) **The valid
    standings (corrected lineup, post-rebuild data), wins error on the top 150:** production v2
    0.617 / 0.515; trees 0.593 / 0.530; 3.5 base + residual 0.619 / 0.507 (NO); v2 set + residual
    0.621 / 0.505 (ADOPT, t = +4.3); 3.5 set + weekly + residual 0.622 / 0.506; **3.5 set +
    pooled + residual 0.628 / 0.499 (ADOPT over production, ordering t = +1.8, error t = +2.9)** -
    the best valid run, the residual target again the live ingredient; the level-target set +
    pooled has no valid run yet. (5) Queued behind the main queue (`chain_rebaseline.sh`):
    `tabpfn35_set_stacked_w_b`, `tabpfn35_stacked_w_b`, `tabpfn_v2_res_w_b` under the current code,
    the re-pairings of every overnight run against `tabpfn35_set_stacked_w_b`, and the market
    backtest of 3.5 set + pooled + residual. (6) The harness now stamps every run with its regime
    (the lineup's starters and the season fact's write time) and `--paired` warns when the two
    runs' regimes differ, so this cannot pass silently again.
    **Five-year harness, valid regime (2026-10-08 10:17-13:27, cohorts 2015-2020, both runs
    stamped with the same regime):** 3.5 set + pooled (level target) 0.632 / 0.619 vs v2 0.605 /
    0.646 -> **ADOPT** (ordering t = +2.3, 5 of 6 cohorts; wins error t = +5.3, 6 of 6; top-decile
    hits 0.687 vs 0.684; all-player ordering 0.633 vs 0.639, t = -2.7; prior-top-12 bias 0.082 vs
    0.092; position-share error worse, 0.134 vs 0.088, t = -3.8). The first clean win for the pooled
    3.5 model, and on the horizon where the per-horizon v2 is weakest (five separate fits, the
    far ones on thin data; the pooled model shares strength across years). Three-year verdict
    pending the re-baseline run `tabpfn35_set_stacked_w_b`.
    **Three-year re-baseline (2026-10-08 13:29-14:25, `tabpfn35_set_stacked_w_b`, regime-stamped):**
    3.5 set + pooled (level) 0.620 / 0.513 vs production v2 0.617 / 0.515 -> NO, a tie on both
    co-primaries (all-player ordering 0.623 vs 0.629, t = -4.5; prior-top-12 bias -0.041 vs +0.058,
    the stars no longer shrunk; share error 0.119 vs 0.101). So pooling alone buys nothing at three
    years and a clear win at five. **Every overnight addition and ablation re-paired against the
    fresh run is a tie** (NO on both co-primaries, |t| < 1.7): team, contract, both, weekly, role,
    drop injury / trend / situation / rookie; drop college prints ADOPT on error at exactly the
    line (0.508 vs 0.513, t = +2.43, ordering -0.005) - noise-adjacent, noted, not acted on. The
    feature groups neither help nor hurt the pooled 3.5 model at three years; the earlier "0.04
    worse" was the regime. **The residual target is the three-year gain:** 3.5 set + pooled +
    residual 0.628 / 0.499 ADOPTS over the fresh level run (error t = +3.2, 7 of 8) and over
    production (t = +2.9); against v2 set + residual (0.621 / 0.505) it is a tie (error t = +1.3)
    with a worse prior-top-12 bias (+0.140 vs +0.107: the residual target over-projects last
    year's stars, the level target under-projects them) and worse share error (0.122 vs 0.098).
    Standing, valid regime, three years: production v2 0.617 / 0.515 < 3.5 set + pooled 0.620 /
    0.513 (tie) < v2 set + residual 0.621 / 0.505 (ADOPT) ~ 3.5 set + pooled + residual 0.628 /
    0.499 (ADOPT, best point). Five years: 3.5 set + pooled ADOPTS over v2 (residual variants
    untested there). Remaining in the chain: 3.5 base + pooled and v2 base + residual re-runs, then
    the market backtest of 3.5 set + pooled + residual.
    **3.5 base + pooled re-run (`tabpfn35_stacked_w_b`, 14:26-17:17, the known slow case: the last
    two cohorts ran in TabPFN's memory-saving mode, 33 and 98 min):** 0.611 / 0.511, a tie with
    production v2 (t = -0.7 / +0.8) and with set + pooled (0.620 / 0.513: ordering t = +1.1, error
    t = -0.5); the feature set's measurable effect on the pooled 3.5 model at three years is the
    prior-top-12 bias (-0.041 vs +0.048, t = 4.5) and the share error (0.119 vs 0.133, t = 2.3),
    not the co-primaries. So at three years v2, 3.5 base + pooled and 3.5 set + pooled are one
    cluster at 0.61-0.62 / 0.51, and only the residual target moves the error.
    **v2 base + residual re-run (`tabpfn_v2_res_w_b`, 17:17-17:44):** 0.620 / 0.511 -> ADOPT over
    production (error t = +3.4, 7 of 8; a 0.004 gain, consistent not large; prior-top-12 bias +0.102
    vs +0.058). The three-year ladder, every rung in the valid regime: production v2 0.515 -> v2 +
    residual 0.511 (t = 3.4) -> v2 set + residual 0.505 (t = 2.5 over the rung below) -> 3.5 set +
    pooled + residual 0.499 (t = 2.4 over v2 + residual, 8 of 8; t = 1.3 over v2 set + residual;
    t = 2.9 over production), ordering flat at 0.617-0.628 all the way up, and the prior-top-12 bias
    climbing with it (+0.058 -> +0.102 -> +0.107 -> +0.140: the residual target over-projects last
    year's stars where the level target under-projects them). The residual target is the robust
    ingredient; the feature set and pooling each add a rung of about one t.
    **Market backtest of 3.5 set + pooled + residual (17:44-18:08, cohorts 2020-22, 364 players,
    KTC 0.673):** 0.692 overall (v2 0.698, v2 + residual 0.695, 3.5 set + pooled 0.695: all one
    cluster), the best top 24 (0.437) but the worst 25-60 (0.433 vs 0.492 for the level twin), the
    highest over-projection (+0.23 wins), and the weakest swap record (60 swaps at +0.40, 70 % won,
    23.8 wins in total vs 30-31 for the other three). The residual target's three-year error gain
    does not carry to the market test; what the market test rewards is the level-target pooled
    3.5 model: best edge correlation (0.368), best top-of-market agreement (0.406 / 0.492 at
    1-24 / 25-60), top-decile hits 0.56 / 0.36 / 0.50, the smallest bias (+0.14).
    **Standing at the end of the bake-off (2026-10-08 18:08), all in the valid regime:**
    | model | 3-yr ordering / error | 5-yr ordering / error | market: all / top 24 / 25-60 / swaps won |
    | production v2 (per horizon, level) | 0.617 / 0.515 | 0.605 / 0.646 | 0.698 / 0.379 / 0.464 / 73 % |
    | 3.5 set + pooled (level) | 0.620 / 0.513 (tie) | **0.632 / 0.619 (ADOPT)** | 0.695 / **0.406 / 0.492** / 73 % |
    | v2 set + residual | 0.621 / 0.505 (ADOPT) | - | - |
    | 3.5 set + pooled + residual | **0.628 / 0.499 (ADOPT)** | - | 0.692 / 0.437 / 0.433 / 70 % |
    **Recommendation: production = 3.5 set + pooled, level target**, for the in-season tail; the
    in-season model itself on 3.5 as well (next season +0.02-0.03 at every checkpoint, item 31).
    It loses nothing at three years, wins at five (the horizon where v2's per-horizon fits are
    thinnest), is the strongest against the market where the value is, and ends the under-shrinkage
    of last year's top 12 (bias -0.04 vs +0.06). The residual variants stay on the shelf as a
    calibration lead (their three-year error gain is real but they over-project the stars and lose
    the market slices); a residual-vs-level blend or a bias correction on the residual target is
    the next cheap experiment. Command: `weekly_refresh.py --device cuda --backend tabpfn --groups
    base,career,injury,trend,situation,rookie,college --stacked --range --inseason-backend tabpfn`
    (about 25 min on the 5070 Ti). Owner's call, as every production switch.

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
    real; capped from 30: 5.6 / 3.8 / 2.5).
    **Tier-aware survival (owner: "an elite WR is more likely to play to 35 than a bad one"), tested
    2026-10-05.** True in the data and already in the games model (30+ starters uncapped 12.8 /
    11.0 / 9.2 / 8.0 / 6.5 games vs 30+ mid 9.7 / 6.3 / 4.1 / 2.5 / 1.7; real 12.7 / 11.0 / 9.1 /
    6.5 / 4.9 vs 8.4 / 6.2 / 3.7 / 2.3 / 1.0); the cap was the tier-blind part. Continuation odds
    fitted per (position, prior tier) and applied from 30 (`--cap 30+t`): 30–32 starters now
    12.9 / 11.3 / 9.6 / 7.9 / 6.4 vs 12.6 / 11.5 / 9.9 / 7.9 / 6.4 real, mid and fringe within a
    game; 33+ starters still too flat at years 4–5 (8.0 / 6.6 vs 5.1 / 3.4; n = 43). Harness vs
    the plain cap from 30: top-150 equal (0.605 vs 0.607 at 3 yr, 0.606 vs 0.609 at 5 yr), top
    decile a touch lower (0.684 vs 0.692; 0.692 vs 0.704, t = 2.0), top-12 bias 0.091 vs 0.135
    and 0.087 vs 0.162 (t = 8, every cohort). The combination (tiered from 30, population curve
    from 34, `30+t34`) fixes the 33+ far tail (4.2 / 2.8 vs 5.1 / 3.4 real at years 4–5) by
    under-projecting the near one (11.5 / 8.2 / 5.8 vs 12.8 / 10.6 / 8.2 at years 1–3, where the
    value is) and scores between the two: ordering equal to both (0.607), top-12 bias 0.113 / 0.125
    (vs 0.135 / 0.162 plain, 0.091 / 0.087 tiered; both t > 4.7), top decile 0.695 vs 0.704 plain
    (t = 2.2) and 0.692 tiered (t = 1.0), share error 0.085 vs 0.094 tiered (t = 1.7).
    **Adopted 2026-10-05: the tier-aware cap from 30 (`career.DEFAULT_CAP = "30+t"`)** — the only
    variant that is right for 30–32-year-old starters, the one where projected games depend on how
    good the player is (the owner's hypothesis, true in the data), the best top-12 bias; the 33+
    far tail (n = 43, mostly QBs; years 4–5, discounted to 0.4) stays a known over-projection rather
    than a tuned fix on 43 players. `career.fit_survival` / `career.apply_cap` parse the spec for the
    harness, the in-season refresh (the snapshot's tier is last season's `prev_ppg` / `prev_games`,
    not three weeks of this one) and the career build.
    **Rookie tails, 2026-10-06.** With the cap fixed the in-season rookies were still rich (3 of 39
    priced rookies a value): the cause moved to `inseason.fill_missing_tail`, which gave a rookie
    the median ratio h{k} / next among players WITH a career tail in his position and age bucket;
    those buckets are mostly fringe players whose projected tails collapse, so Jeremiyah Love (RB,
    next season 13.5 games / 13.9 ppg) ran 9.1 / 3.6 / 4.7 / 1.0 games in years 3-6 while Ashton
    Jeanty, one year older with a real tail, ran 13.3 / 12.1 / 9.8 / 10.1. The record for rookies
    2008-2020 by rookie-year tier (mean games in year +k over year +1, absent = 0): RB starters
    0.95 / 0.81 / 0.71 / 0.65, mid 0.83 / 0.75 / 0.73 / 0.53, fringe 0.95 / 0.85 / 0.64 / 0.47;
    WR mid 0.99 / 0.87 / 0.84 / 0.78; first-round picks keep playing whatever their rookie year
    (RB R1 fringe: 12.8 / 11.6 / 11.2 / 10.0 / 11.1 games). Hold-out (table from 2008-14 rookies,
    scored on 2015-19 anchored on actual year +1, MAE on games +2..+5): position x tier 4.75, position
    only 4.80, position x age band 4.83, x draft round 4.83 — the grouping barely matters; what
    matters is realized ratios instead of the projection medians. **Adopted:** `rookie_tail_table`
    (position x tier on the next-season projection, starter+mid pool and position as fallbacks, ppg
    ratios need their own survivor count and are clipped to 0.6-1.15, leak-safe `through` a season)
    feeds `fill_missing_tail` for rookies; returning veterans keep the old rule. Love's games become
    13.0 / 11.1 / 9.2 / 8.7 / 8.1; 57 of 454 players take the table. Draft-round conditioning is the
    untested refinement (the hold-out says it adds nothing on games; it may on ppg).
    **Miss reasons and the week-by-week shape (owner, 2026-10-06: "played or missed and the reason, not
    just binary; the injury type").** New silver `fact_player_week_status` (data engineering): every
    skill player-week since 2002 as played / bye / injured_reserve / injured_out / suspended /
    practice_squad / inactive / dnp / not_rostered with the body-part class, snap share from 2013.
    327,671 rows; among the misses 17.6k reserve weeks, 9.3k out weeks, 4.6k healthy scratches. The
    ML `weekly` group turns a season into 18 weekly slots (status, injury class, points,
    opportunities, snap share) plus reason counts for the season and the one before: 122 columns,
    195 with every group on. Queued on TabPFN 3.5 behind the feature-set run (`tabpfn35_setw_w` vs
    `tabpfn35_set_w`). Honest limit: a player hurt in a game and placed on reserve usually has no
    report after that, so the class of many reserve stints is unknown (null), not wrong.
    (2) The college data build is in:
    `data_engineering/src/cfbd_ingestion` (CFBD backfill; the owner's key arrived 2026-10-06 and the
    2010-2025 backfill plus the silver build ran: 61,167 college player-seasons; the crosswalk first
    matched 685 of 4,043 drafted players because the nflverse id table is sparse on draft year / pick
    and CFBD spells positions out, fixed to join our own fact_player_season: 1,719, and 58 % of the
    matrix's 2011+ rookies carry college columns),
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

26. **Valuing the owner's own roster: bench players and the range of outcomes.** *(2026-10-06,
    first cut built: the career model keeps TabPFN's 20/50/80 ppg and games quantiles per horizon
    (`--range`), the harness scores coverage and pinball loss, the WAR build adds floor / ceiling
    wins per span, rookies get a band from the position spread, and the page shows WAR
    floor–ceiling and the ppg band per season; on the page since v43 (2026-10-07, week 4, the corrected scoring): e.g. Nabers WAR 0.78 with a 0.4–2.0 band, season-3 ppg 11.5 (8.0–14.9). The first `--range` refresh lost the band to a select in `inseason_value` (fixed); the second wrote to week 4 because the lake had advanced.
    Owner's framing: range, error and ordering together per player; the distribution loss (CRPS /
    pinball) becomes the primary once the harness reads distributions everywhere, item 29 the
    path-dependent version.)* Suspicion: the
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

    **Distribution scoring (2026-10-09, owner: "scoring the distribution is key").** The band's
    pinball loss, 20-80 coverage and asymmetry (`ppg_skew`: (upside - downside) / width of the
    20-50-80 band, > 0 = right-skewed) are ledger columns, paired metrics and a third line of the
    verdict print-out whenever both runs kept a band; the rule stays the two co-primaries until the
    owner moves it. **Are the distributions normal? No.** On the week-4 projections, RB / WR / TE
    bands are right-skewed at every horizon (about half clearly so; the mean sits 0.4-0.7 ppg above
    the median; most extreme for fringe players whose floor is 0), QBs left-skewed (a fat downside:
    benchings; the mean below the median). The point on the page is the mean; the export now
    carries the median (`band.md`) and the detail table shows "med" and a skew marker (▲ right, ▼
    left) next to the band. Next: make the pinball loss a co-primary once the production run
    carries the band in every harness run (`--range` on by default for TabPFN backends).
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

28. **PFF grades as inputs (owner, 2026-10-06).** Pro Football Focus player grades (overall, pass /
    run / receiving / blocking, per game and per season) are the one widely used quality signal the
    model does not have. Constraints: paywalled (PFF+ subscription, no API; exports are manual and
    the terms forbid scraping), NFL grades start in 2006 and college grades in 2014, so a grade column
    is null for the 1999-2005 rows and the model can read "has a grade" as an era marker; rows beat
    features at 12k player-seasons. Backlogged, not planned: if it is ever tried, (a) null-before-2006
    with an explicit era flag, (b) harness `--first-cohort 2015` is unaffected (every cohort trains
    on 2006+ rows for the grade), (c) the test is the same paired harness as every other group, on
    TabPFN 3.5 (the 85-feature pretraining limit of v2 no longer binds there).

**Acceptance rule from 2026-10-06 (owner): ordering and error are co-primaries.** `--paired` prints
the verdict: adopt when the candidate improves `spearman_war_top` or `mae_war_top` past |t| = 2.4
with the other no worse (t > −1). Earlier rounds were judged on ordering with error as the
tiebreaker; nothing adopted before this date would change under the new rule (the cap and rookie
tails gained on both), and the 3.5 base run (0.604 vs 0.605 ordering, 0.516 vs 0.524 wins error,
t = 1.45) stays a tie.

29. **Autoregressive career simulation (owner, 2026-10-06: "LLMs guess the next word; can we guess the
    next season, or game, and keep going instead of synthesizing one number?").** Yes, and the
    frame is close to it already. Today the career model is a set of DIRECT models: one pair
    (games, ppg) per horizon k, each predicting season T+k from the season-T row, chosen over
    rolling a one-year model forward because a rolled mean compounds its own errors and the model
    never saw its own guesses as inputs. The LLM analogy adds the missing piece: SAMPLE, do not
    roll the mean. Draw next season's (games, ppg) from the one-step predictive distribution
    (TabPFN already returns a 5,000-bucket distribution per row, card 7 on the page), rebuild the
    row for T+1 (age + 1, lags shifted, career totals accumulated, the sampled season as "this
    season"), draw T+2 from that, and so on to T+10; a few hundred draws per player give a
    distribution of careers, not a point: the chance of a lost season, the boom and bust paths,
    the bimodal shape of a player like Nabers (item 24), and a mean that respects path dependence
    (an injury season changes what follows, which a direct horizon-k model can only average over).
    Scoring: the same harness (mean of the trajectory distribution -> WAR -> `spearman_war_top`
    against the direct models) PLUS the distribution itself (coverage of the 10-90 % interval of
    realized 3-year WAR, CRPS), which is what items 26 and 27 need. Design: a one-step TabPFN
    model on season-level inputs only (the weekly slots of a simulated season do not exist), the
    context cached once per cohort (`fit_with_cache`, fine at 16 GB) so each step is test rows
    only (450 players x 300 draws = 135k test rows per step); exposure bias is the risk and
    sampling from a calibrated distribution is the mitigation; the game-level version (roll a
    per-game model within a season) comes after the season-level one works. Not started: queued
    behind the 3.5 feature-set and weekly runs (same GPU).

30. **Draft rows: the rookie's own input row, from college and draft capital (owner, 2026-10-07).**
    The career model's input is a complete NFL season, so a rookie gets a hand-made tail
    (`inseason.fill_missing_tail`, item 24). Replace it: for every drafted QB / RB / WR / TE since
    1999 add one row to the career matrix dated the draft (NFL inputs null, `is_draft_row`,
    draft capital, and the `college` group where `dim_college_crosswalk` reaches: final-season
    dominator, usage and touch shares, breakout age, seasons, team SP+, early declaration), with
    the player's NFL seasons 1..k as its targets. TabPFN handles the missingness natively and 3.5's
    column limit lets college, weekly and injury inputs sit together; a tree needs tricks. Then
    (a) the career model projects a rookie's tail directly and the extrapolation goes, (b) rookies
    become scorable in the harness for the first time (every past class has realized WAR: a
    `rookie` cohort slice next to the top-150 metrics), (c) the in-season model gets the same
    inputs for ROS / next season, and (d) the range of outcomes (item 26) applies to rookies, which
    is where it matters most. The question it answers (owner): are rookies overvalued by the
    market, or valuable but slow to arrive (the London / Adams shape)? Test: realized WAR of past
    classes vs their price at the draft (the market backtest's experience segment already shows
    rookies finishing ~14 ranks below the market's rank, 2022-24), and the realized trajectory
    shape by draft capital (years to peak, share who arrive late), with the band's coverage on
    rookies as the calibration check. Prerequisites: the crosswalk reaches 58 % of 2011+ rookies
    (pick join first); raise it with a better name match (suffixes, nicknames, position labels)
    before the rows are built; college data starts 2010 (draft rows before that carry draft
    capital only). Honest limits: ~100 skill rookies a year, the college-to-NFL jump is the
    hardest prediction in the sport; a draft row is adopted only under the co-primary rule on
    held-out classes. Crosswalk, measured 2026-10-07 on the matrix's own 2011+ rookies (1,511):
    66 % matched; the drafted are done (6 unmatched of ~1,000); every other miss is UNDRAFTED
    (502), who are absent from CFBD's draft table by definition, so the name fallback (which runs
    against draft picks) can never reach them. One bounded pass, per the owner ("don't go crazy
    matching"): match the undrafted by normalised name + position + college (our weekly fact
    carries `college_name`) against CFBD rosters / player stats, then stop; whoever is still
    unmatched carries draft capital (undrafted) and null college columns, which is itself
    informative. Build order: crosswalk match rate -> draft rows in `career.build_career_matrix`
    (opt-in) -> harness rookie slice -> 3.5 run with the feature set -> replace the tail in the
    refresh if it wins. CPU work except the run; after the pooled-horizon queue.

    **Built (2026-10-09, owner: "I don't like the hand-made tail... lets do this one first"):**
    `src/draft_rows.py`. One row per drafted QB / RB / WR / TE in the college crosswalk (2010-2026,
    1,352 rows, 1,316 keyed by gsis id, the rest `cfbd:<id>` for players without NFL rows), at
    season = draft year - 1: position, round, pick, age (the rookie-season age from the season fact
    minus one, else the class median), `is_rookie` = 1, `exp_at_season` = 0 (the `rookie` group's
    interactions apply), `is_draft_row` = 1 (a base flag now), production columns null, `cfbd_id`
    for the `college` group, which now joins draft rows by athlete id (college filled on 87 % of
    draft rows vs 63 % of played rookie rows). Horizon targets attach like any row: a never-played
    pick's observable seasons are 0 games (7-15 per class; the attrition the model learns), the
    2026 class is censored. Played-season rows alone feed replacement levels, survival, the
    snapshots and the rookie tables (`draft_rows.drop_draft_rows`; `fit_survival` drops them
    itself). `--draft-rows` on `run_experiment`, `backtest_inseason` (the career tail then projects
    a drafted rookie from his own row and `fill_missing_tail` finds a tail already there),
    `build_intrinsic_value`, `market_backtest`, `weekly_refresh`. Gaps: the crosswalk has no birth
    dates, so `col_breakout_age` is null everywhere (use the class year instead: a small follow-up);
    undrafted rookies keep the extrapolated tail. Queued on the GPU behind the production switch:
    `tabpfn35_set_stacked_draft_w` vs a same-day baseline, and the market backtest with draft rows
    (the rookie slice is the test: 0.48 vs the market's 0.51 before).
31. **Team strength and contracts as feature groups (owner, 2026-10-07).** Two inputs the model has
    never seen: how good the player's team is (offense environment, game script, QB) and what the
    NFL itself pays the player (its own valuation, and the tie to the team that projects forward).
    **Team strength needs no new ingestion.** The lake's `bronze/nflverse/schedules` (1999-2026)
    carries the closing lines for every game (`spread_line`, `total_line`, moneylines from 2006,
    the starting QBs and coaches), and `team_stats` (1999-2026, 102 columns) the per-game offensive
    and defensive EPA. A silver `fact_team_season_strength` (team x season, with a week-level
    variant for the in-season model): the market-implied rating = mean favoritism margin over the
    season's games (2025: BUF +6.2, LA +6.0 ... TEN -7.6, i.e. the market's power rating, which
    already prices the QB and injuries), the mean total line (scoring environment), realized point
    differential, offensive EPA per play, pass rate, the starting QB's prior-season ppg (the
    "QB quality" a pass catcher inherits), and their lags. The `team` ML group joins them to the
    player's team for season t (and lag1), plus the change on a team move. Next season's team
    strength is unknown at projection time; the proxies in order of availability are last season's
    rating (regresses to the mean, the model learns the rate), the week-1 line of the new season
    (published in spring, in the schedules file once posted), and preseason win-total futures,
    which the lake does not have (sportsoddshistory.com has season win totals and Super Bowl odds
    by season, a scrape for later; not needed for the first cut).
    **Contracts: one new nflverse entity.** nflverse's `contracts` release (OverTheCap; 53k
    contracts, `gsis_id` on 90-98 % of rows since 2005, 68 % 2000-04, thin before; 17k skill-position
    contracts since 2000 for 3.3k players) has per contract: year signed, years, value, APY,
    guarantees, **APY as a share of that year's cap** (inflation-free), the drafted / extension /
    free-agent type, and a per-year `season_history` (cap number, cap percent, guaranteed salary,
    cash paid) plus the `contract_history` of renegotiations. Ingest as a daily snapshot
    (`bronze/nflverse/contracts/load_date=...`, 11 MB), model `fact_player_contract_season`
    (player x season: the contract in force that season, years remaining after it, cap percent
    that year, guaranteed money remaining, contract-year flag, rookie-deal flag, APY-cap-share at
    signing relative to the position's top at that time), and a `contract` ML group. Leakage rule:
    in the harness a contract counts only if `year_signed <= t` for a row as of season t (an
    extension signed the following March is real information before season t+1 but the table has no
    signing date, so the strict rule for backtests; the production refresh may use the current year).
    Expected mechanism: cap share and guarantees are the NFL's forward valuation (teams pay for the
    next 2-3 years, which is exactly our horizon), the contract year is a known production bump, the
    years-remaining ties the player to the team's strength. Both groups go on the 3.5 pooled
    candidate under the co-primary rule, after the feature-push chain of item 22.
    **Built (2026-10-07 evening):** `fact_team_season_strength` (893 team-seasons 1999-2026, every
    one lined, EPA on every row, moneylines from 2006; 2026 market top KC +6.1, BAL / LA +4.5, bottom
    MIA -8.5) and `fact_player_contract_season` (16,110 player-seasons 1994-2032 for 3,325 skill
    players; contract type known for 97 %, the team's cap number for 70 %) are on the lake, both T1
    jobs in the daily DAG (`silver-fact-team-season-strength`, `silver-fact-player-contract-season`)
    with the `contracts` table added to `nflverse-daily` as a Tuesday snapshot (first snapshot
    written by hand the same day). The `team` (15 columns) and `contract` (13 columns) groups join
    the career matrix: teams on every row; contracts on 91-98 % of rows from 2015, 57 % in 2010-14,
    20 % in 2005-09, under 5 % before (OTC's history is dense from the 2011 CBA; the model reads the
    null as a state, as with college and injury). Queued on the GPU behind the feature-push chain:
    `tabpfn35_set_team_stacked_w`, `tabpfn35_set_contract_stacked_w`, `tabpfn35_set_tc_stacked_w`,
    each paired against the pooled candidate. **In-season model (same evening):** the week-level
    table `fact_team_week_strength` (15,373 team-weeks, the same quantities to date after each week,
    this week's own line, last season's rating; byes carry the to-date values) is written by the same
    job, and the in-season model takes `--inseason-groups team,contract` (`inseason.EXTRA_GROUPS`:
    11 team-to-date columns at the snapshot week, 8 contract columns for the season). A/B on the
    trees, cohorts 2021-24, weeks 3/6/9/13, on the CPU (45 s a run), rank agreement with
    next-season points / rest-of-season ppg among KTC-priced players at weeks 3 / 6 / 9 / 13:
    none 0.545 / 0.559 / 0.581 / 0.599 and 0.793 / 0.773 / 0.753 / 0.696 (KTC 0.552 / 0.563 /
    0.573 / 0.579 and 0.703 / 0.673 / 0.643 / 0.565); team 0.531 / 0.542 / 0.574 / 0.591 and
    0.795 / 0.777 / 0.754 / 0.696; contract 0.548 / 0.565 / 0.581 / 0.593 and 0.796 / 0.772 /
    0.752 / 0.699; both 0.540 / 0.552 / 0.571 / 0.592 and 0.795 / 0.773 / 0.756 / 0.698. **On the
    trees: no gain.** The team group costs next-season ordering about 0.01 at every checkpoint and
    adds a hair to rest-of-season; the contract group is a wash. The same pattern as the career
    model, where the feature set did nothing for the trees and v2 and only moved 3.5 pooled, so
    the in-season model on TabPFN 3.5 (snapshots subsampled to its 50k-row limit, recent seasons
    first) is the test that counts; not adopted on the trees. Built the same evening:
    `InSeasonModels(backend="tabpfn", train_weeks, max_train_rows)` with `inseason.training_subset`
    (the checkpoint weeks only, then recent seasons whole and a random share of the oldest that
    fits), `--inseason-backend tabpfn` on the backtest and the refresh; queued on the GPU behind the
    groups chain: 3.5 in-season on base features and with team + contract, cohorts 2021-24, read
    against the trees above (`inseason_gpu_base.log`, `inseason_gpu_team_contract.log`).
    **Career model verdicts (2026-10-07 22:21 - 2026-10-08 01:19, on the 3.5 pooled candidate
    0.622 / 0.474, all-player ordering 0.605, prior-top-12 bias -0.043, share error 0.126):**
    + team 0.616 / 0.510 (ordering t = -1.0, error t = -12.3, 8 of 8 worse; all-player 0.627,
    t = +6.2) -> NO; + contract 0.626 / 0.518 (t = +0.4 / -7.0; all-player 0.629, t = +5.9; share
    error 0.110; top-12 bias -0.090, the stars under-projected more) -> NO; + both 0.604 / 0.516
    (t = -1.8 / -13.2; all-player 0.633, t = +7.6) -> NO, and NO against team alone. The same
    shape three times: either group sorts the whole pool better by 0.02-0.03 and sharpens the
    position shares, and costs the top 150 about 0.04 wins of error with no ordering gain there.
    Both groups describe status (the cap share is close to a market price, the team rating prices
    the roster around him), and the pooled model leans on status for the players whose production
    already says everything, so the top of the pool gets noisier while the tail gets sorted. Not
    adopted for the career model. Where the gain would be real is the long tail (bench, the
    players after 150) if that ever becomes a target, and the in-season model if 3.5 says so.
    **In-season model on TabPFN 3.5 (2026-10-08 01:19-02:00, base snapshot features, the four
    checkpoint weeks as the in-context set capped at 50k rows, recent seasons first; cohorts
    2021-24):** next-season rank agreement 0.572 / 0.582 / 0.600 / 0.621 at weeks 3 / 6 / 9 / 13
    against the trees' 0.545 / 0.559 / 0.581 / 0.599 and KTC's 0.552 / 0.563 / 0.573 / 0.579: up
    0.02-0.03 at every checkpoint, and the first in-season model that beats the market on next
    season at every week (the trees only tied it); the in-season value column moves with it
    (0.568-0.592 vs 0.552-0.575). Rest of season 0.803 / 0.770 / 0.745 / 0.693 vs 0.793 / 0.773 /
    0.753 / 0.696: a wash (up at week 3, a hair down later; both far above KTC). 41 min a run.
    No paired t here (the in-season backtest keeps per-cohort rows only in the published summary,
    not with --no-write); the gain is the same sign at all four checkpoints. Candidate for the
    production refresh (`--inseason-backend tabpfn`), next to the 3.5 pooled career tail.
    **+ team + contract on the 3.5 in-season model (02:01-02:43):** next season 0.563 / 0.574 /
    0.591 / 0.612, 0.009 below the 3.5 base at every checkpoint; rest of season 0.803 / 0.777 /
    0.749 / 0.695, a hair above. The same answer as on the trees and on the career model: the
    status groups cost next-season ordering and add nothing that matters. **BACKLOG 31 closes with
    both groups built, tested four ways and not adopted anywhere**; the tables stay on the lake
    (the team-value and roster pages can use them) and the groups stay in the registry.
    Still not built: preseason win-total futures (no source in the lake).

32. **One model for the in-season and the career horizons (owner, 2026-10-09: "Yes! Absolutely do
    this").** Today two models answer "what will he score next season": the in-season model from
    a mid-season snapshot (to-date stats + last season; trees, now TabPFN 3.5) and the career
    model from the season-end row (h1). The career model learnt the horizon curve when horizons
    were pooled; the same move can pool the as-of point. **Design.** One row schema for both
    kinds of row: the player's state at an as-of point (season, `week` = weeks played so far, 18
    for a complete season; to-date rate, games, usage; last season and the career to date; age,
    draft capital, the groups) and a target season `years_ahead` away (1 = next season from a
    snapshot or h1 from a season-end row; 2, 3.. the later seasons), ppg and games as targets.
    Snapshot rows come from `inseason.build_snapshots` (td_* / prev_* columns mapped onto the
    career columns, `week` set), season-end rows from the career matrix (`week` = 18), draft rows
    (item 30) are the `week` = 0 case with college columns. One pooled TabPFN 3.5 model over all
    of it, within the 50k-row in-context cap: every season-end row for horizons 1-3 (~37k) plus
    snapshot rows at two checkpoint weeks of recent seasons, or a sample. **Two steps.** (1)
    Augmentation: the career harness unchanged (season-end test rows), the training table carrying
    snapshot rows too; scored on the three-year co-primaries against the production run. (2) The
    in-season use: the in-season backtest's next-season projection from the unified model instead
    of the in-season model (ROS stays with the in-season model until a `years_ahead` = 0 target
    is added), scored on next-season rank agreement at weeks 3 / 6 / 9 / 13 against the 3.5
    in-season model (0.572-0.621). If (2) wins, the page's seasons two and on, next season and
    the rookie tail all come from one model, and the hand-made bridges between them go away.
    Build after the draft-row and blend verdicts; it needs the GPU for every test.

33. **How the market prices over time: the edges in the pricing community itself (owner,
    2026-10-09).** `analysis/market_trends.py`, on KTC's daily SF history 2020-2026 (players
    priced at 1,000+, every relative move market-adjusted). **Data caveat found and fixed the
    same day:** `dim_players_master` carries a gsis_id for 3,893 of 12,229 players, so the
    id-only crosswalk reached a third of the priced pool (62 of 307 priced players in September
    2025) and the first-pass age / stage, big-game and injury reads were on that biased third.
    `ktc_crosswalk` now falls back to normalised name + position against the season fact
    (ambiguous names to the most recent namesake): 401 of 427 KTC players, 95 % of player-dates,
    300 of the 307. (The market backtests already matched id-then-name, so they were never
    affected; the DE fix, a fuller gsis_id in the master, is a separate item.) **Findings, on the
    full pool:**
    - *The whole market deflates every month* (-0.3 to -2.2 % per 30 days for the median priced
      player, most in April when the draft class takes value out of the pool). Relative moves
      are what matter.
    - *The age curve runs continuously, and in season.* Players 29+ (and year-7+) lose -2.0 to
      -3.1 % a month against the market from October to December and re-rate +4.9 % in
      February; 25-28-year-olds are flat in season and +3.7 % in February; under-25s gain +1.6
      to +3.0 % a month October-December, +3.8 % in April (the draft) and only dip in
      February-March (-0.7 / -1.0 %). Year-2-3 players lose -1.3 / -2.6 % in March-April as the
      new class arrives and gain +1.3 to +1.6 % a month October-December. The incoming rookie
      class (priced before the NFL draft) is -2.9 % in February, then +19 % in April and +4.6 %
      in May.
    - *By position.* QBs +3 % a month in January-February and +2 % in November-December, -1 to
      -3 % April-October; TEs +4.1 % in October and +2 to +2.4 % December-February, -1.5 to
      -1.7 % July-August; RBs +1.6 % April-May and +1.3 to +1.9 % September-October, slightly
      negative January-March; WRs mildly positive most months.
    - *Momentum, not reversion, for risers.* The past 28-day move predicts the next 56 days
      (deciles monotonic: the top decile, +35 % past, goes on to +4.1 % market-adjusted; the
      bottom, -17 %, +0.8). Big moves: up 15 %+ continues +2.7 to +5.2 % over eight weeks;
      down 15 %+ bounces +3.7 to +4.0 % for RB / WR / TE but keeps falling for QBs (-3.1 %).
    - *The age discount is wrong in both directions.* On the 2020-22 market-backtest cohorts
      (364 priced players, three-year realized WAR): under-24s finished 7.5 ranks worse than
      priced (WR +12.6, QB +13.7) at 0.146 wins per 1,000 KTC; 27-29-year-olds finished 18 ranks
      better than priced at 0.241 wins per 1,000; 30+ 7.6 better. The swap test's "29-32-year-old
      buys" result, now as a market property, and the reason the in-season markdown of vets
      (above) is an edge rather than fair.
    - *What the market pays per point* (first September price vs that season's PPG, the top 12
      QB / 24 RB / 36 WR / 12 TE by price): a point a game cost 350-420 KTC at QB, 350-480 at RB,
      420-510 at WR, 350-415 at TE, with no trend worth trading; the market's taste has moved:
      QBs held 28 % of all priced value in 2020-21 and 18 % in 2026 (the priced QB pool tripled,
      the share did not), RBs 22 % to 27 %, WRs 35 % to 40 %, TEs flat at 13-14 %. The preseason
      price ranks the season's scorers at 0.50-0.67 (QB), 0.59-0.78 (RB), 0.55-0.74 (WR),
      0.52-0.88 (TE, falling: 0.63 in 2024, 0.52 in 2025): TE and QB are where a better
      projection pays most.
    - *Rookie picks appreciate into the draft.* A 1st is 9-13 % cheaper 9-18 months before its
      draft than one month before and 22 % cheaper at two years; a 3rd 40-50 % cheaper two years
      out. Part time value, part the hype ramp (and the +19 % April re-rating of the class).
    - *One big game is momentum, not a fade* (553 events, a 2+ sd week): +7.5 % (QB) to +10.5 %
      (TE) in the week, +10 to +15 % at eight weeks for every position, and +1.9 to +4.5 % after
      the first week. (The first pass's "WRs give it back" was the biased third.)
    - *Injuries* (770 absences): the first missed week costs -1.5 to -2.3 % (-3.8 % for 29+);
      week four -0.5 % (concussion) to -4.5 % (ankle / foot), -8 % for 29+; by week 16
      concussions (+3.3 %), soft tissue (+0.1 %) and under-25s (+0.8 %) are fully recovered,
      ankle / foot (-1.9 %) and upper body (-1.1 %) nearly, while knee / Achilles absences
      (-3.7 %) and the 29+ (-4.0 %) stay down. (The first pass had the knee as the one that
      recovered; the biased third.)
    **Edges to act on** (each a few percent, systematic, to combine with the model's own
    mispricing): buy productive 29+ vets in December, sell in February-March; buy year-2-3
    players in March-April and under-25s in February-March; buy rookie picks before the draft;
    ride risers eight weeks, buy non-QB fallers after a 15 % drop, do not catch falling QBs; a
    big game is a buy (or at least not a sell) at every position; buy TEs July-August and sell
    October, buy QBs May-October and sell January-February, RBs cheapest January-March; an
    injured under-25 at week four is a modest buy, an injured 29+ a sell, and a knee / Achilles
    absence stays down. **On the page (2026-10-09):** a Market tab (the seasonality heat-maps by
    age / stage / position and the raw whole-market drift, the calendar of edges, momentum
    deciles and big moves, the age discount, the pick-cycle chart, big-game and injury paths,
    what the market pays per point) from the published summary, which the export carries as
    `market_trends`; and a **Season** column in the Players table's Pricing group: the player's
    seasonal tailwind this month (mean of his position's and his age band's market-adjusted
    30-day change for the run month; green = the calendar favours him, red = works against him;
    blank for picks). **Next:** the age-discount read on the production model's own backtest
    once it runs; `col_breakout_age` needs a class year (the crosswalk has no birth dates); a DE
    item for the master's gsis_id coverage.

34. **Data-quality checks on the lake (owner, 2026-10-09: "when we find bugs are we building data
    quality checks ... to ensure our lake doesn't ever regain these issues?").** We were not: the
    spec suite tests the ETL code on synthetic payloads and two facts quarantine violations, but
    nothing re-checked the lake, so a regression (the KTC parser break, the frozen rollover, the
    sparse gsis_id) only surfaced when a downstream read looked wrong. Built
    `data_engineering/src/data_quality/`: pure expectations (`expectations.py`), a `Check` /
    `Context` / `run_checks` framework (`core.py`, tests seed frames instead of GCS), a catalogue of
    59 checks (`suite.py`) where **every check names the bug it guards**, and the runner
    (`run.py` = Cloud Run job `silver-data-quality`, the last step of the daily DAG; exit 1 on an
    error-severity failure; results to `silver/_quality/run_date=<d>/results.parquet` +
    `latest.parquet` for the drift checks). Layers: bronze feeds landed and look like themselves
    (KTC / FantasyCalc / roster partitions fresh and the right shape, FantasyCalc rows tagged,
    every current league in the snapshot, transactions moving in season, the season's draft
    present); dims (unique keys, statuses, lineage assigned, the NFL season's league per lineage,
    SCD2 on settings, franchises per league); facts (asset values fresh for both sources, unique,
    named, no future dates, no missing day in 30, the player history continuous since 2020-06,
    the priced pool stable, pick tiers present, the ledger SCD2 + current holdings equal to the
    roster snapshot + pick conservation, player-week / season keys, ppg == fpts/games, one scoring
    regime, the current week present, the derived facts keeping up, 32 teams, contracts, college);
    drift (the current leagues' lineup regime unchanged run to run — the BACKLOG 22 trap; the
    league dim not flapping; the big facts never shrinking). 24 spec tests replay the bugs on a
    seeded lake. Rule, in the root and DE CLAUDE.md: a data bug fixed without a check is half
    fixed. **The first dry run (2026-10-09) found three open defects**, recorded as `known_open`
    (reported as warnings with the note until fixed, then enforced):
    - `dim_league_settings`: the three current (2026) leagues' rows have `league_lineage_id`
      null (the settings dim never got the lineage chaining dim_leagues_meta got in PR #12).
    - `dim_players_master`: 7 gsis_ids shared by two players — six are Sleeper's inactive
      "Duplicate Player" placeholders the dim should drop, one is a real conflict (Isaiah Searight
      / Quinnen Williams on 00-0035718); and gsis_id is present for 18 % of the KTC-priced pool
      (item 33).
    - `fact_roster_membership`: 88 overlapping holding intervals (two franchises holding the same
      player or pick at once; most a day or two at a transaction boundary, a few for months, e.g.
      player 11370 in lineage ...304 held by two franchises 2026-06-30 to 08-24).
    **Next:** fix the three (each fix removes its `known_open`); the Cloud Run job + DAG step
    deploy with the merge; add a check with every future data bug.

35. **The jagged 2026 team-value series (owner, 2026-10-09: "seems to not be nearly as smooth
    as other seasons").** Measured: the league-wide mean week-over-week move of a franchise's
    adjusted value was 1.0 % in the 2025 season and 1.8 % in 2026, with 7.5 / 10.5 / 9.3 % on
    the weeks of 2026-09-22 / 09-29 / 10-06 and 4-4.5 % on 2026-06-30 and 08-11. Two causes,
    both now fixed and both now watched by the data-quality suite:
    - *The value lens had a hole.* `fact_asset_values` has no TE-premium rows 2026-09-08 ->
      09-30: the KTC outage was backfilled from per-player history pages, which carry Standard
      only. `fantasy_lib.load_player_values_blend` cut over from Standard to TEP by era, so the
      four September grid dates had no player values at all and the as-of join fell off its
      tolerance. Fix: `blend_values` falls back to Standard per (day, player) (TEP only differs
      for TEs anyway); check `fact.asset_values.tep_lens_no_gaps_30d` (known_open until the
      September window leaves the 30-day lookback).
    - *The ledger booked a frozen second roster per franchise.* The daily roster job
      re-ingested the completed 2025 leagues beside the 2026 ones on alternate days through
      2026-09 (the dim_leagues_meta oscillation, fixed 2026-09-30: from 10-01 the snapshot holds
      the 3 current leagues only). The 2025 and 2026 leagues of a lineage share franchise_id
      (lineage + roster_id), so the stale 2025 roster became a second, overlapping holding that
      opened and closed every other day: 250 interval boundaries a day in late September, the 88
      overlapping stints the quality suite found, and offseason jumps on 06-30 / 08-11. Fix:
      `fact_roster_membership.drop_stale_league_snapshots` keeps, per snapshot day and lineage,
      only the newest season's league; check `bronze.sleeper_rosters.one_league_per_lineage`.
      The ledger repairs itself on the next silver run after the merge (the overlap check's
      known_open note comes off then).
    **Verified on the ledger rebuilt in memory with the fix** (not written to the lake; the DAG
    does that after the merge): intervals 7,542 -> 6,372, overlapping stints 88 -> 5, interval
    boundaries in September at most 57 a day (was 250+); the 2026 season's mean weekly move
    0.94 % (was 1.72 %; 2025 = 0.99 %), the September weeks 1.3 / 1.9 / 2.6 % (were 7.5 / 10.5 /
    9.3 %), 06-30 0.2 % (was 4.1 %), 08-11 3.0 % (was 4.5 %: a real trade week, partly). The Team
    value summary on the page was republished from that rebuilt ledger (run_date 2026-10-09).
    Also closed from BACKLOG 34: the settings dim's lineage (`utils.chain_lineage`, shared with
    dim_leagues_meta) and the players master's gsis_id (the bridge's id was lost to a same-named
    Sleeper column -- the real cause of the 18 % coverage in item 33 -- and the "Duplicate
    Player" placeholders / shared ids are nulled, `dedupe_gsis`). The three dims rebuild on the
    next DAG run; their known_open notes come off once the suite passes on the rebuilt lake.

36. **Accuracy programme for the Monday-night refresh (owner, 2026-10-09: "I'm okay with long runs
    ... as long as we aren't exceeding 8 hours ... #1 prio is model perf").** The weekly refresh
    runs after Monday Night Football so the career projections move weekly; speed is not a goal,
    an 8-hour budget is. The binding constraint for TabPFN is context, not time: the in-season
    model has 199k snapshot rows but holds 50k in 16 GB (also the model's pretraining limit).
    Levers, each through the harness under the co-primary rule, in order:
    1. *Ensemble size* (`--tabpfn-params n_estimators=32`; production = the library default) on
       the career and the in-season model. Zero code; `chain_accuracy.sh` (scratchpad) is
       written, queued only on the owner's word.
    2. *Context bagging*: K models on different 50k-row draws of the snapshots, averaged; uses
       all the data despite the VRAM cap at K x the cost. Needs `InSeasonModels(bags=K)`.
    3. *Relevance-first context*: for a week-w projection fill the context with weeks w +- 1
       across seasons first, then recent seasons, instead of the four checkpoint weeks recent
       seasons first. Needs `training_subset(around_week=)`.
    4. *The unified model* (item 32, queued): what makes the career projection itself move
       week to week. Draft rows and the blend target (items 30 / 26) are queued ahead of it.
    5. *Full-precision inference* (`inference_precision=float32`): doubles the cost, sometimes
       tightens the tails; low expected gain, in the chain.
    Not worth the hours: more checkpoint weeks (the week is a feature), speed-only changes
    (caching the career tail, a 20k cap) -- unless the budget is exceeded. **Where the first
    production run's hours went (py-spy on the live process, 2026-10-09 14:00, 4.5 h in):**
    still in the career tail's `estimate_sigma`, which fits a SECOND complete pooled 3.5 model
    (as of 2022) and predicts the 2023-25 holdout rows for all ten horizons, each predict a full
    8-member pass over the context -- then the production predictions with the quantile bands,
    then the in-season stage. Pooled 10 horizons x stacked x range x the sigma refit is roughly
    three model builds; GPU-bound (cuda synchronize, the v3.5 forward), not stuck. Two
    accuracy-neutral cuts for the weekly run: (a) sigma and the career tail depend only on the
    completed seasons, so cache both per season and config (identical every week until the
    season completes) -- the weekly run becomes the in-season stage alone (~1.5 h); (b) sigma
    from the fitted model's own quantile spread instead of a holdout refit (TabPFN gives the
    predictive distribution; the refit exists for the trees' sake) -- test that the WAR
    pricing is unchanged before switching. **Found and fixed the same afternoon (py-spy
    locals: the loop was on horizon 8 of 10 at 15:20):** `estimate_sigma` predicted the holdout
    rows once PER HORIZON, and with pooled horizons every prediction expands the frame H-fold,
    so the sigma stage was H x H = 100 horizon-predictions of the 8-member ensemble over a 60k
    context instead of 10. It now predicts the union of the holdout rows once and takes each
    horizon's residuals from that frame -- row-wise identical (spec-tested against the
    per-horizon loop), about ten times cheaper. The running production job kept the old code
    (restarting would not have finished sooner); every later run has the fix. **Second cut
    (2026-10-10 02:40):** the production predictions ran TWO forward passes per estimator and
    horizon frame, one for the point (`predict`) and one for the bands (`predict_quantiles`);
    the TabPFN wrapper now asks the model for mean + the wanted quantiles together
    (`output_type="main"`, the same one pass) and memoises the answer for the last X, so the
    bands that follow on the same rows are free -- the predict stage halves, outputs unchanged
    (spec-tested: one call per estimator per frame; other quantiles or new rows still ask).
    **Scheduling catch:** the lake sees Monday's stats only after the Tuesday 10:00 UTC DAG
    (nflverse posts overnight), so the weekly refresh should trigger Tuesday ~08:00 local
    (Task Scheduler weekly task on the chain pattern) and finishes by mid-afternoon. **Hardware:**
    more VRAM would be an accuracy lever (context), a faster GPU only a time one; a Vertex H100
    per run is the cheap way to a bigger context if bagging (2) does not close the gap.

37. **In-season feature groups: usage, role, schedule (owner, 2026-10-09: recency the model cannot
    build itself, role signals, the schedule ahead; "the model should find richer recency right?"
    -- only from columns it is handed).** Three optional in-season groups in `inseason.py`, joined
    per snapshot week like team / contract (`--inseason-groups usage,role,schedule`):
    - *usage*: Next Gen Stats to date, weighted by targets / attempts (receiving: separation,
      cushion, share of intended air yards, depth of target, YAC over expected, catch rate;
      rushing: efficiency, yards over expected per attempt, stacked-box rate, time to the line)
      and snap share to date with its three-week trend. The lake held the passing slice of Next
      Gen only; the receiving and rushing slices are now their own nflverse datasets (2016 on,
      backfilled by the daily reconcile after the merge); `--ngs-dir` reads the slices pulled
      locally until then.
    - *role*: recency windows beyond the three-game form (last game, last five, the last three
      games' targets and touches per game against the season rate) and the status table (games
      since returning from a missed week with byes neutral, misses in the last three weeks,
      injured and dnp weeks to date).
    - *schedule*: from the schedules' own scores, the remaining opponents' point differential
      per game to date (mean and the next opponent's), games left, a bye still ahead. Nothing
      from the future: opponent strength is their games <= the snapshot week.
    **The funnel (owner's idea, adjusted to the trees):** (1) the in-season backtest on the trees,
    CPU, minutes per run, against a fresh trees baseline -- drop the flat groups; (2) 3.5 at
    reduced scope (two cohorts, two weeks, three horizons); (3) the full 3.5 paired run under the
    co-primary rule for adoption. **Stage 1 (trees, 2026-10-09 12:57-12:59, 30 s a run; mean rank
    correlation over cohorts 2021-24, weeks 3 / 6 / 9 / 13):** base next-season 0.546 / 0.559 /
    0.582 / 0.596, ROS 0.793 / 0.773 / 0.750 / 0.696. *usage*: next 0.544 / 0.560 / 0.586 /
    **0.609**, ROS 0.799 / 0.776 / 0.754 / 0.704 -- better at every ROS week and +0.013 next at
    week 13, with Next Gen covering 13 % of training rows (2016 on) -> KEEP. *role*: next 0.538 /
    0.563 / 0.586 / 0.600, ROS 0.793 / 0.780 / 0.753 / 0.699 -- marginal, positive from week 6
    -> keep with usage. *schedule*: next 0.541 / 0.556 / 0.579 / 0.595, ROS 0.796 / 0.775 /
    0.750 / 0.695 -> flat, DROP. All three: next 0.537 / 0.560 / 0.584 / 0.605, ROS 0.798 /
    0.780 / 0.757 / 0.702 = usage + role. **Stage 2 queued** (`chain_stage2.sh`, Task Scheduler
    ClaudeStage2, waits for the unified chain): 3.5 in-season at cohorts 2023-24, weeks 6 / 13,
    three horizons -- base, usage, usage + role; **2b** (`chain_stage2b.sh`, ClaudeStage2b) usage +
    role + opportunity. *opportunity* (nflverse ff_opportunity expected fantasy points to date:
    per game overall / by phase, last-three and trend, actual minus expected for points and
    touchdowns, expected yards, targets, air yards; 2006 on = 71 % of training rows; the lake
    entry was a current-season weekly snapshot, now the seasonal history) on the trees: next
    0.550 / 0.564 / 0.584 / 0.596, ROS 0.795 / 0.775 / 0.749 / 0.697 -- a little early-season
    signal (expected points stabilise before actual points), flat late. usage + role +
    opportunity: next 0.545 / 0.569 / 0.586 / 0.608, ROS 0.800 / 0.778 / 0.753 / 0.705 -- the
    best combination at every rest-of-season week. Next group in the funnel: the free "advanced usage" sources
    beyond Next Gen (FTN charting via play-by-play, PFR advanced stats = one loader), then team
    *change* features (new QB, play-caller, line turnover). PFF stays out (no legitimate feed).

38. **Consensus as an input the model learns to trust or beat (owner, 2026-10-09: "a model that
    learns to trust consensus vs when our signal beats it is actually BRILLIANT ... we may even be
    able to beat ROS projections too").** The idea: hand the model a projection provider's
    point-in-time view as columns next to our usage, role, opportunity and career columns, and let
    the in-context model learn from past seasons when the consensus was right and when rows that
    looked like this one beat it -- stacking, learned in context, no hand-tuned blend.
    **Data.** Sleeper's projections endpoint (`api.sleeper.app/projections/nfl/<season>/<week>`,
    Rotowire-sourced, the full slate of ~3,100 players a week, 2018 on, keyed by Sleeper
    player_id = our player_key; a season-level endpoint too, no as-of for past seasons). Pulled
    2018-2026 locally for the prototype (505k rows; `--proj-dir`); the bronze ingestion
    (`bronze/sleeper/projections/season=<Y>`, rebuilt daily for the current season so every
    Monday's view is captured, backfilled once) is next. FantasyPros stays the consensus of
    record for the live week but its history is survivorship-biased before 2020 (its CLAUDE.md).
    **The group** (`consensus`, `inseason.consensus_features`): the coming week's projected PPR
    points and rank within the position, the mean projection over the weeks so far, the player's
    PPR rate to date minus that mean (beating the consensus), the coming projection minus his
    rate, a has-projection flag (null before 2018). Sleeper id -> gsis through
    `fantasy_player_ids`.
    **Stage 1 (trees, 2026-10-09 14:40; ROS / next-season rank correlation, weeks 3 / 6 / 9 /
    13):** base ROS 0.793 / 0.773 / 0.750 / 0.696, next 0.546 / 0.559 / 0.582 / 0.596.
    *consensus*: ROS **0.806 / 0.787 / 0.760 / 0.708**, next 0.550 / 0.558 / 0.588 / 0.597 --
    the largest single-group gain of the day, with 2018+ = a third of the training rows. The
    provider's own coming-week projection as a ranking: ROS 0.774 / 0.766 / 0.728 / 0.671, next
    0.547 / 0.566 / 0.576 / 0.564; its mean to date: ROS 0.783 / 0.758 / 0.703 / 0.640. So the
    model with the consensus inside it out-ranks the consensus by 0.02-0.04 on rest of season
    and by 0.03-0.04 on next season from week 9 (the week-3 and week-6 next-season reads are a
    wash). Caveat: the provider's weekly number targets one game, not the rest of season; a true
    ROS projection history does not exist in our data, so "beats ROS projections" is not yet the
    claim -- "beats the provider's point-in-time view" is. *All four groups* (usage, role,
    opportunity, consensus): ROS 0.807 / 0.793 / 0.758 / 0.715, next 0.552 / 0.565 / 0.595 /
    0.607 -- the best model at every week on both reads. **Stage 2c queued** (ClaudeStage2c,
    after 2b): consensus alone and all four on 3.5 at the stage-2 scope. **The bronze ingestion** is in
    (`sleeper-incremental-projections`, DAG bronze tier, `PROJ_SEASONS=2018-2025` for the
    backfill; check `bronze.sleeper_projections.coming_week_present`).
    **The projected stat line** (owner: "projected yards + tds ... could be slightly more
    valuable"): its own group `consensus_line` (the coming week's projected attempts, yards,
    touchdowns, targets, receptions by phase; targets and carries expected to date; expected
    touchdowns to date and the player's actual rate minus it = touchdown luck through the
    provider's eyes). Trees: alone next 0.552 / 0.570 / 0.582 / 0.600, ROS 0.803 / 0.783 / 0.755
    / 0.702 -- as good as the points group on ROS and better early on next season (+0.011 at
    week 6); with the points group next 0.546 / 0.564 / 0.582 / 0.592, ROS 0.808 / 0.786 / 0.756
    / 0.706 -- not additive on the trees (the line sums to the points). Both go to stage 2; 3.5
    may use the shape where the trees could not.
    **Before 2018 (owner: "we can definitely get prior to 2018 right?").** Sleeper's endpoint
    serves empty shells before 2018. What exists: (1) *ADP*, the preseason consensus with no
    survivorship: Fantasy Football Calculator's public API (12-team; standard 2009-2011, PPR
    2012 on; ~200 players and 300-1,300 drafts a year) and MyFantasyLeague's ADP export (2011
    on; 320-460 players, 2,000-9,000 drafts a year; MFL ids, which `fantasy_player_ids` maps to
    gsis) -- pulled locally (`adp/ffc.parquet`, `adp/mfl.parquet`); a preseason-consensus group
    for the snapshot AND a career feature (every season since 2009) is next. (2) *FantasyPros
    weekly projections 2012 on* through the owner's scraper, BUT its history is
    survivorship-biased (only players still in their database render) and that is a LEAK, not
    just noise: a consensus column present only for players who went on to long careers encodes
    the future; usable only where coverage is complete (2022 on). (3) *Wayback captures* of
    weekly projection pages (ESPN's old tool 2016-2019 confirmed; others untested, the CDX index
    is slow): point-in-time and unbiased but a scraping project per site. (4) Paid vendors
    (FantasyData, Sportradar) sell projection history to 2009; not pursued. **The ADP group on the
    trees** (`preseason`: the pick, the rank within the position, a drafted flag; 5,993
    player-seasons 2009-2026, MFL by id where a season has it, FFC by name otherwise): alone ROS
    0.809 / 0.780 / 0.758 / 0.702 (base 0.793 / 0.773 / 0.750 / 0.696; the preseason view
    matters most at week 3, +0.016), next-season flat; all five groups ROS **0.811 / 0.797 /
    0.761 / 0.708**, next 0.555 / 0.562 / 0.596 / 0.605 -- the best rest-of-season model at every
    week but the last. Stage 2d (ClaudeStage2d, after 2c): all five on 3.5. **Next:** the bronze
    ingestion for ADP (yearly, both sources) and ADP as a career feature (every season since
    2009, for the career model's own harness); the provider's
    ROS number captured weekly from now on so the ROS claim can be tested in a year; FantasyPros
    consensus for the live week once its backfill runs (owner's call).

39. **Repeated labels on split rows (owner, 2026-10-09: "if we're synthetically creating new rows
    by splitting individual player seasons ... are we fuzzing the data as to not overfit ... the
    number 32.8?").** Not the exact-number memorisation -- neither the trees nor an in-context
    model treats 32.8 as special -- but the replication is real: the four checkpoint snapshots of
    one player-season carry the SAME next-season label, so one outcome gets four votes, and a
    rich feature set lets the model over-trust those near-identical rows. `InSeasonModels(
    next_one_per_season=True)` (`--inseason-next-one-per-season`) keeps one random snapshot week
    per player-season for the next-season models (ROS labels differ by week and keep every
    snapshot). Trees: base 0.546 / 0.559 / 0.582 / 0.596 -> 0.549 / 0.558 / 0.580 / 0.597 (a
    wash on a quarter of the rows); all five groups 0.555 / 0.562 / 0.596 / 0.605 -> **0.554 /
    0.577 / 0.606 / 0.607** (+0.015 at week 6, +0.010 at week 9). Stage 2e (ClaudeStage2e, after
    2d): all five with the option on 3.5. The unified model's snapshot rows (item 32) have the
    same shape (every horizon label repeated per snapshot week) and get the same option next.
    **Stage 2 trimmed (15:35, owner: "our scheduled test runs are going to be way off"):** the
    eight 3.5 runs queued through the afternoon (usage; usage + role; + opportunity; consensus;
    all four; all five; all five deduped) would have been ~14 h; the trees had answered the
    single-group questions, so one chain of three remains -- base, all five, all five with the
    dedupe (~4.5 h, after the unified chain). Every queued run uses the one-pass sigma.
    Fuzzing (1 % noise) is not the remedy: it regularises gradient-trained nets, blurs tree
    splits a little, and only degrades an in-context model's signal; what matters is that a
    repeated outcome counts once, and the harness's train-test gap (`gap_h1`) is the detector.

40. **Title odds: the championship lens next to WAR (owner, 2026-10-09: "maximize our
    championship odds, not our win rate ... it does show up as important in leagues where they
    are top heavy").** `machine_learning/src/title.py`: a Monte Carlo of the remaining
    regular-season matchups from each roster's projected lineup points (the roster view the page
    already uses, plus the league's curve offset) and the league's weekly spread, seeded by record
    then points for, Sleeper's default bracket one week per round (4 teams: two rounds; 6: byes
    for the top two, 3v6 / 4v5, then 1 v the 4/5 winner and 2 v the 3/6 winner) -> each team's
    chance of the playoffs, a bye and the title, expected wins and seed; `title_curve` repeats it
    on common random numbers over a grid of weekly-point shifts for one team, which is what a
    player's marginal title odds interpolate on (a player's weekly lineup points = his `m_par_1`
    lineup gain over his projected games, spread over the NFL weeks left). `analysis/title_odds.py`
    builds the season per league from the latest `war/league=<slug>/.../teams.parquet` +
    `meta.json` and Sleeper live (records, points for, every week's matchups, the playoff format)
    and publishes `backtests/title_odds/run_date=<d>/summary.json`: per league, the teams ranked
    by title odds with their curve, each rostered player's d_title / d_playoffs beside his ROS
    wins, the trade targets by what they add in title odds, and a top-heaviness read (the gap
    between the first and second title favourites). The export carries it as `title_odds`; the
    Rosters tab shows it beside WAR (the standings table's playoffs / bye / title columns, the
    selected team's title curve, a title-odds column per rostered player and per trade target,
    the record and odds in the team head). WAR stays the dynasty currency: for seasons two
    through ten the standings are unknown and a win's title value is the same for everyone.
    **First run (2026-10-09, week-4 roster views, 20k seasons):** Stuck in High School is the
    top-heavy one -- Timmy Becker (4-0, 687 pf) 45.6 % title, the owner (2-2) 14.9 %, Piroozi
    13.0 %; Football Guys of Indianapolis -- the owner (4-0) 35.8 %, Ack 20.1 %, Canales 15.0 %;
    Sigma Chi -- Hagen (4-0) 32.6 %, the owner (4-0) 25.1 %, Woody 10.8 %. In Stuck a point a
    week on the owner's lineup is worth about 0.7 points of title odds; Jaxon Smith-Njigba would
    add 7.0 points of title odds to that roster against 0.88 rest-of-season wins. **Next:** rerun
    after each production refresh (the week-5 views land with tonight's export); a title
    objective in the trade builder; the bye and the seed tiebreak by division where a league
    uses one; dynasty-horizon title equity (next season's odds from projected standings) as a
    second lens.

41. **Production switched to TabPFN 3.5 (2026-10-09, week 5).** `weekly_refresh.py --preset
    production` (3.5, the full feature set, stacked, pooled horizons 1-10, quantile bands,
    in-season on 3.5) ran 09:29 -> 19:16 (9 h 47 m: the sigma loop's H-squared predictions,
    since fixed, plus the quantile pass over the ten-horizon frame, fixed 2026-10-10) and wrote
    `inseason/season=2026/week=5/run_date=2026-10-09` (474 players, 345 priced, rank agreement
    with KTC 0.927), the three league roster views, the trade and draft-slot reports; the export
    carries the Market tab, the Season column, the week-5 title odds, team value and the
    performance tab pinned to the 2026-10-07 3.5 market backtest (the production backtest was cut
    for time). Page v46 published 19:20. The Monday-night job is this preset, after the Tuesday
    DAG, with the career tail and sigma cached per season once item 36 lands.

42. **Positional calibration (owner, 2026-10-09 evening: "why is like every WR over rated?").** The
    pooled mispricing column carries a positional lean: tonight's run gives quarterbacks 36-47 %
    of league WAR against the market's 17 % in the roster views, so every receiver reads rich in
    the pooled ranking while the within-position column is flat (+5 to +8 % median at every
    position). On the 2020-22 market-backtest cohorts (364 priced, three-year realized WAR): QB
    delivered 27 % of value (market 27 %, model 33 %), RB 27 % (market 20 %, model 21 %), TE
    12 % (15 %, 13 %), WR 34 % (38 %, 33 %). So the model over-weights quarterbacks by ~6 points
    on a three-year window and more on ten pooled horizons (long QB careers at the page's default
    discount), the market over-weights receivers (and under-24 receivers finished 12-13 ranks
    worse than priced) and under-weights backs. **Next:** (1) a positional-share calibration in
    the harness as a reported metric (projected vs realized share by position per cohort) and as
    an optional correction (scale PAR by position to the realized share on the training
    cohorts; `HorizonModels.calibrate` has the per-position hook); (2) the far-horizon default:
    the page's discount / years against the market's effective horizon (the ten-horizon pooled
    model + a 10 % rate values years 6-10 more than the market does; test 15-20 % or a 5-year
    default on the market backtest); (3) the page: make "vs position" the default sort for the
    Players table's mispricing columns and label the pooled column as cross-position. Advice
    given tonight: buy productive backs and prime (27-29) quarterbacks, sell young hyped
    receivers, pick within position; the title lens is position-agnostic.

43. **Draft rows: NO on the career model (2026-10-09 22:38, paired on the fresh 3.5 set + stacked
    baseline `_c`, 8 cohorts 2015-22, horizon 3).** With one pre-NFL row per drafted player
    (item 30) the top-150 ordering falls 0.620 -> 0.585 (t = -7.3, 0 of 8 cohorts better), the
    wins error rises 0.513 -> 0.553 (t = +4.9, worse in 8 of 8), the top-12 bias deepens -0.04
    -> -0.14 (the stars under-projected more), all-player ordering -0.009. The fit read is
    unchanged (in-sample h1 error 20.2 vs 20.0 points, train-test gap 13.4 vs 13.7), so it is
    not overfitting: the draft rows' sparse features and zero-production targets pull the prior
    for young players down. The hand-made rookie tail stays. The draft rows remain available
    (`--draft-rows`) for the unified / snapshot tests but are off the production path.
    *The market backtest read (01:15, 2020-22 cohorts, 366 priced):* the draft-rows model
    0.699 vs KTC 0.669 against the candidate's 0.695 vs 0.673; mispricing edge 0.399 vs 0.368,
    cheap-side wins per 1,000 KTC 0.258 vs 0.248, error 0.486 vs 0.491, bias 0.114 vs 0.143 --
    a mild plus on the priced pool, against a decisive minus on the harness (eight cohorts,
    2015-22). The co-primary rule is the harness; the market read is the tie-break and there is
    no tie. NO stands; the rookie-tail question goes to the unified model (item 32).

44. **Blend target: ADOPT over the level target on the 3-year window (2026-10-10 00:43, paired
    on the fresh `_c` baseline, 8 cohorts).** Level + residual averaged (item 26's "blend"):
    top-150 ordering 0.621 vs 0.620 (tie, t = +0.2), wins error **0.502 vs 0.513** (t = +3.5,
    better in 7 of 8), all-player ordering 0.626 vs 0.623 (t = +5.2, 8 of 8), top decile +0.010,
    train-test gap 13.0 vs 13.4 points (less overfit; in-sample error 20.6 vs 20.2, i.e. less
    memorisation), top-12 bias flips from -0.04 (stars under) to +0.05 (stars slightly over).
    Co-primary rule: ordering tie, error gain above the line -> ADOPT. Context: the pure residual
    target's 10-07 run (`tabpfn35_set_stacked_res_w`, valid regime) sits at 0.628 / 0.499, so the
    residual alone may be at least as good as the blend; **next:** a fresh residual run under
    the regime stamp paired against the blend (one hour), then the production preset takes
    whichever wins (`--target residual|blend`); the blend's market backtest (01:53, 364 priced, 2020-22):
    blend 0.696 vs KTC 0.673, level 0.695, residual 0.692 -- a tie on ordering; the blend has the
    lowest error (0.488 vs 0.491 / 0.494), the level the best mispricing edge (0.368 vs 0.364 /
    0.339), the residual the heaviest bias (+0.23 vs +0.14 / +0.19); tiers alike. Harness ADOPT +
    market tie -> **the production preset's target is `blend` from 2026-10-10** (one line in
    `weekly_refresh.py`, reversible), pending the residual rematch; the distribution metrics need `--range` on the harness
    runs (the after-prod chain ran without it, so pinball / coverage / skew are null tonight).
