# Adversarial review of the model, its evaluation and its public claims — 2026-10-13

*Written for the owner and for whoever next changes the model. The owner asked (2026-10-13): "do an adversarial
review of our code AND model. There should be history about the decisions we made, but that doesn't mean they were
all good or sound ML and stats decisions." This is that review: five independent reads (one by the engineer, four by
reviewers briefed to disagree), read-only, no model trained, every number recomputed from saved artifacts — the
experiment ledger, the saved cohort frames, the seam and in-season backtest tables in `_cache/`, the published market
backtest's `players.parquet`, the lake and the ML bucket as of today. Each finding names its source so it can be
re-derived. CONFIRMED = reproduced from code or data; PLAUSIBLE = the mechanism is in the code, the size is inferred.*

**Status codes:** FIXED = in the commit that carries this document; TONIGHT = applied once the running GPU refresh and
its chain finish (they load the files involved); QUEUED = a code change that needs no decision, in priority order below;
OWNER = a production switch, a lake write or a rule change that is the owner's call.

---

## The short version

The model is real and the walk-forward discipline is sound: no look-ahead was found in the career harness, the targets
have no survivorship, the swap test is not cherry-picked, and the current-season career path (item 51) survives honest
statistics. What does not survive is the *size* of several claims, one evaluation convention, and a handful of defects
that quietly shape what the page shows.

1. **The production store is being overwritten by the wrong model.** A Cloud Run job (`ml-weekly-refresh`, Tuesdays
   15:00 UTC, enabled) runs the refresh with its defaults — trees, `base,career` only, 20 % discount — and writes `war/`
   and `pages/` under the day's run date; it ran today at 11:05. Monday's trees smoke wrote `inseason/` under the same
   date because `--no-write` did not cover `--current`. The GPU refresh writes under a later run date, so this only
   surfaces when the GPU run fails — exactly what happened this morning. FIXED (`--no-write`; the exports refuse a run
   built by a non-production model unless told otherwise, on `feat/review-ml-queue`), OWNER (pause the scheduler or
   make the cloud job a dry run).
2. **Players with no game this season are not on the page at all** — not projected, not on their roster, not priceable
   by the trade finder: 24-40 rostered skill players per league, 86 (league, player) pairs priced ≥ 1,000 KTC, among
   them Josh Jacobs, Zach Charbonnet, Jayden Higgins, Anthony Richardson and every devy quarterback. QUEUED, first.
3. **Our statistics were overstated.** The seam verdict's "t +6.9 over 32 cohort-weeks" is t +4.0 with the season as the
   unit (the weeks of one season share players and outcomes); the five in-season groups' next-season gain is not
   established (t +1.5 on the trees, +2.4 on 3.5 over four cohorts, p ≈ 0.1), only the rest-of-season gain is (t +5.5
   and +7.0 by cohort); the headline "3.5 over the trees, error 0.474 vs
   0.515, t 7.3" is the regime pair item 22 itself withdrew — the valid pair is 0.502 vs 0.530, t +4.8, 8 of 8; the
   harness's wins error is MAE on a target where 30 % of the top 150 realize zero, so it rewards shrinkage, and the blend
   target was adopted on it. FIXED (journal, guide, `scripts/cohort_t.py`), QUEUED (a level metric beside MAE).
4. **Four of the page's six headline numbers mislead.** "+14.4 ranks better" is mostly regression to the mean (a
   pure-noise model scores +10 on that construction); the swap headline is raw points across positions (the model's own
   unit says +0.46 wins against a promised +0.78, and +0.3-0.4 net of resale value on 37 independent swaps); "0.784 vs
   0.704" is the 2026-10-01 trees model against a dynasty price (the fair comparison is +0.03-0.05 over the provider,
   and next season on points above replacement is *behind* KTC at weeks 3-6); "0.697 vs 0.675" is a tie (95 % interval
   −0.02 to +0.06); "the market is never an input" has been false since the five in-season groups shipped on 2026-10-12
   (consensus projections and ADP are inputs; KTC is not). FIXED (page, journal, this document), TONIGHT (scorecard, the
   re-scored market summary).
5. **The dominant systematic error is an age gradient, not the position lean**: under-25s realize 1.22× what they are
   priced at, 29+ 0.93×, rookies 2.3-3.5× at seasons T+2/T+3; the joint pricing under test would push 29+ to 0.80×. The
   market's age error runs the other way, so part of the trade finder's "sell young, buy old" is the model's bias.
   QUEUED (an age-band level gate; the survival curve's form), OWNER (joint pricing stays off until then).
6. **Training-data defects in the core**: the injury group teaches 2002-2008 (24 % of rows) that unlisted means healthy;
   the position filter drops Travis Hunter (listed CB) and every fullback; the status fact stamps the rest of 2026 as
   `not_rostered`; two replacement lines on one page; next season priced with two different spreads. QUEUED, with a
   data-quality check for each.

Nothing in this list changes the direction of a decision already made except the blend target (re-judge under a level
metric) and the five groups' next-season claim (withdrawn; the groups stay for the rest-of-season gain).

---

## A. The page's public claims

### A1. "The mispricing is real: +14.4 ranks better / −10.8 worse" — CONFIRMED, mostly arithmetic
`market_backtest.priced_frame` calls the top third by `ktc_rank − model_rank` "cheap" and scores `ktc_rank − real_rank`
on it. Both sides carry `ktc_rank`: a player the model likes *relative to KTC* is one KTC ranks low, and low ranks regress
toward the middle, so the statistic is positive for any model that disagrees with the market. On the actual 369-row
frame: a pure-noise model scores +10.1 / −10.1 (500 reps; the model's 14.4 sits near the 97th-99th percentile of that
null — real signal, but the number is not the signal); a model that is KTC plus noise at the model's own 0.88-0.90
agreement with KTC scores +3-5 / −3-5. Net of the within-cohort regression of realized rank on KTC rank the cheap third
finished **+11.1** above the line and the rich third **−6.8** below it; on the players we disagreed about the model's rank
was **1.6 ranks closer** to the outcome than KTC's (both about 23 ranks off). `market_backtest.score` now carries
`cheap_net / rich_net / cheap_closer / rich_closer` (FIXED), the page shows the net version when the summary has it,
and the 2026-10-12 production summary is re-scored tonight from its saved frames (TONIGHT).

### A2. The swap headline "+123 points per swap" — CONFIRMED, the wrong unit and an overstated size
- Raw points across positions: 31 of 65 sells are receivers and 17 buys are quarterbacks; QB buys gain +171 raw points
  but +20 above replacement and +0.18 wins (65 % won). In the model's own unit the 65 swaps gained **+53 PAR and +0.46
  wins each** against the **+0.78 the model had promised** (`model_said`, never shown).
- Independence: three Februaries with overlapping three-season windows (2023 is in all three); 30 players appear in
  more than one swap. **Player-disjoint swaps: 37, +0.38 wins, 65 % won.** "Every year was positive" is three correlated
  draws with win rates 80 / 74 / 67 % as n grows 10 / 19 / 36.
- Resale ignored: three years on, the bought side holds **480 KTC less** on average (buy side retains more in 42 % of
  swaps); at the backtest's own 0.20 wins per 1,000 KTC that is ≈ −0.10 wins per swap. Honest size ≈ +0.35 wins.
- Composition: 45 of 65 are "sell under-25, buy 25+" (+0.53, 78 %); the other 20 are +0.28 at 55 %. Most of the gain is
  selling the under-25 premium, which the model's own age bias (C1) partly manufactures. TE sells lose (7 pairs, −0.24);
  "RB buys win 84 %" is 19 pairs across eight slice tables, no multiplicity correction.
- Sound: positive across the whole rule grid (tolerance 0.05-0.30 × gap 5-30: +0.28 to +0.77, 59-80 % won; the chosen
  0.15 / 10 is mid-grid); no survivorship (a missing season is zero wins); KTC dated 15 February T+1 against a model fit
  through season T is fair; the pairing matches the copy.
Honest line: "+53 points above replacement, +0.46 wins per swap (the model had said +0.78), 71 % of 65 mid-market swaps
over three overlapping Februaries; +0.38 and 65 % on the 37 swaps that share no player." FIXED on the page.

### A3. "Rest of this season 0.784 vs 0.704, ahead at every checkpoint" — CONFIRMED, stale model and a straw-man yardstick
`performance.inseason` is the **2026-10-01 trees in-season model with no groups**; the summary lacks the keys the script
has written since. The comparison is Spearman of projected rest-of-season *points per game* against the realized rate,
among KTC-priced players *who played again*, versus **KTC's dynasty value** — a multi-year asset price, not a
rest-of-season ranking. The same summary's own naive blend scores 0.756 / 0.744 / 0.734 / 0.674 against the model's
0.784 / 0.771 / 0.745 / 0.692 (+0.01 to +0.03, not +0.08 to +0.13). On the production model (3.5, five groups,
`_cache/inseason_backtests/par35_all5.parquet`, cohorts 2021-24, weeks 3 / 6 / 9 / 13):

| Measure | Model | Provider's coming-week projection | KTC |
| --- | --- | --- | --- |
| Rest of season, raw ppg ordering | 0.818 / 0.793 / 0.764 / 0.722 | 0.776 / 0.769 / 0.730 / 0.674 | 0.707 / 0.677 / 0.648 / 0.569 |
| Rest of season, PAR ordering | 0.637 / 0.591 / 0.573 / 0.554 | 0.614 / 0.592 / 0.558 / 0.490 | 0.615 / 0.590 / 0.550 / 0.462 |
| Next season, PAR ordering | 0.530 / 0.552 / 0.597 / 0.606 | — | 0.558 / 0.575 / 0.587 / 0.587 |

Rest of season: ahead of the provider by +0.03-0.05 on raw points, +0.02-0.06 on PAR. **Next season: behind KTC at weeks
3 and 6, ahead from week 9**, and the page shows no next-season-versus-market tile at all. The card is rebuilt from this
table (TONIGHT) and worded as "ahead of the provider's own projection, not far ahead; next season ties the market".

### A4. "No market input goes into any of it" / "never fed in" — CONFIRMED false since 2026-10-12
The production preset's `inseason_groups` are usage, role, opportunity, **consensus** (Sleeper / Rotowire / FFToday
projections) and **preseason** (MFL / FFC ADP — a draft-market price). KTC is not an input, so the narrow claim holds;
the broad one does not. At the page's 10 % default, **56 % of the top-150's value** (rest of season 29 %, next season
27 %) comes from this in-season model, which has never been through the market backtest; the swap test, the 0.697 and
the mispricing tile all validate the *career* model (market-free). Reworded on the page and in MODEL_PERFORMANCE
(FIXED): "KTC is never an input; provider projections and ADP are, since 2026-10-12; the swap test validates the
career model". Running the in-season model through the market test is QUEUED.

### A5. "Three years out 0.697 vs 0.675" — CONFIRMED, a tie
Bootstrap over players within cohort (2,000 reps): model − KTC = +0.020, 95 % interval [−0.020, +0.061], P(≤ 0) = 0.17.
Per cohort 0.721 / 0.721, 0.683 / 0.651, 0.690 / 0.663. The naive "last season's ppg" scores 0.586 / 0.650 / 0.610 on
the same players: both the model and the market add 0.05-0.13 over naive; the model's edge over the market is a
fraction of that. The document's caveat ("a tie leaning our way") was right; the tile and scorecard headlined a win.
`performance.value`, which carried the paired-bootstrap interval, is suppressed by the export when older than the
market run, so the only statistical-tie indicator had disappeared. The tile now says "a statistical tie" (FIXED).

### A6. "Career projection, 3 years ahead: −30 % error vs repeating last season" — CONFIRMED, old model, weak baseline
`backtests/career_eval` is run 2026-10-01: `evaluate_career.py` with the default `HorizonModels` (trees, base features),
scored on every career-matrix row with a season not played = 0, so carry-forward is penalised by attrition (its error
grows 38 → 58 points from one to five years out while the model's falls). The honest line is **−8 % vs the age-decay
baseline**, shown second; the scorecard printed "Model: tabpfn stacked blend" above a trees number. The tile now names
the baseline and the run (FIXED); re-running the evaluation on the production model is QUEUED.

### A7. The "Model variants" table leads with a known artifact — CONFIRMED
`performance.experiments` is the leaderboard sorted by ordering; row 1 is the no-spread variant at 0.817, which item 45
diagnosed as a yardstick artifact (46 % of the list zero-projected players who realized zero). Filtered on the page (FIXED).

### A8. Two discount rates on one page, and the picks' label — CONFIRMED
The 2026-10-12 payload carries `discount_rate 0.2` (the preset's 0.1 reached the export only from tonight's run), so the
Rosters tab, "Preseason value" and "wins per 1,000 KTC" were at 20 % while the Players tab and the trade builder were
at 10 %: Josh Allen 3.71 vs 2.96 career wins on the same page. Structurally the roster-tab numbers are frozen at the
export's rate while the slider recomputes only the Players table; the Rosters tab now prints the rate it is at (FIXED).
Picks: `fit_pick_curve` fits `war_disc` (ten seasons discounted from the draft year at the export's rate) and
`pick_table` labels it `wins_undiscounted`; the arithmetic is consistent at the export's own rate, the label is wrong, and
a slider setting other than the export's rate compares players at one rate with picks at another. Relabelled (FIXED);
fitting on undiscounted wins and discounting on the page is QUEUED. The hard-coded "your players run 0.15–0.27 wins per
1,000 KTC" is stale (payload median 0.065) and now reads from the payload (FIXED).

### A9. The caveats omit where the market wins — CONFIRMED
From the payload's own segment tables: under-25s (60 % of the priced pool) **0.656 vs KTC 0.688**, rookies 0.508 vs
0.523, QB ordering 0.611 vs 0.662, TE 0.540 vs 0.546, tier 61-120 0.553 vs 0.565; the edge is ages 25-28 (0.761 vs
0.717), 29-32 (0.828 vs 0.755) and running backs (0.716 vs 0.629). Added to the honesty paragraph and the document
(FIXED). "369 priced players" are player-cohorts, not distinct players (fixed wording).

### A10. Smaller — CONFIRMED
MODEL_PERFORMANCE conflates the 8-cohort harness with the 3-cohort market test (KTC exists from 2020); "0.78 against the
market's 0.70" was hard-coded in The words; the WAR floor-ceiling column equals the point for the two in-season spans
(56 % of value) and sums per-season 20/80 for the tail (a band under perfect correlation); the outcome curve never
renders (no grid in production); title odds are printed to 0.1 % from a simulation with no projection error and a
one-week-stale record; the Market tab's seasonality cells are in-sample pattern mining presented as "systematic"; the
header prints "TabPFN" without "3.5"; the scorecard markdown omits the per-row run dates the JSON carries. Wording
fixed where it is wording (FIXED); the band and the title-odds error model are QUEUED.

---

## B. The evaluation statistics

### B1. Cohort-weeks are not independent — CONFIRMED
The weeks of one test season share players and outcomes (intraclass correlation 0.61 for ordering, 0.92 for PAR
error; design effect 2.8-3.8). Averaging within cohort first:

| Verdict | As reported | Cohort as the unit |
| --- | --- | --- |
| Careers see this season (item 51), ordering T+2/T+3 | +0.066, t +6.92, 29 of 32 cohort-weeks | **+0.066, t +3.99, 7 of 8 cohorts** |
| Same, PAR error | t +5.91 | **t +3.51** |
| Same, with the five groups (`seam_35_is5`) | t +7.2 | t +4.0, 8 of 8 |
| Same on the trees | t +6.9 | t +3.8 |
| Five in-season groups, rest of season on PAR (trees, item 52) | +0.019, t +3.03 | +0.019, t +5.46, 4 of 4 |
| Five in-season groups, next season on PAR (trees) | +0.022, t +2.33 | **+0.022, t +1.51, 3 of 4 — below the 2.4 line** |
| Five in-season groups, 3.5 (`par35_all5` vs `par35_base`) | ros +0.030, t +4.8 (15 of 16); next +0.009, t +0.9 | ros t +7.0, 4 of 4; **next t +2.4 over four cohorts (p ≈ 0.1), not established** |
| Careers see this season with the five groups, trees (`seam_trees_rw1_is5`) | +0.070, t +8.6 | +0.070, t +6.0, 8 of 8 |

The adoptions stand (both to-date verdicts clear 2.4; the five groups on the rest-of-season gain), the "+0.01 next
season" justification is withdrawn, and the to-date gain grows with the week (+0.044 at week 3, +0.103 at week 13; PAR
error at week 3 t 1.6, not significant), so at the production weeks 5-6 expect ≈ +0.045 ordering and no error gain.
Rule added to the ML guide and `scripts/cohort_t.py` computes it from any saved table (FIXED). Note the opposite check
passed: adjacent *cohorts* share 67-73 % of their top-150 players, yet the lag-1 autocorrelation of per-cohort paired
differences across 159 same-regime run pairs is −0.05 / −0.09 — the realized noise cancels in the difference, the
effective n is ≈ 7-8 and the 2.4 line is the ~5 % two-sided line it claims to be.

### B2. The headline "3.5 over the trees" is the void regime pair — CONFIRMED
Item 22's own correction (2026-10-08) voids every pairing of a 0.474-0.484 run (wrong lineup, another WAR scale), and
the 0.515 was TabPFN v2, not the trees (0.530). From the saved per-cohort rows, 8 cohorts: production
`tabpfn35_set_stacked_blend_w2` vs the trees `cap30t_w_b` — **ordering 0.621 vs 0.593 (t +2.56, 7 of 8), wins error
0.502 vs 0.530 (t +4.79, 8 of 8)**; vs trees with the same feature set (`xgb_set_c1`): +0.030 (t +2.64) / −0.028
(t +3.84). The claim survives at 70 % of the stated size. "Feature set + pooled, t +7.5" is the same void pair; in the
valid regime 3.5 set + pooled vs v2 per-horizon tied at three years and won at five on error only. 90 of 115 ledger rows
predate the regime stamp. Status header and MODEL_PERFORMANCE corrected (FIXED).

### B3. Wins error is MAE on a zero-inflated target: it rewards shrinkage — CONFIRMED mechanism
30 % of each cohort's top 150 by projection realize zero WAR over three years. On the production frames predicting zero
for everyone scores MAE 0.708, the median 0.650, the model 0.502. The blend target (item 44: error 0.502 vs 0.513,
t +3.5, ordering tie) has exactly that signature: level target bias on the prior top-12 −0.04 wins / −1.8 points, blend
+0.05 / +3.8, residual +0.14 / +9.3 — the code's bias is realized − projected, so **the residual target under-projects
last year's stars by nine points a season and the blend by four; the level target is the unbiased one**, and items 22
and 44 narrate the sign the other way. Every later value-side fix (band, dist, joint) is partly undoing model-side
shrinkage; the production tail runs ~10 % low on the top 150 before pricing (1.10; RB / WR ~1.20), and `age_gate.py`
shows it is further from level than the trees it replaced on the same features (1.11 vs 1.05, t +4.1 over 8 cohorts;
under-25s 1.30 vs 1.21) — the error gain and the level loss are two faces of one change. The "fixed set"
reading (item 45) was proposed after band sigma failed the standard reading and then used to pass dist sigma too.
FIXED on `feat/review-ml-queue`: every verdict now reads the level (the top-150 bias, A vs B) beside the co-primaries
and appends LEVEL CAUTION when an error gain comes with the candidate's level moving further from zero (test). OWNER:
making the level a hard part of the rule; re-judging level vs blend under it (the cheap candidate is level + joint
pricing).

### B4. Multiplicity and the winner's curse — PLAUSIBLE
114 horizon-3 runs on cohorts 2015-2022 (59 since 2026-10-07), 31 "ADOPT" mentions; a simulated per-candidate false
adopt rate of 4-6 % under the null implies 3-5 false adopts across the programme and a winner's curse on every adopted
size (each baseline is the previous winner). OWNER: two hold-out cohorts never used for tuning (a rule change).

### B5. The page's 10 % default sits on seasons the harness never scores — CONFIRMED arithmetic
On the week-5 production top 150 at 10 %, 28-30 % of discounted value is in seasons 4-10 (31-36 % for under-25s), 11-12 %
in seasons 6-10, which nothing has scored (the seven-season run is queued). The harness is three years at 20 %.
QUEUED: gate on the seven-year windows by age once the h7 run lands; a `season` / years-before-as-of feature (the career
model has no era feature; its context is every row 1999-2024).

### B6. Leaks and timing in the in-season groups — CONFIRMED paths, sizes PLAUSIBLE-small
- `preseason`: every reported run used MFL `PERIOD=ALL`, which keeps accruing in-season drafts (2025: 856 vs 642 at
  August 15), so a week-3 breakout raises his "preseason" ADP in September drafts — the signature of the reported
  week-3 gain. OWNER: re-pull 2011-2026 at AUG15 (item 54), then re-screen the group.
- `consensus`: past seasons hold the provider's *final* week-W+1 number (after the week's injury news), the Tuesday run
  sees Tuesday's. QUEUED: snapshot the Tuesday projection from now on; lag the feature a week in backtests to bound it.
- `adp_next` (next August's ADP on a February row, item 50's "one strong input") is a direct leak; it is correctly out of
  production and is now labelled as such in the journal.
- Production's week-5 projection interpolates between training weeks 3 / 6 / 9 / 13 (item 48's concern). QUEUED for
  next week's run: add the current week to `--inseason-train-weeks` (a flag) until the random-weeks result lands.

---

## C. The modelling decisions

### C1. The age gradient — CONFIRMED
Realized / priced on the top 150 (union set, cohorts 2015-22, `tabpfn35_set_stacked_blend_qg` frames):

| Pricing | under 25 | 25-28 | 29+ | QB | RB | WR | TE |
| --- | --- | --- | --- | --- | --- | --- | --- |
| position sigma | 1.32 | 1.04 | 0.83 | 1.00 | 1.24 | 1.20 | 1.13 |
| band (production) | 1.22 | 1.06 | 0.93 | 1.13 | 1.18 | 1.12 | 1.05 |
| joint 0.5 (item 47) | 1.07 | 0.94 | 0.80 | 0.97 | 1.02 | 1.00 | 0.93 |

Item 47's coupling closes the position gap and opens the age one (29+ 20 % over-priced). The seam test shows the same
gradient at T+2/T+3 on the production path (last-season origin ≤24 2.0 vs 29+ 1.25; rookies 2.3-3.5 on both the table
and the to-date model). Causes are on the projection side: the survival tier held constant over the horizon, a linear
logit on every age 22-40 that cannot bend from a flat hazard at 27 to a steep one at 34 (the "33+ too flat" the author
measured), the rookie tail's ratios clipped at 1.15. FIXED on `feat/review-ml-queue`: `scripts/age_gate.py` — realized /
priced by age band and position on the union top-150 set of one or two runs, with the cohort as the unit (test); the
gate to read beside every verdict and the thing a pricing change must not worsen on either end. On the production run's
saved frames (as-run pricing, 8 cohorts): all 1.10 — under-25s **1.28**, 25-28 1.03, 29+ **0.80**; QB 0.97, RB 1.21,
WR 1.18, TE 1.09. Against the trees on the same features (`xgb_set_c1`) the production 3.5 blend run is *further* from
level everywhere that matters: all 1.11 vs 1.05 (t +4.1 over cohorts), under-25s 1.30 vs 1.21 (t +4.1), RB 1.23 vs 1.14
(t +4.8), WR 1.19 vs 1.08 — the shrinkage of B3 made visible. **The correction, built 2026-10-14 on the owner's
"let's get this done tonight" (on `feat/review-ml-queue`, in the production preset):** `value.age_scale_factors`
multiplies the points above replacement of the career model's spans by the player's age band (≤ 24 / 25-28 / 29+). In
the harness it is fitted walk-forward exactly like the positional scale — realized / priced by band on the cohorts
complete by each test season, on their own top-150, shrunk n / (n + 1), clipped 0.6-1.6 (`--rescore RUN
--walk-age-scale`): ordering unchanged (0.621 = 0.621), wins error within noise (0.507 vs 0.502, t −1.3), bias halved
(0.042 vs 0.059), under-25s 1.285 → 1.178 (t −2.3), 29+ 0.80 → 0.87 (t +2.6), RB 1.21 → 1.15; it only reaches the
cohorts from 2018 (nothing is complete before) and is shrunk, so it closes about 40 % of the gap in the test. Production
takes the table fitted on all eight test seasons with no shrinkage (`experiments.age_scale_table`: ≤ 24 ×1.27, 25-28
×1.03, 29+ ×0.80; by horizon the young side grows, 1.23 / 1.26 / 1.38 for h1 / h2 / h3, the old side 0.80 / 0.76 /
0.85) on the career spans only — the in-season model's own level is not measured, so rest of season and next season
are untouched — in `inseason_value`, the preseason view and `build_war` from the run's `metrics.json`. Not fixed by it:
the cause. **By position (owner, 2026-10-14 afternoon):** realized / priced by position × band on the test seasons —
under 25 every position 1.18-1.31; 25-28 QB 0.86 (over-priced) against RB / WR / TE 1.06-1.11; 29+ QB 0.84, TE 0.88,
WR 0.71, **RB 0.32** (28 player-cohorts, per-season spread 0.47). A position × band table (each cell shrunk toward its
band, `--age-by-position`), walk-forward: ordering 0.620 vs the band table's 0.621, wins error 0.511 vs 0.507 (t −2.2
against the base), RB 29+ unchanged at 0.33, QB 25-28 0.86 → 0.92, WR 29+ 0.71 → 0.81, TE 29+ overshooting to 1.04.
Not adopted: a scale cannot express a cliff in games, and the cells are noisy; the per-position answer is the survival
curve. **A curve and the variance (owner, 2026-10-14 afternoon):** by single year of age the level is smooth (1.3 at
22 to 0.75 at 33, flat after) and a kernel-smoothed curve was built (`--age-scale-curve`), but walk-forward it is no
better than the bands (ordering 0.617 vs 0.621, error t −1.8) — and the season-by-season table explains every "NO"
here: **the level drifts.** Under 25 is stable (1.15-1.26 every season since 2016); 29-31 was 0.46-0.71 in 2015-17 and
about 1.0 since 2018; 32+ was level to 2019 and 0.2-0.5 since. A correction fitted on older seasons over-corrects the
recent 29-31s, the three-band table on the page included. Adopted in the preset instead: four bands fitted to the recent
seasons and each shrunk toward 1 by its own season-to-season variance (≤ 24 ×1.18, 25-28 ×1.06, 29-31 ×0.93, 32+ ×0.80;
`experiments.age_table_recent`), which on the recent seasons leaves the bands at 1.02 / 1.02 / 1.08 / 0.74. It reaches
the page with the next refresh. QUEUED: the spread by age for the uncertainty band (coverage 52-59 % against 60 %, the
young missing high and the old low); the survival curve for the 29-plus side (a logit linear in age applied from 30, fitted on every age,
cannot bend from a flat hazard at 27 to a steep one at 34; the quadratic variant per position and tier,
`tabpfn35_surv_q`, runs in the chain), the rookie tail's ratios clipped at 1.15, and a per-horizon table once the
seven-year windows are in; joint pricing stays off until then (OWNER).

### C2. The band is too narrow to price upside with — CONFIRMED
20-80 coverage 0.55-0.56 at one year (QB 0.52 at three), band sigma 17-22 % below the holdout residual sigma, 9 % of
players at the 1-ppg floor; bands are right-skewed for skill positions and the sigma assumes a normal. The upside term
is a third of value and the missing value sits in the skill-position mid tiers. QUEUED: walk-forward widening per
position × horizon, or the holdout sigma scaled by each player's relative band width.

### C3. `injury_now` hurts the injured on 3.5 — CONFIRMED
`next|parb_injured_model` 0.496 → 0.484 (t −3.0, worse in 10 of 11), the opposite of the trees' +0.02 in item 53. It is
not in the preset; it stays out. Recorded in item 53.

### C4. Two spreads for one span — PLAUSIBLE
`inseason_value` prices rest-of-season and next season with the career h1 holdout sigma; `build_war` prices next season
with sigma[2] — a 5-10 % upside difference between the `iv` column and the WAR column for the same player. Both are
season-end residual spreads applied to mid-season projections that are ~60 % of value; the in-season model's own
quantiles exist behind `--quantile-grid`. FIXED on `feat/review-ml-queue`: the `iv` column prices next season with sigma[2]
as the WAR column does (test); the model's own band for the in-season spans stays QUEUED.

### C5. `estimate_sigma` with pooled horizons sees the holdout players' other-horizon outcomes — PLAUSIBLE
The temporary model fitted as of T−3 trains on every (row, k) with season + k ≤ cut, and a 2019 row trained with k = 3
is scored with k = 4: the same features sit in the context with a neighbouring-year label. Production prices the tail
with the per-player band, so the dict matters for the in-season spans and the fallback. QUEUED: hold out whole rows.

---

## D. Code and data defects

| # | Defect | Where | Status |
| --- | --- | --- | --- |
| D1 | `--no-write` did not cover `--current`: the trees smoke wrote `inseason/…/run_date=2026-10-13` | `backtest_inseason.py` | FIXED |
| D2 | `ml-weekly-refresh` (Cloud Run, Tuesdays 15:00 UTC) writes a trees `base,career` payload at the latest run date | `.github/workflows/deploy-machine-learning.yaml`, the scheduler | OWNER: pause, or run it `--dry-run` |
| D3 | `build_war` / `export_projections` read a run by its run date whatever its backend | both scripts | FIXED on `feat/review-ml-queue`: `runs.check_backend` — both refuse a run whose stamped models are not the production ones (`--require-backend`, default `tabpfn`; `weekly_refresh` passes `any` when its own backends are not production, so the trees, a smoke or the cloud job say so explicitly); tests |
| D4 | Players with no game this season have no row (`to_date_features` emits only players with a stat line): no projection, no roster slot, unpriceable | `inseason.py`, `build_war.py` | FIXED on `feat/review-ml-queue` (merges when the running chain ends): `inseason.no_game_snapshots` gives every player with a career tail and no stat line a zero-game snapshot carrying the preseason projection (rest of season pro-rated to the games left, zero on reserve now; next season h2; `snapshot_origin="no_game_yet"`); `build_war.placeholder_rows` keeps rostered players with no projection at all (devy picks, rookies without an NFL row) on their roster with no wins and the KTC value by name — smoked on the 2026-10-12 run: 27 / 35 / 41 placeholders per league, 24 / 34 / 34 priced |
| D5 | The injury group fills "never on the report" for 2002-2008 (the fact has reserve-only rows from 2002, the report from 2009): 3,646 of 15,267 training rows | `feature_groups.py` `_coverage_fill` | FIXED on `feat/review-ml-queue` (a feature change: merges when the running chain ends; the tail cache re-keys itself): the reserve lists floor at the fact's first season, the report-derived columns and their lags at the first season with a report row, with a replay test. FIXED: the quality check `fact.player_injury_week.report_rows_per_season` |
| D6 | The position filter drops offensive production by players nflverse lists at CB / FB: Travis Hunter has no rows at all, Juszczyk's receiving is gone | `fact_player_week.py` | FIXED (code, tests): the fantasy position is Sleeper's — the primary position, else the first skill position among the positions he may fill (`dim_players_master.fantasy_positions`, "DB/WR" for Hunter) — and only then nflverse's with FB → RB; the facts rebuild on the first daily run after the merge |
| D7 | `fact_player_week_status` stamps weeks 6-18 of 2026 `not_rostered` (11,414 rows); the career `weekly` group would read every 2026 player as cut for 13 weeks | `fact_player_week_status.py` | FIXED (code, tests): the grid ends at the latest week any roster row or stat line carries; the quality check `fact.player_week_status.no_rows_past_production` |
| D8 | Two replacement lines: `backtest_inseason` prices `iv` with the share line (QB 15.4), `build_war` with the league's weekly line (QB 13.5); a QB at 14.5 ppg shows zero PAR and positive WAR in one row; the outcome curves draw against the global line | `export_projections.py`, the page | FIXED on `feat/review-ml-queue`: the outcome curves draw against the chosen league's line (the WAR columns' line); the payload labels the internal share line (`replacement_note`). The `iv` / `v` columns still price on the internal line (QUEUED: price them per league too) |
| D9 | The export and the page key players by name (34 same-name pairs in the fact; none among today's 496, so latent) | `export_projections.py`, the page | QUEUED: key by `player_id` |
| D10 | The scoring league is "the lineage with the most settings rows"; a fourth league would silently re-score the fact | `fact_player_week.py` | FIXED (code, test): the pinned lineage `PRIMARY_LINEAGE_ID`, the same pin as the model's replacement line; the most-rows rule only as a loud fallback |
| D11 | `build_war --source career` indexes `[0]` on in-season leagues: IndexError in the offseason | `build_war.py` | FIXED on `feat/review-ml-queue`: `pick_leagues` takes the in-season leagues, else each lineage's newest season, and stops with a message when the primary lineage is absent (test) |
| D12 | A partially loaded latest week reads as missed games (freshness is checked, completeness is not) | `data_quality/suite.py` | FIXED (check, replay test): `fact.player_week.latest_week_complete` — teams with a stat line in the latest week against 2 × the games played by today in the season's schedule partition |
| D13 | Base features left-censored at 1999 (`career_seasons = 1, exp = 10`); `lag1_*` null for both "sat out" and "before 1999" | `career.py` | QUEUED (minor): a `career_censored` flag |

---

## Checked and found sound

- **Walk-forward discipline.** `HorizonModels.fit(as_of)` keeps only targets with season + k ≤ T (pooled too); sigma from
  a refit as of T−3; survival and rookie tables through T−1; replacement from T−4..T; KTC at 15 February T+1 within 14
  days; the situation group compares T with T−1; the in-season fit keeps rest-of-season rows with season < T and
  next-season rows with season + 1 < T; snapshots use weeks ≤ W; to-date rows' lags and careers come from complete
  seasons only and are never targets; `shift_to_tail` / `merge_todate_tail` horizon arithmetic is right.
- **No survivorship in the targets**: `attach_horizon_targets` marks T+k observable whenever T+k is complete and fills a
  missing row with zero games; the realized side of the swap test counts a missing season as zero wins.
- **Keys and joins**: the season fact unique on (player, season), the week fact on (player, season, week); the market
  joins cannot duplicate; 425 of 425 KTC-priced players map to a gsis id; no player on two rosters in the teams view.
- **The swap rule is not cherry-picked** (positive across the whole tolerance × gap grid) and the pairing, dating and
  realized sums match the copy; the regime stamp guards new pairings.
- **The expected-excess term** is consistent in expectation with the floored realized PAR, and the "no spread" ADOPT was
  correctly diagnosed as a yardstick artifact (reproduced).
- **The cap from age 30 only** (item 24), the weekly lineup-based replacement, the WAR curve with the lineup offset, the
  Vincentised blend quantiles, the leak controls in the consensus bridge, the rejections of draft rows, snapshot
  augmentation, team / contract, schedule, team-change and the provider flag — all on the evidence.
- **The KTC trade calculator** on the page and in `analysis/trades.py` is the recovered `processVNew`; the Trades, Draft
  slots and title-odds mechanisms match their copy.
- **The to-date career path** (item 51) is robust per cohort (t ≈ 4, 7-8 of 8).

---

## What changes, in order

**In this commit (FIXED).** `--no-write` covers `--current`; `market_backtest.score` carries the net-of-regression and
"closer" mispricing scores with tests; `scripts/cohort_t.py`; the ML guide's two rules (the smoke writes nothing and
never runs the refresh on the trees against the bucket; the unit of evidence is the cohort); the page's performance
tiles, words and caveats; the picks label and the payload-driven copy; the leaderboard filter; BACKLOG status header
and items 51-53 corrected, item 55 added; MODEL_PERFORMANCE rewritten with the honest numbers. On the data side (the
GPU refresh never loads it): the fantasy position from Sleeper's `fantasy_positions` (D6, Travis Hunter and the
fullbacks come in), the status grid capped at the latest observed week (D7), the pinned scoring lineage (D10), and
three quality checks — the latest week's completeness against the schedule (D12), injury-report rows per season (D5's
guard), no status rows past production (D7's guard) — each with a replay test; the facts rebuild on the first daily
run after the merge. On the model side, on the branch `feat/review-ml-queue` because the running GPU chain imports
these modules (it merges into the PR branch when the chain ends): D4 (zero-game snapshots for players with a career
tail and no game yet; roster placeholders with the KTC value by name for players with no NFL row), D5's feature floor
per source, C4 (one spread for next season), each with tests, the roster side smoked on the 2026-10-12 run.

**Also done on 2026-10-13, after the first commit.** The scorecard's head rows (with tests); the 2026-10-12 market
summary re-scored in place from its saved frames with the net-of-regression fields (317 prior values identical); the
in-season card's summary rebuilt from the production model's PAR table (`backtests/inseason/run_date=2026-10-13`), so
tonight's export carries both; the exports' backend guard (D3) on `feat/review-ml-queue`. **The night of 2026-10-14
(owner, 00:35: "when I come back in the morning we've addressed some of the larger gaps ... young talent and old
talent"):** the experiment chain is halted the moment the first refresh exits; `feat/review-ml-queue` merges into the
PR branch, the ML guide's smoke runs on the trees, and a second production refresh runs on the merged code (the age-band
correction, the zero-game players, the injury floor, one spread, the backend guard) — the page is republished from
it; the halted experiments restart afterwards under the merged code with a fresh baseline (`tabpfn35_young_base2`) so
every pairing is like for like, with the window report by age and the age gate on each (`chain_night2.sh`). **Outcome:**
none of it ran overnight — the first refresh's career tail from this season computed for ten hours (its usual two and a
half) after a CUDA allocator warning at the start of the run, so the stopper never fired. Killed 09:57; the branch
merged (f2ee8fe, 249 tests), the smoke passed end to end on the trees (701 players projected, 205 zero-game snapshots,
the age correction applied), and the chain relaunched at 09:59 with a six-hour budget on the refresh and a fallback
off the last complete season's tail (`weekly_refresh --no-career-todate`).

**Queued code changes, no decision needed (QUEUED), in priority order.** D9 (player_id keys) → C1's level correction
(an age-band × horizon calibration or the survival form) → C2 (band widening, and the in-season model's own band for
its spans) → D8's second half (the `iv` / `v` columns per league) → D13 → C5 → A6 (re-run the career eval on the
production model) → A4 (the in-season model through the market test) → B6's consensus snapshot. Done on
`feat/review-ml-queue`, merging when the chain ends: D3, D4, D5, D11, C4, D8 (the curves), B3 (the level read in every
verdict), C1's gate (`scripts/age_gate.py`).

**The owner's calls, decided 2026-10-14.** (1) `ml-weekly-refresh` is PAUSED (00:05 EDT) until a better arrangement is
chosen; it no longer writes trees payloads into the production store. (2) MFL 2011-2026 re-pulled at AUG15 the same
night (the lake write; FFC's past seasons rewritten from their frozen preseason windows, FFC 2026 skipped as an
in-season window); re-screening the preseason group on the clean inputs is queued after the running chain. (3) Two
hold-out cohorts: explained, not yet decided — today every decision is tested on the same eight seasons 2015-2022 and
~110 variants have been tried against them; holding two of them back from every tuning decision and reporting them
only when something ships is the clean check that the adopted changes were not luck, at the cost of deciding on six
seasons instead of eight. (4) Agreed: joint pricing stays off until the age-band correction exists, and **the level is
part of the acceptance rule** — an error gain alone does not adopt when the top-150 level moves further from zero by
more than 0.02 wins (`experiments.verdict`, on `feat/review-ml-queue`). (5) The page's default stays at 10 %; the
seven-year windows by age (the h7 run in tonight's chain) become the gate for the seasons the three-year harness never
scores. (6) The owner merges PR #37; `feat/review-ml-queue` follows as its own PR when the chain ends.
