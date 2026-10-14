# Model scorecard — as of 2026-10-14 (season 2026, week 5)

*walk-forward: the model rebuilt with only what was known at the time, scored on what the players then delivered; KTC is the yardstick, never an input (the in-season model does read the providers' projections and preseason ADP since 2026-10-12; the career model the swap test validates reads neither); the unit of evidence is the test year, and the review of 2026-10-13 (docs/MODEL_REVIEW_2026-10-13.md) re-derived every number here.* Model: tabpfn stacked blend [base,career,injury,trend,situation,rookie,college].

| Headline | Value | Context |
| --- | --- | --- |
| The swap test: same-price trades the model would have made | **53 more points above replacement per swap over three seasons** | 65 swaps made each February 2021-2023: three seasons later the player bought delivered that much more above a replacement-level player at his position, ahead in 68% of swaps; in wins +0.46 per swap, 71% won (the model had said +0.78); 123 raw points, a number that mixes positions |
| The mispricing column, net of regression to the mean | **11.2 ranks better than their market rank predicts** | players the model called cheap finished that many ranks better than a player at their market rank usually does, the ones it called rich 6.8 worse; on the disagreed players the model's rank sat 1.6 ranks closer to the finish than the market's; the raw gap (+14.4 / -10.8) is inflated by regression to the mean (a model with no skill scores about +10 on it) |
| Rest of this season: ordering players by the points above replacement they go on to deliver | **0.637 rank agreement** | vs the provider's own projection 0.614 and the market's 0.615 at week 3; 0.554 vs 0.49 / 0.462 at week 13; cohorts 2021-2024; a dynasty price is not a rest-of-season ranking, so the provider's projection is the yardstick that counts |
| Next season, from a mid-season snapshot: ordering by points above replacement | **0.53 rank agreement** | vs the market's 0.558 at week 3; 0.606 vs 0.587 at week 13: behind or level with the market early in the season, ahead from week 9 |
| 3 years out: ordering by the wins players then delivered (a statistical tie) | **0.697 rank agreement** | vs the market's 0.675 on 369 priced player-cohorts, priced each February 2021-2023; the difference sits inside its noise (a 95 % interval of about −0.02 to +0.06); the market is better on under-25s and rookies, the model on ages 25-32 and running backs |
| Career projection, 3 years ahead: error vs an age-decay curve | **-8 % error** | −30% vs repeating last season (a baseline attrition punishes); 7,338 player-seasons since 2010, seasons never played counted as zero; the 2026-10-01 evaluation (the tree walk-forward, not the production model) |

**The words.** *Rank agreement*: put the players in the order we projected them and in the order they actually finished; 1.0 is the same order, 0 is no relation (Spearman). *Points above replacement*: a player's points minus what a freely available player at his position would have scored in the same games. *Wins above replacement*: those points turned into wins on this league's scoring and lineup, the number the model is tested on. *The market*: KeepTradeCut's dynasty values, the yardstick, never an input. *Walk-forward*: a past year is scored by rebuilding the model with only what was known then.

## The swaps, by name

Points are what each player scored over the three seasons after the swap, in this league's scoring; "above replacement" is the part a freely available player at his position would not have given you; wins are the model's own unit.

| Swap (Feb.) | Sold (KTC) | Points | Above repl. | Bought (KTC) | Points | Above repl. | Gain (points) | Gain (wins) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2022 | Elijah Moore, WR (5,112) | 263 | 0 | Derrick Henry, RB (4,974) | 846 | 390 | +583 | +3.16 |
| 2023 | AJ Dillon, RB (3,634) | 114 | 0 | Derrick Henry, RB (3,955) | 832 | 363 | +717 | +2.91 |
| 2021 | D'Andre Swift, RB (7,653) | 525 | 130 | Travis Kelce, TE (6,924) | 798 | 362 | +274 | +1.91 |
| 2022 | Terry McLaurin, WR (5,019) | 581 | 111 | Travis Kelce, TE (4,900) | 731 | 301 | +150 | +1.83 |
| 2023 | Tyquan Thornton, WR (2,167) | 99 | 0 | Baker Mayfield, QB (1,940) | 949 | 209 | +850 | +1.71 |
| 2023 | Kyle Pitts, TE (5,476) | 479 | 57 | Tyreek Hill, WR (5,393) | 538 | 194 | +58 | +1.53 |
| 2023 | Christian Watson, WR (4,595) | 293 | 26 | Jared Goff, QB (4,061) | 942 | 202 | +649 | +1.48 |
| 2023 | Romeo Doubs, WR (2,813) | 392 | 0 | James Conner, RB (2,832) | 448 | 153 | +56 | +1.39 |

Win rate by year of the swap: February 2021: 80% of 10 · February 2022: 74% of 19 · February 2023: 67% of 36.

By the position bought: RB 84% (+0.87 wins per swap) · WR 69% (+0.33 wins per swap) · QB 65% (+0.18 wins per swap) · TE 62% (+0.38 wins per swap).
