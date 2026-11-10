# Does the model help the team? The numbers, in plain words

*Written for a reader of the owner's personal site: what the dynasty model does, how it is tested,
and what it has delivered against the market. The figures are from the production model's own market backtest of
2026-10-12 and its in-season backtest of 2026-10-13, as corrected by the adversarial review of 2026-10-13
([MODEL_REVIEW_2026-10-13.md](MODEL_REVIEW_2026-10-13.md)); the page's Model performance tab updates as the model does.*

## The words

- **Rank agreement.** Put the players in the order we projected them, and in the order they actually finished. 1.0 means the same order, 0 means no relation. 0.70 against a yardstick's 0.65 means our order was closer to what happened; a few hundredths is inside the noise of three test years. (The statistic is Spearman's correlation.)
- **Points above replacement (PAR).** A player's points minus what a freely available player at his position would have scored in the same games: the part of his production you could not have had for nothing.
- **Wins above replacement (WAR).** The same thing turned into wins on this league's own scoring and lineup. The model is tested on this.
- **The market.** KeepTradeCut's dynasty values, the consensus price managers trade on. Measured against, never fed in. Two market-adjacent inputs do enter the *in-season* model since October 2026: the providers' weekly projections and preseason draft position. The career model, the one the swap test validates, reads neither.
- **Fair value and mispricing.** Fair value is what the market would pay a player if it agreed with our order at his position; mispricing is how far his price sits above (rich) or below (cheap) that.
- **Walk-forward.** To score a past year, the model is rebuilt with only what was known then and compared with what happened years later. No peeking.
- **The swap test.** Two players priced within 15 % of each other that the model disagreed on by at least ten places: sell the one it called rich, buy the one it called cheap, then see who delivered more.

## The question the model answers

Dynasty fantasy football is a market: players and draft picks are traded on a consensus price
(KeepTradeCut, "KTC"). The model asks a different question than the market does. Not *what is this
player worth to the average team*, but **how many wins does he add to this roster, in this league,
this season and in each of the next nine**. It projects every player's games and points per game for
ten seasons ahead, turns the points above a freely available replacement into wins on the league's own
win curve, discounts later seasons, and prices draft picks on the same scale. KTC never enters the
projection; it is only the yardstick the model is measured against.

## How it is tested (the part that makes the numbers mean something)

Every result is **walk-forward**. To score the 2020 class of projections, the model is rebuilt using only
what was known at the end of the 2020 season, its projection is written down, and it is compared with
what those players actually delivered in 2021, 2022 and 2023. Eight such test years (2015-2022) feed every
modelling decision. The comparison with the market is narrower: KTC's history starts in 2020, so the market
is scored on three Februaries (2021-2023) and 369 player-cohorts.

A change to the model is adopted only when it beats the same-day baseline on **two measures at once**:
how well it orders the 150 players a manager would actually weigh by the wins they went on to deliver,
and how far off it is on those wins, past a statistical line (|t| = 2.4 across the eight years, with the
test year as the unit of evidence). Every idea tried, adopted or rejected, is in the project's journal with
its numbers, and the review of 2026-10-13 re-derived the ones below.

## What it has delivered

**The headline: the swap test.** Every February from 2021 to 2023, each player the model called rich
was paired with the closest-priced player it called cheap: two players within 15 % of each other on
KTC, the model at least ten places apart on them. That is a trade a manager could have made at zero cost
in market value. Over the three seasons after each swap, **the player bought delivered 53 more points above
a replacement-level player at his position than the one sold, on average, over the 65 pairs**, and
**+0.46 wins in this league's own unit, winning 71 % of the swaps** (the model had said +0.78). The raw
fantasy-point gain was 123 per swap, but that number mixes positions: half the sells were receivers and a
quarter of the buys quarterbacks, who score more raw points by construction.

Two honest reductions. The three windows overlap (2023 is in all of them) and 30 players appear in more than
one swap; on the **37 swaps that share no player the gain is +0.38 wins, won 65 %**. And three years on, the
bought side holds about 480 KTC less of resale value on average, worth roughly −0.1 wins at the backtest's
own rate; net of that the gain is about +0.35 wins per swap. Most of the gain comes from one direction, selling
an under-25 and buying a 25-plus (45 of the 65 swaps, +0.53 wins, 78 % won; the other 20 swaps +0.28, 55 %),
which is also where the model's own bias runs (see the caveats).

| Swap (Feb.) | Sold (KTC) | Points, next 3 seasons | Bought (KTC) | Points, next 3 seasons | Gain |
| --- | --- | --- | --- | --- | --- |
| 2022 | Elijah Moore, WR (5,112) | 263 | Derrick Henry, RB (4,974) | 846 | +583 points (+3.2 wins) |
| 2023 | AJ Dillon, RB (3,634) | 114 | Derrick Henry, RB (3,955) | 832 | +717 (+2.9) |
| 2021 | D'Andre Swift, RB (7,653) | 525 | Travis Kelce, TE (6,924) | 798 | +274 (+1.9) |
| 2022 | Terry McLaurin, WR (5,019) | 581 | Travis Kelce, TE (4,900) | 731 | +150 (+1.8) |
| 2023 | Tyquan Thornton, WR (2,167) | 99 | Baker Mayfield, QB (1,940) | 949 | +850 (+1.7) |

Points are this league's scoring over the three seasons after the swap. About three in ten swaps went
the other way (the worst: Saquon Barkley for Matthew Stafford in February 2022, −2.7 wins), which is
what a 71 % win rate means.

**And it is on the page today, with a smaller number than first claimed.** Players the model called cheap
against the market finished **11 places better than a player at their market rank usually does**; the ones
it called rich finished 7 places worse; on the players we disagreed about, our rank sat 1.6 places closer
to the finish than the market's. The page used to say 14.4 and 10.8: those were the raw gaps, and most of a
raw gap is regression to the mean (a model with no skill at all scores about +10 on that construction,
because disagreeing with the market selects players who regress toward the middle). That signal is the
Mispricing column on the page's Players tab, within each position.

| Supporting number | Value | What it means |
| --- | --- | --- |
| Rest of this season | **0.64 vs the provider's projection 0.61** (points above replacement, week 3); 0.55 vs 0.49 at week 13 | From a mid-season snapshot the model orders players by what they deliver the rest of the way a little better than the providers' own weekly projection does, at every checkpoint. On raw points per game the same comparison is 0.82 vs 0.78. A dynasty price is not a rest-of-season ranking, so KTC (0.62 / 0.46) is not the fair yardstick here. |
| Next season, from mid-season | **0.53 vs the market's 0.56 at week 3; 0.60 vs 0.59 at week 13** | Behind the market early in the season, level by week 9, slightly ahead late. The players hurt right now are where the market knows more than the model. |
| Three years out | **0.697 vs 0.675** on 369 player-cohorts, priced each February 2021-2023 — a statistical tie | Projected before each season, scored on the wins delivered over the next three: the model's ordering edges the market's by less than its own noise (a 95 % interval of about −0.02 to +0.06). The market orders under-25s and rookies better; the model, ages 25-32 and running backs. |
| Career projection, three years ahead | **−8 % error vs an age-decay curve** (−30 % vs "repeat last season", a baseline attrition punishes) | Over 7,300 player-seasons since 2010, seasons never played counted as zero. This evaluation is of the tree walk-forward of 2026-10-01, not yet re-run on the production model. |

The living version of these numbers is [model_scorecard.md](model_scorecard.md), regenerated with
every weekly refresh.

## What it changes in practice

- **Trades are priced in wins, within position.** The page shows where each player's market price sits
  against the model's value among players of his position, so a swap is judged on wins added to this
  roster rather than on a value chart.
- **Packages are built by the trade finder**, which searches two-for-ones and pick-plus-player deals that
  raise this roster's wins now and later, and prices every package the way KTC's own calculator would,
  so the other side sees a fair offer.
- **Championships, not just wins.** A Monte Carlo of the rest of the season gives each roster its odds
  of the playoffs, a bye and the title, and each player's marginal title odds, because in a top-heavy
  league the right wins matter more than the number of wins. A second lens does the same for next
  season from projected lineups and the picks each roster holds.
- **Picks are on the same scale as players**, priced as the expected wins of the player taken at that
  slot, so a pick-for-player trade is one comparison, not two charts.
- **It refreshes weekly** after the Monday games, so the rest-of-season view moves with the season.

## The caveats, stated

- The market test is three overlapping years and 65 swaps, 37 of them independent. It says *ahead*, not
  *far ahead*, and the three-year ordering is a statistical tie.
- The market is better than the model on young players: under-25s (60 % of the priced pool) 0.66 vs 0.69,
  rookies 0.51 vs 0.52, quarterbacks 0.61 vs 0.66. The model's edge is ages 25-32 and running backs.
- The model under-prices young players and over-prices old ones: the realized-to-projected ratio runs from
  about 1.2 under 25 to 0.9 past 29, and 2-3 for rookies two and three seasons out. Part of its "sell young,
  buy old" is therefore its own bias, and a correction is queued before any further pricing change.
- It leans toward quarterbacks when it compares positions, which is why the page's mispricing column
  works within position.
- More than a quarter of a player's value at the page's default 10 % discount sits in seasons four to ten,
  which the three-year test never scores; a seven-year test is queued.
- The rest-of-season and next-season spans, more than half of today's value, are validated against the
  providers' projections and the market, not through the swap test, and the in-season model does read the
  providers' projections and preseason draft position.
- Projections are uncertain and the page shows it: every value carries a 20th-to-80th percentile band. The
  band is narrower than the outcomes (it covers about 55 % of them, not 60 %), and widening it is queued.
- The model is trained on the owner's leagues' scoring; another league's values are a rescaling, not a
  new model.

## Under the hood, briefly

The projection model is **TabPFN 3.5**, a transformer pre-trained on synthetic tables that makes
predictions by reading the whole training table at once rather than by fitting weights, run on a
consumer GPU; it replaced gradient-boosted trees after beating them on the same test (wins error 0.502
vs 0.530, eight of eight years; ordering 0.62 vs 0.59, seven of eight). Features are grouped (career
history, injuries, in-season trend, situation changes, draft capital, college production; in season also
usage, role, opportunity, the providers' consensus and preseason draft position) and each group earned its
place through the same walk-forward test. The data lake behind it ingests the league platform, two value
markets, the NFL's public statistics, provider projections back to 2010 and college production, every day,
with a data-quality suite as the last step.
