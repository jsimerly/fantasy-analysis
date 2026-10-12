# Does the model help the team? The numbers, in plain words

*Written for a reader of the owner's personal site: what the dynasty model does, how it is tested,
and what it has delivered against the market. Every figure below is from the page's Model performance
tab as of 2026-10-12 (the production model's own market backtest of that date); the tab updates as the model does.*

## The words

- **Rank agreement.** Put the players in the order we projected them, and in the order they actually finished. 1.0 means the same order, 0 means no relation. 0.78 against the market's 0.70 means our order was closer to what happened. (The statistic is Spearman's correlation.)
- **Points above replacement (PAR).** A player's points minus what a freely available player at his position would have scored in the same games: the part of his production you could not have had for nothing.
- **Wins above replacement (WAR).** The same thing turned into wins on this league's own scoring and lineup. The model is tested on this.
- **The market.** KeepTradeCut's dynasty values, the consensus price managers trade on. Measured against, never fed in.
- **Fair value and mispricing.** Fair value is what the market would pay a player if it agreed with our order at his position; mispricing is how far his price sits above (rich) or below (cheap) that.
- **Walk-forward.** To score a past year, the model is rebuilt with only what was known then and compared with what happened years later. No peeking.
- **The swap test.** Two players priced within 15 % of each other that the model disagreed on by at least ten places: sell the one it called rich, buy the one it called cheap, then see who delivered more.

## The question the model answers

Dynasty fantasy football is a market: players and draft picks are traded on a consensus price
(KeepTradeCut, "KTC"). The model asks a different question than the market does. Not *what is this
player worth to the average team*, but **how many wins does he add to this roster, in this league,
this season and in each of the next nine**. It projects every player's games and points per game for
ten seasons ahead, turns the points above a freely available replacement into wins on the league's own
win curve, discounts later seasons, and prices draft picks on the same scale. The market never enters
the projection; it is only the yardstick the model is measured against.

## How it is tested (the part that makes the numbers mean something)

Every result is **walk-forward**. To score the 2020 class of projections, the model is rebuilt using only
what was known at the end of the 2020 season, its projection is written down, and it is compared with
what those players actually delivered in 2021, 2022 and 2023. Eight such test years feed every decision; the market's ranking at the same
moment is scored the same way on the same players.

A change to the model is adopted only when it beats the same-day baseline on **two measures at once**:
how well it orders the 150 players a manager would actually weigh by the wins they went on to deliver,
and how far off it is on those wins, past a statistical line (|t| = 2.4 across the eight years). Every
idea tried, adopted or rejected, is in the project's journal with its numbers.

## What it has delivered

**The headline: the swap test.** Every February from 2021 to 2023, each player the model called rich
was paired with the closest-priced player it called cheap: two players within 15 % of each other on
KTC, the model at least ten places apart on them. That is a trade a manager could have made at zero cost
in market value. Over the three seasons after each swap, **the player bought scored 123 more fantasy
points than the one sold, on average, over the 65 pairs**: 53 more above a replacement-level player at
his position, and ahead in 68 % of the swaps. In the model's own unit, wins, 71 % of swaps won, +0.46
wins each, and every year was positive (80 % in February 2021, 74 % in 2022, 67 % in 2023). Buying a
running back won 84 % of the time: the market under-prices productive backs.

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

**And it is actionable today.** Players the model called cheap against the market went on to finish
**14.4 places better** than the market had them; the ones it called rich finished **10.8 places worse**.
That gap is the Mispricing column on the page's Players tab, within each position: buy the first
group, sell the second.

| Supporting number | Value | What it means |
| --- | --- | --- |
| Rest of this season | **0.784 vs 0.704** rank agreement at week 3, ahead at every checkpoint through week 13 | From a mid-season snapshot, the model orders players by what they will average the rest of the way better than the market's ranking does. |
| Three years out | **0.697 vs 0.675** on 369 priced players, priced each February 2021-2023 | Projected before each season, scored on the wins delivered over the next three, the model's ordering edges the market's. |
| Career projection, three years ahead | **−30 % error vs "repeat last season", −8 % vs an age curve** | Over 7,300 player-seasons since 2010, the season-points projection beats the two naive rules a manager would otherwise use. |

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

- The market test is three years of swaps, 65 of them. It says *ahead*, not *far ahead*, and the honest
  reading of the three-year ordering is "a tie leaning our way".
- The model leans toward quarterbacks when it compares positions (it projects them about six points of
  league value above what they deliver), which is why the page's mispricing column works within
  position and a positional calibration is under test.
- Projections are uncertain and the page shows it: every value carries a 20th-to-80th percentile band.
- The model is trained on the owner's leagues' scoring; another league's values are a rescaling, not a
  new model.

## Under the hood, briefly

The projection model is **TabPFN 3.5**, a transformer pre-trained on synthetic tables that makes
predictions by reading the whole training table at once rather than by fitting weights, run on a
consumer GPU; it replaced gradient-boosted trees after beating them on the same test (wins error 0.474
vs 0.515, eight of eight years). Features are grouped (career history, injuries, in-season trend,
situation changes, draft capital, college production) and each group earned its place through the
same walk-forward test. The data lake behind it ingests the league platform, two value markets, the
NFL's public statistics, provider projections back to 2010 and college production, every day, with a
data-quality suite as the last step.
