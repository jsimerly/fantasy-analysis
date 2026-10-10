"""export_scorecard: the headline numbers of the performance block become a small card and a markdown
table, in the order the owner reads them (swap test, mispricing, then the supporting three)."""
import importlib.util
from pathlib import Path


def _mod():
    p = Path(__file__).resolve().parents[1] / "scripts" / "export_scorecard.py"
    spec = importlib.util.spec_from_file_location("export_scorecard", p)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _payload():
    return {"run_date": "2026-10-09", "season": 2026, "week": 5, "career_backend": "tabpfn stacked blend",
            "performance": {
                "market": {"meta": {"cohorts": "2020-2022", "run_date": "2026-10-07"},
                           "overall": [{"variant": "v", "horizon": 3, "n": 364, "rho_model": 0.6949, "rho_ktc": 0.6733, "cheap_gap_real": 12.85, "rich_gap_real": -10.64}],
                           "swap_overall": [{"variant": "v", "horizon": 3, "pairs": 66, "win_rate": 0.7273, "gain_mean": 0.463}],
                           "swap_by_cohort": [{"variant": "v", "horizon": 3, "cohort": 2021, "pairs": 17, "win_rate": 0.71, "gain_mean": 0.52},
                                              {"variant": "v", "horizon": 3, "cohort": 2020, "pairs": 11, "win_rate": 0.73, "gain_mean": 0.53}],
                           "swap_by_buy_pos": [{"variant": "v", "horizon": 3, "buy_pos": "RB", "pairs": 19, "win_rate": 0.89, "gain_mean": 1.18},
                                               {"variant": "v", "horizon": 3, "buy_pos": "TE", "pairs": 13, "win_rate": 0.54, "gain_mean": 0.09}],
                           "top_swaps": [{"cohort": 2021, "sell": "A", "sell_pos": "WR", "sell_ktc": 5112, "sell_real": 0.0, "buy": "B", "buy_pos": "RB", "buy_ktc": 4974, "buy_real": 3.16, "gain": 3.16},
                                         {"cohort": 2021, "sell": "C", "sell_pos": "RB", "sell_ktc": 5145, "sell_real": 2.96, "buy": "D", "buy_pos": "QB", "buy_ktc": 5114, "buy_real": 0.28, "gain": -2.68}]},
                "inseason": {"run_date": "2026-10-07", "cohorts": "2021-2024", "ros": [{"W": 3, "ros|model": 0.7836, "ros|ktc": 0.7044}, {"W": 13, "ros|model": 0.6939, "ros|ktc": 0.6522}]},
                "career_eval": {"run_date": "2026-10-01", "start_season": 2010, "mae": [{"horizon": 1, "model_%_vs_carry": 10.7, "model_%_vs_decay": 5.2, "n_total": 8502},
                                                                                         {"horizon": 3, "model_%_vs_carry": 30.2, "model_%_vs_decay": 7.6, "n_total": 7338}]}}}


def test_scorecard_leads_with_the_swap_test_and_the_mispricing():
    m = _mod()
    card = m.scorecard(_payload())
    keys = [h["key"] for h in card["headline"]]
    assert keys == ["swap_test", "mispricing", "rest_of_season", "three_years_out", "career_error"]
    swap, mis, ros, three, err = card["headline"]
    assert swap["value"] == 73 and swap["unit"] == "% won" and "66 swaps" in swap["detail"] and swap["secondary"] == 0.46
    assert mis["value"] == 12.9 and mis["secondary"] == -10.6 and "10.6 worse" in mis["detail"]
    assert ros["value"] == 0.784 and ros["secondary"] == 0.704 and "week 13" in ros["detail"]
    assert three["value"] == 0.695 and three["secondary"] == 0.673 and "364 priced" in three["detail"]
    assert err["value"] == -30 and "7,338 player-seasons since 2010" in err["detail"]        # the three-year row, not the last
    assert card["swaps"][0]["gain"] == 3.16 and card["swaps"][-1]["gain"] == -2.68 and card["swaps_by_cohort"][0]["cohort"] == 2020
    assert {r["position"]: r["win_rate"] for r in card["swaps_by_position_bought"]} == {"RB": 89, "TE": 54}
    assert card["as_of"] == "2026-10-09" and card["week"] == 5


def test_markdown_table_carries_every_headline_and_the_named_swaps():
    m = _mod()
    md = m.to_markdown(m.scorecard(_payload()))
    assert md.startswith("# Model scorecard — as of 2026-10-09 (season 2026, week 5)")
    assert "| The swap test: same-price trades the model would have made | **73 % won** |" in md
    assert "| The mispricing is real | **12.9 ranks better** |" in md
    assert "**-30 % error**" in md
    assert "| 2021 | A, WR (5,112) | 0.00 | B, RB (4,974) | 3.16 | +3.16 |" in md
    assert "Win rate by year: 2020: 73% of 11 · 2021: 71% of 17." in md and "RB 89% (+1.18 wins per swap)" in md


def test_an_empty_performance_block_gives_an_empty_card_not_an_error():
    m = _mod()
    card = m.scorecard({"run_date": "2026-10-09", "performance": {}})
    assert card["headline"] == [] and card["swaps"] == [] and "Model scorecard" in m.to_markdown(card)
