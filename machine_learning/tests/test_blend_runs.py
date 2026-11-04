"""blend_runs: two runs' saved cohort frames blended and the weight chosen walk-forward (owner, 2026-10-25)."""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import blend_runs as br  # noqa: E402


def _run(cohorts, war_fn, n=40):
    rows = []
    for T in cohorts:
        for i in range(n):
            real = float(i)                                     # the outcome: player i realizes i wins
            rows.append({"cohort": T, "player_id": f"p{i}", "position": "WR", "realized_iv": real * 10, "realized_war": real,
                         "war": war_fn(T, i, real), "iv": war_fn(T, i, real) * 10, "par": war_fn(T, i, real) * 10,
                         "h1_fpts_hat": war_fn(T, i, real) * 100, "h3_ppg_q20": 1.0, "ktc_value": None})
    return pl.DataFrame(rows)


def test_the_shared_projected_columns_are_blended_and_the_rest_stay_a():
    a = _run([2015], lambda T, i, r: r + 2.0)
    b = _run([2015], lambda T, i, r: r - 2.0)
    assert set(br.blend_columns(a, b)) == {"war", "iv", "par", "h1_fpts_hat"}          # not the band, not the outcomes
    half = br.blend_frames(a, b, lambda T: 0.5)[0]
    assert half["war"].to_list() == pytest.approx(half["realized_war"].to_list())          # the errors cancel at 0.5
    assert half["h3_ppg_q20"].to_list() == a["h3_ppg_q20"].to_list() and half["blend_w"][0] == 0.5
    assert br.blend_frames(a, b, lambda T: 1.0)[0]["war"].to_list() == a["war"].to_list()
    assert br.blend_frames(a, b, lambda T: 0.0)[0]["war"].to_list() == b["war"].to_list()
    missing = br.blend_frames(a, b.filter(pl.col("player_id") != "p3"), lambda T: 0.5)[0]   # a player B lacks keeps A's value
    assert missing.filter(pl.col("player_id") == "p3")["war"][0] == a.filter(pl.col("player_id") == "p3")["war"][0]


def test_the_walk_forward_weight_is_chosen_on_the_complete_cohorts_only():
    # A is biased +2 everywhere; B is perfect on the early cohorts, awful on the last one -- a weight that peeked at the
    # last cohort would stay with A there; the walk-forward weight for it sees only the complete (early) cohorts
    a = _run([2015, 2016, 2019], lambda T, i, r: r + 2.0)
    b = _run([2015, 2016, 2019], lambda T, i, r: r if T < 2019 else r + 20.0)
    assert br.walk_weight(a, b, 2015, horizon=3) == 1.0                                 # nothing complete by 2015: A alone
    assert br.walk_weight(a, b, 2019, horizon=3, metric="mae") == 0.0                   # 2015 and 2016 complete: B was perfect
    assert br.walk_weight(a, b, 2019, horizon=3, metric="spearman") == 1.0              # on ordering they tie, the tie keeps A
    assert br.top_metrics(br.blend_frames(a, b, lambda T: 1.0)[0]) == pytest.approx((1.0, 2.0))


def test_the_market_map_orders_the_priced_players_by_the_blended_rank():
    # the model orders the outcome perfectly, the market orders it backwards: the map must fall from 1 at w=1 to -1 at w=0
    a = _run([2020], lambda T, i, r: r).with_columns(pl.Series("ktc_value", [float(40 - i) for i in range(40)]))
    m = br.market_map(a)
    assert m["cohorts"].to_list() == [1] * 11 and m["players"][0] == 40
    by_w = dict(zip(m["w_on_model"].to_list(), m["spearman_value"].to_list()))
    assert by_w[1.0] == pytest.approx(1.0) and by_w[0.0] == pytest.approx(-1.0)
    assert by_w[0.8] == pytest.approx(1.0) and by_w[0.2] == pytest.approx(-1.0)     # the weight decides which ordering wins (at 0.5 they cancel)
    assert br.market_map(_run([2020], lambda T, i, r: r)).height == 0                   # nothing priced: no map


def test_identical_runs_get_t_zero_not_infinity_so_the_verdict_is_no():
    import experiments as ex
    assert ex.t_stat(0.0, 0.0) == 0.0 and ex.t_stat(0.3, 0.0) == float("inf") and ex.t_stat(-0.3, 0.0) == float("-inf")
    assert ex.t_stat(0.2, 0.1) == pytest.approx(2.0)
    table = pl.DataFrame([{"metric": m, "a": 0.6, "b": 0.6, "diff_b_minus_a": 0.0, "se": 0.0, "t": 0.0, "b_wins": 0, "cohorts": 8} for m in ("spearman_war_top", "mae_war_top")])
    assert ex.verdict(table).startswith("co-primary verdict (A = candidate vs B = baseline): NO")
