"""scoring: per-league ppg scale from re-scoring real weeks under two rule sets."""
import polars as pl

import scoring

A = {"pass_yd": 0.04, "pass_td": 4, "pass_int": -1, "rush_yd": 0.1, "rush_td": 6, "rec": 0.5, "rec_yd": 0.1, "rec_td": 6, "bonus_rec_te": 0.5}
B = {**A, "pass_int": -2}
TEP = {**A, "bonus_rec_te": 1.0}


def _weeks():
    rows = []
    for wk in range(1, 11):
        rows.append(dict(player_id="qb", position="QB", season=2025, week=wk, pass_yds=250, pass_tds=2, pass_int=1, rush_yds=10, rush_tds=0, rec=0, rec_yds=0, rec_tds=0))
        rows.append(dict(player_id="wr", position="WR", season=2025, week=wk, pass_yds=0, pass_tds=0, pass_int=0, rush_yds=0, rush_tds=0, rec=5, rec_yds=70, rec_tds=0))
        rows.append(dict(player_id="te", position="TE", season=2025, week=wk, pass_yds=0, pass_tds=0, pass_int=0, rush_yds=0, rush_tds=0, rec=4, rec_yds=40, rec_tds=0))
    rows.append(dict(player_id="tiny", position="RB", season=2025, week=1, pass_yds=0, pass_tds=0, pass_int=0, rush_yds=5, rush_tds=0, rec=0, rec_yds=0, rec_tds=0))
    return pl.DataFrame(rows)


def test_interception_penalty_scales_only_quarterbacks():
    s = scoring.ppg_scale(_weeks(), A, B).sort("player_id")
    d = dict(zip(s["player_id"], s["scale"]))
    qb_a = 0.04 * 250 + 8 - 1 + 1.0
    assert abs(d["qb"] - (qb_a - 1) / qb_a) < 1e-9 and d["wr"] == 1.0 and d["te"] == 1.0
    assert d["tiny"] == 1.0                                     # too few points to form a ratio


def test_te_premium_scales_tight_ends_only():
    s = scoring.ppg_scale(_weeks(), A, TEP)
    d = dict(zip(s["player_id"], s["scale"]))
    te_a = 0.5 * 4 + 4 + 0.5 * 4
    assert abs(d["te"] - (te_a + 2) / te_a) < 1e-9 and d["wr"] == 1.0 and d["qb"] == 1.0


def test_apply_scale_and_diff():
    proj = pl.DataFrame({"player_id": ["qb", "new"], "ros_ppg_hat": [20.0, 10.0], "h3_ppg_hat": [18.0, 9.0], "other": [1.0, 1.0]})
    out = scoring.apply_scale(proj, pl.DataFrame({"player_id": ["qb"], "scale": [0.9]}), ["ros_ppg_hat", "h3_ppg_hat", "missing"])
    assert out["ros_ppg_hat"].to_list() == [18.0, 10.0] and out["h3_ppg_hat"].to_list() == [16.2, 9.0] and "scale" not in out.columns
    assert scoring.scoring_diff(A, B) == {"pass_int": (-1, -2)}


def test_league_scoring_reads_current_row_with_nulls_as_zero():
    st = pl.DataFrame({"league_id": ["L", "L"], "is_current": [False, True], "valid_from": ["2025-01-01", "2026-01-01"],
                       "pass_yd": [0.04, 0.04], "pass_td": [4.0, 4.0], "pass_int": [-1.0, -2.0], "rush_yd": [0.1, 0.1], "rush_td": [6.0, 6.0],
                       "rec": [0.5, 0.5], "rec_yd": [0.1, 0.1], "rec_td": [6.0, 6.0], "bonus_rec_te": [0.5, None]})
    sc = scoring.league_scoring(st, "L")
    assert sc["pass_int"] == -2.0 and sc["bonus_rec_te"] == 0.0 and sc["bonus_rec_wr"] == 0.0
