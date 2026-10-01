"""inseason.fill_missing_tail: rookies get a career tail extrapolated from their next-season projection
with the decay of same-position, same-age players who have one; players with a tail are untouched."""
import polars as pl

import inseason


def _frame():
    rows = []
    # 10 veteran RBs aged 22-23 with career tails: h3 = 0.9x next, h4 = 0.5x next (ppg), games 0.9x / 0.6x
    for i in range(10):
        rows.append(dict(player_id=f"v{i}", position="RB", age_at_season=22.5, next_ppg_hat=10.0 + i, next_games_hat=15.0,
                         h3_ppg_hat=0.9 * (10.0 + i), h3_games_hat=13.5, h4_ppg_hat=0.5 * (10.0 + i), h4_games_hat=9.0))
    # a rookie RB (no tail) and a rookie WR with no same-position bucket (falls back to position: none -> stays none)
    rows.append(dict(player_id="rookie", position="RB", age_at_season=21.0, next_ppg_hat=12.0, next_games_hat=16.0,
                     h3_ppg_hat=None, h3_games_hat=None, h4_ppg_hat=None, h4_games_hat=None))
    rows.append(dict(player_id="wr", position="WR", age_at_season=22.0, next_ppg_hat=9.0, next_games_hat=16.0,
                     h3_ppg_hat=None, h3_games_hat=None, h4_ppg_hat=None, h4_games_hat=None))
    return pl.DataFrame(rows)


def test_rookie_tail_from_bucket_ratios_and_veterans_unchanged():
    out = inseason.fill_missing_tail(_frame(), [1, 2, 3, 4])
    r = out.filter(pl.col("player_id") == "rookie").row(0, named=True)
    assert r["tail_source"] == "extrapolated"
    assert abs(r["h3_ppg_hat"] - 0.9 * 12.0) < 1e-9 and abs(r["h4_ppg_hat"] - 0.5 * 12.0) < 1e-9
    assert abs(r["h3_games_hat"] - 16.0 * 0.9) < 1e-9 and abs(r["h4_games_hat"] - 16.0 * 0.6) < 1e-9
    v = out.filter(pl.col("player_id") == "v3").row(0, named=True)
    assert v["tail_source"] == "career" and abs(v["h3_ppg_hat"] - 0.9 * 13.0) < 1e-9
    w = out.filter(pl.col("player_id") == "wr").row(0, named=True)
    assert w["tail_source"] == "none" and w["h3_ppg_hat"] is None            # no WR with a tail anywhere


def test_inseason_value_uses_the_filled_tail():
    snaps = pl.DataFrame({"player_id": ["rookie", "v0"], "position": ["RB", "RB"], "age_at_season": [21.0, 23.0],
                          "ros_ppg_hat": [12.0, 12.0], "ros_games_hat": [10.0, 10.0], "next_ppg_hat": [12.0, 12.0], "next_games_hat": [16.0, 16.0]})
    career_pred = pl.DataFrame({"player_id": ["v0"] + [f"x{i}" for i in range(8)], "h3_ppg_hat": [11.0] * 9, "h3_games_hat": [14.0] * 9})
    # the extra x players carry tails but are not in snaps: ratios come only from rows present in the frame,
    # so add them to snaps too
    extra = pl.DataFrame({"player_id": [f"x{i}" for i in range(8)], "position": ["RB"] * 8, "age_at_season": [23.0] * 8,
                          "ros_ppg_hat": [12.0] * 8, "ros_games_hat": [10.0] * 8, "next_ppg_hat": [12.0] * 8, "next_games_hat": [16.0] * 8})
    out = inseason.inseason_value(pl.concat([snaps, extra]), career_pred, {"RB": 8.0}, None, [1, 2, 3], discount_rate=0.5)
    rk = out.filter(pl.col("player_id") == "rookie").row(0, named=True)
    v0 = out.filter(pl.col("player_id") == "v0").row(0, named=True)
    assert rk["tail_source"] == "extrapolated" and rk["h3_vorp_hat"] > 0
    assert abs(rk["iv_inseason"] - v0["iv_inseason"]) < 1e-9                  # same projections -> same value, tail or not
