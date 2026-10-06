"""inseason.rookie_tail_table / fill_missing_tail(rookie_table=...): a rookie's tail follows the realized
trajectories of past rookies of his position and projected tier; the table is leak-safe through a season;
thin groups fall back to the starter+mid pool, then the position; non-rookies keep the projection medians."""
import polars as pl

import inseason


def _fact():
    """Rookie classes 2010-2013: per class 12 RB starters (14 games, 15 ppg in year 1; 12 games, 14 ppg in
    year 2; 10 games, 13 ppg in year 3), 12 RB fringe (year 1: 4 games, 4 ppg; year 2: 2 games; year 3: gone),
    and 3 RB mid (too few on their own)."""
    rows = []
    for yr in (2010, 2011, 2012, 2013):
        for i in range(12):
            pid = f"s{yr}{i}"
            rows += [dict(player_id=pid, position="RB", season=yr, games=14, ppg=15.0, is_rookie=True, season_complete=True),
                     dict(player_id=pid, position="RB", season=yr + 1, games=12, ppg=14.0, is_rookie=False, season_complete=True),
                     dict(player_id=pid, position="RB", season=yr + 2, games=10, ppg=13.0, is_rookie=False, season_complete=True)]
        for i in range(12):
            pid = f"f{yr}{i}"
            rows += [dict(player_id=pid, position="RB", season=yr, games=4, ppg=4.0, is_rookie=True, season_complete=True),
                     dict(player_id=pid, position="RB", season=yr + 1, games=2, ppg=4.0, is_rookie=False, season_complete=True)]
        for i in range(3):
            pid = f"m{yr}{i}"
            rows += [dict(player_id=pid, position="RB", season=yr, games=10, ppg=9.0, is_rookie=True, season_complete=True),
                     dict(player_id=pid, position="RB", season=yr + 1, games=8, ppg=9.0, is_rookie=False, season_complete=True),
                     dict(player_id=pid, position="RB", season=yr + 2, games=8, ppg=9.0, is_rookie=False, season_complete=True)]
    return pl.DataFrame(rows)


def test_table_ratios_tiers_and_fallbacks():
    t = inseason.rookie_tail_table(_fact(), [1, 2, 3, 4], through=2015, first_season=2010, min_n=10)
    st = t.filter((pl.col("position") == "RB") & (pl.col("_tier") == "starter")).row(0, named=True)
    assert abs(st["_rg3"] - 10 / 12) < 1e-9 and abs(st["_rp3"] - 13 / 14) < 1e-9       # year +2 over year +1
    assert st["_rg4"] == 0.0                                                               # gone by year +3 (absent = 0 games)
    fr = t.filter((pl.col("position") == "RB") & (pl.col("_tier") == "fringe")).row(0, named=True)
    assert fr["_rg3"] == 0.0                                                               # fringe rookies vanish after year +1
    assert fr["_rp3"] is None                                                              # no survivor carries a rate
    md = t.filter((pl.col("position") == "RB") & (pl.col("_tier") == "mid")).row(0, named=True)
    assert md["_rg3"] == 1.0                                                               # 12 mid rookies (3 per class x 4) clear min_n
    reg = t.filter((pl.col("position") == "RB") & (pl.col("_tier") == "regular")).row(0, named=True)
    assert 10 / 12 < reg["_rg3"] < 1.0                                                     # starter + mid pooled
    assert t.filter(pl.col("_tier") == "__all__").height == 1
    thin = inseason.rookie_tail_table(_fact(), [1, 2, 3, 4], through=2015, first_season=2010, min_n=15)
    assert thin.filter(pl.col("_tier") == "mid").height == 0                               # dropped below min_n; the pool remains
    assert thin.filter((pl.col("position") == "RB") & (pl.col("_tier") == "regular")).height == 1


def test_table_is_leak_safe_through_a_season():
    t = inseason.rookie_tail_table(_fact(), [1, 2, 3, 4], through=2011, first_season=2010, min_n=10)
    # through 2011 only the 2010 class has a complete year +1 (12 starters, 12 fringe): horizon 3 (year +2) needs 2012 -> absent
    assert "_rg3" in t.columns and t.filter(pl.col("_tier") == "starter")["_rg3"].drop_nulls().len() == 0
    assert "_rg4" not in t.columns or t["_rg4"].drop_nulls().len() == 0


def test_rookie_takes_the_realized_ratios_and_veteran_keeps_the_projection_medians():
    table = inseason.rookie_tail_table(_fact(), [1, 2, 3, 4], through=2015, first_season=2010, min_n=15)   # mid too thin: pooled
    rows = []
    for i in range(10):      # veterans with tails: h3 = 0.9x next, h4 = 0.5x next (ppg); games 0.9x / 0.6x
        rows.append(dict(player_id=f"v{i}", position="RB", age_at_season=22.5, is_rookie=False, next_ppg_hat=10.0 + i, next_games_hat=15.0,
                         h3_ppg_hat=0.9 * (10.0 + i), h3_games_hat=13.5, h4_ppg_hat=0.5 * (10.0 + i), h4_games_hat=9.0))
    rows.append(dict(player_id="rookie", position="RB", age_at_season=21.0, is_rookie=True, next_ppg_hat=14.0, next_games_hat=14.0,
                     h3_ppg_hat=None, h3_games_hat=None, h4_ppg_hat=None, h4_games_hat=None))
    rows.append(dict(player_id="mid_rookie", position="RB", age_at_season=21.0, is_rookie=True, next_ppg_hat=9.0, next_games_hat=9.0,
                     h3_ppg_hat=None, h3_games_hat=None, h4_ppg_hat=None, h4_games_hat=None))
    rows.append(dict(player_id="returning_vet", position="RB", age_at_season=27.0, is_rookie=False, next_ppg_hat=12.0, next_games_hat=16.0,
                     h3_ppg_hat=None, h3_games_hat=None, h4_ppg_hat=None, h4_games_hat=None))
    out = inseason.fill_missing_tail(pl.DataFrame(rows), [1, 2, 3, 4], rookie_table=table)
    r = out.filter(pl.col("player_id") == "rookie").row(0, named=True)
    assert r["tail_source"] == "rookie_table"
    assert abs(r["h3_games_hat"] - 14.0 * 10 / 12) < 1e-9 and abs(r["h3_ppg_hat"] - 14.0 * 13 / 14) < 1e-9   # starter table
    assert r["h4_games_hat"] == 0.0
    m = out.filter(pl.col("player_id") == "mid_rookie").row(0, named=True)
    assert m["tail_source"] == "rookie_table" and 9.0 * 10 / 12 < m["h3_games_hat"] < 9.0                    # mid -> starter+mid pool
    v = out.filter(pl.col("player_id") == "returning_vet").row(0, named=True)
    assert v["tail_source"] == "extrapolated" and abs(v["h3_games_hat"] - 16.0 * 0.9) < 1e-9                  # projection medians
    assert out.filter(pl.col("player_id") == "v3").row(0, named=True)["tail_source"] == "career"
    assert not any(c.startswith("_r") for c in out.columns)


def test_without_a_table_rookies_use_the_old_rule():
    rows = [dict(player_id=f"v{i}", position="RB", age_at_season=22.5, is_rookie=False, next_ppg_hat=10.0, next_games_hat=15.0,
                 h3_ppg_hat=9.0, h3_games_hat=13.5) for i in range(10)]
    rows.append(dict(player_id="rookie", position="RB", age_at_season=21.0, is_rookie=True, next_ppg_hat=14.0, next_games_hat=14.0, h3_ppg_hat=None, h3_games_hat=None))
    out = inseason.fill_missing_tail(pl.DataFrame(rows), [1, 2, 3], rookie_table=None)
    r = out.filter(pl.col("player_id") == "rookie").row(0, named=True)
    assert r["tail_source"] == "extrapolated" and abs(r["h3_games_hat"] - 14.0 * 0.9) < 1e-9
