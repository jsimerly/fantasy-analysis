"""silver_fantasy/fact_player_contract_season.py: the contract in force per season (the richer deal
when two overlap), years left / contract year / contract age, the rookie-deal flag from the history
type or the draft year, the cap number per year from the nested career history, the guaranteed share
of the cap, the position rank, and the skill-position / known-id filters."""
import polars as pl

import silver_fantasy.fact_player_contract_season as m


def _contracts():
    hist_a = [dict(year_signed=2014, apy=1.0, contract_type="Drafted", status="Renegotiated"),
              dict(year_signed=2017, apy=14.5, contract_type="Extension", status="Active")]
    cap_a = [dict(year="2014", team="GB", cap_percent=0.005, guaranteed_salary=0.0, cash_paid=1.0),
             dict(year="2017", team="GB", cap_percent=0.02, guaranteed_salary=1.0, cash_paid=5.0),
             dict(year="2018", team="GB", cap_percent=0.08, guaranteed_salary=4.0, cash_paid=20.0),
             dict(year="2018", team="LV", cap_percent=0.01, guaranteed_salary=0.0, cash_paid=1.0),    # moved mid-year: summed
             dict(year="Total", team=None, cap_percent=None, guaranteed_salary=5.0, cash_paid=27.0)]
    rows = [
        dict(player="A", position="WR", team="Packers", is_active=False, year_signed=2014, years=4, value=4.0, apy=1.0, guaranteed=1.5, apy_cap_pct=0.007,
             otc_id=1, gsis_id="00-A", draft_year=2014, season_history=cap_a, contract_history=hist_a),
        dict(player="A", position="WR", team="Packers", is_active=True, year_signed=2017, years=4, value=58.0, apy=14.5, guaranteed=18.0, apy_cap_pct=0.087,
             otc_id=1, gsis_id="00-A", draft_year=2014, season_history=cap_a, contract_history=hist_a),
        dict(player="B", position="WR", team="Giants", is_active=True, year_signed=2017, years=2, value=20.0, apy=10.0, guaranteed=0.0, apy_cap_pct=0.06,
             otc_id=2, gsis_id="00-B", draft_year=None, season_history=[], contract_history=[]),
        dict(player="C", position="QB", team="Jets", is_active=True, year_signed=2017, years=1, value=2.0, apy=2.0, guaranteed=2.0, apy_cap_pct=0.012,
             otc_id=3, gsis_id="00-C", draft_year=2017, season_history=[], contract_history=[]),
        dict(player="D", position="CB", team="Jets", is_active=True, year_signed=2017, years=3, value=30.0, apy=10.0, guaranteed=0.0, apy_cap_pct=0.06,
             otc_id=4, gsis_id="00-D", draft_year=None, season_history=[], contract_history=[]),
        dict(player="E", position="RB", team="Jets", is_active=True, year_signed=2017, years=3, value=30.0, apy=10.0, guaranteed=0.0, apy_cap_pct=0.06,
             otc_id=5, gsis_id=None, draft_year=None, season_history=[], contract_history=[]),
        dict(player="F", position="RB", team="Jets", is_active=False, year_signed=1990, years=3, value=3.0, apy=1.0, guaranteed=0.0, apy_cap_pct=0.02,
             otc_id=6, gsis_id="00-F", draft_year=None, season_history=[], contract_history=[]),
    ]
    schema_overrides = {"season_history": pl.List(pl.Struct({"year": pl.Utf8, "team": pl.Utf8, "cap_percent": pl.Float64, "guaranteed_salary": pl.Float64, "cash_paid": pl.Float64})),
                        "contract_history": pl.List(pl.Struct({"year_signed": pl.Int64, "apy": pl.Float64, "contract_type": pl.Utf8, "status": pl.Utf8}))}
    return pl.DataFrame(rows, schema_overrides=schema_overrides)


class TestContractInForce:
    def test_the_richer_deal_wins_the_overlap_and_the_terms_count_down(self):
        out = m.build_fact_player_contract_season(_contracts())
        a = out.filter(pl.col("gsis_id") == "00-A").sort("season")
        assert a["season"].to_list() == [2014, 2015, 2016, 2017, 2018, 2019, 2020]
        assert a["year_signed"].to_list() == [2014, 2014, 2014, 2017, 2017, 2017, 2017]       # 2017 overlaps: the extension is in force
        assert a["years_left"].to_list() == [3, 2, 1, 3, 2, 1, 0] and a["contract_year"].to_list() == [False, False, False, False, False, False, True]
        assert a["contract_age"].to_list() == [0, 1, 2, 0, 1, 2, 3] and a["n_contracts_signed"].to_list() == [1, 1, 1, 2, 2, 2, 2]
        r14, r17 = a.row(0, named=True), a.row(3, named=True)
        assert r14["contract_type"] == "Drafted" and r14["is_rookie_deal"] is True and r14["contract_years"] == 4
        assert r17["contract_type"] == "Extension" and r17["is_rookie_deal"] is False and r17["apy_cap_pct"] == 0.087
        assert abs(r17["guaranteed_cap_pct"] - 18.0 * 0.087 / 14.5) < 1e-9 and abs(r14["guaranteed_cap_pct"] - 1.5 * 0.007 / 1.0) < 1e-9

    def test_rookie_deal_from_the_draft_year_when_the_history_is_empty(self):
        out = m.build_fact_player_contract_season(_contracts())
        c = out.filter(pl.col("gsis_id") == "00-C").row(0, named=True)
        b = out.filter(pl.col("gsis_id") == "00-B").sort("season")
        assert c["is_rookie_deal"] is True and c["contract_type"] is None and c["contract_year"] is True
        assert b["is_rookie_deal"].to_list() == [False, False] and b["contract_type"].to_list() == [None, None]


class TestCapHistoryAndRank:
    def test_cap_number_per_year_summed_across_teams_and_next_year_scheduled(self):
        out = m.build_fact_player_contract_season(_contracts())
        a = {r["season"]: r for r in out.filter(pl.col("gsis_id") == "00-A").to_dicts()}
        assert a[2014]["cap_pct_season"] == 0.005 and a[2017]["cap_pct_season"] == 0.02 and abs(a[2018]["cap_pct_season"] - 0.09) < 1e-9
        assert abs(a[2018]["guaranteed_salary_season"] - 4.0) < 1e-9 and abs(a[2017]["cap_pct_next"] - 0.09) < 1e-9 and a[2015]["cap_pct_season"] is None
        assert a[2019]["cap_pct_season"] is None                                                   # the "Total" row is not a year
        assert out.filter(pl.col("gsis_id") == "00-B")["cap_pct_season"].is_null().all()

    def test_position_rank_within_the_season(self):
        out = m.build_fact_player_contract_season(_contracts())
        wr17 = out.filter((pl.col("season") == 2017) & (pl.col("position") == "WR")).sort("apy_cap_pct_pos_rank")
        assert wr17["gsis_id"].to_list() == ["00-A", "00-B"] and wr17["apy_cap_pct_pos_rank"].to_list() == [1, 2]
        assert wr17["apy_cap_pct_pos_pctl"].to_list() == [1.0, 0.5]
        assert out.filter((pl.col("season") == 2017) & (pl.col("position") == "QB"))["apy_cap_pct_pos_rank"].to_list() == [1]


class TestFilters:
    def test_skill_positions_with_a_gsis_id_in_the_cap_era_only(self):
        out = m.build_fact_player_contract_season(_contracts())
        assert set(out["gsis_id"].unique().to_list()) == {"00-A", "00-B", "00-C"}
        assert out.group_by("gsis_id", "season").len()["len"].max() == 1
        assert set(out.columns) >= {"gsis_id", "season", "position", "apy_cap_pct", "years_left", "contract_year", "is_rookie_deal", "cap_pct_season"}
