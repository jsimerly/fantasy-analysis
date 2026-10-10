"""fftoday_ingestion/_parse.py on a snippet in the site's own markup (a 2010 week-5 RB page)."""
import polars as pl

import fftoday_ingestion._parse as mod

RB_PAGE = """
<TABLE><TR><TD class="tableclmhead">Player</TD><TD>Team</TD><TD>Opp</TD><TD>Att</TD><TD>Yard</TD><TD>TD</TD><TD>Rec</TD><TD>Yard</TD><TD>TD</TD><TD>FPts</TD></TR>
<TR> <TD class="bodycontent" ALIGN="center" BGCOLOR="#ffffff">&nbsp;</TD> <TD class="smallbody" ALIGN="LEFT">&nbsp;<A HREF="/stats/players/3080/Steven_Jackson?LeagueID=1">Steven Jackson</A> <img src="../images/icn_injury_bag2.gif" title="Probable: Groin"> </TD> <TD class="smallbody">STL</TD> <TD class="smallbody">@DET</TD> <TD>21.0</TD> <TD>90.0</TD> <TD>1.0</TD> <TD>4.0</TD> <TD>30.0</TD> <TD>0.0</TD> <TD BGCOLOR="#e0e0e0">18.0</TD> </TR>
<TR> <TD>&nbsp;</TD> <TD>&nbsp;<A HREF="/stats/players/2932/Chris_Johnson?LeagueID=1">Chris Johnson</A> </TD> <TD>TEN</TD> <TD>@DAL</TD> <TD>20.0</TD> <TD>100.0</TD> <TD>1.0</TD> <TD>3.0</TD> <TD>20.0</TD> <TD>0.0</TD> <TD>19.0</TD> </TR>
<TR> <TD>&nbsp;</TD> <TD>&nbsp;<A HREF="/stats/players/9999/Broken_Row?LeagueID=1">Broken Row</A> </TD> <TD>NE</TD> <TD>BUF</TD> <TD>n/a</TD> </TR>
</TABLE>
"""


def test_rows_carry_the_id_name_team_opponent_injury_and_the_positions_stat_line():
    df = mod.parse_week_page(RB_PAGE, 2010, 5, "RB")
    assert df.height == 2 and df.columns == list(mod.SCHEMA)
    sj = df.filter(pl.col("fft_id") == "3080").to_dicts()[0]
    assert sj["player"] == "Steven Jackson" and sj["team"] == "STL" and sj["opponent"] == "@DET" and sj["injury"] == "Probable: Groin"
    assert sj["rush_att"] == 21.0 and sj["rush_yd"] == 90.0 and sj["rec"] == 4.0 and sj["rec_yd"] == 30.0 and sj["fpts"] == 18.0 and sj["pass_att"] is None
    cj = df.filter(pl.col("fft_id") == "2932").to_dicts()[0]
    assert cj["injury"] is None and cj["fpts"] == 19.0 and cj["season"] == 2010 and cj["week"] == 5 and cj["position"] == "RB"


def test_empty_page_and_the_url():
    assert mod.parse_week_page("<html></html>", 2009, 5, "QB").height == 0
    assert mod.page_url(2010, 5, "RB") == "https://www.fftoday.com/rankings/playerwkproj.php?Season=2010&GameWeek=5&PosID=20&LeagueID=1"
