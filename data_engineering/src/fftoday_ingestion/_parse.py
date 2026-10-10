"""FFToday's weekly projection pages (``rankings/playerwkproj.php?Season=&GameWeek=&PosID=``): one row
per projected player with the stat line and the FFToday id. The site serves every season from 2010 on
its own pages (2009 and earlier return an empty shell), which makes it the weekly consensus history
before Sleeper's 2018 floor (BACKLOG 38). Pure parser; the fetching lives in ``backfill.py``."""
from __future__ import annotations

import re
from html import unescape

import polars as pl

POSITIONS = {"QB": 10, "RB": 20, "WR": 30, "TE": 40}
# the numeric cells after Player / Team / Opp, in page order, per position
COLUMNS = {
    "QB": ["pass_cmp", "pass_att", "pass_yd", "pass_td", "pass_int", "rush_att", "rush_yd", "rush_td", "fpts"],
    "RB": ["rush_att", "rush_yd", "rush_td", "rec", "rec_yd", "rec_td", "fpts"],
    "WR": ["rec", "rec_yd", "rec_td", "rush_att", "rush_yd", "rush_td", "fpts"],
    "TE": ["rec", "rec_yd", "rec_td", "fpts"],
}
STAT_COLS = ["pass_cmp", "pass_att", "pass_yd", "pass_td", "pass_int", "rush_att", "rush_yd", "rush_td", "rec", "rec_yd", "rec_td", "fpts"]
SCHEMA = {"season": pl.Int64, "week": pl.Int64, "position": pl.Utf8, "fft_id": pl.Utf8, "player": pl.Utf8, "team": pl.Utf8, "opponent": pl.Utf8,
          "injury": pl.Utf8, **{c: pl.Float64 for c in STAT_COLS}}
_ROW = re.compile(r"<TR[^>]*>(.*?)</TR>", re.I | re.S)
_PLAYER = re.compile(r'/stats/players/(\d+)/[^"\']*["\'][^>]*>([^<]+)</A>', re.I)
_INJURY = re.compile(r'title=["\']([^"\']+)["\']', re.I)
_CELL = re.compile(r"<TD[^>]*>(.*?)</TD>", re.I | re.S)


def _text(cell: str) -> str:
    return unescape(re.sub(r"<[^>]+>", " ", cell)).replace("\xa0", " ").strip()


def parse_week_page(html: str, season: int, week: int, position: str) -> pl.DataFrame:
    """Every player row on a projection page -> ``SCHEMA``; a page with no rows -> an empty typed frame."""
    cols = COLUMNS[position]
    rows = []
    for tr in _ROW.findall(html):
        m = _PLAYER.search(tr)
        if not m:
            continue
        cells = [_text(c) for c in _CELL.findall(tr)]
        try:
            name_ix = next(i for i, c in enumerate(cells) if m.group(2).strip() in c)
        except StopIteration:
            continue
        after = cells[name_ix + 1:]
        if len(after) < 2 + len(cols):
            continue
        team, opp, nums = after[0], after[1], after[2:2 + len(cols)]
        try:
            vals = [float(v) for v in nums]
        except ValueError:
            continue
        inj = _INJURY.search(tr)
        rows.append({"season": season, "week": week, "position": position, "fft_id": m.group(1), "player": unescape(m.group(2)).strip(),
                     "team": team or None, "opponent": opp or None, "injury": inj.group(1) if inj else None,
                     **{c: None for c in STAT_COLS}, **dict(zip(cols, vals))})
    if not rows:
        return pl.DataFrame(schema=SCHEMA)
    return pl.DataFrame(rows, schema_overrides=SCHEMA).select(list(SCHEMA))


def page_url(season: int, week: int, position: str, league_id: int = 1) -> str:
    return f"https://www.fftoday.com/rankings/playerwkproj.php?Season={season}&GameWeek={week}&PosID={POSITIONS[position]}&LeagueID={league_id}"
