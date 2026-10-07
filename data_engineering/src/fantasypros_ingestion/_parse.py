"""Era-robust parsing of FantasyPros projection pages (live + Wayback snapshots).

The header is stable across eras (a 2-row group/stat `thead`, e.g. PASSING colspan=5 over
ATT/CMP/YDS/TDS/INTS), so stat columns map by GROUP+STAT label. The **player cell** changed
shape several times, so `parse_player_cell` handles all three:
  * 2012:  ``<a>Matt Ryan</a><span class="tiny"> (<a>ATL</a>, QB)</span>``  (no fp-id)
  * 2018+: ``<a class="player-name">Ben Roethlisberger</a> PIT <a class="fp-id-9039" ...>``
  * live:  ``<a class="player-name fp-id-16413" fp-player-name="...">Patrick Mahomes II</a> KC``

Everything here is pure (html -> rows) so it's unit-tested without network.
"""
from __future__ import annotations

import re

from bs4 import BeautifulSoup

_FP_ID = re.compile(r"fp-id-(\d+)")
_TEAM_IN_PARENS = re.compile(r"\(\s*([A-Za-z]{2,4})\s*,")   # 2012 "(ATL, QB)"
_TEAM_TOKEN = re.compile(r"\b([A-Z]{2,4})\b")               # bare "PIT" / "KC"


def _norm(group: str, stat: str) -> str:
    if stat.lower() == "player":
        return "player"
    if stat.upper() == "FPTS":
        return "fpts"
    base = f"{group}_{stat}" if group else stat
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", base.lower())).strip("_")


def _header_keys(thead) -> list[str] | None:
    """Map each column to a GROUP_STAT key. Handles 2-row (group+stat, colspan) + 1-row heads."""
    rows = thead.find_all("tr")
    if not rows:
        return None
    if len(rows) == 1:
        labels = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
        groups = [""] * len(labels)
    else:
        groups = []
        for c in rows[0].find_all(["th", "td"]):
            groups += [c.get_text(strip=True)] * int(c.get("colspan") or 1)
        labels = [c.get_text(strip=True) for c in rows[1].find_all(["th", "td"])]
        if len(groups) < len(labels):
            groups += [""] * (len(labels) - len(groups))
    return [_norm(g, l) for g, l in zip(groups, labels)]


def _to_float(s: str):
    s = (s or "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def parse_player_cell(td):
    """-> (fp_id|None, name|None, team|None) across all three era layouts."""
    fp_id = None
    for a in td.find_all("a"):
        for c in (a.get("class") or []):
            m = _FP_ID.fullmatch(c) or _FP_ID.search(c)
            if m:
                fp_id = m.group(1)
                break
        if fp_id:
            break

    # name: prefer the fp-player-name attribute; else first non-team anchor text
    name = None
    for a in td.find_all("a"):
        if a.get("fp-player-name"):
            name = a["fp-player-name"].strip()
            break
    if not name:
        for a in td.find_all("a"):
            if "/teams/" in (a.get("href") or ""):
                continue
            txt = a.get_text(strip=True)
            if txt:
                name = txt
                break

    # team: 2012 has a `.tiny` span "(ATL, QB)"; later eras have a bare text node "PIT"/"KC"
    team = None
    span = td.find("span", class_="tiny")
    if span:
        m = _TEAM_IN_PARENS.search(span.get_text(" ", strip=True))
        if m:
            team = m.group(1).upper()
        else:
            ta = span.find("a", href=re.compile("/teams/"))
            if ta:
                team = ta.get_text(strip=True).upper()
    if not team:
        direct = " ".join(s.strip() for s in td.find_all(string=True, recursive=False) if s.strip())
        toks = _TEAM_TOKEN.findall(direct)
        if toks:
            team = toks[-1]
    return fp_id, name, team


def parse_projection_page(html: str | None):
    """-> (rows: list[dict], keys: list[str], status in {ok, empty, parse_fail}).

    Header-driven + validated, so a layout change fails loud rather than mis-mapping values.
    """
    if html is None:
        return [], [], "empty"
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id="data") or soup.find("table")
    if table is None:
        return [], [], "empty"
    thead = table.find("thead")
    keys = _header_keys(thead) if thead else None
    if not keys or keys[0] != "player" or "fpts" not in keys:
        return [], (keys or []), "parse_fail"
    body = table.find("tbody")
    trs = body.find_all("tr") if body else []
    rows = []
    for tr in trs:
        tds = tr.find_all("td")
        if len(tds) <= 1:                       # colspan placeholder / ad row
            continue
        if len(tds) != len(keys):               # genuine mismatch -> fail loud
            return rows, keys, "parse_fail"
        fp_id, name, team = parse_player_cell(tds[0])
        if not name:
            continue
        rec = {"fp_id": fp_id, "player": name, "team": team}
        for k, td in zip(keys[1:], tds[1:]):
            rec[k] = _to_float(td.get_text(strip=True))
        rows.append(rec)
    return rows, keys, ("ok" if rows else "empty")
