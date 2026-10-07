"""Era-robust parsing of the three FantasyPros player-cell layouts (no network)."""
from fantasypros_ingestion._parse import parse_projection_page

HEAD = ('<thead>'
        '<tr><th></th><th colspan="2">PASSING</th><th></th></tr>'
        '<tr><th>Player</th><th>YDS</th><th>TDS</th><th>FPTS</th></tr>'
        '</thead>')
CELL_2012 = ('<td class="aleft"><a href="/nfl/players/matt-ryan.php">Matt Ryan</a>'
             '<span class="tiny"> (<a href="/nfl/teams/atlanta-falcons.php">ATL</a>, QB)</span></td>')
CELL_2018 = ('<td class="player-label"><a class="player-name" href="/x">Ben Roethlisberger</a> PIT '
             '<a class="fp-player-link fp-id-9039" fp-player-name="Ben Roethlisberger" href="#"></a></td>')
CELL_LIVE = ('<td class="player-label"><a class="player-name fp-player-link fp-id-16413" '
             'fp-player-name="Patrick Mahomes II" href="/x">Patrick Mahomes II</a> KC</td>')


def _page(cell):
    return f'<table id="data">{HEAD}<tbody><tr>{cell}<td>300</td><td>3</td><td>22.7</td></tr></tbody></table>'


def test_2012_no_fpid():
    rows, keys, status = parse_projection_page(_page(CELL_2012))
    assert status == "ok" and keys[:2] == ["player", "passing_yds"] and "fpts" in keys
    r = rows[0]
    assert r["fp_id"] is None and r["player"] == "Matt Ryan" and r["team"] == "ATL"
    assert r["fpts"] == 22.7 and r["passing_yds"] == 300.0


def test_2018_fpid_on_separate_link():
    r = parse_projection_page(_page(CELL_2018))[0][0]
    assert r["fp_id"] == "9039" and r["player"] == "Ben Roethlisberger" and r["team"] == "PIT"


def test_live_fpid_on_name_link():
    r = parse_projection_page(_page(CELL_LIVE))[0][0]
    assert r["fp_id"] == "16413" and r["player"] == "Patrick Mahomes II" and r["team"] == "KC"


def test_unmappable_header_fails_loud():
    rows, keys, status = parse_projection_page(
        '<table id="data"><thead><tr><th>Nope</th></tr></thead><tbody></tbody></table>')
    assert status == "parse_fail"


def test_no_table_is_empty():
    assert parse_projection_page("<html><body>No Player Found</body></html>")[2] == "empty"
