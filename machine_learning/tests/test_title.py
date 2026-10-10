"""title.py: the season simulation and the title curve on toy leagues."""
import numpy as np

import title


def _round_robin(n, weeks):
    """Circle-method schedule: every team plays exactly once a week."""
    teams = list(range(n)); out = []
    for w in range(weeks):
        pairs = [(teams[i], teams[n - 1 - i]) for i in range(n // 2)]
        out.append((w + 1, pairs))
        teams = [teams[0]] + [teams[-1]] + teams[1:-1]
    return out


def test_symmetric_league_splits_the_title_evenly_and_the_cut_matches_the_format():
    s = title.Season(mu=np.full(6, 120.0), sd=20.0, wins=np.zeros(6), pf=np.zeros(6), matchups=_round_robin(6, 10), playoff_teams=4)
    r = title.simulate(s, n_sims=8000, seed=1)
    assert abs(r["p_title"].sum() - 1.0) < 1e-9 and np.allclose(r["p_title"], 1 / 6, atol=0.03)
    assert np.allclose(r["p_playoffs"], 4 / 6, atol=0.04) and np.allclose(r["exp_wins"], 5.0, atol=0.2) and (r["p_bye"] == 0).all()


def test_a_juggernaut_takes_most_titles_and_the_six_team_bracket_gives_byes():
    mu = np.array([160.0, 130, 128, 126, 124, 122, 120, 118, 116, 114, 112, 110])
    s = title.Season(mu=mu, sd=25.0, wins=np.zeros(12), pf=np.zeros(12), matchups=_round_robin(12, 10), playoff_teams=6)
    r = title.simulate(s, n_sims=8000, seed=2)
    assert r["p_title"][0] > 0.45 and r["p_bye"][0] > 0.9 and r["p_playoffs"][0] > 0.99
    assert r["p_title"][-1] < 0.01 and abs(r["p_title"].sum() - 1.0) < 1e-9 and r["p_bye"].sum() < 2.0001


def test_standings_to_date_count_and_the_curve_is_monotone_on_common_random_numbers():
    mu = np.full(4, 120.0); wins = np.array([4.0, 3, 1, 0]); pf = np.array([520.0, 500, 450, 430])
    s = title.Season(mu=mu, sd=20.0, wins=wins, pf=pf, matchups=_round_robin(4, 2), playoff_teams=4)
    r = title.simulate(s, n_sims=4000, seed=3)
    assert r["exp_seed"][0] < r["exp_seed"][3] and r["p_playoffs"].min() == 1.0          # four of four make it
    c = title.title_curve(s, team=3, shifts=np.array([-10.0, 0.0, 10.0, 20.0]), n_sims=4000, seed=3)
    assert np.all(np.diff(c["p_title"]) >= -1e-9) and c["p_title"][-1] > c["p_title"][0]
    assert abs(title.interp(c, "p_title", 0.0) - c["p_title"][1]) < 1e-12


def test_round_robin_plays_every_team_once_a_week_and_gives_an_odd_league_a_bye():
    for n in (10, 9):
        sched = title.round_robin(n, 13)
        assert len(sched) == 13 and sched[0][0] == 1
        for _, pairs in sched:
            seen = [t for p in pairs for t in p]
            assert len(seen) == len(set(seen)) and len(pairs) == n // 2 and all(0 <= t < n for t in seen)
    # every pair meets once in the first n - 1 weeks of an even league
    met = {tuple(sorted(p)) for _, pairs in title.round_robin(6, 5) for p in pairs}
    assert len(met) == 15
