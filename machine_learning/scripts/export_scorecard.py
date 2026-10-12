"""The model scorecard: the headline numbers of the page's Model performance tab as a small JSON and a
markdown table, regenerated with every export so a site can embed a living table (owner, 2026-10-10:
"a model performance table that can continue updated and we can use on the site").

    uv run python scripts/export_scorecard.py --payload projections.json --out-dir ../docs [--publish]

Reads the ``performance`` block of a page export (``export_projections.py``) and writes
``model_scorecard.json`` and ``model_scorecard.md`` to ``--out-dir``; ``--publish`` also writes the JSON
next to the page payload in the ML bucket (``pages/season=S/week=W/run_date=D/scorecard.json``).
The weekly refresh runs it after the export. Pure functions (``scorecard``, ``to_markdown``) so the
shape is spec-tested without GCS.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import gcs_io  # noqa: E402


def _f(x, d=3):
    return None if x is None else round(float(x), d)


def feb_years(cohorts: str) -> str:
    """'2020-2022' (the backtest's cohorts) -> '2021-2023': cohort T is priced at KTC on 15 February T+1 and scored
    on seasons T+1..T+3, so a swap is dated by that February."""
    import re
    m = re.match(r"\s*(\d{4})\D+(\d{4})", str(cohorts or ""))
    return f"{int(m.group(1)) + 1}-{int(m.group(2)) + 1}" if m else str(cohorts or "")


def scorecard(payload: dict) -> dict:
    """The headline numbers, each with its label, value, context and the run it came from."""
    P = payload.get("performance") or {}
    mk, ins, ce = P.get("market") or {}, P.get("inseason") or {}, P.get("career_eval") or {}
    meta = mk.get("meta") or {}
    head: list[dict] = []
    ov = sorted(mk.get("overall") or [], key=lambda r: -r.get("horizon", 0))
    ov0 = ov[0] if ov else None
    sw0 = next((r for r in mk.get("swap_overall") or [] if ov0 and r.get("variant") == ov0.get("variant") and r.get("horizon") == ov0.get("horizon")), None)
    if sw0 and sw0.get("pairs"):
        if sw0.get("gain_pts_mean") is not None:
            head.append({"key": "swap_test", "label": "The swap test: same-price trades the model would have made", "value": _f(sw0["gain_pts_mean"], 0), "unit": "more fantasy points per swap over three seasons",
                         "detail": f"{sw0['pairs']} swaps made each February {feb_years(meta.get('cohorts', ''))}: three seasons later the player bought out-scored the one sold by that much; {round(sw0.get('gain_par_mean') or 0):,} more above a replacement-level player, ahead in {round((sw0.get('win_rate_par') or 0) * 100)}% of swaps; in wins {round(sw0['win_rate'] * 100)}% won, +{_f(sw0['gain_mean'], 2)} per swap",
                         "secondary": _f(sw0["win_rate"] * 100, 0), "run_date": meta.get("run_date")})
        else:
            head.append({"key": "swap_test", "label": "The swap test: same-price trades the model would have made", "value": _f(sw0["win_rate"] * 100, 0), "unit": "% won",
                         "detail": f"{sw0['pairs']} swaps made each February {feb_years(meta.get('cohorts', ''))}, scored {ov0['horizon']} seasons later; +{_f(sw0['gain_mean'], 2)} wins per swap",
                         "secondary": _f(sw0["gain_mean"], 2), "run_date": meta.get("run_date")})
    if ov0 and ov0.get("cheap_gap_real") is not None:
        head.append({"key": "mispricing", "label": "The mispricing is real", "value": _f(ov0["cheap_gap_real"], 1), "unit": "ranks better",
                     "detail": f"players the model called cheap finished that many ranks better than the market had them; the ones it called rich finished {_f(abs(ov0['rich_gap_real']), 1)} worse",
                     "secondary": _f(ov0["rich_gap_real"], 1), "run_date": meta.get("run_date")})
    ros = ins.get("ros") or []
    if ros:
        w, last = ros[0], ros[-1]
        head.append({"key": "rest_of_season", "label": "Rest of this season: ordering players by what they go on to average", "value": _f(w.get("ros|model")), "unit": "rank agreement",
                     "detail": f"vs the market's {_f(w.get('ros|ktc'))} at week {w.get('W')}; {_f(last.get('ros|model'))} vs {_f(last.get('ros|ktc'))} at week {last.get('W')}; cohorts {ins.get('cohorts', '')}",
                     "secondary": _f(w.get("ros|ktc")), "run_date": ins.get("run_date")})
    if ov0:
        head.append({"key": "three_years_out", "label": f"{ov0['horizon']} years out: ordering by the wins players then delivered", "value": _f(ov0.get("rho_model")), "unit": "rank agreement",
                     "detail": f"vs the market's {_f(ov0.get('rho_ktc'))} on {ov0.get('n')} priced players, priced each February {feb_years(meta.get('cohorts', ''))}",
                     "secondary": _f(ov0.get("rho_ktc")), "run_date": meta.get("run_date")})
    mae = ce.get("mae") or []
    r3 = next((r for r in mae if r.get("horizon") == 3), mae[-1] if mae else None)
    if r3:
        head.append({"key": "career_error", "label": f"Career projection, {r3['horizon']} years ahead: error vs repeating last season", "value": _f(-abs(r3.get("model_%_vs_carry", 0)), 0), "unit": "% error",
                     "detail": f"−{_f(abs(r3.get('model_%_vs_decay', 0)), 0)}% vs an age-decay curve; {int(r3.get('n_total', 0)):,} player-seasons since {ce.get('start_season')}",
                     "secondary": _f(-abs(r3.get("model_%_vs_decay", 0)), 0), "run_date": ce.get("run_date")})
    swaps = sorted([r for r in mk.get("top_swaps") or [] if r.get("gain") is not None], key=lambda r: -r["gain"])
    named = [{"cohort": r["cohort"], "february": r["cohort"] + 1, "sold": r["sell"], "sold_pos": r["sell_pos"], "sold_ktc": r["sell_ktc"], "sold_delivered": _f(r["sell_real"], 2),
              "bought": r["buy"], "bought_pos": r["buy_pos"], "bought_ktc": r["buy_ktc"], "bought_delivered": _f(r["buy_real"], 2), "gain": _f(r["gain"], 2),
              "sold_pts": _f(r.get("sell_pts"), 0), "bought_pts": _f(r.get("buy_pts"), 0), "sold_par": _f(r.get("sell_par"), 0), "bought_par": _f(r.get("buy_par"), 0),
              "gain_pts": _f(r.get("gain_pts"), 0), "gain_par": _f(r.get("gain_par"), 0)} for r in swaps[:8]]
    by_cohort = [{"cohort": r["cohort"], "february": r["cohort"] + 1, "pairs": r["pairs"], "win_rate": _f(r["win_rate"] * 100, 0), "gain_mean": _f(r["gain_mean"], 2), "gain_pts_mean": _f(r.get("gain_pts_mean"), 0)}
                 for r in sorted(mk.get("swap_by_cohort") or [], key=lambda r: r["cohort"]) if sw0 and r.get("variant") == sw0.get("variant") and r.get("horizon") == sw0.get("horizon")]
    by_pos = [{"position": r["buy_pos"], "pairs": r["pairs"], "win_rate": _f(r["win_rate"] * 100, 0), "gain_mean": _f(r["gain_mean"], 2), "gain_pts_mean": _f(r.get("gain_pts_mean"), 0)}
              for r in mk.get("swap_by_buy_pos") or [] if sw0 and r.get("variant") == sw0.get("variant") and r.get("horizon") == sw0.get("horizon")]
    return {"as_of": payload.get("run_date"), "season": payload.get("season"), "week": payload.get("week"), "model": payload.get("career_backend"),
            "headline": head, "swaps": named, "swaps_by_cohort": by_cohort, "swaps_by_position_bought": by_pos,
            "method": "walk-forward: the model rebuilt with only what was known at the time, scored on what the players then delivered; the market (KTC) is the yardstick, never an input"}


def to_markdown(card: dict) -> str:
    L = [f"# Model scorecard — as of {card.get('as_of')} (season {card.get('season')}, week {card.get('week')})", "",
         f"*{card.get('method')}.* Model: {card.get('model')}.", "", "| Headline | Value | Context |", "| --- | --- | --- |"]
    for h in card.get("headline", []):
        v = h["value"]
        val = f"{v:+.0f}" if h["key"] == "career_error" else (f"{v:g}" if v is not None else "–")
        L.append(f"| {h['label']} | **{val} {h['unit']}** | {h['detail']} |")
    L += ["", "**The words.** *Rank agreement*: put the players in the order we projected them and in the order they actually finished; 1.0 is the same order, 0 is no relation (Spearman). "
          "*Points above replacement*: a player's points minus what a freely available player at his position would have scored in the same games. "
          "*Wins above replacement*: those points turned into wins on this league's scoring and lineup, the number the model is tested on. "
          "*The market*: KeepTradeCut's dynasty values, the yardstick, never an input. *Walk-forward*: a past year is scored by rebuilding the model with only what was known then."]
    if card.get("swaps"):
        pts = any(r.get("sold_pts") is not None for r in card["swaps"])
        if pts:
            L += ["", "## The swaps, by name", "", "Points are what each player scored over the three seasons after the swap, in this league's scoring; \"above replacement\" is the part a freely available player at his position would not have given you; wins are the model's own unit.", "",
                  "| Swap (Feb.) | Sold (KTC) | Points | Above repl. | Bought (KTC) | Points | Above repl. | Gain (points) | Gain (wins) |", "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
            for r in card["swaps"]:
                L.append(f"| {r['february']} | {r['sold']}, {r['sold_pos']} ({r['sold_ktc']:,}) | {r['sold_pts']:,.0f} | {r['sold_par']:,.0f} | {r['bought']}, {r['bought_pos']} ({r['bought_ktc']:,}) | {r['bought_pts']:,.0f} | {r['bought_par']:,.0f} | {r['gain_pts']:+,.0f} | {r['gain']:+.2f} |")
        else:
            L += ["", "## The swaps, by name", "", "| Swap (Feb.) | Sold (KTC) | Delivered | Bought (KTC) | Delivered | Gain (wins) |", "| --- | --- | --- | --- | --- | --- |"]
            for r in card["swaps"]:
                L.append(f"| {r['february']} | {r['sold']}, {r['sold_pos']} ({r['sold_ktc']:,}) | {r['sold_delivered']:.2f} | {r['bought']}, {r['bought_pos']} ({r['bought_ktc']:,}) | {r['bought_delivered']:.2f} | {r['gain']:+.2f} |")
    if card.get("swaps_by_cohort"):
        L += ["", "Win rate by year of the swap: " + " · ".join(f"February {r['february']}: {r['win_rate']:.0f}% of {r['pairs']}" for r in card["swaps_by_cohort"]) + "."]
    if card.get("swaps_by_position_bought"):
        L += ["", "By the position bought: " + " · ".join(f"{r['position']} {r['win_rate']:.0f}% ({r['gain_mean']:+.2f} wins per swap)" for r in sorted(card["swaps_by_position_bought"], key=lambda r: -r["win_rate"])) + "."]
    L.append("")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--payload", required=True, help="a page export (projections.json)")
    ap.add_argument("--out-dir", default=str(ROOT.parent / "docs"))
    ap.add_argument("--publish", action="store_true", help="also write scorecard.json next to the page payload in the ML bucket")
    args = ap.parse_args()
    payload = json.loads(Path(args.payload).read_text(encoding="utf-8"))
    card = scorecard(payload)
    out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)
    (out / "model_scorecard.json").write_text(json.dumps(card, indent=1), encoding="utf-8")
    (out / "model_scorecard.md").write_text(to_markdown(card), encoding="utf-8")
    print(f"scorecard: {len(card['headline'])} headline numbers, {len(card['swaps'])} named swaps -> {out / 'model_scorecard.md'}")
    if args.publish:
        print("published", gcs_io.write_ml_json(card, "pages", f"season={card['season']}", f"week={card['week']}", f"run_date={card['as_of']}", "scorecard.json"))


if __name__ == "__main__":
    main()
