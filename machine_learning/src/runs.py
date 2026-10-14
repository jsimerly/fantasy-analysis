"""Which run a weekly export may read (review 2026-10-13, D1-D3).

A run's ``metrics.json`` names the models that built it (``career_backend``, ``inseason_backend``). The production
store took a trees payload twice on 2026-10-13: the Cloud Run job ``ml-weekly-refresh`` (plain defaults: trees,
``base,career``) at 11:05 and a trees smoke at 21:51, both under a run date next to the GPU run's. ``build_war`` and
``export_projections`` therefore refuse a run whose models are not the production ones unless told otherwise
(``--require-backend any`` for a smoke or an explicitly non-production run; ``weekly_refresh`` passes it when its own
``--backend`` / ``--inseason-backend`` are not the production ones).
"""
from __future__ import annotations

PRODUCTION_BACKEND = "tabpfn"


def backends_of(meta: dict) -> dict[str, str]:
    """The run's model names from its metrics, empty when the run predates the stamp."""
    return {k: str(meta[k]) for k in ("career_backend", "inseason_backend") if meta.get(k)}


def check_backend(meta: dict, require: str | None, what: str) -> None:
    """Raise SystemExit when a stamped backend of ``meta`` does not start with ``require`` (``None`` / ``any`` = no check)."""
    if not require or require == "any":
        return
    bad = {k: v for k, v in backends_of(meta).items() if not v.startswith(require)}
    if bad:
        raise SystemExit(f"{what}: run {meta.get('run_date')} was built with " + ", ".join(f"{k}={v!r}" for k, v in bad.items())
                         + f"; the production exports require {require!r} (review 2026-10-13: a trees payload must not land in the "
                         "production store under a production run date). Pass --require-backend any for a smoke or an explicitly "
                         "non-production run.")
