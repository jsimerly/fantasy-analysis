"""Keep the machine awake while a long job runs (Windows: a power request; elsewhere a no-op).

A weekly refresh or a TabPFN harness is hours of GPU time; Windows' idle sleep paused the 3.5 harness
for 2.5 h on 2026-10-06. ES_SYSTEM_REQUIRED | ES_CONTINUOUS holds the system awake until the process
exits or ``release()`` is called. The display is held on too by default (ES_DISPLAY_REQUIRED): on
2026-10-07 a run at 99 % GPU sat behind a black screen that keyboard and mouse did not bring back,
the owner held the power button, and the machine hard-reset three hours into the run. Set
``KEEP_DISPLAY=0`` to let the screen turn off."""
from __future__ import annotations

import os
import sys

_ES_CONTINUOUS, _ES_SYSTEM_REQUIRED, _ES_DISPLAY_REQUIRED = 0x80000000, 0x00000001, 0x00000002


def keep_awake(display: bool | None = None) -> bool:
    """Hold a no-sleep (and, by default, screen-on) power request for the life of this process.
    Returns True when it took effect. ``display`` None reads KEEP_DISPLAY (default on)."""
    if sys.platform != "win32":
        return False
    if display is None:
        display = os.environ.get("KEEP_DISPLAY", "1") != "0"
    try:
        import ctypes
        flags = _ES_CONTINUOUS | _ES_SYSTEM_REQUIRED | (_ES_DISPLAY_REQUIRED if display else 0)
        return bool(ctypes.windll.kernel32.SetThreadExecutionState(flags))
    except Exception:  # noqa: BLE001
        return False


def release() -> None:
    if sys.platform == "win32":
        try:
            import ctypes
            ctypes.windll.kernel32.SetThreadExecutionState(_ES_CONTINUOUS)
        except Exception:  # noqa: BLE001
            pass
