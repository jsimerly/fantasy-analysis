"""Keep the machine awake while a long job runs (Windows: a power request; elsewhere a no-op).

A weekly refresh or a TabPFN harness is hours of GPU time; Windows' idle sleep paused the 3.5 harness
for 2.5 h on 2026-10-06. ES_SYSTEM_REQUIRED | ES_CONTINUOUS holds the system (not the display) awake
until the process exits or ``release()`` is called."""
from __future__ import annotations

import sys

_ES_CONTINUOUS, _ES_SYSTEM_REQUIRED = 0x80000000, 0x00000001


def keep_awake() -> bool:
    """Hold a no-sleep power request for the life of this process. Returns True when it took effect."""
    if sys.platform != "win32":
        return False
    try:
        import ctypes
        return bool(ctypes.windll.kernel32.SetThreadExecutionState(_ES_CONTINUOUS | _ES_SYSTEM_REQUIRED))
    except Exception:  # noqa: BLE001
        return False


def release() -> None:
    if sys.platform == "win32":
        try:
            import ctypes
            ctypes.windll.kernel32.SetThreadExecutionState(_ES_CONTINUOUS)
        except Exception:  # noqa: BLE001
            pass
