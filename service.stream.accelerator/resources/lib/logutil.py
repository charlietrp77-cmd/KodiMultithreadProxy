"""Centralized Kodi logging helper with optional verbose mode.

When 'verbose_logging' is enabled in add-on settings, verbose diagnostic
messages (probe latency, per-chunk timing, buffer starvation stalls, window
scheduling) are promoted from LOGDEBUG to LOGINFO so they appear in kodi.log
without needing Kodi's system-wide Debug Overlay.
"""

try:
    import xbmc
    import xbmcaddon
    _HAS_KODI = True
except ImportError:
    _HAS_KODI = False

_verbose_cached = None


def refresh_verbose():
    """Force-refresh verbose setting (called at startup and onSettingsChanged)."""
    global _verbose_cached
    _verbose_cached = None
    return is_verbose()


def is_verbose():
    """Return True if verbose logging is enabled in settings (cached until settings change)."""
    global _verbose_cached
    if not _HAS_KODI:
        return False
    if _verbose_cached is not None:
        return _verbose_cached
    try:
        addon = xbmcaddon.Addon("service.stream.accelerator")
        _verbose_cached = addon.getSettingBool("verbose_logging")
    except Exception:
        _verbose_cached = False
    return _verbose_cached


def log_info(tag, msg):
    if _HAS_KODI:
        xbmc.log(f"[{tag}] {msg}", xbmc.LOGINFO)


def log_warning(tag, msg):
    if _HAS_KODI:
        xbmc.log(f"[{tag}] {msg}", xbmc.LOGWARNING)


def log_error(tag, msg):
    if _HAS_KODI:
        xbmc.log(f"[{tag}] {msg}", xbmc.LOGERROR)


def log_verbose(tag, msg):
    """Log at LOGINFO when verbose_logging=true, otherwise LOGDEBUG."""
    if not _HAS_KODI:
        return
    level = xbmc.LOGINFO if is_verbose() else xbmc.LOGDEBUG
    xbmc.log(f"[{tag}] {msg}", level)

