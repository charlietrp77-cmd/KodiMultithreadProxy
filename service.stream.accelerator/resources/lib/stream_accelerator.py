"""stream_accelerator — importable helper module for other Kodi add-ons.

Usage from any add-on:
    import stream_accelerator
    proxied_url = stream_accelerator.proxify(raw_url, threads=8)
    xbmcplugin.setResolvedUrl(handle, True, xbmcgui.ListItem(path=proxied_url))
"""
from urllib.parse import quote
import os

try:
    import xbmcaddon
    import xbmcvfs
except ImportError:
    xbmcaddon = None
    xbmcvfs = None


def get_active_port():
    """Return the active bound port (including fallback port if primary was in use)."""
    if xbmcaddon:
        try:
            addon = xbmcaddon.Addon("service.stream.accelerator")
            if xbmcvfs:
                profile = xbmcvfs.translatePath(addon.getAddonInfo("profile"))
                port_file = os.path.join(profile, "active_port")
                if os.path.exists(port_file):
                    with open(port_file, "r", encoding="utf-8") as f:
                        val = f.read().strip()
                        if val.isdigit():
                            return val
            return addon.getSetting("port") or "48990"
        except Exception:
            pass
    return "48990"


def proxify(url, threads=None, chunk_kb=None, read_ahead=None):
    """Wraps any Kodi stream URL (including |Header=Val) into a local accelerated URL.

    Args:
        url: Raw upstream URL, optionally with Kodi pipe headers.
        threads: Override worker thread count (or None for addon default).
        chunk_kb: Override chunk size in KB (or None for addon default).
        read_ahead: Override read-ahead buffer depth (or None for addon default).

    Returns:
        A localhost proxy URL that Kodi can play.
    """
    port = get_active_port()

    proxy_url = f"http://127.0.0.1:{port}/stream?url={quote(url, safe='')}"
    if threads is not None:
        proxy_url += f"&threads={int(threads)}"
    if chunk_kb is not None:
        proxy_url += f"&chunk_kb={int(chunk_kb)}"
    if read_ahead is not None:
        proxy_url += f"&read_ahead={int(read_ahead)}"
    return proxy_url

