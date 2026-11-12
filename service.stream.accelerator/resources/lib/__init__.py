"""stream_accelerator — importable helper for other Kodi add-ons.

Usage from any add-on:
    import stream_accelerator
    proxied_url = stream_accelerator.proxify(raw_url, threads=8)
    xbmcplugin.setResolvedUrl(handle, True, xbmcgui.ListItem(path=proxied_url))
"""
from urllib.parse import quote

try:
    import xbmcaddon
except ImportError:
    xbmcaddon = None


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
    port = "48990"
    if xbmcaddon:
        try:
            addon = xbmcaddon.Addon("service.stream.accelerator")
            port = addon.getSetting("port") or "48990"
        except Exception:
            pass

    proxy_url = f"http://127.0.0.1:{port}/stream?url={quote(url, safe='')}"
    if threads is not None:
        proxy_url += f"&threads={int(threads)}"
    if chunk_kb is not None:
        proxy_url += f"&chunk_kb={int(chunk_kb)}"
    if read_ahead is not None:
        proxy_url += f"&read_ahead={int(read_ahead)}"
    return proxy_url

