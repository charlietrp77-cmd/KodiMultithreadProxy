"""Script entry point for RunScript() hotkey actions.

Called via:
  RunScript(service.stream.accelerator, overlay)   → Ctrl+D
  RunScript(service.stream.accelerator, toggle)    → Ctrl+Shift+D

CRITICAL: RunScript() runs in a SEPARATE Python sub-interpreter from
service.py. We CANNOT share memory (StreamStats, StatsOverlay, etc.)
with the service process. Instead, we send HTTP requests to the proxy's
/control endpoint which runs in the service interpreter.
"""
import sys

try:
    import xbmcaddon
except ImportError:
    xbmcaddon = None


def _get_port():
    port = "48990"
    if xbmcaddon:
        try:
            addon = xbmcaddon.Addon("service.stream.accelerator")
            port = addon.getSetting("port") or "48990"
        except Exception:
            pass
    return port


def _send_control(action):
    """Send a control command to the proxy server via HTTP."""
    import urllib.request
    port = _get_port()
    url = f"http://127.0.0.1:{port}/control?action={action}"
    try:
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req, timeout=3)
        resp.read()
        resp.close()
    except Exception as e:
        try:
            import xbmc
            xbmc.log(f"[StreamAccelerator.Script] Control request failed: {e}", xbmc.LOGWARNING)
        except ImportError:
            pass


def main():
    if len(sys.argv) < 2:
        return

    command = sys.argv[1].strip().lower()

    if command == "overlay":
        _send_control("overlay")
    elif command == "toggle":
        _send_control("toggle")


if __name__ == "__main__":
    main()
