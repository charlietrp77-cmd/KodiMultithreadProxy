"""Kodi service entry point — starts the proxy daemon and manages lifecycle.

This runs as an xbmc.service (start="login"), meaning it launches when
Kodi starts and runs until Kodi exits. It:
  1. Starts the ThreadingHTTPServer on 127.0.0.1:<port>
  2. Installs the hotkey keymap (Ctrl+D, Ctrl+Shift+D)
  3. Waits for Kodi abort signal
  4. Cleans up server, keymap, and threads
"""
import os
import threading
import xbmc
import xbmcaddon
import xbmcvfs
from http.server import ThreadingHTTPServer
from resources.lib.server import ProxyHandler
from resources.lib.logutil import refresh_verbose

# ── Keymap XML content ────────────────────────────────────────────────────────
# Ctrl+D (key 0x10064 = 65636): Toggle stats overlay
# Ctrl+Shift+D (key 0x30064 = 196708): Toggle proxy on/off
KEYMAP_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<keymap>
    <global>
        <keyboard>
            <key id="65636">RunScript(service.stream.accelerator,overlay)</key>
            <key id="196708">RunScript(service.stream.accelerator,toggle)</key>
        </keyboard>
    </global>
    <FullscreenVideo>
        <keyboard>
            <key id="65636">RunScript(service.stream.accelerator,overlay)</key>
            <key id="196708">RunScript(service.stream.accelerator,toggle)</key>
        </keyboard>
    </FullscreenVideo>
</keymap>
"""

KEYMAP_FILENAME = "accelerator_keymap.xml"


def _install_keymap():
    """Copy the hotkey keymap to Kodi's keymaps directory and reload."""
    try:
        keymaps_dir = xbmcvfs.translatePath("special://profile/keymaps/")
        os.makedirs(keymaps_dir, exist_ok=True)
        keymap_path = os.path.join(keymaps_dir, KEYMAP_FILENAME)
        with open(keymap_path, "w", encoding="utf-8") as f:
            f.write(KEYMAP_XML)
        xbmc.executebuiltin("Action(reloadkeymaps)")
        xbmc.log(f"[StreamAccelerator] Keymap installed: {keymap_path}", xbmc.LOGINFO)
    except Exception as e:
        xbmc.log(f"[StreamAccelerator] Failed to install keymap: {e}", xbmc.LOGWARNING)


def _uninstall_keymap():
    """Remove the hotkey keymap on shutdown."""
    try:
        keymaps_dir = xbmcvfs.translatePath("special://profile/keymaps/")
        keymap_path = os.path.join(keymaps_dir, KEYMAP_FILENAME)
        if os.path.exists(keymap_path):
            os.remove(keymap_path)
            xbmc.log(f"[StreamAccelerator] Keymap removed: {keymap_path}", xbmc.LOGINFO)
    except Exception as e:
        xbmc.log(f"[StreamAccelerator] Failed to remove keymap: {e}", xbmc.LOGWARNING)


def _write_active_port(port):
    """Persist the actual bound port so script.py and proxify() find fallback ports."""
    try:
        addon = xbmcaddon.Addon("service.stream.accelerator")
        profile = xbmcvfs.translatePath(addon.getAddonInfo("profile"))
        os.makedirs(profile, exist_ok=True)
        with open(os.path.join(profile, "active_port"), "w", encoding="utf-8") as f:
            f.write(str(port))
    except Exception as e:
        xbmc.log(f"[StreamAccelerator] Failed to write active_port: {e}", xbmc.LOGWARNING)


def _remove_active_port():
    """Clean up active_port marker on shutdown."""
    try:
        addon = xbmcaddon.Addon("service.stream.accelerator")
        profile = xbmcvfs.translatePath(addon.getAddonInfo("profile"))
        path = os.path.join(profile, "active_port")
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


class AcceleratorService(xbmc.Monitor):
    """Kodi service that runs the proxy server in a background thread."""

    def __init__(self):
        super().__init__()
        addon = xbmcaddon.Addon("service.stream.accelerator")
        self.port = int(addon.getSetting("port") or 48990)
        self.server = None
        self.thread = None

    def run(self):
        # ── Start the HTTP proxy server ───────────────────────────────────
        ThreadingHTTPServer.allow_reuse_address = True
        try:
            self.server = ThreadingHTTPServer(("127.0.0.1", self.port), ProxyHandler)
        except OSError as e:
            xbmc.log(
                f"[StreamAccelerator] FATAL: Cannot bind to 127.0.0.1:{self.port} — {e}. "
                f"Another instance may be running, or the port is in use. "
                f"Change the port in add-on settings.",
                xbmc.LOGERROR
            )
            # Try one fallback port
            fallback_port = self.port + 1
            try:
                self.server = ThreadingHTTPServer(("127.0.0.1", fallback_port), ProxyHandler)
                self.port = fallback_port
                xbmc.log(
                    f"[StreamAccelerator] Bound to fallback port {fallback_port}",
                    xbmc.LOGWARNING
                )
            except OSError:
                xbmc.log("[StreamAccelerator] Fallback port also failed. Service not started.", xbmc.LOGERROR)
                return

        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        _write_active_port(self.port)
        xbmc.log(
            f"[StreamAccelerator] Listening on 127.0.0.1:{self.port}",
            xbmc.LOGINFO
        )

        # ── Install hotkey keymap ─────────────────────────────────────────
        _install_keymap()

        # ── Wait for Kodi to exit ─────────────────────────────────────────
        self.waitForAbort()

        # ── Cleanup ──────────────────────────────────────────────────────
        xbmc.log("[StreamAccelerator] Shutting down...", xbmc.LOGINFO)
        _remove_active_port()
        _uninstall_keymap()
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        xbmc.log("[StreamAccelerator] Service stopped cleanly.", xbmc.LOGINFO)

    def onSettingsChanged(self):
        """Called by Kodi when the user changes add-on settings."""
        verbose = refresh_verbose()
        xbmc.log(
            f"[StreamAccelerator] Settings changed (verbose_logging={verbose}) — will take effect on next stream",
            xbmc.LOGINFO
        )


if __name__ == "__main__":
    AcceleratorService().run()

