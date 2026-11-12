"""OSD Stats Overlay and hotkey toggle handlers.

Fixes from consider.txt:
  - onAction() override prevents overlay from stealing keyboard/remote focus
  - Back/Escape closes overlay; all other keys pass through to player
  - Overlay is managed from the service interpreter (via /control endpoint)
"""
import os
import base64
import threading

try:
    import xbmc
    import xbmcgui
    import xbmcvfs
    import xbmcaddon
    _HAS_KODI = True
except ImportError:
    _HAS_KODI = False

from .stats import StreamStats

_WHITE_PIXEL_B64 = (
    b"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4"
    b"2mP8/58BAwAI/AL+hc2rNAAAAABJRU5ErkJggg=="
)

OVERLAY_WIDTH = 440
OVERLAY_HEIGHT = 310
MARGIN = 30

# Kodi action IDs
ACTION_PREVIOUS_MENU = 10
ACTION_NAV_BACK = 92
ACTION_BACKSPACE = 110


def _ensure_bg_texture():
    if not _HAS_KODI:
        return ""
    try:
        addon = xbmcaddon.Addon("service.stream.accelerator")
        profile = xbmcvfs.translatePath(addon.getAddonInfo("profile"))
        os.makedirs(profile, exist_ok=True)
        path = os.path.join(profile, "white.png")
        if not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(base64.b64decode(_WHITE_PIXEL_B64))
        return path
    except Exception:
        return ""


def _format_size(bytes_val):
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024 * 1024:
        return f"{bytes_val / 1024:.1f} KB"
    elif bytes_val < 1024 * 1024 * 1024:
        return f"{bytes_val / (1024 * 1024):.1f} MB"
    else:
        return f"{bytes_val / (1024 * 1024 * 1024):.2f} GB"


def _format_elapsed(seconds):
    seconds = int(seconds)
    if seconds < 3600:
        return f"{seconds // 60}:{seconds % 60:02d}"
    return f"{seconds // 3600}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"


class StatsOverlay(xbmcgui.WindowDialog):
    """Transparent OSD overlay showing live proxy streaming stats.

    Renders in the top-right corner. Updates every 500ms.
    Does NOT steal focus — onAction() passes all keys through except Back.
    """

    def __init__(self):
        super().__init__()
        self.stats = StreamStats.instance()
        self._running = False
        self._update_thread = None
        self._labels = {}
        self._bg_path = _ensure_bg_texture()
        self._build_ui()

    def onAction(self, action):
        """Handle keyboard/remote actions without stealing focus from player.

        Only Back/Escape closes the overlay. All other actions (pause, stop,
        seek, OSD toggle, volume) are ignored here so they fall through to
        the underlying Kodi player window.
        """
        action_id = action.getId()
        if action_id in (ACTION_PREVIOUS_MENU, ACTION_NAV_BACK, ACTION_BACKSPACE):
            self.hide_overlay()
        # All other actions: do nothing, let Kodi handle them

    def _build_ui(self):
        screen_w = self.getWidth() or 1920
        x = screen_w - OVERLAY_WIDTH - MARGIN
        y = MARGIN

        if self._bg_path:
            bg = xbmcgui.ControlImage(
                x, y, OVERLAY_WIDTH, OVERLAY_HEIGHT,
                self._bg_path, colorDiffuse="CC000000"
            )
            self.addControl(bg)

        title = xbmcgui.ControlLabel(
            x + 12, y + 8, OVERLAY_WIDTH - 24, 28,
            "[B]⚡ Stream Accelerator[/B]",
            textColor="FF00CCFF", font="font13"
        )
        self.addControl(title)

        self._status = xbmcgui.ControlLabel(
            x + 12, y + 38, OVERLAY_WIDTH - 24, 24,
            "", textColor="FF00FF00", font="font12"
        )
        self.addControl(self._status)

        if self._bg_path:
            sep = xbmcgui.ControlImage(
                x + 12, y + 64, OVERLAY_WIDTH - 24, 1,
                self._bg_path, colorDiffuse="66FFFFFF"
            )
            self.addControl(sep)

        fields = [
            ("speed", "FFFFFFFF"),
            ("avg_speed", "FFFFFFFF"),
            ("workers", "FFFFFFFF"),
            ("buffer", "FFFFFFFF"),
            ("delivered", "FFFFFFFF"),
            ("chunks", "FFFFFFFF"),
            ("seeks", "FFFFFFFF"),
            ("elapsed", "FFFFFFFF"),
            ("file_info", "FF999999"),
        ]
        for i, (field, color) in enumerate(fields):
            label = xbmcgui.ControlLabel(
                x + 12, y + 72 + (i * 25), OVERLAY_WIDTH - 24, 22,
                "", textColor=color, font="font10"
            )
            self.addControl(label)
            self._labels[field] = label

    def _update_loop(self):
        monitor = xbmc.Monitor()
        while self._running and not monitor.abortRequested():
            try:
                self._refresh()
            except Exception:
                pass
            monitor.waitForAbort(0.5)

    def _refresh(self):
        s = self.stats

        # Import here to read module-level flag from service interpreter
        try:
            from . import server as _srv
            proxy_enabled = _srv._proxy_enabled
        except Exception:
            proxy_enabled = True

        if proxy_enabled and s.active:
            mode_str = s.mode.upper()
            if s.mode == "accelerated":
                self._status.setLabel(f"[B]● PROXY ACTIVE — {mode_str}[/B]")
            elif s.mode == "bypass_small":
                self._status.setLabel("[B]● SMALL READ BYPASS (direct)[/B]")
            else:
                self._status.setLabel(f"[B]● {mode_str}[/B]")

            live_speed = s.get_speed_mbps()
            avg_speed = s.get_avg_speed_mbps()

            if live_speed >= 20:
                speed_color = "FF00FF00"
            elif live_speed >= 5:
                speed_color = "FFFFFF00"
            else:
                speed_color = "FFFF4444"

            self._labels["speed"].setLabel(
                f"[COLOR {speed_color}]Speed (live):   {live_speed:.1f} Mbit/s[/COLOR]"
            )
            self._labels["avg_speed"].setLabel(f"Speed (avg):    {avg_speed:.1f} Mbit/s")
            self._labels["workers"].setLabel(f"Workers:        {s.active_workers} / {s.threads_configured} active")

            buf_pct = (s.buffer_fill / max(1, s.buffer_max)) * 100
            if buf_pct >= 60:
                buf_color = "FF00FF00"
            elif buf_pct >= 25:
                buf_color = "FFFFFF00"
            else:
                buf_color = "FFFF4444"
            self._labels["buffer"].setLabel(
                f"Buffer:         [COLOR {buf_color}]{s.buffer_fill} / {s.buffer_max} chunks ({buf_pct:.0f}%)[/COLOR]"
            )

            delivered = _format_size(s.bytes_delivered)
            total = _format_size(s.file_size) if s.file_size else "?"
            self._labels["delivered"].setLabel(f"Delivered:      {delivered} / {total}")

            fail_str = f" [COLOR FFFF4444]/ {s.chunks_failed} FAILED[/COLOR]" if s.chunks_failed > 0 else ""
            retry_str = f" ({s.chunks_retried} retries)" if s.chunks_retried > 0 else ""
            self._labels["chunks"].setLabel(f"Chunks:         {s.chunks_completed} OK{fail_str}{retry_str}")
            self._labels["seeks"].setLabel(f"Seeks/aborts:   {s.seeks}")
            self._labels["elapsed"].setLabel(f"Elapsed:        {_format_elapsed(s.get_elapsed())}")

            chunk_kb = s.chunk_size // 1024 if s.chunk_size else 0
            self._labels["file_info"].setLabel(
                f"{s.content_type}  |  {chunk_kb} KB chunks  |  {s.threads_configured}T"
            )

        elif not proxy_enabled:
            self._status.setLabel("[B][COLOR FFFF8800]○ PROXY DISABLED — next stream goes direct[/COLOR][/B]")
            self._labels["speed"].setLabel("Toggle back on with Ctrl+Shift+D")
            for field in list(self._labels.keys())[1:]:
                self._labels[field].setLabel("")

        else:
            self._status.setLabel("[B][COLOR FF888888]◌ IDLE — no active stream[/COLOR][/B]")
            for lbl in self._labels.values():
                lbl.setLabel("")
            self._labels["speed"].setLabel("Start playing a proxied URL to see stats")

    def show_overlay(self):
        self.show()
        self._running = True
        self._update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self._update_thread.start()

    def hide_overlay(self):
        self._running = False
        self.close()


# ─── Global overlay instance (lives in service interpreter) ───────────────

_overlay = None
_overlay_lock = threading.Lock()


def toggle_overlay():
    global _overlay
    with _overlay_lock:
        if _overlay is not None and _overlay._running:
            _overlay.hide_overlay()
            _overlay = None
        else:
            _overlay = StatsOverlay()
            _overlay.show_overlay()
