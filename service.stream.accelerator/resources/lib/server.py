"""HTTP 206 Range Proxy Server with probe cache and control endpoint.

Fixes from consider.txt:
  - Probe cache: UpstreamProbeResult cached per clean_url with 15-min TTL
  - Phase 2 GET uses final_url from Phase 1 HEAD (not clean_url) for TorBox
  - /control endpoint for sub-interpreter IPC (toggle, overlay, stats)
  - _proxy_enabled flag lives in this module (service interpreter), not stats.py
  - Content-Range only on 206 responses
  - Kodi curl flags (seekable, verifypeer) filtered from pipe headers
"""
import json
import re
import time
import threading
import requests
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote, parse_qsl

try:
    import xbmc
    import xbmcaddon
    _HAS_KODI = True
except ImportError:
    _HAS_KODI = False

from .engine import ParallelRangeStream
from .stats import StreamStats
from .logutil import log_info, log_warning, log_error, log_verbose

TAG = "StreamAccelerator.Server"

RANGE_RE = re.compile(r"bytes=(\d+)-(\d*)")

_DEFAULTS = {
    "port": 48990,
    "threads": 6,
    "chunk_size_kb": 1024,
    "read_ahead": 12,
    "retries": 3,
    "small_read_threshold_mb": 4,
}

# ── Module-level state (lives in the service interpreter) ─────────────────
_proxy_enabled = True          # Toggled via /control?action=toggle
_proxy_enabled_lock = threading.Lock()

# ── Probe cache (15-minute TTL) ──────────────────────────────────────────
_PROBE_CACHE = {}              # {clean_url: (UpstreamProbeResult, timestamp)}
_PROBE_CACHE_LOCK = threading.Lock()
_PROBE_CACHE_TTL = 900         # 15 minutes


def parse_kodi_url(raw_url):
    """Split a Kodi URL|Header1=Val1&Header2=Val2 into (clean_url, headers_dict).
    Filters out Kodi-internal curl parameters (seekable, verifypeer).
    """
    if "|" not in raw_url:
        return raw_url, {}
    clean_url, header_str = raw_url.split("|", 1)
    raw_params = dict(parse_qsl(header_str))
    kodi_curl_flags = {"seekable", "verifypeer"}
    headers = {k: v for k, v in raw_params.items() if k.lower() not in kodi_curl_flags}
    return clean_url, headers


def _get_addon_setting(key, fallback=None):
    if not _HAS_KODI:
        return _DEFAULTS.get(key, fallback)
    try:
        addon = xbmcaddon.Addon("service.stream.accelerator")
        val = addon.getSetting(key)
        if val:
            return val
    except Exception:
        pass
    return _DEFAULTS.get(key, fallback)


class UpstreamProbeResult:
    __slots__ = ("total_size", "content_type", "final_url", "supports_ranges")

    def __init__(self, total_size, content_type, final_url, supports_ranges):
        self.total_size = total_size
        self.content_type = content_type
        self.final_url = final_url
        self.supports_ranges = supports_ranges


def _get_cached_probe(clean_url):
    """Return cached probe result if still valid, else None."""
    with _PROBE_CACHE_LOCK:
        entry = _PROBE_CACHE.get(clean_url)
        if entry is None:
            return None
        result, ts = entry
        if time.monotonic() - ts > _PROBE_CACHE_TTL:
            del _PROBE_CACHE[clean_url]
            return None
        return result


def _set_cached_probe(clean_url, result):
    with _PROBE_CACHE_LOCK:
        _PROBE_CACHE[clean_url] = (result, time.monotonic())


def probe_upstream(clean_url, headers):
    """Probe upstream for file size, content type, final URL, and range support.

    Uses a two-phase strategy:
      Phase 1: HEAD (cheapest)
      Phase 2: GET Range: bytes=0-0 (fallback, uses final_url from Phase 1)

    Results are cached per clean_url for 15 minutes so FFmpeg's multiple
    startup requests (header → cues → video) don't re-probe (and don't
    burn single-use redirect tokens on TorBox).
    """
    cached = _get_cached_probe(clean_url)
    if cached is not None:
        log_verbose(
            TAG,
            f"Probe CACHE HIT: {urlparse(clean_url).netloc} -> "
            f"size={cached.total_size}, ranges={cached.supports_ranges}"
        )
        return cached

    total_size = 0
    content_type = "application/octet-stream"
    final_url = clean_url
    supports_ranges = False

    # Phase 1: HEAD
    t0 = time.monotonic()
    try:
        resp = requests.head(clean_url, headers=headers.copy(), timeout=10,
                             allow_redirects=True)
        head_ms = (time.monotonic() - t0) * 1000.0
        final_url = resp.url
        content_type = resp.headers.get("Content-Type", content_type)
        if "Content-Length" in resp.headers:
            total_size = int(resp.headers["Content-Length"])
        if resp.headers.get("Accept-Ranges", "").lower() == "bytes":
            supports_ranges = True
        head_status = resp.status_code
        resp.close()
        log_verbose(
            TAG,
            f"HEAD probe ({head_ms:.0f}ms): status={head_status}, size={total_size}, "
            f"type={content_type}, ranges={supports_ranges}, "
            f"final_host={urlparse(final_url).netloc}"
        )
        if total_size > 0 and supports_ranges:
            result = UpstreamProbeResult(total_size, content_type, final_url, True)
            _set_cached_probe(clean_url, result)
            return result
    except Exception as e:
        log_warning(TAG, f"HEAD probe failed ({(time.monotonic() - t0)*1000:.0f}ms): {e}, falling back to GET probe")

    # Phase 2: GET Range: bytes=0-0
    # CRITICAL: Use final_url (from HEAD redirect), NOT clean_url.
    # This prevents burning a second single-use redirect token on TorBox.
    probe_url = final_url if final_url != clean_url else clean_url
    t1 = time.monotonic()
    try:
        get_headers = headers.copy()
        get_headers["Range"] = "bytes=0-0"
        resp = requests.get(probe_url, headers=get_headers, stream=True,
                            timeout=10, allow_redirects=True)
        get_ms = (time.monotonic() - t1) * 1000.0
        final_url = resp.url
        content_type = resp.headers.get("Content-Type", content_type)

        if resp.status_code == 206:
            supports_ranges = True
            if "Content-Range" in resp.headers:
                cr = resp.headers["Content-Range"]
                if "/" in cr:
                    size_str = cr.split("/")[-1].strip()
                    if size_str != "*":
                        total_size = int(size_str)
        elif resp.status_code == 200:
            supports_ranges = False
            if "Content-Length" in resp.headers:
                total_size = int(resp.headers["Content-Length"])
        resp_status = resp.status_code
        resp.close()
        log_verbose(
            TAG,
            f"GET probe ({get_ms:.0f}ms): status={resp_status}, size={total_size}, "
            f"type={content_type}, ranges={supports_ranges}, "
            f"final_host={urlparse(final_url).netloc}"
        )
    except Exception as e:
        log_error(TAG, f"GET probe failed ({(time.monotonic() - t1)*1000:.0f}ms): {e}")

    if total_size <= 0:
        raise ValueError(
            "Cannot determine file size: upstream returned no Content-Length or "
            "Content-Range. The URL may be invalid or expired."
        )

    result = UpstreamProbeResult(total_size, content_type, final_url, supports_ranges)
    _set_cached_probe(clean_url, result)
    return result


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass

    def do_HEAD(self):
        if urlparse(self.path).path == "/stream":
            self._handle_request(send_body=False)
        else:
            self.send_error(404)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/stream":
            self._handle_request(send_body=True)
        elif parsed.path == "/control":
            self._handle_control()
        else:
            self.send_error(404)

    def _send_json(self, payload_obj):
        body = json.dumps(payload_obj).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    # ── /control endpoint (sub-interpreter IPC) ──────────────────────────
    def _handle_control(self):
        """Handle control commands from script.py via HTTP.

        Since RunScript() spawns a separate Python interpreter, script.py
        cannot share memory with service.py. Instead it sends HTTP requests
        to this endpoint which runs in the service interpreter.
        """
        global _proxy_enabled
        params = parse_qs(urlparse(self.path).query)
        action = params.get("action", [""])[0]
        log_verbose(TAG, f"/control?action={action}")

        if action == "toggle":
            with _proxy_enabled_lock:
                _proxy_enabled = not _proxy_enabled
                state = _proxy_enabled
            log_info(TAG, f"Proxy toggled: {'enabled' if state else 'disabled'}")

            # Show notification from within the service interpreter
            if _HAS_KODI:
                import xbmcgui
                label = "ON — next stream will be accelerated" if state else "OFF — next stream goes direct"
                xbmcgui.Dialog().notification(
                    "Stream Accelerator", f"Proxy {label}",
                    xbmcgui.NOTIFICATION_INFO, 3000
                )

            self._send_json({"enabled": state})

        elif action == "overlay":
            # Toggle overlay from within the service interpreter
            if _HAS_KODI:
                try:
                    addon = xbmcaddon.Addon("service.stream.accelerator")
                    overlay_allowed = addon.getSettingBool("overlay_enabled")
                except Exception:
                    overlay_allowed = True
                if overlay_allowed:
                    from .overlay import toggle_overlay
                    toggle_overlay()
                else:
                    log_verbose(TAG, "Overlay hotkey ignored (overlay_enabled=false in settings)")

            self._send_json({"ok": True})

        elif action == "stats":
            stats = StreamStats.instance()
            data = stats.to_dict()
            with _proxy_enabled_lock:
                data["proxy_enabled"] = _proxy_enabled

            self._send_json(data)

        else:
            self.send_error(400, f"Unknown control action: {action}")

    # ── /stream endpoint ─────────────────────────────────────────────────
    def _handle_request(self, send_body=True):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        if "url" not in params:
            self.send_error(400, "Missing 'url' parameter")
            return

        target_url = unquote(params["url"][0])
        clean_url, upstream_headers = parse_kodi_url(target_url)

        threads = int(params.get("threads", [_get_addon_setting("threads", 6)])[0])
        chunk_kb = int(params.get("chunk_kb", [_get_addon_setting("chunk_size_kb", 1024)])[0])
        read_ahead = int(params.get("read_ahead", [_get_addon_setting("read_ahead", 12)])[0])
        retries = int(_get_addon_setting("retries", 3))
        small_threshold = int(_get_addon_setting("small_read_threshold_mb", 2)) * 1024 * 1024
        chunk_size = chunk_kb * 1024

        try:
            probe = probe_upstream(clean_url, upstream_headers)
        except Exception as e:
            log_error(TAG, f"Upstream probe failed: {e}")
            self.send_error(502, f"Upstream probe failed: {e}")
            return

        total_size = probe.total_size
        content_type = probe.content_type
        final_url = probe.final_url

        # Parse Range header from Kodi
        range_header = self.headers.get("Range")
        start_byte = 0
        end_byte = total_size - 1

        if range_header:
            match = RANGE_RE.search(range_header)
            if match:
                start_byte = int(match.group(1))
                if match.group(2):
                    end_byte = min(int(match.group(2)), total_size - 1)

        content_length = (end_byte - start_byte) + 1
        method = "GET" if send_body else "HEAD"
        log_verbose(
            TAG,
            f"Incoming {method}: host={urlparse(clean_url).netloc}, "
            f"Range={range_header or 'none'} -> bytes {start_byte}-{end_byte} "
            f"({content_length:,} B / {total_size:,} B)"
        )

        # Send response headers
        if range_header:
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start_byte}-{end_byte}/{total_size}")
        else:
            self.send_response(200)

        self.send_header("Content-Type", content_type)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(content_length))
        self.send_header("Connection", "close")
        self.end_headers()

        if not send_body:
            return

        stats = StreamStats.instance()

        # Check if proxy is disabled
        with _proxy_enabled_lock:
            enabled = _proxy_enabled

        if not enabled:
            log_verbose(TAG, "Mode=DIRECT (proxy toggled OFF by user)")
            sid = stats.reset(total_size, content_type, clean_url, 1, 0, 0, mode="direct")
            self._direct_passthrough(final_url, upstream_headers, start_byte, end_byte, sid)
            return

        if not probe.supports_ranges:
            log_verbose(TAG, "Mode=DIRECT (upstream does not support byte ranges)")
            sid = stats.reset(total_size, content_type, clean_url, 1, 0, 0, mode="direct")
            self._direct_passthrough(final_url, upstream_headers, start_byte, end_byte, sid)
            return

        # Small read bypass
        if content_length <= small_threshold:
            log_verbose(
                TAG,
                f"Mode=BYPASS_SMALL ({content_length:,} B <= threshold {small_threshold:,} B)"
            )
            sid = stats.reset(total_size, content_type, clean_url, 1, chunk_size, 1, mode="bypass_small")
            self._single_range_fetch(final_url, upstream_headers, start_byte, end_byte, sid)
            return

        # Full parallel acceleration
        log_verbose(
            TAG,
            f"Mode=ACCELERATED: {content_length:,} B, threads={threads}, "
            f"chunk={chunk_kb}KB, read_ahead={read_ahead}, retries={retries}"
        )
        sid = stats.reset(total_size, content_type, clean_url, threads, chunk_size, read_ahead, mode="accelerated")

        streamer = ParallelRangeStream(
            clean_url=final_url,
            headers=upstream_headers,
            start_byte=start_byte,
            end_byte=end_byte,
            threads=threads,
            chunk_size=chunk_size,
            read_ahead=read_ahead,
            retries=retries,
        )
        streamer.set_stream_id(sid)

        bytes_sent = 0
        try:
            for chunk in streamer.stream_chunks():
                self.wfile.write(chunk)
                self.wfile.flush()
                bytes_sent += len(chunk)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            log_verbose(
                TAG,
                f"Client disconnected (seek/stop) on stream #{sid} after {bytes_sent:,} B"
            )
            stats.record_seek()
        finally:
            streamer.close()

    def _single_range_fetch(self, final_url, headers, start_byte, end_byte, stream_id):
        stats = StreamStats.instance()
        req_headers = headers.copy()
        req_headers["Range"] = f"bytes={start_byte}-{end_byte}"
        t0 = time.monotonic()
        sent = 0
        max_attempts = 3
        try:
            for attempt in range(max_attempts):
                resp = None
                try:
                    resp = requests.get(final_url, headers=req_headers, stream=True, timeout=(4, 8))
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            try:
                                self.wfile.write(chunk)
                                self.wfile.flush()
                            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                                log_verbose(TAG, f"Small read #{stream_id} client disconnected after {sent:,} B")
                                stats.record_seek()
                                return
                            sent += len(chunk)
                            stats.record_bytes_delivered(len(chunk))
                    log_verbose(
                        TAG,
                        f"Small read #{stream_id} complete: {sent:,} B in {(time.monotonic()-t0)*1000:.0f}ms"
                    )
                    return
                except requests.exceptions.RequestException as e:
                    if sent == 0 and attempt < max_attempts - 1:
                        log_warning(
                            TAG,
                            f"Small read #{stream_id} attempt {attempt+1}/{max_attempts} stalled/failed ({e}), retrying..."
                        )
                        time.sleep(0.2)
                        continue
                    log_warning(TAG, f"Small read #{stream_id} upstream error after {sent:,} B: {e}")
                    return
                except Exception as e:
                    log_warning(TAG, f"Small read #{stream_id} error: {e}")
                    return
                finally:
                    if resp is not None:
                        resp.close()
        finally:
            stats.stop(stream_id=stream_id)

    def _direct_passthrough(self, final_url, headers, start_byte, end_byte, stream_id):
        stats = StreamStats.instance()
        req_headers = headers.copy()
        req_headers["Range"] = f"bytes={start_byte}-{end_byte}"
        resp = None
        t0 = time.monotonic()
        sent = 0
        try:
            resp = requests.get(final_url, headers=req_headers, stream=True, timeout=(5, 60))
            for chunk in resp.iter_content(chunk_size=131072):
                if chunk:
                    try:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                        elapsed = max(0.001, time.monotonic() - t0)
                        mbps = (sent * 8) / (elapsed * 1_000_000)
                        log_verbose(
                            TAG,
                            f"Direct passthrough #{stream_id} disconnected: {sent:,} B in {elapsed:.1f}s ({mbps:.1f} Mbit/s)"
                        )
                        stats.record_seek()
                        return
                    sent += len(chunk)
                    stats.record_bytes_delivered(len(chunk))
            elapsed = max(0.001, time.monotonic() - t0)
            mbps = (sent * 8) / (elapsed * 1_000_000)
            log_verbose(
                TAG,
                f"Direct passthrough #{stream_id} finished: {sent:,} B in {elapsed:.1f}s ({mbps:.1f} Mbit/s)"
            )
        except Exception as e:
            log_warning(TAG, f"Direct passthrough #{stream_id} error: {e}")
        finally:
            if resp is not None:
                resp.close()
            stats.stop(stream_id=stream_id)
