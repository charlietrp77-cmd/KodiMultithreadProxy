"""HTTP 206 Range Proxy Server.

Handles Kodi's HTTP range requests and dispatches them to the parallel
chunk engine or a direct single-connection passthrough.

Fixes applied from design review:
  - HEAD probe first, GET bytes=0-0 fallback (fix #5)
  - Resolve redirects once in probe (fix #2)
  - Validate range support — reject 200 (fix #1)
  - Handle total_size=0 with HEAD fallback (fix #4)
  - Small-read bypass for requests < threshold (fix #3)
  - Content-Range only on 206 responses (fix #7)
  - Parse Kodi URL once, pass clean values (fix #12)
  - Comprehensive logging (fix #10)
  - Stats integration for overlay and toggle
"""
import re
import requests
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote, parse_qsl

try:
    import xbmc
    import xbmcaddon
    _HAS_KODI = True
    def _log(msg, level=None):
        if level is None:
            level = xbmc.LOGDEBUG
        xbmc.log(f"[StreamAccelerator.Server] {msg}", level)
except ImportError:
    _HAS_KODI = False
    def _log(msg, level=None):
        pass

from .engine import ParallelRangeStream
from .stats import StreamStats

RANGE_RE = re.compile(r"bytes=(\d+)-(\d*)")

# Default settings when Kodi is not available (for testing)
_DEFAULTS = {
    "port": 48990,
    "threads": 6,
    "chunk_size_kb": 1024,
    "read_ahead": 12,
    "retries": 3,
    "small_read_threshold_mb": 2,
}


def parse_kodi_url(raw_url):
    """Split a Kodi URL|Header1=Val1&Header2=Val2 into (clean_url, headers_dict).

    Kodi-style pipe headers use & as separator and = for key-value pairs,
    identical to URL query string encoding.
    """
    if "|" not in raw_url:
        return raw_url, {}
    clean_url, header_str = raw_url.split("|", 1)
    headers = dict(parse_qsl(header_str))
    return clean_url, headers


def _get_addon_setting(key, fallback=None):
    """Read a setting from the Kodi addon, with fallback."""
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
    """Result of probing the upstream URL for size, type, and capabilities."""
    __slots__ = ("total_size", "content_type", "final_url", "supports_ranges")

    def __init__(self, total_size, content_type, final_url, supports_ranges):
        self.total_size = total_size
        self.content_type = content_type
        self.final_url = final_url
        self.supports_ranges = supports_ranges


def probe_upstream(clean_url, headers):
    """Probe the upstream server for file size, content type, and range support.

    Strategy:
      1. Try HEAD first (cheapest, no body transfer).
      2. If HEAD doesn't give us Content-Length or Accept-Ranges, fall back
         to GET with Range: bytes=0-0 to check for 206 support.
      3. Capture the final URL after redirects (critical for TorBox/debrid).

    Returns:
        UpstreamProbeResult

    Raises:
        ValueError: If the file size cannot be determined.
    """
    total_size = 0
    content_type = "application/octet-stream"
    final_url = clean_url
    supports_ranges = False

    # ── Step 1: HEAD probe ────────────────────────────────────────────────
    try:
        head_headers = headers.copy()
        resp = requests.head(clean_url, headers=head_headers, timeout=10,
                             allow_redirects=True)
        final_url = resp.url  # Capture post-redirect URL
        content_type = resp.headers.get("Content-Type", content_type)

        if "Content-Length" in resp.headers:
            total_size = int(resp.headers["Content-Length"])
        if resp.headers.get("Accept-Ranges", "").lower() == "bytes":
            supports_ranges = True

        resp.close()
        _log(f"HEAD probe: size={total_size}, type={content_type}, "
             f"ranges={supports_ranges}, final_url={final_url[:100]}")

        if total_size > 0 and supports_ranges:
            return UpstreamProbeResult(total_size, content_type, final_url, True)
    except Exception as e:
        _log(f"HEAD probe failed: {e}, falling back to GET probe")

    # ── Step 2: GET with Range: bytes=0-0 ─────────────────────────────────
    try:
        get_headers = headers.copy()
        get_headers["Range"] = "bytes=0-0"
        resp = requests.get(clean_url, headers=get_headers, stream=True,
                            timeout=10, allow_redirects=True)
        final_url = resp.url
        content_type = resp.headers.get("Content-Type", content_type)

        if resp.status_code == 206:
            supports_ranges = True
            if "Content-Range" in resp.headers:
                # Format: bytes 0-0/12345678
                cr = resp.headers["Content-Range"]
                if "/" in cr:
                    size_str = cr.split("/")[-1].strip()
                    if size_str != "*":
                        total_size = int(size_str)
        elif resp.status_code == 200:
            # Server doesn't support ranges — try Content-Length
            supports_ranges = False
            if "Content-Length" in resp.headers:
                total_size = int(resp.headers["Content-Length"])

        resp.close()
        _log(f"GET probe: size={total_size}, type={content_type}, "
             f"ranges={supports_ranges}, status={resp.status_code}")
    except Exception as e:
        _log(f"GET probe also failed: {e}")

    if total_size <= 0:
        raise ValueError(
            "Cannot determine file size: upstream returned no Content-Length or "
            "Content-Range. The URL may be invalid, expired, or the server may "
            "use chunked transfer encoding without range support."
        )

    return UpstreamProbeResult(total_size, content_type, final_url, supports_ranges)


class ProxyHandler(BaseHTTPRequestHandler):
    """Handles Kodi's HTTP requests and proxies them through the accelerator."""

    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass  # Silence per-request logs; we have our own logging

    def do_HEAD(self):
        self._handle_request(send_body=False)

    def do_GET(self):
        self._handle_request(send_body=True)

    def _handle_request(self, send_body=True):
        parsed = urlparse(self.path)
        if parsed.path != "/stream":
            self.send_error(404)
            return

        params = parse_qs(parsed.query)
        if "url" not in params:
            self.send_error(400, "Missing 'url' parameter")
            return

        target_url = unquote(params["url"][0])

        # ── Parse Kodi URL once (fix #12) ─────────────────────────────────
        clean_url, upstream_headers = parse_kodi_url(target_url)

        # ── Read settings (with per-URL overrides) ────────────────────────
        threads = int(params.get("threads", [_get_addon_setting("threads", 6)])[0])
        chunk_kb = int(params.get("chunk_kb", [_get_addon_setting("chunk_size_kb", 1024)])[0])
        read_ahead = int(params.get("read_ahead", [_get_addon_setting("read_ahead", 12)])[0])
        retries = int(_get_addon_setting("retries", 3))
        small_threshold = int(_get_addon_setting("small_read_threshold_mb", 2)) * 1024 * 1024
        chunk_size = chunk_kb * 1024

        # ── Probe upstream ────────────────────────────────────────────────
        try:
            probe = probe_upstream(clean_url, upstream_headers)
        except Exception as e:
            _log(f"Upstream probe failed: {e}", xbmc.LOGERROR if _HAS_KODI else None)
            self.send_error(502, f"Upstream probe failed: {e}")
            return

        total_size = probe.total_size
        content_type = probe.content_type
        final_url = probe.final_url

        _log(f"Request: {clean_url[:80]}... → resolved to {final_url[:80]}...")
        _log(f"File: {total_size} bytes, type={content_type}, "
             f"ranges={'yes' if probe.supports_ranges else 'NO'}")

        # ── Parse Range header from Kodi ──────────────────────────────────
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

        # ── Send response headers ────────────────────────────────────────
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

        # ── Check if proxy is disabled (user toggle) ─────────────────────
        stats = StreamStats.instance()

        if not stats.enabled:
            _log("Proxy disabled by user — direct passthrough")
            stats.reset(total_size, content_type, clean_url, 1, 0, 0, mode="direct")
            self._direct_passthrough(final_url, upstream_headers, start_byte, end_byte)
            return

        # ── Check if upstream supports ranges ─────────────────────────────
        if not probe.supports_ranges:
            _log("Upstream doesn't support ranges — falling back to direct passthrough")
            stats.reset(total_size, content_type, clean_url, 1, 0, 0, mode="direct")
            self._direct_passthrough(final_url, upstream_headers, start_byte, end_byte)
            return

        # ── Small read bypass (fix #3) ────────────────────────────────────
        if content_length <= small_threshold:
            _log(f"Small read ({content_length} bytes ≤ {small_threshold} threshold) — single request")
            stats.reset(total_size, content_type, clean_url, 1, chunk_size, 1, mode="bypass_small")
            self._single_range_fetch(final_url, upstream_headers, start_byte, end_byte)
            return

        # ── Full parallel acceleration ────────────────────────────────────
        _log(f"Parallel stream: {content_length} bytes, {threads} threads, "
             f"{chunk_kb}KB chunks, read_ahead={read_ahead}")
        stats.reset(total_size, content_type, clean_url, threads, chunk_size, read_ahead, mode="accelerated")

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

        try:
            for chunk in streamer.stream_chunks():
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            # Normal: Kodi closed the connection (seek, stop, etc.)
            _log("Client disconnected (seek/stop)")
            stats.record_seek()
        finally:
            streamer.close()

    def _single_range_fetch(self, final_url, headers, start_byte, end_byte):
        """Fetch a small byte range in a single request (no thread pool)."""
        stats = StreamStats.instance()
        req_headers = headers.copy()
        req_headers["Range"] = f"bytes={start_byte}-{end_byte}"

        resp = None
        try:
            resp = requests.get(final_url, headers=req_headers,
                                stream=True, timeout=(5, 30))
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    self.wfile.write(chunk)
                    self.wfile.flush()
                    stats.record_bytes_delivered(len(chunk))
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            _log("Client disconnected during small read")
            stats.record_seek()
        except Exception as e:
            _log(f"Small read error: {e}")
        finally:
            if resp is not None:
                resp.close()
            stats.stop()

    def _direct_passthrough(self, final_url, headers, start_byte, end_byte):
        """Single-connection passthrough when proxy is disabled or ranges unsupported."""
        stats = StreamStats.instance()
        req_headers = headers.copy()
        req_headers["Range"] = f"bytes={start_byte}-{end_byte}"

        resp = None
        try:
            resp = requests.get(final_url, headers=req_headers,
                                stream=True, timeout=(5, 60))
            for chunk in resp.iter_content(chunk_size=131072):
                if chunk:
                    self.wfile.write(chunk)
                    self.wfile.flush()
                    stats.record_bytes_delivered(len(chunk))
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            _log("Client disconnected during direct passthrough")
            stats.record_seek()
        except Exception as e:
            _log(f"Direct passthrough error: {e}")
        finally:
            if resp is not None:
                resp.close()
            stats.stop()

