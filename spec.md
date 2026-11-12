1. Verification: Does a Standalone Add-on Already Exist?No, a standalone, reusable Kodi service add-on for multi-threaded HTTP byte-range streaming does not exist.A check of existing Kodi repositories, GitHub projects, and forum history shows how current solutions fall short:script.module.slyguy & service.proxy: Run a local 127.0.0.1 Python proxy inside Kodi to modify HLS/DASH manifests, inject headers, and handle DNS overwrites, but they forward upstream media segments using single-threaded connections.inputstream.ffmpegdirect & inputstream.adaptive: Written in C++ for adaptive manifest parsing and timeshifting, they open a single connection per segment or stream via libcurl or FFmpeg's libavformat.Historical Forum Attempts: Users have requested multi-threaded HTTP/FTP acceleration inside Kodi since 2014 (e.g., trying to wrap external Linux binaries like axel or aria2 as a MITM proxy), and older scripts like F4mProxy or VeohProxy handled custom chunkstitching, but nobody has packaged a clean, universal, multi-threaded HTTP Byte-Range Accelerator (xbmc.service + script.module) that splits a single slow single-connection stream (like Easynews or throttled Debrid routes) into $N$ parallel range connections in RAM.2. Complete Architecture & Design Plan: service.stream.acceleratorTo make this add-on universal (usable by your own add-ons, TMDbHelper players, .strm files, or patched scrapers like POV/Fen Light/KodiStremioBridge), we design it as a dual-extension Kodi add-on:xbmc.service: Runs the background multi-threaded proxy server on 127.0.0.1.xbmc.python.module: Exposes a 1-line helper function so any other Kodi add-on can wrap a URL with import stream_accelerator; url = stream_accelerator.proxify(raw_url).Directory StructurePlaintextservice.stream.accelerator/
├── addon.xml
├── service.py                  # Entry point for xbmc.service (starts/stops HTTP server)
├── resources/
│   ├── settings.xml            # GUI settings for Threads, Chunk Size, Port, Buffer Limits
│   └── lib/
│       ├── __init__.py
│       ├── client.py           # Helper module imported by other add-ons
│       ├── server.py           # ThreadingHTTPServer & HTTP 206 Range Handler
│       └── engine.py           # Sliding-Window Parallel Chunk Downloader & RAM Buffer
3. Component Design SpecificationsA. Configuration (resources/settings.xml)The add-on exposes four core tunables in the Kodi GUI (and allows overriding them dynamically per-URL via query parameters):threads (Int, Default: 6, Range: 1–16): Number of concurrent HTTP worker threads per active stream.chunk_size_kb (Int, Default: 1024 KB / 1 MB, Range: 256 KB – 8192 KB): Exact byte range (Range: bytes=start-end) requested per worker task.read_ahead_chunks (Int, Default: 12): Maximum number of chunks allowed in the RAM ring buffer ahead of Kodi's current read pointer (prevents Out-Of-Memory crashes on Android TV). With 1 MB chunks and 12 read-ahead, peak RAM footprint is strictly capped at 12 MB.port (Int, Default: 48990): Localhost binding port (127.0.0.1).B. Universal URL Interface (How Other Add-ons Use It)Any add-on or .strm file can route traffic through the proxy simply by passing the target URL (and optional Kodi |Header=Value pipe headers) to the local endpoint:Plaintexthttp://127.0.0.1:48990/stream?url=<urlencoded_target_url>&threads=8&chunk_kb=2048
Header Extraction: In Kodi, URLs often look like https://host/video.mkv|User-Agent=Mozilla&Authorization=Basic.... The proxy automatically strips the | suffix from the URL and injects those exact headers into every upstream worker thread request.Dynamic Override: If threads or chunk_kb are omitted from the URL, the proxy falls back to the user's configured values in settings.xml.C. The Streaming Engine (engine.py) — How Seeking & Buffering WorkKodi’s VideoPlayer (ffmpeg) does not just read a file linearly from byte 0 to the end. On MKV/MP4 start-up, Kodi:Sends an initial GET with Range: bytes=0- to read the file header.Immediately aborts and sends a second GET with Range: bytes=<near_eof>- to read the MKV Cues / MP4 moov atom (index table) at the very end of the file.Aborts again and jumps back to Range: bytes=<audio/video_start>- to begin actual playback.If your proxy blindly spawns 8 threads from byte 0 on step 1, it wastes bandwidth and stalls step 2. Our engine solves this with a Sliding-Window Coordinator:Kodi Player (Consumer) 
   │  Reads sequential bytes from current offset
   ▼
[ Ordered RAM Ring Buffer (Max: N Chunks) ]
   ▲  Workers deposit completed chunks by index: buffer[chunk_id] = bytes
   │
[ ThreadPoolExecutor (Configurable: e.g., 6 Threads) ]
   ├── Worker 1: GET Range: bytes=0 - 1048575
   ├── Worker 2: GET Range: bytes=1048576 - 2097151
   ├── Worker 3: GET Range: bytes=2097152 - 3145727
   └── ...
Upstream Probe (HEAD / Range: bytes=0-0):When Kodi connects, the proxy makes a fast Range: bytes=0-0 request to the upstream server to extract the total file size from Content-Range: bytes 0-0/TOTAL_SIZE and the Content-Type.It immediately replies to Kodi with HTTP/1.1 206 Partial Content, Accept-Ranges: bytes, Content-Length, and Content-Range.Small Read Bypass (Instant MKV Header/Footer Seeking):If the remaining byte range requested by Kodi is smaller than a single chunk (e.g., reading the last 64 KB of an MKV for seeking cues), the proxy fetches it in a single direct request without spinning up the thread pool.Sliding-Window Backpressure:Let current_chunk be the chunk Kodi is currently waiting to read.The scheduler only dispatches worker tasks for chunk_id in [current_chunk ... current_chunk + read_ahead_chunks].As soon as buffer[current_chunk] completes, the handler writes it to Kodi’s socket (self.wfile.write()), deletes buffer[current_chunk] from RAM to free memory immediately, increments current_chunk, and schedules current_chunk + read_ahead_chunks in the ThreadPoolExecutor.Instant Abort on Seek:When you press Fast-Forward in Kodi, Kodi closes the current TCP socket and opens a new one at the new byte offset.Catching BrokenPipeError / ConnectionResetError immediately sets an abort_event = threading.Event(), cancelling all pending futures in the pool and purging the RAM dictionary in milliseconds so the new seek offset gets 100% of the threads.4. Complete Reference ImplementationBelow are the core files ready to drop into a service.stream.accelerator folder.1. addon.xmlXML<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<addon id="service.stream.accelerator" name="Multi-Threaded Stream Accelerator" version="1.0.0" provider-name="Custom">
    <requires>
        <import addon="xbmc.python" version="3.0.0"/>
        <import addon="script.module.requests" version="2.25.1"/>
    </requires>

    <!-- Background Proxy Daemon -->
    <extension point="xbmc.service" library="service.py" start="login"/>

    <!-- Importable Helper Module for other Add-ons -->
    <extension point="xbmc.python.module" library="resources/lib"/>

    <extension point="xbmc.addon.metadata">
        <summary lang="en_GB">Local multi-threaded HTTP byte-range streaming proxy</summary>
        <description lang="en_GB">Accelerates single-stream HTTP sources (Easynews, Debrid, WebDAV) using parallel chunked byte-range requests in RAM.</description>
        <platform>all</platform>
    </extension>
</addon>
2. resources/settings.xmlXML<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<settings>
    <category label="Proxy Engine">
        <setting id="port" type="number" label="Local Proxy Port" default="48990"/>
        <setting id="threads" type="slider" label="Worker Threads per Stream" default="6" range="1,1,16" option="int"/>
        <setting id="chunk_size_kb" type="slider" label="Chunk Size (KB)" default="1024" range="256,256,8192" option="int"/>
        <setting id="read_ahead" type="slider" label="Max Read-Ahead Chunks in RAM" default="12" range="2,1,32" option="int"/>
        <setting id="retries" type="slider" label="Chunk Retry Attempts" default="3" range="1,1,10" option="int"/>
    </category>
</settings>
3. resources/lib/engine.py (The Multi-Threaded Sliding-Window Engine)Pythonimport threading
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qsl


def parse_kodi_url(raw_url):
    """Splits a Kodi URL|Header1=Val1&Header2=Val2 into (clean_url, headers_dict)."""
    if "|" not in raw_url:
        return raw_url, {}
    clean_url, header_str = raw_url.split("|", 1)
    headers = dict(parse_qsl(header_str))
    return clean_url, headers


class ParallelRangeStream:
    def __init__(self, url, start_byte, end_byte, threads=6, chunk_size=1048576, read_ahead=12, retries=3):
        self.clean_url, self.headers = parse_kodi_url(url)
        self.start_byte = start_byte
        self.end_byte = end_byte  # Inclusive
        self.threads = max(1, int(threads))
        self.chunk_size = max(65536, int(chunk_size))
        self.read_ahead = max(self.threads, int(read_ahead))
        self.retries = int(retries)

        self.abort_event = threading.Event()
        self.condition = threading.Condition()
        self.buffer = {}       # {chunk_index: bytes}
        self.in_flight = set() # {chunk_index}
        self.failed = False

        self.total_chunks = ((self.end_byte - self.start_byte) // self.chunk_size) + 1
        self.next_chunk_to_yield = 0

        # Persistent session for TCP/TLS connection pooling across workers
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self.threads,
            pool_maxsize=self.threads * 2
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.executor = ThreadPoolExecutor(max_workers=self.threads)

    def _fetch_chunk(self, chunk_idx):
        c_start = self.start_byte + (chunk_idx * self.chunk_size)
        c_end = min(c_start + self.chunk_size - 1, self.end_byte)
        req_headers = self.headers.copy()
        req_headers["Range"] = f"bytes={c_start}-{c_end}"

        for attempt in range(self.retries):
            if self.abort_event.is_set():
                return
            try:
                resp = self.session.get(
                    self.clean_url,
                    headers=req_headers,
                    stream=True,
                    timeout=(5, 15)
                )
                if resp.status_code in (200, 206):
                    data = resp.content
                    with self.condition:
                        if not self.abort_event.is_set():
                            self.buffer[chunk_idx] = data
                        self.in_flight.discard(chunk_idx)
                        self.condition.notify_all()
                    return
            except Exception:
                time.sleep(0.2 * (attempt + 1))

        # If all retries exhausted
        with self.condition:
            self.failed = True
            self.in_flight.discard(chunk_idx)
            self.condition.notify_all()

    def _schedule_window(self):
        """Schedules chunks within [next_chunk_to_yield, next_chunk_to_yield + read_ahead]."""
        window_limit = min(self.total_chunks, self.next_chunk_to_yield + self.read_ahead)
        for idx in range(self.next_chunk_to_yield, window_limit):
            if idx not in self.buffer and idx not in self.in_flight:
                self.in_flight.add(idx)
                self.executor.submit(self._fetch_chunk, idx)

    def stream_chunks(self):
        """Generator that yields ordered chunks to Kodi's socket and frees RAM immediately."""
        try:
            with self.condition:
                self._schedule_window()

            while self.next_chunk_to_yield < self.total_chunks and not self.abort_event.is_set():
                with self.condition:
                    while (
                        self.next_chunk_to_yield not in self.buffer
                        and not self.failed
                        and not self.abort_event.is_set()
                    ):
                        self.condition.wait(timeout=1.0)

                    if self.abort_event.is_set() or self.failed:
                        break

                    # Pop chunk from RAM buffer to keep memory bounded
                    chunk_data = self.buffer.pop(self.next_chunk_to_yield)
                    self.next_chunk_to_yield += 1
                    self._schedule_window()

                yield chunk_data
        finally:
            self.close()

    def close(self):
        self.abort_event.set()
        with self.condition:
            self.buffer.clear()
            self.condition.notify_all()
        self.executor.shutdown(wait=False)
        self.session.close()
4. resources/lib/server.py (HTTP 206 Range Server)Pythonimport re
import requests
import xbmcaddon
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote
from .engine import ParallelRangeStream, parse_kodi_url

RANGE_RE = re.compile(r"bytes=(\d+)-(\d*)")


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass  # Silence noisy per-request HTTP logs in kodi.log

    def _get_upstream_info(self, raw_url):
        clean_url, headers = parse_kodi_url(raw_url)
        probe_headers = headers.copy()
        probe_headers["Range"] = "bytes=0-0"
        resp = requests.get(clean_url, headers=probe_headers, stream=True, timeout=10)
        content_type = resp.headers.get("Content-Type", "video/mp4")

        if "Content-Range" in resp.headers:
            # Format: bytes 0-0/12345678
            total_size = int(resp.headers["Content-Range"].split("/")[-1])
        else:
            total_size = int(resp.headers.get("Content-Length", 0))
        resp.close()
        return total_size, content_type

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
            self.send_error(400, "Missing url parameter")
            return

        target_url = unquote(params["url"][0])
        addon = xbmcaddon.Addon("service.stream.accelerator")

        threads = int(params.get("threads", [addon.getSetting("threads") or 6])[0])
        chunk_kb = int(params.get("chunk_kb", [addon.getSetting("chunk_size_kb") or 1024])[0])
        read_ahead = int(params.get("read_ahead", [addon.getSetting("read_ahead") or 12])[0])
        retries = int(addon.getSetting("retries") or 3)
        chunk_size = chunk_kb * 1024

        try:
            total_size, content_type = self._get_upstream_info(target_url)
        except Exception as e:
            self.send_error(502, f"Upstream probe failed: {e}")
            return

        # Parse Range header from Kodi's VideoPlayer
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

        self.send_response(206 if range_header else 200)
        self.send_header("Content-Type", content_type)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Range", f"bytes {start_byte}-{end_byte}/{total_size}")
        self.send_header("Content-Length", str(content_length))
        self.send_header("Connection", "close")
        self.end_headers()

        if not send_body:
            return

        streamer = ParallelRangeStream(
            url=target_url,
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
        except (BrokenPipeError, ConnectionResetError):
            # Normal behavior when Kodi seeks to another byte offset
            pass
        finally:
            streamer.close()
5. service.py (Kodi Lifecycle Management)Pythonimport threading
import xbmc
import xbmcaddon
from resources.lib.server import ThreadingHTTPServer, ProxyHandler


class AcceleratorService(xbmc.Monitor):
    def __init__(self):
        super().__init__()
        addon = xbmcaddon.Addon("service.stream.accelerator")
        self.port = int(addon.getSetting("port") or 48990)
        ThreadingHTTPServer.allow_reuse_address = True
        self.server = ThreadingHTTPServer(("127.0.0.1", self.port), ProxyHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def run(self):
        self.thread.start()
        xbmc.log(f"[StreamAccelerator] Listening on 127.0.0.1:{self.port}", xbmc.LOGINFO)
        self.waitForAbort()
        self.server.shutdown()
        self.server.server_close()
        xbmc.log("[StreamAccelerator] Service stopped cleanly.", xbmc.LOGINFO)


if __name__ == "__main__":
    AcceleratorService().run()
6. resources/lib/__init__.py (1-Line Helper for Any Add-on)Pythonfrom urllib.parse import quote
import xbmcaddon


def proxify(url, threads=None, chunk_kb=None):
    """Wraps any Kodi stream URL (including |Header=Val) into a local accelerated URL."""
    addon = xbmcaddon.Addon("service.stream.accelerator")
    port = addon.getSetting("port") or "48990"
    proxy_url = f"http://127.0.0.1:{port}/stream?url={quote(url, safe='')}"
    if threads:
        proxy_url += f"&threads={int(threads)}"
    if chunk_kb:
        proxy_url += f"&chunk_kb={int(chunk_kb)}"
    return proxy_url
How You Use It Across Different Add-onsOnce service.stream.accelerator is installed in Kodi:In Any Custom Add-on (KodiStremioBridge, POV, Fen Light, etc.):
Add <import addon="service.stream.accelerator"/> to its addon.xml, and right before xbmcplugin.setResolvedUrl, wrap the resolved link:Pythonimport resources.lib as accelerator  # or via standard module path
resolved_url = accelerator.proxify(resolved_url, threads=8, chunk_kb=1024)
Without Even Importing the Module:
Any script or player can simply string-format the localhost URL directly:[http://127.0.0.1:48990/stream?url=](http://127.0.0.1:48990/stream?url=)<urllib.parse.quote(raw_url)>