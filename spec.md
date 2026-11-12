# service.stream.accelerator — Complete Specification

> Multi-threaded HTTP byte-range streaming proxy for Kodi.
> **Version:** 1.0.0 · **Status:** Implementation Complete · **Repo:** [github.com/charlietrp77-cmd/KodiMultithreadProxy](https://github.com/charlietrp77-cmd/KodiMultithreadProxy)

---

## Table of Contents

1. [Background & Motivation](#1-background--motivation)
2. [Architecture Overview](#2-architecture-overview)
3. [Directory Structure](#3-directory-structure)
4. [Provider Compatibility](#4-provider-compatibility)
5. [Component Design](#5-component-design)
   - 5A. [Configuration (settings.xml)](#5a-configuration)
   - 5B. [Universal URL Interface](#5b-universal-url-interface)
   - 5C. [Upstream Probe (server.py)](#5c-upstream-probe)
   - 5D. [Streaming Engine (engine.py)](#5d-streaming-engine)
   - 5E. [Stats Collector (stats.py)](#5e-stats-collector)
   - 5F. [OSD Overlay (overlay.py)](#5f-osd-stats-overlay)
   - 5G. [Hotkey System](#5g-hotkey-system)
   - 5H. [Service Lifecycle (service.py)](#5h-service-lifecycle)
   - 5I. [Client Helper (__init__.py)](#5i-client-helper)
6. [Request Flow — How a Stream Works End-to-End](#6-request-flow)
7. [Design Decisions & Fixes Applied](#7-design-decisions--fixes-applied)
8. [Testing & A/B Comparison](#8-testing--ab-comparison)
9. [Integration Guide](#9-integration-guide)
10. [Complete Implementation Reference](#10-complete-implementation-reference)

---

## 1. Background & Motivation

No standalone, reusable Kodi service add-on for multi-threaded HTTP byte-range streaming exists. Existing solutions fall short:

| Existing Solution | What It Does | Why It's Not Enough |
|---|---|---|
| **script.module.slyguy / service.proxy** | Local 127.0.0.1 Python proxy for HLS/DASH manifest modification, header injection, DNS overwrites | Single-threaded upstream connections |
| **inputstream.ffmpegdirect** | C++ adaptive manifest parsing, timeshifting | Single connection per segment via libcurl |
| **inputstream.adaptive** | DASH/HLS/Smooth adaptive streaming | Single connection per segment |
| **Historical attempts (F4mProxy, VeohProxy, axel/aria2 wrappers)** | Custom chunk stitching, external binary wrappers | Never packaged as clean reusable Kodi addon |

**The gap:** Nobody has built a clean `xbmc.service + xbmc.python.module` that splits a single slow HTTP stream (Easynews, throttled Debrid routes, WebDAV) into N parallel byte-range connections in RAM.

---

## 2. Architecture Overview

The add-on is a **triple-extension** Kodi package:

| Extension Point | Purpose |
|---|---|
| `xbmc.service` | Background daemon — starts the HTTP proxy on `127.0.0.1` at Kodi login |
| `xbmc.python.module` | Importable `proxify()` helper for other add-ons |
| `xbmc.python.script` | `RunScript()` entry point for hotkey actions (overlay, toggle) |

```
┌──────────────────────────────────────────────────────────────────┐
│  Kodi VideoPlayer (ffmpeg)                                       │
│  Sends: GET /stream?url=<upstream> with Range: bytes=X-Y         │
└────────────────────────┬─────────────────────────────────────────┘
                         │ HTTP on 127.0.0.1:48990
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│  ProxyHandler (server.py)                                        │
│  ┌──────────────┐  ┌───────────────┐  ┌────────────────────────┐ │
│  │ Upstream      │  │ Small-Read    │  │ Direct Passthrough     │ │
│  │ Probe (HEAD+  │  │ Bypass        │  │ (when proxy disabled   │ │
│  │ GET fallback) │  │ (< 2MB)       │  │  or no range support)  │ │
│  └──────┬───────┘  └───────┬───────┘  └───────────┬────────────┘ │
│         │                  │                      │              │
│         ▼                  │                      │              │
│  ┌──────────────────────┐  │                      │              │
│  │ ParallelRangeStream  │◄─┘  (if content_length  │              │
│  │ (engine.py)          │      > threshold)        │              │
│  │                      │                          │              │
│  │ ThreadPoolExecutor   │                          │              │
│  │  ├─ Worker 1: GET Range: bytes=0-1048575        │              │
│  │  ├─ Worker 2: GET Range: bytes=1048576-2097151  │              │
│  │  ├─ Worker 3: ...                               │              │
│  │  └─ Worker N                                    │              │
│  │                      │                          │              │
│  │ Ordered RAM Buffer   │                          │              │
│  │ [chunk_0][chunk_1]...[chunk_N]                  │              │
│  └──────────┬───────────┘                          │              │
│             │                                      │              │
│             ▼                                      ▼              │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  StreamStats singleton ◄──── OSD Overlay (Ctrl+D)           │ │
│  │  (stats.py)                  (overlay.py)                   │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. Directory Structure

```
service.stream.accelerator/
├── addon.xml                   # Triple extension: service + module + script
├── service.py                  # xbmc.service entry — daemon lifecycle, keymap install, active_port
├── script.py                   # xbmc.python.script — RunScript() hotkey client via /control HTTP IPC
└── resources/
    ├── settings.xml            # GUI settings (threads, chunk size, port, buffer, threshold)
    └── lib/
        ├── __init__.py         # Package helper exposing proxify() + get_active_port()
        ├── stream_accelerator.py # Top-level importable module (`import stream_accelerator`)
        ├── logutil.py          # Optional verbose logging helper (LOGDEBUG -> LOGINFO)
        ├── stats.py            # Thread-safe StreamStats singleton with stream_id race guard
        ├── engine.py           # ParallelRangeStream — slow-start, streaming fetch, sliding window
        ├── server.py           # ProxyHandler — HTTP 206 range server, probe cache, /control IPC
        └── overlay.py          # StatsOverlay — non-focus-stealing OSD window
```

---

## 4. Provider Compatibility

The proxy has been designed to handle the specific behaviors of these providers:

| Provider | Range Support | Redirect Behavior | Token Lifetime | Proxy Strategy |
|:---|:---:|:---|:---|:---|
| **Easynews** | ✅ 206 | Single 302 → CDN | Session-based (long) | Full acceleration. May rate-limit excessive parallelism; default 6 threads is safe. |
| **Premiumize.me** | ✅ 206 | 302 → CDN edge | URL-token, ~hours | Full acceleration. CDN handles parallel ranges well. |
| **TorBox** | ✅ 206 | 302 → **single-use token** | Short-lived | **Critical:** Redirect must be resolved once during probe and the final URL reused by all workers. Implemented in `probe_upstream()`. |
| **Real-Debrid** | ✅ 206 | 302 → CDN | URL-token, ~hours | Full acceleration. Max ~16 connections recommended. |
| **AllDebrid** | ✅ 206 | 302 → CDN | URL-token | Similar to Premiumize. |
| **WebDAV** | Varies | Usually direct | N/A | Auto-detected: if no range support, falls back to direct passthrough. |
| **Generic HTTP** | Varies | Varies | Varies | HEAD+GET dual probe determines capability; automatic fallback. |

### Automatic Fallback Modes

The proxy automatically selects the best strategy per-request:

1. **Accelerated** — Full parallel threading (when ranges supported and content > threshold)
2. **Small-Read Bypass** — Single direct request (when content ≤ 2 MB, e.g. MKV header/cue reads)
3. **Direct Passthrough** — Single-connection pipe (when proxy disabled by user, or upstream doesn't support ranges)

---

## 5. Component Design

### 5A. Configuration

**File:** `resources/settings.xml`

| Setting | Type | Default | Range | Description |
|---|---|---|---|---|
| `port` | Int | 48990 | — | Localhost binding port |
| `threads` | Slider | 6 | 1–16 | Concurrent HTTP workers per stream |
| `chunk_size_kb` | Slider | 1024 (1 MB) | 256–8192 | Byte range per worker task |
| `read_ahead` | Slider | 12 | 2–32 | Max chunks buffered ahead in RAM |
| `retries` | Slider | 3 | 1–10 | Retry attempts per failed chunk |
| `small_read_threshold_mb` | Slider | 4 | 1–8 | Below this size, skip threading |
| `overlay_enabled` | Bool | `true` | — | Allow Ctrl+D stats overlay hotkey |
| `verbose_logging` | Bool | `false` | — | Write detailed diagnostics (timings, chunks, stalls) to `kodi.log` at `LOGINFO` without enabling Kodi's debug overlay |

**RAM footprint:** With defaults (1 MB chunks × 12 read-ahead), peak RAM is capped at **12 MB**. On Android TV with limited RAM, reduce `read_ahead` to 6–8.

All settings except `port` can be overridden per-URL via query parameters (`threads`, `chunk_kb`, `read_ahead`).

### 5B. Universal URL Interface

Any add-on, `.strm` file, or player can route traffic through the proxy:

```
http://127.0.0.1:48990/stream?url=<urlencoded_target_url>&threads=8&chunk_kb=2048
```

**Kodi pipe-header extraction:** URLs like `https://host/video.mkv|User-Agent=Mozilla&Authorization=Basic...` are automatically parsed. The `|` suffix is stripped from the URL, and the key-value pairs are injected as HTTP headers into every upstream request.

**Dynamic override:** If `threads` or `chunk_kb` are omitted, the proxy uses the user's values from `settings.xml`.

### 5C. Upstream Probe

**File:** `resources/lib/server.py` → `probe_upstream()`

Before serving any request, the proxy probes the upstream server to determine file size, content type, final URL (post-redirect), and range support. This uses a **two-phase strategy**:

**Phase 1 — HEAD request** (cheapest, no body transfer):
```
HEAD https://upstream.example/video.mkv
→ Content-Length: 4294967296
→ Accept-Ranges: bytes
→ (follow redirects → capture resp.url as final_url)
```

If HEAD returns both `Content-Length` and `Accept-Ranges: bytes`, we're done.

**Phase 2 — GET with `Range: bytes=0-0`** (fallback):
```
GET https://upstream.example/video.mkv
Range: bytes=0-0
→ 206 Partial Content
→ Content-Range: bytes 0-0/4294967296
```

If this returns `206`, we confirm range support and extract the total size from `Content-Range`. If it returns `200`, the server doesn't support ranges — we fall back to direct passthrough.

**Critical: Redirect resolution & Probe Cache** — Two protections are built into `probe_upstream()`:
1. **Phase 2 uses `final_url` from Phase 1:** If Phase 1 (`HEAD`) follows a redirect to `final_url` before falling back to Phase 2 (`GET bytes=0-0`), Phase 2 queries `final_url` directly instead of re-hitting `clean_url`.
2. **15-Minute Probe Cache (`_PROBE_CACHE`):** `UpstreamProbeResult` is cached per `clean_url` for 900 seconds (`_PROBE_CACHE_TTL`). When Kodi/FFmpeg opens multiple startup connections (header → MKV Cues at EOF → video stream), subsequent requests reuse the cached `final_url` and metadata without re-probing. This guarantees single-use redirect tokens (TorBox) are consumed only once per stream.

### 5D. Streaming Engine

**File:** `resources/lib/engine.py` → `ParallelRangeStream`

#### How Kodi reads files (and why naive parallelism breaks)

Kodi's VideoPlayer (ffmpeg) doesn't read linearly from byte 0. On MKV/MP4 startup, it:

1. `GET Range: bytes=0-` → reads file header (a few KB)
2. **Immediately aborts** → `GET Range: bytes=<near_eof>-` → reads MKV Cues / MP4 moov atom at the end
3. **Aborts again** → `GET Range: bytes=<audio_start>-` → begins actual playback

If the proxy blindly spawned 6 threads from byte 0 on step 1, it would waste bandwidth and block step 2. The engine solves this with:

#### Small-Read Bypass & Chunk-0 Slow Start

- **Small-Read Bypass:** Requests with a bounded `Content-Length ≤ small_read_threshold` (default 2 MB, such as MKV Cues at EOF) are served by a single direct HTTP request (`_single_range_fetch`) — no thread pool, no buffer overhead.
- **Chunk-0 Slow Start:** FFmpeg's initial probe (`Range: bytes=0-`, open-ended) has `content_length == total_size`, so it cannot trigger small-read bypass. To avoid spinning up all 6 workers before FFmpeg aborts 40ms later to read EOF cues, `_schedule_window()` only dispatches Chunk 0 initially (`next_chunk_to_yield == 0`). Once Kodi actually reads Chunk 0 and requests Chunk 1, the full `read_ahead` window is dispatched across all workers.

#### Sliding-Window Coordinator (for large reads)

```
Kodi Player (Consumer)
   │  Reads sequential bytes from current offset
   ▼
[ Ordered RAM Buffer: {chunk_0: bytes, chunk_1: bytes, ...} ]
   ▲  Workers deposit completed chunks by index
   │
[ ThreadPoolExecutor (e.g., 6 threads) ]
   ├── Worker 1: GET Range: bytes=0-1048575
   ├── Worker 2: GET Range: bytes=1048576-2097151
   ├── Worker 3: GET Range: bytes=2097152-3145727
   └── ...
```

**Backpressure control:**
- Let `current_chunk` = the chunk Kodi is currently waiting to read
- The scheduler only dispatches work for `chunk_id ∈ [current_chunk, current_chunk + read_ahead)`
- When `buffer[current_chunk]` completes, the handler writes it to Kodi's socket, **deletes it from RAM immediately**, increments `current_chunk`, and schedules `current_chunk + read_ahead` into the executor

**Instant Abort on Seek:**
- When Kodi seeks (fast-forward, chapter skip), it closes the TCP socket → triggers `BrokenPipeError` / `ConnectionResetError`
- The handler sets `abort_event` (a `threading.Event`), which:
  - Causes workers streaming chunks via `resp.iter_content(chunk_size=65536)` (`stream=True`) to abort mid-download immediately rather than waiting for an entire 1 MB+ chunk
  - Cancels queued futures via `executor.shutdown(wait=False, cancel_futures=True)`
  - Clears the RAM buffer immediately
- Each stream receives a unique monotonic `stream_id` from `stats.reset()`. Calling `stats.stop(stream_id=...)` ignores stale threads so overlapping socket closes during fast seeks never mark a newly started stream as inactive.
- Kodi then opens a new connection at the new byte offset and gets 100% of the threads

#### Safety: Only HTTP 206 accepted

Workers **reject** `200` responses. If the upstream ignores the `Range` header and returns `200` (the full file), accepting it would mean every thread downloads the entire file — causing immediate OOM. Instead, the engine marks the stream as failed and logs a clear error.

### 5E. Stats Collector

**File:** `resources/lib/stats.py` → `StreamStats`

A thread-safe singleton (living inside the `xbmc.service` interpreter) that the engine writes to and the overlay reads from:

| Metric | Type | Updated By |
|---|---|---|
| `active` | bool | engine start/stop (guarded by `stream_id`) |
| `stream_id` | int | monotonic counter incremented on `reset()`, prevents stale thread clobber |
| `mode` | str | server.py ("accelerated" / "direct" / "bypass_small") |
| `file_size` | int | probe |
| `content_type` | str | probe |
| `bytes_delivered` | int | each chunk write / passthrough write |
| `chunks_completed` | int | each successful chunk |
| `chunks_failed` | int | exhausted retries |
| `chunks_retried` | int | each retry attempt |
| `active_workers` | int | explicit worker counter in `_fetch_chunk` |
| `buffer_fill` | int | schedule_window / chunk pop |
| `buffer_max` | int | read_ahead setting |
| `seeks` | int | each BrokenPipe/ConnectionReset |
| `_speed_samples` | deque | rolling 5-second window of (timestamp, bytes) |

*(Note: `_proxy_enabled` lives as a module-level flag in `server.py` inside the service interpreter, toggled via `/control?action=toggle`.)*

**Derived metrics:**
- `get_speed_mbps()` — live speed from rolling 5s window, in Mbit/s
- `get_avg_speed_mbps()` — average speed since stream start
- `get_elapsed()` — seconds since stream start
- `to_dict()` — plain JSON-serializable snapshot served via `/control?action=stats`

### 5F. OSD Stats Overlay

**File:** `resources/lib/overlay.py` → `StatsOverlay`

A non-focus-stealing `xbmcgui.WindowDialog` rendered in the top-right corner of the screen (1080p). Updates every 500ms via a background thread. Overriding `onAction()` closes the overlay on Back/Escape/Backspace while letting all other player controls (Pause, Seek, Stop, Volume, OSD) pass through to Kodi's player.

```
┌──────────────────────────────────────┐
│  ⚡ Stream Accelerator               │
│  ● PROXY ACTIVE — ACCELERATED       │  ← Green when active
│  ──────────────────────────────────  │     Orange when disabled
│  Speed (live):   42.3 Mbit/s        │  ← Color-coded: green/yellow/red
│  Speed (avg):    38.7 Mbit/s        │
│  Workers:        5 / 6 active       │
│  Buffer:         8 / 12 chunks (67%)│  ← Color-coded fill level
│  Delivered:      847.3 MB / 4.1 GB  │
│  Chunks:         823 OK / 0 FAILED  │
│  Seeks/aborts:   2                  │
│  Elapsed:        3:42               │
│  video/x-matroska | 1024KB | 6T     │
└──────────────────────────────────────┘
```

**Status modes displayed:**
- `● PROXY ACTIVE — ACCELERATED` (green) — full parallel streaming
- `● SMALL READ BYPASS (direct)` (green) — single request for tiny reads
- `○ PROXY DISABLED — next stream goes direct` (orange) — user toggled off
- `◌ IDLE — no active stream` (gray) — proxy running but nothing playing

**Speed color coding:**
- ≥ 20 Mbit/s → 🟢 Green
- 5–20 Mbit/s → 🟡 Yellow
- < 5 Mbit/s → 🔴 Red

### 5G. Hotkey System

| Shortcut | Keymap Tag | Action |
|---|---|---|
| **Ctrl+D** | `<d mod="ctrl">` (`0x1f044`) | Toggle the OSD stats overlay on/off |
| **Ctrl+Shift+D** | `<d mod="ctrl,shift">` (`0x3f044`) | Toggle proxy acceleration on/off (affects next stream) |

**Implementation (Sub-Interpreter IPC):** In Kodi's `XBPython`, `xbmc.service` (`service.py`) and `RunScript()` (`script.py`) run in separate Python sub-interpreters with isolated memory heaps. Therefore:
1. The service installs a keymap XML to `special://profile/keymaps/accelerator_keymap.xml` (covering `<global>`, `<FullscreenVideo>`, and `<VideoOSD>`) on startup and removes it on shutdown.
2. Pressing Ctrl+D or Ctrl+Shift+D launches `script.py`, which discovers the active port and sends an HTTP request to `http://127.0.0.1:<port>/control?action=overlay` or `?action=toggle`.
3. `ProxyHandler._handle_control()` handles the request inside the `service.py` interpreter, where live `StreamStats`, `_proxy_enabled`, and `StatsOverlay` live.

**Toggle behavior:**
- Flips `server._proxy_enabled` in the service interpreter
- Shows a Kodi notification: "Proxy ON — next stream will be accelerated" or "Proxy OFF — next stream goes direct"
- Does **not** affect a currently-playing stream (the active `ParallelRangeStream` continues)
- The next `/stream` request checks `server._proxy_enabled` and routes to either the parallel engine or direct passthrough

### 5H. Service Lifecycle

**File:** `service.py` → `AcceleratorService(xbmc.Monitor)`

1. **Startup** (`start="login"`):
   - Reads port from settings
   - Creates `ThreadingHTTPServer` on `127.0.0.1:<port>`
   - **Port-in-use handling:** If the port is taken (previous un-killed instance, conflict), catches `OSError` and tries `port + 1` as fallback. Logs a clear error if both fail.
   - Writes the bound port to `special://profile/addon_data/service.stream.accelerator/active_port` so `script.py` and `proxify()` automatically discover fallback ports.
   - Starts server in a daemon thread
   - Installs hotkey keymap → triggers `Action(reloadkeymaps)`

2. **Running:**
   - `waitForAbort()` — blocks until Kodi exits

3. **Shutdown:**
   - Removes `active_port` file and hotkey keymap
   - `server.shutdown()` + `server.server_close()`

### 5I. Client Helper

**Files:** `resources/lib/stream_accelerator.py` & `resources/lib/__init__.py` → `proxify()`

One-line URL wrapper for other add-ons (supports `import stream_accelerator`):

```python
import stream_accelerator
proxy_url = stream_accelerator.proxify(raw_url, threads=8, chunk_kb=1024)
```

Automatically reads `active_port` (or falls back to the `port` setting / `48990`).

---

## 6. Request Flow

End-to-end flow for a single Kodi playback session:

```
1. User plays a proxied URL:
   http://127.0.0.1:48990/stream?url=https%3A%2F%2Feasynews.example%2Fvideo.mkv

2. ProxyHandler._handle_request():
   a. Parse query params → extract target URL, settings overrides
   b. parse_kodi_url() → split pipe headers (once)
   c. Check StreamStats.enabled → if False, go to direct passthrough

3. probe_upstream(clean_url, headers):
   a. HEAD request → capture Content-Length, Accept-Ranges, final_url
   b. If incomplete → GET Range: bytes=0-0 → check for 206, extract Content-Range
   c. If total_size=0 → raise ValueError (502 to Kodi)
   d. If 200 returned (no range support) → mark supports_ranges=False

4. Routing decision:
   a. If !supports_ranges → direct passthrough
   b. If content_length ≤ 2MB → single_range_fetch (small-read bypass)
   c. Else → ParallelRangeStream (full acceleration)

5. ParallelRangeStream.stream_chunks():
   a. Calculate total_chunks from byte range and chunk_size
   b. Schedule initial read-ahead window [0, read_ahead)
   c. Workers fetch chunks in parallel → deposit into buffer dict
   d. Generator yields buffer[next_chunk] in order → handler writes to Kodi
   e. RAM freed immediately after each yield
   f. On BrokenPipe → abort_event → cancel futures → clear buffer

6. Stats updated throughout → OSD overlay reads at 500ms intervals
```

---

## 7. Design Decisions & Fixes Applied

During design review, 12 bugs/gaps were identified in the original spec. All have been fixed in the implementation:

### Critical (would have broken playback)

| # | Issue | Fix Applied |
|---|---|---|
| 1 | **200 response = OOM crash.** Workers accepted `200`, meaning every thread would download the entire file if the server ignored Range headers. | Workers only accept `206`. A `200` response triggers immediate abort with logged error. |
| 2 | **Redirect amplification.** Each worker independently followed 302 redirects. TorBox single-use tokens would fail for N-1 threads. | `probe_upstream()` captures `resp.url` (final redirected URL) and passes it to all workers. |
| 3 | **Small-read bypass described but not implemented.** MKV header/cue reads (~few KB) would spin up full thread pool, adding 50-200ms latency per seek. | `_handle_request()` checks `content_length ≤ small_read_threshold` → routes to `_single_range_fetch()`. |
| 4 | **`total_size=0` on chunked-encoding servers.** No Content-Length and no Content-Range → `end_byte = -1`, Kodi gets `Content-Length: 0`. | HEAD probe first; if no size, GET `bytes=0-0` fallback; if still 0, raise clear `ValueError`. |

### Medium (silent degradation or edge-case failures)

| # | Issue | Fix Applied |
|---|---|---|
| 5 | Probe used `GET` instead of `HEAD`. | HEAD first, GET fallback only if needed. |
| 6 | `executor.shutdown(wait=False)` didn't cancel queued futures — workers hung until 15s timeout. | `shutdown(wait=False, cancel_futures=True)` with Python <3.9 fallback. |
| 7 | `Content-Range` sent on `200` responses (HTTP spec violation). | Only included in `206` responses. |
| 8 | Response objects not explicitly closed in `_fetch_chunk`. | `resp.close()` in `finally` block on every path. |
| 9 | No port-in-use error handling — service silently died. | `OSError` caught, fallback to `port+1`, clear error logging. |
| 10 | No logging anywhere in engine or server. | `xbmc.log` calls at all key points: probe, chunk fetch, retry, abort, failure. |

### Minor (defensive improvements)

| # | Issue | Fix Applied |
|---|---|---|
| 11 | `read_ahead` minimum was clamped to `threads`, silently inflating RAM. | Minimum is now `2`, independent of thread count. |
| 12 | `parse_kodi_url()` called redundantly in both server and engine. | Parsed once in `_handle_request()`, clean URL + headers passed to engine. |

### Kodi Sub-Interpreter & Real-World Edge-Case Fixes (`consider.txt`)

| # | Issue | Fix Applied |
|---|---|---|
| 13 | **Sub-interpreter isolation.** `RunScript()` runs `script.py` in a separate Python interpreter from `service.py`, so toggling `StreamStats` or opening `StatsOverlay` failed across memory heaps. | Added HTTP IPC endpoint `/control?action=overlay\|toggle\|stats` in `server.py`; `script.py` queries `127.0.0.1:<port>/control`, and `_proxy_enabled` lives in `server.py`. |
| 14 | **TorBox single-use token broken across FFmpeg startup requests.** Phase 2 GET re-hit `clean_url`, and uncached `probe_upstream()` re-hit `clean_url` on Request #2 (MKV Cues). | Cached `UpstreamProbeResult` in `_PROBE_CACHE` (15-min TTL) and made Phase 2 GET query `final_url` from Phase 1 HEAD. |
| 15 | **`import stream_accelerator` threw `ModuleNotFoundError`.** `addon.xml` exports `resources/lib` directly on `sys.path`, so `__init__.py` wasn't importable as `stream_accelerator`. | Added `resources/lib/stream_accelerator.py` exposing `proxify()` and `get_active_port()`. |
| 16 | **FFmpeg Step 1 (`bytes=0-`) bypassed small-read check & couldn't abort mid-chunk.** Open-ended range meant `content_length == total_size`, launching 6 blocking threads for a 32 KB probe. | Added Chunk-0 Slow Start (`next_chunk_to_yield == 0`) and streaming chunk fetch (`stream=True` + `iter_content(65536)` checking `abort_event`). |
| 17 | **Modal overlay stole player focus.** `xbmcgui.WindowDialog` trapped Pause/Stop/Seek keys. | Overrode `onAction()` in `StatsOverlay` to close only on Back/Escape and pass playback keys through. |
| 18 | **Seek race & inaccurate worker count.** Overlapping closes could clear new stream's `active` flag, and `len(in_flight)` counted queued tasks. | Added monotonic `stream_id` guard in `StreamStats.stop(stream_id=...)` and explicit `_active_worker_count` in `engine.py`. |
| 19 | **Fallback port mismatch.** If `service.py` fell back to `port + 1`, `script.py` and `proxify()` still used the configured port. | `service.py` writes `active_port` in addon profile on bind (removed on shutdown); `script.py` and `proxify()` read it first. |

---

## 8. Testing & A/B Comparison

### A/B Testing Procedure

```
┌─────────────────────────────────────────────────────────────┐
│                  A/B TESTING WORKFLOW                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  SETUP                                                      │
│  1. Install service.stream.accelerator in Kodi              │
│  2. Pick a test file (ideally ≥2 GB from target provider)   │
│  3. Route the URL through the proxy (proxify or manual URL) │
│                                                             │
│  TEST A — PROXY ON (accelerated)                            │
│  4. Start playback                                          │
│  5. Press Ctrl+D → overlay appears (top-right)              │
│  6. Record:                                                 │
│     • Live speed (Mbit/s)                                   │
│     • Average speed (Mbit/s)                                │
│     • Buffer fill level (stays near max = good)             │
│     • Failed chunks (should be 0)                           │
│     • Time-to-first-frame (stopwatch from play press)       │
│  7. Test seeking: skip forward 3 times, note responsiveness │
│  8. Let it play for 60 seconds minimum                      │
│  9. Stop playback                                           │
│                                                             │
│  TEST B — PROXY OFF (direct comparison)                     │
│  10. Press Ctrl+Shift+D → "Proxy OFF" notification          │
│  11. Start the SAME file again                              │
│  12. Press Ctrl+D → overlay shows "PROXY DISABLED"          │
│      (Kodi's built-in player info: press 'O' for OSD info)  │
│  13. Record same metrics via Kodi's debug info              │
│  14. Test seeking: skip forward 3 times, note responsiveness│
│  15. Let it play for 60 seconds minimum                     │
│  16. Stop playback                                          │
│                                                             │
│  COMPARE                                                    │
│  17. Press Ctrl+Shift+D → "Proxy ON" to re-enable           │
│  18. Compare:                                               │
│      • Proxy speed vs. direct speed (expect 2-6x faster)    │
│      • Time-to-first-frame (proxy may be slightly slower     │
│        on initial probe, but faster for large files)         │
│      • Seek responsiveness (proxy should be similar or       │
│        faster due to pre-buffered chunks)                   │
│      • Buffer stalls (proxy should have fewer/none)          │
│                                                             │
│  REPEAT per provider (Easynews, Premiumize, TorBox)         │
└─────────────────────────────────────────────────────────────┘
```

### What to Look For in the Overlay

| Metric | Healthy | Warning | Problem |
|---|---|---|---|
| Live speed | ≥ 20 Mbit/s (green) | 5–20 Mbit/s (yellow) | < 5 Mbit/s (red) |
| Buffer fill | 60–100% | 25–60% | < 25% (starving) |
| Failed chunks | 0 | 1–3 (retries worked) | ≥ 4 (upstream issue) |
| Active workers | Near threads count | Fluctuating | 0 for extended time |
| Seeks/aborts | Low | — | High without user input = bug |

### Debugging via kodi.log

All operations log to `kodi.log` with the `[StreamAccelerator]` prefix:

```
[StreamAccelerator.Server] HEAD probe: size=4294967296, type=video/x-matroska, ranges=True, final_url=https://cdn.example.com/video.mkv
[StreamAccelerator.Server] Parallel stream: 4294967296 bytes, 6 threads, 1024KB chunks, read_ahead=12
[StreamAccelerator.Engine] Stream init: 4294967296 bytes, 4096 chunks, 6 threads, 1048576 B/chunk, read_ahead=12
[StreamAccelerator.Engine] Chunk 42 attempt 1: timeout
[StreamAccelerator.Engine] Client disconnected (seek/stop)
[StreamAccelerator.Engine] Closing stream engine
```

Set Kodi's log level to **Debug** to see per-chunk messages.

---

## 9. Integration Guide

### Method 1: Import the module (recommended)

In your add-on's `addon.xml`:
```xml
<import addon="service.stream.accelerator"/>
```

In your Python code, right before `setResolvedUrl`:
```python
import stream_accelerator
resolved_url = stream_accelerator.proxify(resolved_url, threads=8, chunk_kb=1024)
xbmcplugin.setResolvedUrl(handle, True, xbmcgui.ListItem(path=resolved_url))
```

### Method 2: Direct URL formatting (no import needed)

```python
from urllib.parse import quote
proxy_url = f"http://127.0.0.1:48990/stream?url={quote(raw_url, safe='')}"
```

### Method 3: .strm files

```
http://127.0.0.1:48990/stream?url=https%3A%2F%2Feasynews.example%2Fmovie.mkv
```

### Compatible add-ons

| Add-on | Integration Point |
|---|---|
| **KodiStremioBridge** | Wrap resolved URL before `setResolvedUrl` |
| **POV / Fen Light** | Wrap in `resolve_url()` method |
| **TMDbHelper** | Wrap in player file URL resolution |
| **Any custom add-on** | Before `setResolvedUrl` or in `.strm` files |

---

## 10. Complete Implementation Reference

The full implementation is in the `service.stream.accelerator/` directory. Each file's role:

| File | Lines | Purpose |
|---|---|---|
| [`addon.xml`](service.stream.accelerator/addon.xml) | 22 | Kodi add-on manifest: service + module + script extensions |
| [`service.py`](service.stream.accelerator/service.py) | 106 | Daemon lifecycle: server start, keymap install, abort wait, cleanup |
| [`script.py`](service.stream.accelerator/script.py) | 22 | RunScript entry: dispatches "overlay" and "toggle" commands |
| [`resources/settings.xml`](service.stream.accelerator/resources/settings.xml) | 14 | GUI settings definition |
| [`resources/lib/__init__.py`](service.stream.accelerator/resources/lib/__init__.py) | 40 | `proxify()` helper function |
| [`resources/lib/stats.py`](service.stream.accelerator/resources/lib/stats.py) | 103 | `StreamStats` singleton: thread-safe metrics, rolling speed window |
| [`resources/lib/engine.py`](service.stream.accelerator/resources/lib/engine.py) | 166 | `ParallelRangeStream`: ThreadPoolExecutor, sliding window, abort |
| [`resources/lib/server.py`](service.stream.accelerator/resources/lib/server.py) | 270 | `ProxyHandler` + `probe_upstream()`: HTTP 206, dual probe, routing |
| [`resources/lib/overlay.py`](service.stream.accelerator/resources/lib/overlay.py) | 224 | `StatsOverlay` window + `toggle_overlay()` / `toggle_proxy()` |

**Total:** ~967 lines of Python + XML across 9 files.
