"""Sliding-window parallel chunk downloader with bounded RAM buffer.

This is the core acceleration engine. It splits a byte range into chunks,
fetches them in parallel using a ThreadPoolExecutor, and yields them in
order to the caller (the HTTP proxy handler) which writes them to Kodi.

Key design decisions (see design-review.md for rationale):
  - Only accepts HTTP 206 responses; rejects 200 to prevent OOM
  - Accepts pre-resolved (clean_url, headers) — no pipe-header parsing here
  - cancel_futures=True on shutdown for fast abort
  - read_ahead minimum is 2, NOT clamped to thread count
  - Explicit response.close() on every path
  - Stats integration for the overlay
"""
import threading
import time
import requests
from concurrent.futures import ThreadPoolExecutor

try:
    import xbmc
    def _log(msg, level=None):
        if level is None:
            level = xbmc.LOGDEBUG
        xbmc.log(f"[StreamAccelerator.Engine] {msg}", level)
except ImportError:
    def _log(msg, level=None):
        pass

from .stats import StreamStats


class ParallelRangeStream:
    """Downloads a byte range in parallel chunks and yields them in order.

    Args:
        clean_url: The final resolved upstream URL (no Kodi pipe headers).
        headers: Dict of HTTP headers to send with every request.
        start_byte: First byte to fetch (inclusive).
        end_byte: Last byte to fetch (inclusive).
        threads: Number of concurrent worker threads.
        chunk_size: Bytes per chunk.
        read_ahead: Max chunks to buffer ahead of the consumer.
        retries: Retry attempts per chunk before marking failure.
    """

    def __init__(self, clean_url, headers, start_byte, end_byte,
                 threads=6, chunk_size=1048576, read_ahead=12, retries=3):
        self.clean_url = clean_url
        self.headers = dict(headers) if headers else {}
        self.start_byte = start_byte
        self.end_byte = end_byte  # Inclusive
        self.threads = max(1, int(threads))
        self.chunk_size = max(65536, int(chunk_size))
        self.read_ahead = max(2, int(read_ahead))
        self.retries = max(1, int(retries))

        self.abort_event = threading.Event()
        self.condition = threading.Condition()
        self.buffer = {}       # {chunk_index: bytes}
        self.in_flight = set() # {chunk_index}
        self.failed = False
        self.fail_reason = ""

        total_bytes = (self.end_byte - self.start_byte) + 1
        self.total_chunks = max(1, (total_bytes + self.chunk_size - 1) // self.chunk_size)
        self.next_chunk_to_yield = 0

        # Persistent session for TCP/TLS connection pooling across workers
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self.threads,
            pool_maxsize=self.threads + 2
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.executor = ThreadPoolExecutor(max_workers=self.threads)

        self.stats = StreamStats.instance()
        _log(f"Stream init: {total_bytes} bytes, {self.total_chunks} chunks, "
             f"{self.threads} threads, {self.chunk_size} B/chunk, "
             f"read_ahead={self.read_ahead}")

    def _fetch_chunk(self, chunk_idx):
        """Worker: fetch a single chunk by byte range. Runs in ThreadPoolExecutor."""
        if self.abort_event.is_set():
            return

        c_start = self.start_byte + (chunk_idx * self.chunk_size)
        c_end = min(c_start + self.chunk_size - 1, self.end_byte)
        req_headers = self.headers.copy()
        req_headers["Range"] = f"bytes={c_start}-{c_end}"

        for attempt in range(self.retries):
            if self.abort_event.is_set():
                return

            resp = None
            try:
                resp = self.session.get(
                    self.clean_url,
                    headers=req_headers,
                    stream=False,  # We want the full chunk in memory
                    timeout=(5, 30)
                )

                if resp.status_code == 206:
                    data = resp.content
                    with self.condition:
                        if not self.abort_event.is_set():
                            self.buffer[chunk_idx] = data
                            self.stats.update_buffer(len(self.buffer), len(self.in_flight) - 1)
                        self.in_flight.discard(chunk_idx)
                        self.condition.notify_all()
                    return

                elif resp.status_code == 200:
                    # Server ignored our Range header — returned entire file.
                    # This is catastrophic (OOM) so abort immediately.
                    _log(f"CRITICAL: Upstream returned 200 (not 206) for range request. "
                         f"Server does not support byte ranges. Aborting.", xbmc.LOGERROR if hasattr(xbmc, 'LOGERROR') else None)
                    with self.condition:
                        self.failed = True
                        self.fail_reason = "Upstream does not support byte-range requests (returned 200)"
                        self.in_flight.discard(chunk_idx)
                        self.condition.notify_all()
                    return

                else:
                    _log(f"Chunk {chunk_idx} attempt {attempt+1}: HTTP {resp.status_code}")
                    if attempt < self.retries - 1:
                        self.stats.record_retry()

            except requests.exceptions.ConnectionError as e:
                _log(f"Chunk {chunk_idx} attempt {attempt+1}: connection error: {e}")
                if attempt < self.retries - 1:
                    self.stats.record_retry()
            except requests.exceptions.Timeout as e:
                _log(f"Chunk {chunk_idx} attempt {attempt+1}: timeout: {e}")
                if attempt < self.retries - 1:
                    self.stats.record_retry()
            except Exception as e:
                _log(f"Chunk {chunk_idx} attempt {attempt+1}: unexpected error: {e}")
                if attempt < self.retries - 1:
                    self.stats.record_retry()
            finally:
                if resp is not None:
                    resp.close()

            # Backoff before retry
            if attempt < self.retries - 1:
                time.sleep(0.3 * (attempt + 1))

        # All retries exhausted
        _log(f"Chunk {chunk_idx}: all {self.retries} retries exhausted, marking failure")
        self.stats.record_failure()
        with self.condition:
            self.failed = True
            self.fail_reason = f"Chunk {chunk_idx} failed after {self.retries} retries"
            self.in_flight.discard(chunk_idx)
            self.condition.notify_all()

    def _schedule_window(self):
        """Submit fetch tasks for chunks within the read-ahead window.

        Must be called while holding self.condition.
        """
        window_limit = min(self.total_chunks, self.next_chunk_to_yield + self.read_ahead)
        for idx in range(self.next_chunk_to_yield, window_limit):
            if idx not in self.buffer and idx not in self.in_flight and not self.abort_event.is_set():
                self.in_flight.add(idx)
                self.executor.submit(self._fetch_chunk, idx)

    def stream_chunks(self):
        """Generator: yields ordered byte chunks to the caller.

        The caller (proxy handler) writes each yielded chunk to Kodi's socket.
        RAM is freed immediately after each yield. Backpressure is maintained
        by the read_ahead window — we never have more than read_ahead chunks
        in memory at once.
        """
        try:
            with self.condition:
                self._schedule_window()

            while self.next_chunk_to_yield < self.total_chunks and not self.abort_event.is_set():
                with self.condition:
                    # Wait for the next chunk to arrive
                    while (
                        self.next_chunk_to_yield not in self.buffer
                        and not self.failed
                        and not self.abort_event.is_set()
                    ):
                        self.condition.wait(timeout=1.0)

                    if self.abort_event.is_set():
                        break

                    if self.failed:
                        _log(f"Stream failed: {self.fail_reason}")
                        break

                    # Pop chunk from RAM buffer
                    chunk_data = self.buffer.pop(self.next_chunk_to_yield)
                    self.next_chunk_to_yield += 1
                    self._schedule_window()
                    self.stats.update_buffer(len(self.buffer), len(self.in_flight))

                # Yield outside the lock so wfile.write() doesn't hold the condition
                self.stats.record_chunk_delivered(len(chunk_data))
                yield chunk_data

        finally:
            self.close()

    def close(self):
        """Abort all workers, purge buffer, release resources."""
        if self.abort_event.is_set():
            return  # Already closed
        _log("Closing stream engine")
        self.abort_event.set()
        with self.condition:
            self.buffer.clear()
            self.condition.notify_all()
        try:
            self.executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            # Python < 3.9 doesn't have cancel_futures
            self.executor.shutdown(wait=False)
        self.session.close()
        self.stats.stop()

