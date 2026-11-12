"""Sliding-window parallel chunk downloader with bounded RAM buffer.

Fixes from consider.txt applied:
  - Slow-start: chunk 0 fetched alone before full window (fix for FFmpeg probe-and-abort)
  - Streaming fetch: stream=True + iter_content with abort_event checks (instant abort mid-chunk)
  - Explicit active_workers counter (not len(in_flight) which includes queued tasks)
  - stream_id passed to stats.stop() to prevent race on seek overlap
  - Only HTTP 206 accepted; 200 = immediate abort (OOM prevention)
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
    def __init__(self, clean_url, headers, start_byte, end_byte,
                 threads=6, chunk_size=1048576, read_ahead=12, retries=3):
        self.clean_url = clean_url
        self.headers = dict(headers) if headers else {}
        self.start_byte = start_byte
        self.end_byte = end_byte
        self.threads = max(1, int(threads))
        self.chunk_size = max(65536, int(chunk_size))
        self.read_ahead = max(2, int(read_ahead))
        self.retries = max(1, int(retries))

        self.abort_event = threading.Event()
        self.condition = threading.Condition()
        self.buffer = {}
        self.in_flight = set()
        self.failed = False
        self.fail_reason = ""

        total_bytes = (self.end_byte - self.start_byte) + 1
        self.total_chunks = max(1, (total_bytes + self.chunk_size - 1) // self.chunk_size)
        self.next_chunk_to_yield = 0

        # Explicit active worker counter (not len(in_flight) which includes queued)
        self._active_worker_count = 0
        self._worker_count_lock = threading.Lock()

        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self.threads,
            pool_maxsize=self.threads + 2
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.executor = ThreadPoolExecutor(max_workers=self.threads)

        self.stats = StreamStats.instance()
        self._stream_id = 0  # Set by caller via set_stream_id()
        _log(f"Stream init: {total_bytes} bytes, {self.total_chunks} chunks, "
             f"{self.threads} threads, {self.chunk_size} B/chunk, "
             f"read_ahead={self.read_ahead}")

    def set_stream_id(self, stream_id):
        self._stream_id = stream_id

    def _fetch_chunk(self, chunk_idx):
        if self.abort_event.is_set():
            return

        # Track active workers explicitly
        with self._worker_count_lock:
            self._active_worker_count += 1

        try:
            self._do_fetch(chunk_idx)
        finally:
            with self._worker_count_lock:
                self._active_worker_count = max(0, self._active_worker_count - 1)

    def _do_fetch(self, chunk_idx):
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
                    stream=True,
                    timeout=(5, 30)
                )

                if resp.status_code == 206:
                    # Stream the chunk in 64KB pieces, checking abort between each
                    pieces = []
                    for piece in resp.iter_content(chunk_size=65536):
                        if self.abort_event.is_set():
                            return
                        if piece:
                            pieces.append(piece)
                    data = b"".join(pieces)

                    with self.condition:
                        if not self.abort_event.is_set():
                            self.buffer[chunk_idx] = data
                            self.stats.update_buffer(
                                len(self.buffer), self._active_worker_count
                            )
                        self.in_flight.discard(chunk_idx)
                        self.condition.notify_all()
                    return

                elif resp.status_code == 200:
                    _log(f"CRITICAL: Upstream returned 200 for range request. "
                         f"Server does not support byte ranges. Aborting.")
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
                _log(f"Chunk {chunk_idx} attempt {attempt+1}: error: {e}")
                if attempt < self.retries - 1:
                    self.stats.record_retry()
            finally:
                if resp is not None:
                    resp.close()

            if attempt < self.retries - 1:
                time.sleep(0.3 * (attempt + 1))

        # All retries exhausted
        _log(f"Chunk {chunk_idx}: all {self.retries} retries exhausted")
        self.stats.record_failure()
        with self.condition:
            self.failed = True
            self.fail_reason = f"Chunk {chunk_idx} failed after {self.retries} retries"
            self.in_flight.discard(chunk_idx)
            self.condition.notify_all()

    def _schedule_window(self):
        """Submit fetch tasks for chunks within the read-ahead window.

        Slow-start: When next_chunk_to_yield == 0, only fetch chunk 0.
        This prevents wasting all threads on FFmpeg's initial probe request
        which it will abort 40ms later to read MKV Cues at EOF.
        After chunk 0 is consumed, the full window opens up.
        """
        if self.next_chunk_to_yield == 0:
            # Slow-start: only fetch chunk 0 initially
            if 0 not in self.buffer and 0 not in self.in_flight and not self.abort_event.is_set():
                self.in_flight.add(0)
                self.executor.submit(self._fetch_chunk, 0)
            return

        window_limit = min(self.total_chunks, self.next_chunk_to_yield + self.read_ahead)
        for idx in range(self.next_chunk_to_yield, window_limit):
            if idx not in self.buffer and idx not in self.in_flight and not self.abort_event.is_set():
                self.in_flight.add(idx)
                self.executor.submit(self._fetch_chunk, idx)

    def stream_chunks(self):
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

                    if self.abort_event.is_set():
                        break

                    if self.failed:
                        _log(f"Stream failed: {self.fail_reason}")
                        break

                    chunk_data = self.buffer.pop(self.next_chunk_to_yield)
                    self.next_chunk_to_yield += 1
                    self._schedule_window()
                    self.stats.update_buffer(
                        len(self.buffer), self._active_worker_count
                    )

                self.stats.record_chunk_delivered(len(chunk_data))
                yield chunk_data

        finally:
            self.close()

    def close(self):
        if self.abort_event.is_set():
            return
        _log("Closing stream engine")
        self.abort_event.set()
        with self.condition:
            self.buffer.clear()
            self.condition.notify_all()
        try:
            self.executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            self.executor.shutdown(wait=False)
        self.session.close()
        self.stats.stop(stream_id=self._stream_id)
