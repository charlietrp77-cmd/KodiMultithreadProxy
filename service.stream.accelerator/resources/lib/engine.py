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

from .stats import StreamStats
from .logutil import log_warning, log_error, log_verbose

TAG = "StreamAccelerator.Engine"


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
        self._start_time = time.monotonic()
        self._total_stall_sec = 0.0
        self._chunks_yielded = 0
        log_verbose(
            TAG,
            f"Stream init: {total_bytes:,} B ({self.total_chunks} chunks), "
            f"threads={self.threads}, chunk_size={self.chunk_size // 1024}KB, "
            f"read_ahead={self.read_ahead}, retries={self.retries}"
        )

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
            t0 = time.monotonic()
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
                            log_verbose(
                                TAG,
                                f"Chunk {chunk_idx} aborted mid-transfer after "
                                f"{(time.monotonic()-t0)*1000:.0f}ms"
                            )
                            return
                        if piece:
                            pieces.append(piece)
                    data = b"".join(pieces)
                    fetch_sec = max(0.0001, time.monotonic() - t0)
                    chunk_mbps = (len(data) * 8) / (fetch_sec * 1_000_000)

                    with self.condition:
                        if not self.abort_event.is_set():
                            self.buffer[chunk_idx] = data
                            buf_len = len(self.buffer)
                            self.stats.update_buffer(
                                buf_len, self._active_worker_count
                            )
                        else:
                            buf_len = len(self.buffer)
                        self.in_flight.discard(chunk_idx)
                        self.condition.notify_all()
                    log_verbose(
                        TAG,
                        f"Chunk {chunk_idx} ({c_start}-{c_end}): 206 OK "
                        f"{len(data):,} B in {fetch_sec*1000:.0f}ms ({chunk_mbps:.1f} Mbit/s) | "
                        f"buf={buf_len}/{self.read_ahead} workers={self._active_worker_count}/{self.threads}"
                    )
                    return

                elif resp.status_code == 200:
                    log_error(
                        TAG,
                        f"CRITICAL: Upstream returned 200 for range request on chunk {chunk_idx}. "
                        f"Server does not support byte ranges. Aborting."
                    )
                    with self.condition:
                        self.failed = True
                        self.fail_reason = "Upstream does not support byte-range requests (returned 200)"
                        self.in_flight.discard(chunk_idx)
                        self.condition.notify_all()
                    return
                else:
                    log_warning(
                        TAG,
                        f"Chunk {chunk_idx} attempt {attempt+1}/{self.retries}: "
                        f"HTTP {resp.status_code} ({(time.monotonic()-t0)*1000:.0f}ms)"
                    )
                    if attempt < self.retries - 1:
                        self.stats.record_retry()

            except requests.exceptions.ConnectionError as e:
                log_warning(TAG, f"Chunk {chunk_idx} attempt {attempt+1}/{self.retries}: connection error: {e}")
                if attempt < self.retries - 1:
                    self.stats.record_retry()
            except requests.exceptions.Timeout as e:
                log_warning(TAG, f"Chunk {chunk_idx} attempt {attempt+1}/{self.retries}: timeout: {e}")
                if attempt < self.retries - 1:
                    self.stats.record_retry()
            except Exception as e:
                log_warning(TAG, f"Chunk {chunk_idx} attempt {attempt+1}/{self.retries}: error: {e}")
                if attempt < self.retries - 1:
                    self.stats.record_retry()
            finally:
                if resp is not None:
                    resp.close()

            if attempt < self.retries - 1:
                time.sleep(0.3 * (attempt + 1))

        # All retries exhausted
        log_error(TAG, f"Chunk {chunk_idx}: all {self.retries} retries exhausted")
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
                log_verbose(TAG, "Slow-start: dispatching chunk 0 only")
                self.in_flight.add(0)
                self.executor.submit(self._fetch_chunk, 0)
            return

        window_limit = min(self.total_chunks, self.next_chunk_to_yield + self.read_ahead)
        dispatched = []
        for idx in range(self.next_chunk_to_yield, window_limit):
            if idx not in self.buffer and idx not in self.in_flight and not self.abort_event.is_set():
                self.in_flight.add(idx)
                self.executor.submit(self._fetch_chunk, idx)
                dispatched.append(idx)
        if dispatched and self.next_chunk_to_yield == 1:
            log_verbose(TAG, f"Slow-start complete: expanding window, dispatched chunks {dispatched}")

    def stream_chunks(self):
        try:
            with self.condition:
                self._schedule_window()

            while self.next_chunk_to_yield < self.total_chunks and not self.abort_event.is_set():
                with self.condition:
                    wait_start = None
                    while (
                        self.next_chunk_to_yield not in self.buffer
                        and not self.failed
                        and not self.abort_event.is_set()
                    ):
                        if wait_start is None:
                            wait_start = time.monotonic()
                        self.condition.wait(timeout=1.0)

                    if wait_start is not None:
                        stall = time.monotonic() - wait_start
                        self._total_stall_sec += stall
                        if stall >= 0.1 and not self.abort_event.is_set():
                            log_verbose(
                                TAG,
                                f"Consumer stalled {stall*1000:.0f}ms waiting for chunk {self.next_chunk_to_yield} "
                                f"(buf={len(self.buffer)}/{self.read_ahead}, workers={self._active_worker_count})"
                            )

                    if self.abort_event.is_set():
                        break

                    if self.failed:
                        log_error(TAG, f"Stream #{self._stream_id} failed: {self.fail_reason}")
                        break

                    chunk_data = self.buffer.pop(self.next_chunk_to_yield)
                    self.next_chunk_to_yield += 1
                    self._chunks_yielded += 1
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
        elapsed = max(0.001, time.monotonic() - self._start_time)
        delivered = self.stats.bytes_delivered
        avg_mbps = (delivered * 8) / (elapsed * 1_000_000)
        log_verbose(
            TAG,
            f"Closing stream #{self._stream_id}: yielded={self._chunks_yielded}/{self.total_chunks} chunks, "
            f"elapsed={elapsed:.2f}s, stall={self._total_stall_sec:.2f}s, avg_speed={avg_mbps:.1f} Mbit/s"
        )
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
