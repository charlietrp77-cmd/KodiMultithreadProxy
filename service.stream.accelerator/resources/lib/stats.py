import threading
import time
from collections import deque


class StreamStats:
    """Thread-safe singleton collecting real-time streaming metrics.

    The engine writes to this; the overlay reads from it.
    Uses a stream_id counter to prevent stale threads from clobbering
    a new stream's stats on seek/abort overlap.
    """
    _instance = None
    _lock = threading.Lock()

    @classmethod
    def instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.active = False
        self.lock = threading.Lock()
        self.stream_id = 0             # Monotonic counter, incremented on reset()

        # Per-stream metadata (reset on each new stream)
        self.file_size = 0
        self.content_type = ""
        self.upstream_url = ""
        self.threads_configured = 0
        self.chunk_size = 0
        self.mode = "idle"             # "idle" | "accelerated" | "direct" | "bypass_small"

        # Live counters
        self.bytes_delivered = 0
        self.chunks_completed = 0
        self.chunks_failed = 0
        self.chunks_retried = 0
        self.active_workers = 0
        self.buffer_fill = 0
        self.buffer_max = 0
        self.seeks = 0

        # Speed tracking (rolling 5-second window)
        self._speed_samples = deque(maxlen=100)
        self.start_time = 0.0

    def reset(self, file_size, content_type, url, threads, chunk_size, read_ahead, mode="accelerated"):
        """Called at the start of each new stream. Returns the new stream_id."""
        with self.lock:
            self.stream_id += 1
            sid = self.stream_id
            self.active = True
            self.file_size = file_size
            self.content_type = content_type
            self.upstream_url = url[:120] if url else ""
            self.threads_configured = threads
            self.chunk_size = chunk_size
            self.bytes_delivered = 0
            self.chunks_completed = 0
            self.chunks_failed = 0
            self.chunks_retried = 0
            self.active_workers = 0
            self.buffer_fill = 0
            self.buffer_max = read_ahead
            self.seeks = 0
            self.mode = mode
            self._speed_samples.clear()
            self.start_time = time.monotonic()
            return sid

    def record_chunk_delivered(self, size_bytes):
        with self.lock:
            self.bytes_delivered += size_bytes
            self.chunks_completed += 1
            self._speed_samples.append((time.monotonic(), size_bytes))

    def record_bytes_delivered(self, size_bytes):
        with self.lock:
            self.bytes_delivered += size_bytes
            self._speed_samples.append((time.monotonic(), size_bytes))

    def record_seek(self):
        with self.lock:
            self.seeks += 1

    def record_failure(self):
        with self.lock:
            self.chunks_failed += 1

    def record_retry(self):
        with self.lock:
            self.chunks_retried += 1

    def update_buffer(self, fill, workers):
        with self.lock:
            self.buffer_fill = fill
            self.active_workers = workers

    def get_speed_mbps(self):
        with self.lock:
            now = time.monotonic()
            cutoff = now - 5.0
            recent = [(t, b) for t, b in self._speed_samples if t >= cutoff]
            if len(recent) < 2:
                return 0.0
            window = now - recent[0][0]
            if window <= 0.01:
                return 0.0
            total_bytes = sum(b for _, b in recent)
            return (total_bytes * 8) / (window * 1_000_000)

    def get_elapsed(self):
        if self.start_time <= 0:
            return 0.0
        return time.monotonic() - self.start_time

    def get_avg_speed_mbps(self):
        elapsed = self.get_elapsed()
        if elapsed <= 0.01:
            return 0.0
        with self.lock:
            return (self.bytes_delivered * 8) / (elapsed * 1_000_000)

    def stop(self, stream_id=None):
        """Mark stream as inactive. If stream_id is given, only stop if it
        matches the current stream — prevents stale threads from clobbering."""
        with self.lock:
            if stream_id is not None and stream_id != self.stream_id:
                return  # Stale thread, ignore
            self.active = False
            self.active_workers = 0

    def to_dict(self):
        """Snapshot of all stats as a plain dict (for JSON serialization)."""
        with self.lock:
            return {
                "active": self.active,
                "stream_id": self.stream_id,
                "mode": self.mode,
                "file_size": self.file_size,
                "content_type": self.content_type,
                "upstream_url": self.upstream_url,
                "threads_configured": self.threads_configured,
                "chunk_size": self.chunk_size,
                "bytes_delivered": self.bytes_delivered,
                "chunks_completed": self.chunks_completed,
                "chunks_failed": self.chunks_failed,
                "chunks_retried": self.chunks_retried,
                "active_workers": self.active_workers,
                "buffer_fill": self.buffer_fill,
                "buffer_max": self.buffer_max,
                "seeks": self.seeks,
                "speed_mbps": self.get_speed_mbps(),
                "avg_speed_mbps": self.get_avg_speed_mbps(),
                "elapsed": self.get_elapsed(),
            }
