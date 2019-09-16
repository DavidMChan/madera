"""
Based on https://github.com/timberio/timber-python/blob/master/timber/flusher.py
"""

import sys
import time
import threading
import queue


RETRY_SCHEDULE = (1, 10, 60)  # seconds


def _initial_time_remaining(flush_interval):
    return flush_interval


def _calculate_time_remaining(last_flush, flush_interval):
    elapsed = time.time() - last_flush
    time_remaining = max(flush_interval - elapsed, 0)
    return time_remaining


def _should_retry(status_code):
    return 500 <= status_code < 600


class Publisher(threading.Thread):
    def __init__(self, upload, pipe, buffer_capacity, flush_interval):
        super(Publisher, self).__init__()

        # Save some thread details
        self.parent_thread = threading.currentThread()
        self.upload = upload
        self.pipe = pipe
        self.buffer_capacity = buffer_capacity
        self.flush_interval = flush_interval

    def run(self,):
        while True:
            self.step()

    def step(self,):
        frame = []
        while True:
            shutdown = not self.parent_thread.is_alive()
            try:
                # Get an entry from the frame, with up to 1s of wait time
                entry = self.pipe.get(block=(not shutdown), timeout=1.0)
                frame.append(entry)
                self.pipe.task_done()
            except queue.Empty:
                if shutdown or frame:
                    break

        self.upload(frame)
        if shutdown:
            sys.exit(0)
