"""
Based on https://github.com/timberio/timber-python
"""

import logging
import multiprocessing
import queue

from madera.logging.publisher import Publisher
from madera.logging.uploader import Uploader
from madera.logging.frame import create_frame


DEFAULT_BUFFER_CAPACITY = 1000
DEFAULT_FLUSH_INTERVAL = 2
DEFAULT_RAISE_EXCEPTIONS = False
DEFAULT_DROP_EXTRA_EVENTS = True


def _is_main_process():
    return multiprocessing.current_process()._parent_pid is None  # pylint: disable=protected-access


class MaderaHandler(logging.Handler):

    def __init__(self,
                 host,
                 experiment,
                 run_id,
                 api_key,
                 port=None,
                 buffer_capacity=DEFAULT_BUFFER_CAPACITY,
                 flush_interval=DEFAULT_FLUSH_INTERVAL,
                 raise_exceptions=DEFAULT_RAISE_EXCEPTIONS,
                 drop_extra_events=DEFAULT_DROP_EXTRA_EVENTS,
                 level=logging.NOTSET):

        super(MaderaHandler, self).__init__(level=level)

        # Store some settings
        self.host = host
        self.buffer_capacity = buffer_capacity
        self.flush_interval = flush_interval
        self.raise_exceptions = raise_exceptions
        self.drop_extra_events = drop_extra_events
        self.experiment = experiment
        self.run_id = run_id
        self.api_key = api_key

        if port is None:
            self.host, self.port = self.host.split(':')
        else:
            self.port = port

        # Connect to the log server, and announce the experiment
        self.uploader = Uploader(self.host, self.port, self.api_key, self.experiment, self.run_id)

        # Build the publisher
        self.pipe = multiprocessing.JoinableQueue(maxsize=buffer_capacity)
        self.publisher_thread = Publisher(
            self.uploader,
            self.pipe,
            self.buffer_capacity,
            self.flush_interval)

        # Stat tracking
        self.dropped_events = 0

        if _is_main_process():
            self.publisher_thread.start()

    def emit(self, record):
        # If the publisher has died, then we need to restart the thread
        if _is_main_process() and not self.publisher_thread.is_alive():
            self.publisher_thread.start()
        message = self.format(record)  # Format the log string
        frame = create_frame(record, message, self.uploader.rank_id)  # Build an element to send

        # Add the frame to the multiprocessing queue for flushing to the server
        try:
            print('Emitting message...')
            self.pipe.put(frame, block=(not self.drop_extra_events))
        except queue.Full:
            self.dropped_events += 1
