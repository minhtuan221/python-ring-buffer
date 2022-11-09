"""This module is about consumer of ring buffer
"""
import time
import typing as t
import threading

from ring_buffer.model import event
from ring_buffer.services import buffer


class ConsumerIsNotStopError(Exception):
    """Something wrong when start consumer"""


class ConsumerAlreadyRunningError(Exception):
    """Something wrong when start consumer"""


class Consumer:
    """Simple one Consumer"""

    def __init__(self, name: str, ring_buffer: buffer.RingBuffer):
        """Init consumer

        Args:
            name (str): name of the consumer
            ring_buffer (buffer.RingBuffer): RingBuffer
        """
        self.name = name
        self._ring_buffer = ring_buffer
        self._callback: t.Optional[t.Callable[[event.Event], None]] = None
        self._thread: t.Optional[threading.Thread] = None
        self._is_stop: bool = True

    def stop(self):
        """Stop the consumer thread
        """
        self._is_stop = True

    def is_running(self) -> bool:
        """Check if the consumer is running or stop

        Returns:
            bool: True if it's running
        """
        return not self._is_stop

    def register_callback(self,
                          callback: t.Callable[[event.Event], None]):
        """Add callback function

        Args:
            callback (t.Callable[[event.Event]]):
                callback function should receive Event and return None
        """
        self._callback = callback

    def _consume(self):
        """consume message from ring buffer
        """
        while not self._is_stop:
            qsize = self._ring_buffer.qsize()
            for _ in range(qsize):
                # get the event from buffer
                # put message input callback function
                try:
                    self._callback(self._ring_buffer.get())
                except buffer.RingEmptyError:
                    break
            time.sleep(0.1)

    def start(self):
        """Start consume from ring buffer

        Raises:
            ValueError: callback function cannot be None
            ConsumerIsNotStopError: consumer is not stopped
            ConsumerAlreadyRunningError: a thread is already running
        """
        if self._callback is None:
            raise ValueError("callback function cannot be None")
        if self._is_stop is False:
            raise ConsumerIsNotStopError('consumer is not stopped')
        if self._thread and self._thread.is_alive():
            raise ConsumerAlreadyRunningError('a thread is already running')
        self._thread = threading.Thread(name=self.name,
                                        target=self._consume,
                                        daemon=True)
        self._thread.start()
        self._is_stop = False
