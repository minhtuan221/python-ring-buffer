"""RingBuffer
"""
import typing as t
import threading

from ring_buffer.model.event import Event


class RingFullError(Exception):
    """Ring is Full"""


class RingEmptyError(Exception):
    """Ring is Empty"""


class RingBuffer:
    """A circular queue, faster than normal queue
    """

    def __init__(self, size: int = 2**10):
        """Init RingBuffer

        Args:
            size (int, optional): size of the queue. Defaults to 2**10.
        """
        self._size: int = size
        self._ring: t.List[t.Optional[Event]] = [None] * size
        self._producer_counter: int = 0
        self._consumer_counter: int = 0
        self._lock = threading.Lock()

    def _get_new_event_index(self) -> int:
        """Get the index of new event in ring

        Returns:
            int: the index of new event in the ring
        """
        self._producer_counter = self._producer_counter + 1 % self._size
        return self._producer_counter

    def _get_first_event_index(self) -> int:
        """Get the first event index in the ring

        Returns:
            int: the index of the first event
        """
        self._consumer_counter = self._consumer_counter + 1 % self._size
        return self._consumer_counter

    def is_full(self) -> bool:
        """Check if the ring is full or not

        Returns:
            bool: True if the ring is full
        """
        return (self._producer_counter + 1) % self._size \
               == self._consumer_counter

    def is_empty(self) -> bool:
        """Check if the ring is empty or not

        Returns:
            bool: True if empty
        """
        return self._consumer_counter == self._producer_counter

    def put(self, event: Event) -> int:
        """put a new event in ring buffer

        Args:
            event (Event): New event is put by a producer

        Returns:
            int: index of event in ring buffer

        Raises:
            RingFullError: Ring is full
        """
        with self._lock:
            if self.is_full():
                raise RingFullError('ring is full')
            new_event_index = self._get_new_event_index()
            self._ring[new_event_index] = event
            return new_event_index

    def get(self) -> Event:
        """get the first event in the queue, this equivalent to popleft

        Returns:
            Event: The first event

        Raises:
            RingEmptyError: Ring is Empty
        """
        with self._lock:
            if self.is_empty():
                raise RingEmptyError('Ring is empty')
            first_event_index = self._get_first_event_index()
            first_event = self._ring[first_event_index]
            self._ring[self._get_first_event_index()] = None
            return first_event
