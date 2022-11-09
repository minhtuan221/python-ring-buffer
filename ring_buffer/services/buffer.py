"""RingBuffer
"""
import typing as t
import threading

from ring_buffer.model.event import Event


class RingFullError(Exception):
    """Ring is Full"""


class RingEmptyError(Exception):
    """Ring is Empty"""


class NullEventError(Exception):
    """Event in ring is Null/None"""


class RingBuffer:
    """A circular queue, faster than normal queue
    """

    def __init__(self, size: int = 2**10):
        """Init RingBuffer

        Args:
            size (int, optional): size of the queue. Defaults to 2**10.
        """
        if size <= 0:
            raise ValueError('Size cannot be lesser than 0')
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
        _index = self._producer_counter
        # increase counter by 1
        self._producer_counter = (self._producer_counter + 1) % self._size
        # return current position
        return _index

    def _get_first_event_index(self) -> int:
        """Get the first event index in the ring

        Returns:
            int: the index of the first event
        """
        _index = self._consumer_counter
        self._consumer_counter = (self._consumer_counter + 1) % self._size
        return _index

    def _is_duplicate_index_counter(self) -> bool:
        """Check if the next index of producer is the first consumer index

        Returns:
            bool: True if they are in the same position
        """
        return self._consumer_counter == self._producer_counter

    def qsize(self) -> int:
        """Return size of the ring buffer

        Returns:
            int: the size of ring
        """
        with self._lock:
            if self._consumer_counter <= self._producer_counter:
                return self._producer_counter - self._consumer_counter
            return self._size - self._consumer_counter + self._producer_counter

    def count_none_pointer(self) -> int:
        """return the total number of None in ring

        Returns:
            int: The total number of None type in ring
        """
        with self._lock:
            none_type_counter = 0
            for i in self._ring:
                if i is None:
                    none_type_counter += 1
            return none_type_counter

    def is_full(self) -> bool:
        """Check if the ring is full or not

        Returns:
            bool: True if the ring is full
        """
        if self._is_duplicate_index_counter():
            return self._ring[self._consumer_counter] is not None
        return False

    def is_empty(self) -> bool:
        """Check if the ring is empty or not

        Returns:
            bool: True if empty
        """
        if self._is_duplicate_index_counter():
            return self._ring[self._consumer_counter] is None
        return False

    def put(self, event: Event) -> int:
        """put a new event in ring buffer

        Args:
            event (Event): New event is put by a producer

        Returns:
            int: index of event in ring buffer

        Raises:
            RingFullError: Ring is full
        """
        if not isinstance(event, Event):
            raise ValueError('Cannot put anything other than Event in queue')
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
            if not isinstance(first_event, Event):
                raise NullEventError(f"Event get from ring buffer is not \
                    Event, receive {first_event}")
            self._ring[first_event_index] = None
            return first_event
