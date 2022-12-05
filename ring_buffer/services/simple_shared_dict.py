"""Simple shared Dict
"""
from ring_buffer.services import shared_mem_tracker
import multiprocessing as mp

from ring_buffer.services.shared_obj import SharedObject


class SimpleSharedDict:
    """Simple shared memory Dict A Dict with key is the name of shared
    memory, backend by memory tracker contain a shared memory name which
    point to sharable Memory List. Add new member to the dict will cause the
    inner List reallocate memory
    """

    def __init__(
            self,
            name: str,
            sync_queue: mp.SimpleQueue,
            create: bool = False,
    ):
        self._list_key = list()
        self._sync_queue = sync_queue
        self._tracker = shared_mem_tracker.SharedMemoryTracker(self._sync_queue)
        self._shared_set = SharedObject(name, create=True)
        self._tracker.set_change(1, name, self._shared_set.pointer)
        new_value, old_value = self._shared_set.set(set())
        self._tracker.set_change(1, new_value.name, new_value)

    def set(self, key, value):
        pass

    def get(self, key, default=None):
        pass

    def pop(self, key):
        pass

    def keys(self):
        pass

    def shutdown(self):
        self._tracker.shutdown()
