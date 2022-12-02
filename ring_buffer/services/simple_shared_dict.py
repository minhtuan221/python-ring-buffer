"""Simple shared Dict
"""
from ring_buffer.services import shared_mem_tracker
import multiprocessing as mp


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
