"""This module have to solve the problems
 - Add/remove shared memory => use tracker and simple queue
 - synchronize shared memory
 - shared memory pointer ?
 - Dict/list shared memory structure ?
 - dynamic length list/dict ?
 - pad/unpad name from buffer ? => unpad object, get name from buffer
"""
import multiprocessing
import pickle
import queue
import time
import typing as t
from multiprocessing import shared_memory

from ring_buffer.services import padding_name as pad

"""
Dynamic shared memory list/dict:
1. Linkedlist which all shared memory name is pointers in node
2. Dict with key is the shared memory name => loop over a dict ? 
limit the length of name ?
"""


class SharedMemoryTracker:
    """Shared Memory tracker
     - consume a queue => add/remove shared memory name
     - save all memory in a dict
     - shutdown => close all memory
    """

    def __init__(
            self,
            sync_queue: multiprocessing.SimpleQueue,
            interval: float = 0.005
    ):
        self._track_map: t.Dict[str, shared_memory.SharedMemory] = {}
        self._queue = sync_queue
        self._stop = False
        self._interval = interval

    @property
    def queue(self) -> multiprocessing.SimpleQueue:
        return self._queue

    @property
    def is_stop(self) -> bool:
        return self._stop

    def stop(self):
        self._stop = False

    @staticmethod
    def _new_key_value(key: str, value):
        if not isinstance(key, str):
            raise ValueError(f'key must be string type, receive {key}')
        # calculate the size of value
        value_bytes = pickle.dumps(value)
        smm = shared_memory.SharedMemory(
            name=key,
            size=len(value_bytes),
            create=True,
        )
        pad.set_name(smm.buf, value_bytes)
        return smm

    def _signal_to_action(self, _signal: int, key: str, value):
        if _signal == -1:
            smm = self._track_map.pop(key)
            smm.unlink()
        elif _signal == 1:
            self._track_map[key] = self._new_key_value(key, value)
        elif _signal == 0:
            self.shutdown()

    def set_change(self, _signal: int, key: str, value):
        self._queue.put((_signal, key, value))

    def flush(self):
        while not self._queue.empty():
            _signal, key, value = self._queue.get()
            self._signal_to_action(_signal, key, value)
            if _signal == 0:
                # signal to stop the manager
                return False
        return True

    def run(self):
        self._stop = False
        while not self._stop:
            should_continue = self.flush()
            if not should_continue:
                break
            time.sleep(self._interval)

    def values(self):
        return self._track_map.values()

    def items(self):
        return self._track_map.items()

    def shutdown(self):
        self._stop = True
        # remove all shared memories
        for v in self._track_map.values():
            v.unlink()


def test_add_remove_to_tracker(smt: SharedMemoryTracker):
    smt.set_change(1, 'test1', 100)
    time.sleep(1)
    smt.set_change(1, 'test2', "i liked it")
    smt.set_change(1, 'test3', "i don't like it")
    time.sleep(1)
    smt.set_change(-1, 'test1', None)
    time.sleep(1)
    smt.set_change(0, None, None)


def test_shared_memory_tracker():
    simple_queue = multiprocessing.SimpleQueue()
    smt = SharedMemoryTracker(simple_queue)
    # create a process which put change in to smt
    process = multiprocessing.Process(
        name='test_tracker',
        target=test_add_remove_to_tracker,
        args=(smt,),
        daemon=True
    )
    process.start()

    time.sleep(1)
    smt.flush()
    print(smt.items())
    assert len(smt.items()) == 1
    smt.flush()
    time.sleep(1)
    smt.flush()
    print(smt.items())
    assert len(smt.items()) == 3
    time.sleep(1)
    smt.flush()
    print(smt.items())
    assert len(smt.items()) == 2
    # smt.shutdown()
    time.sleep(3)
    smt.flush()


if __name__ == '__main__':
    test_shared_memory_tracker()


