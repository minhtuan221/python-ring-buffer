import pickle
import time
import typing as t
from multiprocessing import shared_memory, Process
from ring_buffer.services import padding_name as pad

_MAX_NAME_LENGTH = 32  # shared_memory._SHM_SAFE_NAME_LENGTH


def _padding_name(n: bytes, max_length=_MAX_NAME_LENGTH):
    cleaned_n = n.strip()
    return cleaned_n + b'\x00' * max(max_length - len(cleaned_n), 0)


def _unpad_name(n: bytes, max_length=_MAX_NAME_LENGTH):
    return n.rstrip(b'\x00')


def construct_key_value(key_str: str, value: t.Any) -> bytes:
    return _padding_name(key_str.encode()) + pickle.dumps(value)


def destruct_key_value(data: bytes) -> t.Tuple[str, t.Any]:
    key = data[:32].decode().strip()
    v = pickle.loads(data[32:])
    return key, v


class SharedObject:
    """The shared Object is a shared memory block, save the name of other memory block
    1. Read the block in other process => acquire lock => ok
    2. Write to the block in other process => acquire lock
    => delete the existing block => create new block => same name or other name ?
    if keep same name: => why we need object and store it memory name ?, performance ?
    if different name => how to notify the main thread ?
    if both => keep all name in an array in shared object memory and let other process read the first one
        => need limit the length of array, this act like a channel or queue, which is better ?
        => we need flush old shared memory, each process must keep the pointer of its shared memory alive

    """
    pointer: shared_memory.SharedMemory

    def __init__(
            self,
            name: t.Optional[str] = None,
            size: int = _MAX_NAME_LENGTH,
            create: bool = False,
    ):
        self._size = size
        if create:
            self.pointer = shared_memory.SharedMemory(
                name,
                size=self._size,
                create=True
            )
            pad.set_name(self.pointer.buf, b'')
        else:
            self.pointer = shared_memory.SharedMemory(name)
        self._value = None
        self._create = create

    def _get_object_shared_memory(
            self
    ) -> t.Optional[shared_memory.SharedMemory]:
        v = self._get_pointer_value()
        if v:
            return shared_memory.SharedMemory(name=v.decode())
        else:
            return None

    def name(self):
        return self.pointer.name

    def size(self):
        return self.pointer.size

    def _get_pointer_value(self) -> bytes:
        return pad.get_name(self.pointer.buf)

    def set(self, new_object: t.Any):
        # calculate the size of dict
        obj = pickle.dumps(new_object)
        # create new ShareMemory
        obj_shared_memory = shared_memory.SharedMemory(
            name=None,
            size=len(obj),
            create=True
        )
        # print(dict_pointer, dict_pointer.size, dict_size)
        # assign data to new shared memory
        pad.set_name(obj_shared_memory.buf, obj)
        # assign new dict value to share memory
        pad.set_name(self.pointer.buf, obj_shared_memory.name.encode())
        # remove old memory
        old_obj_shared_memory = self._get_object_shared_memory()
        return obj_shared_memory, old_obj_shared_memory

    def get(self):
        last_dict = self._get_object_shared_memory()
        if not last_dict:
            return None
        res = pickle.loads(pad.get_object(last_dict.buf))
        return res


class Node:
    """Double Linked Node: contain pointer to previous node, next node and contain
    a value of it self (the key)"""

    def __init__(self,
                 name: t.Optional[str] = None,
                 create: bool = False,
                 data_size: int = 32):
        self.size = _MAX_NAME_LENGTH * 2 + data_size
        self._data_size = data_size
        self.create = create
        if create:
            self.pointer = shared_memory.SharedMemory(
                name, size=self.size, create=True)
        else:
            self.pointer = shared_memory.SharedMemory(name)

    def build(self, previous_node_name: bytes, data: bytes,
              next_node_name: bytes):
        if self.create:
            # build the buffer
            bs = _padding_name(previous_node_name) + data + _padding_name(
                next_node_name)
            self.pointer.buf[:] = bs

    def _unpack(self):
        n = bytes(self.pointer.buf).split(_BYTES_DELIMITER)
        return n[0], n[1], n[2]

    def name(self) -> str:
        return self.pointer.name

    def set_previous_node_name(self, name: bytes):
        self.pointer.buf[:_MAX_NAME_LENGTH] = _padding_name(name)

    def previous_node_name(self) -> str:
        p = bytes(self.pointer.buf)[:32]
        return p.decode().strip()

    def value(self) -> str:
        p = bytes(self.pointer.buf)[32:-32]
        return p.decode().strip()

    def data(self) -> bytes:
        p = bytes(self.pointer.buf)[32:-32]
        return p

    def set_next_node_name(self, name: bytes):
        self.pointer.buf[-_MAX_NAME_LENGTH:] = _padding_name(name)

    def next_node_name(self) -> str:
        p = bytes(self.pointer.buf)[-32:]
        return p.decode().strip()

    def close(self):
        self.pointer.close()

    def shutdown(self):
        self.pointer.unlink()

    def to_dict(self) -> dict:
        _d = {k: v for k, v in self.__dict__.items() if not k.startswith(' ')}
        _d['previous_node'] = self.previous_node_name()
        _d['data'] = self.data()
        _d['next_node'] = self.next_node_name()
        return _d

    def __repr__(self):
        return f"{self.__class__.__name__}({','.join([f'{k}={v!r}' for k, v in self.to_dict().items()])})"


_BYTES_DELIMITER = b'|'


def test_shared_obj():
    sd = SharedObject('test1', create=True)
    sd.set('this is a test')
    print(sd.get())
    # set new value to sd
    sd.set("this is new value")
    print(sd.get())
    sd.delete()


def test_read_obj():
    sd = SharedObject('test')
    print(sd.get())


def test_long_run_shared_dict():
    sd = SharedObject('test', create=True)
    sd.set('this is a test')
    print('dict value is:', sd.get(), sd.size())
    # set new value to sd
    sd.set("this is new value")
    print('dict value is:', sd.get(), sd.size())

    # open other process to read the value
    time.sleep(10)
    print('dict value is:', sd.get(), sd.size())
    print(bytes(sd.pointer.buf))
    so = SharedObject('test')
    print(bytes(so.pointer.buf))
    sd.delete()


def read_shared_mem_loop():
    smm = shared_memory.SharedMemory('test')
    while True:
        print(bytes(smm.buf))
        time.sleep(1)


def test_delete_and_recreate_shared_memory():
    smm = shared_memory.SharedMemory('test1', create=True, size=16)
    smm.buf[:5] = b'here1'
    time.sleep(1)
    print(bytes(smm.buf).rstrip(b'\x00'))
    smm.unlink()
    smm = shared_memory.SharedMemory('test1', create=True, size=16)
    smm.buf[:5] = b'here2'
    print(bytes(smm.buf).rstrip(b'\x00'))
    smm.unlink()


if __name__ == '__main__':
    test_shared_obj()
    # test_long_run_shared_dict()
    # test_delete_and_recreate_shared_memory()
