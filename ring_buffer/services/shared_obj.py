import pickle
import time
import typing as t
from multiprocessing import shared_memory

_MAX_NAME_LENGTH = 32 # shared_memory._SHM_SAFE_NAME_LENGTH


def _padding_name(n: bytes, max_length=_MAX_NAME_LENGTH):
    cleaned_n = n.strip()
    return cleaned_n + b'\x00' * max(max_length - len(cleaned_n), 0)


def _unpad_name(n: bytes, max_length=_MAX_NAME_LENGTH):
    return n.replace(b'\x00', b'').strip()


def construct_key_value(key_str: str, value: t.Any) -> bytes:
    return _padding_name(key_str.encode()) + pickle.dumps(value)


def destruct_key_value(data: bytes) -> t.Tuple[str, t.Any]:
    key = data[:32].decode().strip()
    v = pickle.loads(data[32:])
    return key, v


class SharedObject:

    def __init__(
            self,
            name: t.Optional[str] = None,
            size: int = _MAX_NAME_LENGTH,
            create: bool = False,
    ):
        self._size = size
        if create:
            self.pointer = shared_memory.SharedMemory(
                name, size=self._size, create=True)
            self.pointer.buf[:] = _padding_name(b'')
        else:
            print('get current share memory', name)
            self.pointer = shared_memory.SharedMemory(name)
        self._value = None
        self._create = create
        print('shared_memory size is', self.pointer.size)

    def _get_object_shared_memory(self) -> t.Optional[shared_memory.SharedMemory]:
        v = self._get_pointer_value()
        print('pointer value', v, self.pointer.size)
        if v:
            return shared_memory.SharedMemory(name=v.decode())
        else:
            return None

    def name(self):
        return self.pointer.name

    def size(self):
        return self.pointer.size

    def _get_pointer_value(self) -> bytes:
        return _unpad_name(bytes(self.pointer.buf[:self._size]))

    def set(self, d: dict, dict_size: int = 0):
        # calculate the size of dict
        d_in_bytes = pickle.dumps(d)
        if not dict_size:
            dict_size = len(d_in_bytes)
        # create new ShareMemory
        dict_pointer = shared_memory.SharedMemory(name=None, size=dict_size, create=True)
        # print(dict_pointer, dict_pointer.size, dict_size)
        # assign data to new shared memory
        dict_pointer.buf[:] = d_in_bytes
        # assign new dict value to share memory
        self.pointer.buf[:self._size] = _padding_name(dict_pointer.name.encode())
        # remove old memory
        last_dict = self._get_object_shared_memory()
        if last_dict:
            last_dict.unlink()
        self._value = dict_pointer
        return dict_pointer

    def get(self):
        last_dict = self._get_object_shared_memory()
        if not last_dict:
            return None
        res = pickle.loads(bytes(last_dict.buf))
        return res

    def delete(self):
        last_dict = self._get_object_shared_memory()
        if last_dict:
            last_dict.unlink()
            print('remove pointer', last_dict.name)
        print('remove pointer', self.name())
        self.pointer.unlink()


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

    def build(self, previous_node_name: bytes, data: bytes, next_node_name: bytes):
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


def test_shared_dict():
    sd = SharedObject('test', create=True)
    sd.set('this is a test')
    print(sd.get())
    # set new value to sd
    sd.set("this is new value")
    print(sd.get())
    sd.delete()


def test_long_run_shared_dict():
    sd = SharedObject('test', create=True)
    sd.set('this is a test')
    print('dict value is:', sd.get(), sd.size())
    # set new value to sd
    sd.set("this is new value")
    print('dict value is:', sd.get(), sd.size())
    time.sleep(10)
    print('dict value is:', sd.get(), sd.size())
    print(bytes(sd.pointer.buf))
    so = SharedObject('test')
    print(bytes(so.pointer.buf))
    sd.delete()

if __name__ == '__main__':
    test_long_run_shared_dict()
