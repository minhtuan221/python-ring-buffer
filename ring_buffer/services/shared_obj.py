import pickle
import time
import typing as t
from multiprocessing import shared_memory

_MAX_NAME_LENGTH = 32 # shared_memory._SHM_SAFE_NAME_LENGTH


def _padding_name(n: bytes, max_length=_MAX_NAME_LENGTH):
    cleaned_n = n.strip()
    return cleaned_n + b' ' * max(max_length - len(cleaned_n), 0)


def construct_key_value(key_str: str, value: t.Any) -> bytes:
    return _padding_name(key_str.encode()) + pickle.dumps(value)


def destruct_key_value(data: bytes) -> (str, t.Any):
    key = data[:32].decode().strip()
    v = pickle.loads(data[32:])
    return key, v


class SharedObject:

    def __init__(
            self,
            name: t.Optional[str] = None,
            d: dict = None,
            create: bool = False,
            dict_size: int = 0
    ):
        self._size = _MAX_NAME_LENGTH
        if create:
            self.pointer = shared_memory.SharedMemory(
                name, size=self._size, create=True)
            self.pointer.buf[:] = _padding_name(b'')
        else:
            self.pointer = shared_memory.SharedMemory(name)
        self._value = None
        if d:
            self.set(d, dict_size)

    def _get_object_shared_memory(self) -> t.Optional[shared_memory.SharedMemory]:
        v = self._get_pointer_value()
        print('pointer value', v)
        if v:
            return shared_memory.SharedMemory(name=v.decode())
        else:
            return None

    def name(self):
        return self.pointer.name

    def _get_pointer_value(self) -> bytes:
        return bytes(self.pointer.buf).strip()

    def set(self, d: dict, dict_size: int = 0):
        # calculate the size of dict
        d_in_bytes = pickle.dumps(d)
        # if not dict_size:
        dict_size = len(d_in_bytes)
        # check if we need a new ShareMemory
        last_dict = self._get_object_shared_memory()
        dict_pointer = shared_memory.SharedMemory(name=None, size=dict_size, create=True)
        # if not last_dict or last_dict.size < dict_size:
        #     # create new pointer
        #     dict_pointer = shared_memory.SharedMemory(name=None, size=dict_size, create=True)
        #     self.pointer.buf[:] = _padding_name(dict_pointer.name.encode())
        #     # # remove last dict
        #     # if last_dict:
        #     #     print('remove pointer', last_dict.name)
        #     #     last_dict.unlink()
        # else:
        #     dict_pointer = last_dict

        # assign new dict value to share memory
        if last_dict:
            last_dict.unlink()
        print(dict_pointer, dict_pointer.size, dict_size)
        self.pointer.buf[:] = _padding_name(dict_pointer.name.encode())
        dict_pointer.buf[:] = d_in_bytes  # _padding_name(d_in_bytes, max_length=dict_size)
        print(dict_pointer)
        self._value = dict_pointer
        return dict_pointer

        # close the value
        # dict_pointer.close()

    def get(self):
        last_dict = self._get_object_shared_memory()
        if not last_dict:
            return None
        res = pickle.loads(bytes(last_dict.buf))
        last_dict.close()
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
    m1 = sd.set('this is a test')
    print(sd.get())
    # set new value to sd
    sd.set("this is new value")
    print(sd.get())
    sd.delete()

if __name__ == '__main__':
    test_shared_dict()