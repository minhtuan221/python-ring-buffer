import pickle
import typing as t
from multiprocessing import shared_memory

_MAX_NAME_LENGTH = 14
_BYTES_DELIMITER = b'|'


def _padding_name(n: bytes, max_length=_MAX_NAME_LENGTH):
    cleaned_n = n.strip()
    return cleaned_n + b' ' * max(max_length - len(cleaned_n), 0)


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
            self.pointer.buf[:] = _padding_name(b'') * 2
        else:
            self.pointer = shared_memory.SharedMemory(name)
        if d:
            self.set(d, dict_size)

    def _get_object_shared_memory(self) -> t.Optional[shared_memory.SharedMemory]:
        v = self._get_pointer_value()
        if v:
            return shared_memory.SharedMemory(name=v.decode())
        else:
            return None

    def _get_pointer_value(self) -> bytes:
        return bytes(self.pointer.buf).strip()

    def set(self, d: dict, dict_size: int = 0):
        # calculate the size of dict
        d_in_bytes = pickle.dumps(d)
        if not dict_size:
            dict_size = len(d_in_bytes)
        # check if we need a new ShareMemory
        last_dict = self._get_object_shared_memory()
        if not last_dict or last_dict.size < dict_size:
            # create new pointer
            dict_pointer = shared_memory.SharedMemory(name=None, size=dict_size, create=True)
            self.pointer.buf[:] = _padding_name(dict_pointer.name.encode())
        else:
            dict_pointer = last_dict

        # assign new dict value to share memory
        dict_pointer.buf[:] = _padding_name(d_in_bytes, max_length=dict_size)

        # close the value
        dict_pointer.close()

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
        self.pointer.unlink()
