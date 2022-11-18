from multiprocessing import shared_memory
import typing as t
import pickle


_MAX_NAME_LENGTH = 14
_BYTES_DELIMITER = b'|'


def _padding_name(n: bytes, max_length=_MAX_NAME_LENGTH):
    cleaned_n = n.strip()
    return cleaned_n + b' ' * max(max_length - len(cleaned_n), 0)


class SharedObject:

    def __init__(self, name: t.Optional[str]=None, d: dict = None, create: bool = False):
        self._size = _MAX_NAME_LENGTH
        if create:
            self.pointer = shared_memory.SharedMemory(
                name, size=self._size, create=True)
            self.pointer.buf[:] = _padding_name(b'') * 2
        else:
            self.pointer = shared_memory.SharedMemory(name)

    def _get_object_shared_memory(self) -> t.Optional[shared_memory.SharedMemory]:
        v = self._get_pointer_value()
        if v:
            return shared_memory.SharedMemory(name=v.decode())
        else:
            return None

    def _get_pointer_value(self) -> bytes:
        return bytes(self.pointer.buf).strip()
        
    def set(self, d: dict, dict_size: int = 0):
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

        # assign new dict to pointer
        dict_pointer = _padding_name(dict_pointer.name.encode())

    def get(self):
        last_dict = self._get_object_shared_memory()
        if not last_dict:
            return None
        return pickle.loads(bytes(last_dict.buf))

