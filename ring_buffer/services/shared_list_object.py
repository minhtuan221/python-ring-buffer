from multiprocessing import shared_memory
import typing as t
import pickle
import ctypes
import time


class SharedListObject:
    """Shared Fix Lenght Memory List of Object
    """

    def __init__(self,
                 name: str,
                 size: int,
                 element_size: int = 255,
                 create: bool = False) -> None:
        """Init Share Fix Memory List of Object

        Args:
            name (str): Name of share memory space
            size (int): Number of element can have in the list
            element_size (int, optional):
                Size by bytes of one element. Defaults to 255.
            create (bool, optional):
                True if create new shared memory zone. Defaults to False.
        """
        self._element_size = element_size
        self.name = name
        # size of the element in ShareableList
        # element size = math.ceil((element_size+1)/8)*8)
        if create:
            self.share_list = \
                shared_memory.ShareableList(
                    [bytes(self._element_size) for _ in range(size)],
                    name=self.name
                )
        else:
            self.share_list = \
                shared_memory.ShareableList(name=self.name)

    def set(self, _index: int, obj: t.Any):
        """Set an object into list by index

        Args:
            _index (int): index must be less than size of list
            obj (t.Any): Python Pickleable Object
        """
        obj_bytes = pickle.dumps(obj)
        self.share_list[_index] = obj_bytes

    def get(self, _index: int):
        """Get object by index from list

        Args:
            _index (int): index must be less than size of list

        Returns:
            _type_: _description_
        """
        obj_bytes = self.share_list[_index]
        if len(obj_bytes) == 0:
            return None
        return pickle.loads(obj_bytes)

    def remove(self, _index: int = -1) -> t.Any:
        """Remove an element from the list. The position will be empty bytes

        Args:
            _index (int, optional): index of remove element. Defaults to -1.

        Returns:
            t.Any: _description_
        """
        obj_bytes = self.share_list[_index]
        self.share_list[_index] = bytes(self._element_size)
        if len(obj_bytes) == 0:
            return None
        return pickle.loads(obj_bytes)

    def shutdown(self):
        """Release the shared memory after use
        """
        self.share_list.shm.unlink()


_LIST_POINTER_LENGTH = 38
# is the byte lenght of (b'n'*14, 2**64) after pickled


class SharedDynamicListObject:
    """Shared list with dynamic length and size of element
    It will be slower than fix length shared list
    """

    def __init__(self, name: str, create: bool = False):
        if len(name) > shared_memory._SHM_SAFE_NAME_LENGTH:
            raise ValueError("name cannot be too long")
        self.name = name
        self._size = 0
        self._name_length = shared_memory._SHM_SAFE_NAME_LENGTH + 7
        self._create = create
        self._list_pointer = \
            shared_memory.SharedMemory(
                name=self.name,
                size=_LIST_POINTER_LENGTH,
                create=self._create)
        self._create_shared_list_element()

    def shutdown(self):
        self._list_pointer.unlink()
        self._shared_list_element.shm.unlink()

    def _size_as_byte(self):
        return f"{'0'*10}{str(self._size)}"[-10:]

    def _update_list_pointer(self):
        self._list_pointer.buf[:] = \
            bytearray(pickle.dumps(f"{self._shared_list_element.shm.name}|{self._size_as_byte()}"))

    def _create_shared_list_element(self):
        if self._create:
            self._shared_list_element = shared_memory.ShareableList()
        self._shared_list_element = shared_memory.ShareableList([])
        self._update_list_pointer()


def test_dynamic_list_obj():
    pointer_name = pickle.dumps((b'n'*14, 2**64))
    print(len(pointer_name), type(pointer_name))
    print(pointer_name)
    print(bytearray(pointer_name))
    pointer = pickle.loads(pointer_name)
    print(pointer)
    # return

    shm = SharedDynamicListObject('test', create=True)
    buf = shm._list_pointer.buf
    print(buf)
    print(pickle.loads(buf))
    shm.shutdown()


def maintain_share_list_object():
    smm = SharedListObject('test1', 255)
    smm.set(0, 'first element')
    while True:
        time.sleep(10)
    smm.shutdown()


def test_share_list_object_1():
    smm = SharedListObject('test', 16)
    _next = ''
    for i in range(20):
        _next += f'{i}'
        smm.set(1, bytes(_next, encoding='utf8'))
        v = smm.get(i)
        print(v, type(v), len(v))
        # print(len(smm.share_list))
    print(smm.share_list)


if __name__ == "__main__":
    test_dynamic_list_obj()
    # maintain_share_list_object()
