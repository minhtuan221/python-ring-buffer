import dataclasses
import pickle
import time
import typing as t
from multiprocessing import shared_memory


@dataclasses.dataclass
class _T:
    event: str
    data: dict


class SharedListObject:
    """Shared Fix Lenght Memory List of Object
    """

    def __init__(self,
                 name: str,
                 size: int = 1,
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


class SharedDictObject:
    """Shared Dict with dynamic length
    Key is the name of the shared memory
    Iterate over the dict by a Stack, each node is contain name of 2 share memory
    """

    def __init__(self, name: str, create: bool = False):
        if len(name) > shared_memory._SHM_SAFE_NAME_LENGTH:
            raise ValueError("name cannot be too long")
        self.name = name
        self._name_length = shared_memory._SHM_SAFE_NAME_LENGTH + 7
        self._create = create
        # the first node of the stack
        if create:
            self._first_node = self.create_node(b'', b'', node_name=self.name)

    def _padding_name(self, n: bytes):
        cleaned_n = n.strip()
        return b' ' * max(14 - len(cleaned_n), 0) + cleaned_n

    def _couple_padding_name(self, n1: bytes, n2: bytes):
        return self._padding_name(n1) + b'|' + self._padding_name(n2)

    def create_node(self, key: bytes, next_node_name: bytes, node_name: str = '', size: int = 0):
        if not node_name:
            node_name = None
        # _key = self._padding_name(key)
        # _next_node_name = self._padding_name(next_node_name)
        node_in_bytes = self._couple_padding_name(key, next_node_name)
        if not size:
            size = 14 * 2 + 1  # size of 2 name and one delimiter '|'
        else:
            size = len(node_in_bytes)
        sm = shared_memory.SharedMemory(node_name, size=size, create=True)
        sm.buf[:] = node_in_bytes
        print('create_node', sm.name, node_in_bytes)
        return sm

    def _get_next_node_name(self, s: shared_memory.SharedMemory = None):
        if not s:
            s = self._first_node
        node: t.List[bytes] = bytes(s.buf).split(b'|')
        return node[1].strip()

    def append_node(self, key: str):
        # get the last node name (first in, last out)
        node: t.List[bytes] = bytes(self._first_node.buf).split(b'|')
        print('last node name', node[1])
        if not node[1]:
            print('this is the first element of dict')
        # create new node in the stack
        new_stack_node = self.create_node(key.encode(), node[1])
        self._first_node.buf[:] = self._couple_padding_name(b'', new_stack_node.name.encode())
        print('_first_node', self._first_node.name, bytes(self._first_node.buf))
        print('_new_appended_node', new_stack_node.name, bytes(new_stack_node.buf))

        # self._first_node.buf[:] =

    def get_node(self, node_name: str):
        """Get existing share memory

        Args:
            node_name: The of the pointer

        Returns:

        """
        return shared_memory.SharedMemory(node_name, create=False)

    def set(self, key: str, value: object):
        # create new share memory with key
        value_in_bytes = pickle.dumps(value)
        new_share_memory = shared_memory.SharedMemory(name=key, create=True, size=len(value_in_bytes))
        # assign new object to new share_memory
        new_share_memory.buf[:] = value_in_bytes
        # add new share to the stack
        print('new shared value', new_share_memory.name, bytes(new_share_memory.buf))
        self.append_node(key)
        new_share_memory.close()

    def get(self, key: str):
        shared_memory_in_byte = bytes(shared_memory.SharedMemory(name=key).buf)
        return pickle.loads(shared_memory_in_byte)

    def keys(self):
        res = []
        next_node_name = self._get_next_node_name()

        while next_node_name:
            res.append(next_node_name)
            print('next_node_name', next_node_name)
            s = self.get(next_node_name.decode())
            next_node_name = self._get_next_node_name(s)
            if not next_node_name:
                break
        return res

    def shutdown(self):
        self._first_node.unlink()
        # self._shared_list_element.shm.unlink()

    def _size_as_byte(self):
        return f"{'0' * 10}{str(self._size)}"[-10:]

    def _update_list_pointer(self):
        self._first_node.buf[:] = \
            bytearray(pickle.dumps(f"{self._shared_list_element.shm.name}|{self._size_as_byte()}"))

    def _create_shared_list_element(self):
        if self._create:
            self._shared_list_element = shared_memory.ShareableList()
        self._shared_list_element = shared_memory.ShareableList([])
        self._update_list_pointer()


def test_dynamic_dict_obj():
    # pointer_name = pickle.dumps((b'n'*14, 2**64))
    # print(len(pointer_name), type(pointer_name))
    # print(pointer_name)
    # print(bytearray(pointer_name))
    # pointer = pickle.loads(pointer_name)
    # print(pointer)
    # return

    shm = SharedDictObject('test', create=True)
    buf = shm._first_node.buf
    print('first_node_name',shm._first_node.name, bytes(buf))
    event1 = _T('bla', {'name': 'foo'})
    shm.set('test1234', event1)
    shm.get('test1234')
    print(shm.keys())
    shm.shutdown()


def maintain_share_list_object():
    smm = SharedListObject('test1', 255, create=True)
    smm.set(0, 'first element')
    while True:
        print(time.time(), smm.share_list[0])
        time.sleep(1)
    smm.shutdown()


def test_share_list_object_1():
    smm = SharedListObject('test', 16, create=True)
    _next = ''
    for i in range(20):
        _next += f'{i}'
        smm.set(i, bytes(_next, encoding='utf8'))
        v = smm.get(i)
        print(v, type(v), len(v))
        # print(len(smm.share_list))
    print(smm.share_list)


if __name__ == "__main__":
    test_share_list_object_1()
    # maintain_share_list_object()
