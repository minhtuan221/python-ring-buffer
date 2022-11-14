import typing as t
import pickle
from multiprocessing import shared_memory


_MAX_NAME_LENGTH = shared_memory._SHM_SAFE_NAME_LENGTH
_BYTES_DELIMITER = b'|'


def _padding_name(n: bytes, max_length=_MAX_NAME_LENGTH):
    cleaned_n = n.strip()
    return cleaned_n + b' ' * max(max_length - len(cleaned_n), 0)


class Node:
    """Double Linked Node: contain pointer to previous node, next node and contain
    a value of it self (the key)"""

    def __init__(self,
                 name: t.Optional[str] = None,
                 create: bool = False,
                 data_size: int = 14):
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
            bs = _padding_name(previous_node_name) + _padding_name(data) + _padding_name(
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
        p = bytes(self.pointer.buf)[:14]
        return p.decode().strip()

    def value(self) -> str:
        p = bytes(self.pointer.buf)[14:-14]
        return p.decode().strip()

    def data(self) -> bytes:
        p = bytes(self.pointer.buf)[14:-14]
        return p

    def set_next_node_name(self, name: bytes):
        self.pointer.buf[-_MAX_NAME_LENGTH:] = _padding_name(name)

    def next_node_name(self) -> str:
        p = bytes(self.pointer.buf)[-14:]
        return p.decode().strip()

    def to_dict(self) -> dict:
        _d = {k: v for k, v in self.__dict__.items() if not k.startswith(' ')}
        _d['previous_node'] = self.previous_node_name()
        _d['data'] = self.data()
        _d['next_node'] = self.next_node_name()
        return _d

    def __repr__(self):
        return f"{self.__class__.__name__}({','.join([f'{k}={v!r}' for k,v in self.to_dict().items()])})"


class ValueNode(Node):

    def __init__(self,
                 name: t.Optional[str] = None,
                 value: t.Any = None,
                 key: str = ''):
        if not name and not value:
            raise ValueError('value and name cannot be None togetther')
        if value:
            create = True
            value_in_bytes = pickle.dumps(value)
            key_in_bytes = _padding_name(key.encode(), max_length=32)
            buf_size = len(key_in_bytes) + len(value_in_bytes)
            super().__init__(name, create=create, data_size=buf_size)
            
        else:
            create = False


class SharedStackObject:

    def __init__(self, name: str, create: bool = False):
        self.name = name
        self.create = create
        self.size = _MAX_NAME_LENGTH * 2  # name of the first node in stack
        if create:
            self.pointer = shared_memory.SharedMemory(
                name, size=self.size, create=True)
        else:
            self.pointer = shared_memory.SharedMemory(name)
        self.pointer.buf[:] = _padding_name(b'') * 2
        self._node_cache_map: t.Dict[str, Node] = {}

    def _get_last_node(self):
        n = bytes(self.pointer.buf[-14:]).decode()
        return self.get_node(n.strip())

    def _get_first_node(self):
        n = bytes(self.pointer.buf[:14]).decode()
        return self.get_node(n.strip())

    def _is_first_node(self) -> bool:
        return len(bytes(self.pointer.buf).strip()) == 0

    def append_node(self, data: bytes) -> Node:
        # create new node
        node = Node(create=True)
        # check if it is the first node
        if not self._is_first_node():
            last_node = self._get_last_node()
            node.build(last_node.name().encode(),
                       data,
                       _padding_name(b''))
            last_node.set_next_node_name(node.name().encode())
        else:
            node.build(_padding_name(b''),
                       data,
                       _padding_name(b''))
            # the first element name is the same as last
            self.pointer.buf[:14] = _padding_name(node.name().encode())
        self.pointer.buf[-14:] = _padding_name(node.name().encode())
        self._node_cache_map[node.name()] = node
        return node

    def remove_node(self, node_name: str) -> Node:
        # find the node by shared memory name
        _node = self.get_node(node_name)
        if not _node:
            raise KeyError(f"cannot find node name {node_name}")
        # get the previous node
        _previous_node = self.get_node(_node.previous_node_name())
        # get the next node
        _next_node = self.get_node(_node.next_node_name())
        # swap previous node to next node
        if _previous_node:
            _previous_node.set_next_node_name(_node.next_node_name().encode())
        else:
            # this is removing the first element in linkedlist
            if not _next_node:
                self.pointer.buf[:14] = _padding_name(b' ')
            else:
                self.pointer.buf[:14] = _padding_name(_next_node.name().encode())
        if _next_node:
            _next_node.set_previous_node_name(_node.previous_node_name().encode())
        else:
            # this is removing the last element in linkedlist
            if not _previous_node:
                self.pointer.buf[-14:] = _padding_name(b' ')
            else:
                self.pointer.buf[-14:] = _padding_name(_previous_node.name().encode())
        # free removed node name memory
        _node.pointer.unlink()
        # remove node_name in cache
        self._node_cache_map.pop(node_name)
        return  _node

    def get_node(self, node_name: str) -> t.Optional[Node]:
        _node_name = node_name.strip()
        if not _node_name:
            return None
        if _node_name in self._node_cache_map:
            return self._node_cache_map[_node_name]
        return Node(_node_name)

    def get_all_nodes(self):
        res = []
        first_node = self._get_first_node()
        while first_node:
            res.append(first_node)
            first_node = self.get_node(first_node.next_node_name())
            if not first_node:
                break
        return res

    def shutdown(self):
        nodes = self.get_all_nodes()
        for n in nodes:
            n.pointer.unlink()
        self.pointer.unlink()


class SharedDictObject:

    def __init__(self, name: str, create: bool = False):
        self.name = name
        self.create = create
        # create a stack to save the hash of all key
        self._stack = SharedStackObject(name, create=create)
        self._value_cache_map: t.Dict[str, shared_memory.SharedMemory] = {}

    def set(self, key: str, value):
        pass

    def get(self, key: str):
        pass

    def pop(self, key: str):
        pass

    def keys(self) -> t.List[str]:
        pass

    def values(self) -> list:
        pass

    def items(self) -> t.List[t.Tuple[str, t.Any]]:
        pass


def test_stack_object():
    stack = SharedStackObject('test', create=True)
    stack.append_node(b'e_1')
    stack.append_node(b'e_2')
    stack.append_node(b'e_3')
    stack.append_node(b'e_4')
    stack.append_node(b'e_5')
    print(stack._node_cache_map)
    nodes = stack.get_all_nodes()
    for n in nodes:
        if n.value() == 'e_1':
            stack.remove_node(n.name())
        print(n)
    print('------ stack after remove the second element ------ ')
    nodes = stack.get_all_nodes()
    for n in nodes:
        print(n)

    stack.shutdown()


if __name__ == '__main__':
    test_stack_object()
