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


class SharedLinkedList:

    def __init__(self, name: str, create: bool = False):
        self._size = _MAX_NAME_LENGTH * 2  # name of the first and last node
        if create:
            self.pointer = shared_memory.SharedMemory(
                name, size=self._size, create=True)
            self.pointer.buf[:] = _padding_name(b'') * 2
        else:
            self.pointer = shared_memory.SharedMemory(name)
        self._node_cache_map: t.Dict[str, Node] = {}

    def get_last_node(self) -> t.Optional[Node]:
        n = bytes(self.pointer.buf[-14:]).decode()
        return self.get_node(n.strip())

    def get_first_node(self) -> t.Optional[Node]:
        n = bytes(self.pointer.buf[:14]).decode()
        return self.get_node(n.strip())

    def is_empty(self) -> bool:
        return len(bytes(self.pointer.buf).strip()) == 0

    def name(self) -> str:
        return self.pointer.name

    def append_node(self,
                    data: bytes,
                    name: t.Optional[str] = None) -> Node:
        # create new node
        node = Node(name=name, create=True, data_size=len(data))
        # check if it is the first node
        if self.is_empty():
            node.build(_padding_name(b''),
                       data,
                       _padding_name(b''))
            # the first element name is the same as last
            # assign name to first element
            self.pointer.buf[:14] = _padding_name(node.name().encode())
        else:
            last_node = self.get_last_node()
            node.build(last_node.name().encode(),
                       data,
                       _padding_name(b''))
            last_node.set_next_node_name(node.name().encode())
        # assign name to last element
        self.pointer.buf[-14:] = _padding_name(node.name().encode())
        self._node_cache_map[node.name()] = node
        return node

    def append_left_node(self, data: bytes) -> Node:
        # create new node
        node = Node(create=True, data_size=len(data))
        # check if it is the first node
        if self.is_empty():
            node.build(_padding_name(b''),
                       data,
                       _padding_name(b''))
            # the first element name is the same as last
            # assign name to last element
            self.pointer.buf[-14:] = _padding_name(node.name().encode())
        else:
            first_node = self.get_first_node()
            node.build(_padding_name(b''),
                       data,
                       first_node.name().encode())
            first_node.set_previous_node_name(node.name().encode())
        # assign name to first element
        self.pointer.buf[:14] = _padding_name(node.name().encode())
        self._node_cache_map[node.name()] = node
        return node

    def insert_node(self, node_name: str, data: bytes) -> Node:
        if self.is_empty():
            # insert first element
            return self.append_node(data)
        if not node_name:
            # append_left
            return self.append_left_node(data)
        # find the node by shared memory name
        current_node = self.get_node(node_name)
        if not current_node:
            raise KeyError(f"cannot find node name {node_name}")
        # get the next node
        _next_node = self.get_node(current_node.next_node_name())
        if not _next_node:
            # we cannot find next node, so
            # current node is last node and we are inserting at the end
            return self.append_node(data)
        # create new node
        node = Node(create=True, data_size=len(data))
        # swap node to next node
        _next_node.set_previous_node_name(node.name().encode())
        current_node.set_next_node_name(node.name().encode())
        node.build(current_node.name().encode(),
                   data, _next_node.name().encode())
        # remove node_name in cache
        # print(current_node, _next_node)
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
                self.pointer.buf[:14] = _padding_name(
                    _next_node.name().encode())
        if _next_node:
            _next_node.set_previous_node_name(
                _node.previous_node_name().encode())
        else:
            # this is removing the last element in linkedlist
            if not _previous_node:
                self.pointer.buf[-14:] = _padding_name(b' ')
            else:
                self.pointer.buf[-14:] = _padding_name(
                    _previous_node.name().encode())
        # free removed node name memory
        _node.pointer.unlink()
        # remove node_name in cache
        self._node_cache_map.pop(node_name)
        return _node

    def next_node(self, node: Node) -> t.Optional[Node]:
        return self.get_node(node.next_node_name())

    def get_node(self, node_name: str) -> t.Optional[Node]:
        _node_name = node_name.strip()
        if not _node_name:
            return None
        if _node_name in self._node_cache_map:
            return self._node_cache_map[_node_name]
        try:
            return Node(_node_name)
        except FileNotFoundError:
            return None

    def get_all_nodes(self):
        res = []
        first_node = self.get_first_node()
        while first_node:
            res.append(first_node)
            first_node = self.get_node(first_node.next_node_name())
            if not first_node:
                break
        return res

    def to_dict(self) -> dict:
        _d = {k: v for k, v in self.__dict__.items() if not k.startswith(' ')}
        _d['first_node'] = self.get_first_node().name()
        _d['last_node'] = self.get_last_node().name()
        return _d

    def __repr__(self):
        return f"{self.__class__.__name__}({','.join([f'{k}={v!r}' for k,v in self.to_dict().items()])})"

    def shutdown(self):
        nodes = self.get_all_nodes()
        for n in nodes:
            n.pointer.unlink()
        self.pointer.unlink()


def contruct_key_value(key_str: str, value: t.Any) -> bytes:
    return _padding_name(key_str.encode()) + pickle.dumps(value)

def destruct_key_value(data:bytes) -> (str, t.Any):
    key = data[:14].decode().strip()
    v = pickle.loads(data[14:])
    return key, v

class SharedDictObject:

    def __init__(self, name: str, create: bool = False):
        self._size = _MAX_NAME_LENGTH * 2  # 2 name of 2 list
        if create:
            self.pointer = shared_memory.SharedMemory(
                name, size=self._size, create=True)
            self.pointer.buf[:] = _padding_name(b'') * 2
            self._hash_map = SharedLinkedList(name=None, create=True)
        else:
            self.pointer = shared_memory.SharedMemory(name)
            self._hash_map = \
                SharedLinkedList(self._get_hash_map_mem_name())
        
        self._key_value_map = {}

    def name(self) -> str:
        return self.pointer.name

    def _get_hash_map_mem_name(self) -> bytes:
        return bytes(self.pointer.buf)[:14]

    def _get_key_value_list_mem_name(self) -> bytes:
        return bytes(self.pointer.buf)[-14:]

    def _create_hash_map_node_name(self, key: t.Hashable) -> str:
        hash_result = hash(key) % 10**12
        # print('create hash', f'hm{hash_result}')
        return f'hm{hash_result}'

    def _get_hash_linked_list(self,
                              name: str,
                              create: bool = False) -> SharedLinkedList:
        if name in self._key_value_map:
            return self._key_value_map[name]

        sll = SharedLinkedList(name, create=create)
        self._key_value_map[sll.name()] = sll
        return sll

    def set(self, key: t.Hashable, value):
        # check if we have the hash already
        hash_node_name = self._create_hash_map_node_name(key)
        hash_node = self._hash_map.get_node(hash_node_name)
        if hash_node:
            # append value to the hash linked list
            hash_linked_list = self._get_hash_linked_list(hash_node.value())
            
        else:
            # create hash linked list
            hash_linked_list = self._get_hash_linked_list(
                name=None, create=True)
        # hash node need to be create after that
        hash_node = self._hash_map.append_node(
            data=hash_linked_list.name().encode(),
            name=hash_node_name)
        # assign value to key_value list
        data = contruct_key_value(str(key), value)
        hash_linked_list.append_node(data)
        return hash_node

    def get(self, key: t.Hashable, default=None) -> t.Any:
        # check if we have the hash already
        hash_node_name = self._create_hash_map_node_name(key)
        hash_node = self._hash_map.get_node(hash_node_name)
        if not hash_node:
            return default
        hash_linked_list = self._get_hash_linked_list(hash_node.value())
        node = hash_linked_list.get_first_node()
        while node:
            k, v = destruct_key_value(node.data())
            if k == key:
                return v
            node = hash_linked_list.next_node(node)
            if not node:
                break
        return None

    def pop(self, key: str):
        pass

    def keys(self) -> t.List[str]:
        pass

    def values(self) -> list:
        pass

    def items(self) -> t.List[t.Tuple[str, t.Any]]:
        pass



def test_shared_dict():

    sd = SharedDictObject('test2', create=True)
    sd.set('k1', 'First Value')
    print(sd.get('k1'))


def test_linked_list():
    stack = SharedLinkedList('test', create=True)
    stack.append_node(b'e_1')
    stack.append_node(b'e_2')
    stack.append_node(b'e_3')
    stack.append_node(b'append_long_element_4')
    stack.append_node(b'e_5')
    nodes = stack.get_all_nodes()
    for n in nodes:
        if n.value() == 'e_1':
            # remove first element
            stack.remove_node(n.name())
        elif n.value() == 'e_3':
            # remove middle element
            stack.remove_node(n.name())
        elif n.value() == 'e_5':
            # remove last element
            stack.remove_node(n.name())
        elif n.value() == 'e_2':
            # insert element
            stack.insert_node(n.name(), b'e_2.5')
            # insert element at the first pos
            stack.insert_node('', b'e_1.5')
        elif n.value() == 'append_long_element_4':
            # insert element at the end
            stack.insert_node(n.name(), b'e_6')
        print(n)
    print('------ stack after remove the second element ------ ')
    nodes = stack.get_all_nodes()
    for n in nodes:
        print(n)

    stack.shutdown()


if __name__ == '__main__':
    # test_linked_list()
    test_shared_dict()

