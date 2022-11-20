import typing as t
from multiprocessing import shared_memory

from ring_buffer.services.shared_obj import _padding_name, construct_key_value, destruct_key_value, Node, \
    _MAX_NAME_LENGTH


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
        n = bytes(self.pointer.buf[-32:]).decode()
        return self.get_node(n.strip())

    def get_first_node(self) -> t.Optional[Node]:
        n = bytes(self.pointer.buf[:32]).decode()
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
            self.pointer.buf[:32] = _padding_name(node.name().encode())
        else:
            last_node = self.get_last_node()
            node.build(last_node.name().encode(),
                       data,
                       _padding_name(b''))
            last_node.set_next_node_name(node.name().encode())
        # assign name to last element
        self.pointer.buf[-32:] = _padding_name(node.name().encode())
        # self._node_cache_map[node.name()] = node
        return node

    def append_left_node(self,
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
            # assign name to last element
            self.pointer.buf[-32:] = _padding_name(node.name().encode())
        else:
            first_node = self.get_first_node()
            node.build(_padding_name(b''),
                       data,
                       first_node.name().encode())
            first_node.set_previous_node_name(node.name().encode())
        # assign name to first element
        self.pointer.buf[:32] = _padding_name(node.name().encode())
        # self._node_cache_map[node.name()] = node
        return node

    def insert_node(self,
                    node_name: str,
                    data: bytes,
                    name: t.Optional[str] = None) -> Node:
        if self.is_empty():
            # insert first element
            return self.append_node(data, name=name)
        if not node_name:
            # append_left
            return self.append_left_node(data, name=name)
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
        # self._node_cache_map[node.name()] = node
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
                self.pointer.buf[:32] = _padding_name(b' ')
            else:
                self.pointer.buf[:32] = _padding_name(
                    _next_node.name().encode())
        if _next_node:
            _next_node.set_previous_node_name(
                _node.previous_node_name().encode())
        else:
            # this is removing the last element in linkedlist
            if not _previous_node:
                self.pointer.buf[-32:] = _padding_name(b' ')
            else:
                self.pointer.buf[-32:] = _padding_name(
                    _previous_node.name().encode())
        # free removed node name memory
        _node.pointer.unlink()
        # remove node_name in cache
        # self._node_cache_map.pop(node_name)
        return _node

    def next_node(self, node: Node) -> t.Optional[Node]:
        return self.get_node(node.next_node_name())

    def get_node(self, node_name: str) -> t.Optional[Node]:
        _node_name = node_name.strip()
        if not _node_name:
            return None
        # if _node_name in self._node_cache_map:
        #     return self._node_cache_map[_node_name]
        try:
            return Node(_node_name)
        except FileNotFoundError:
            print('file not found error',node_name)
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
        return f"{self.__class__.__name__}({','.join([f'{k}={v!r}' for k, v in self.to_dict().items()])})"

    def shutdown(self):
        nodes = self.get_all_nodes()
        for n in nodes:
            n.pointer.unlink()
        self.pointer.unlink()


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
                SharedLinkedList(self._get_hash_map_mem_name().decode())

        self._key_value_map = {}

    def name(self) -> str:
        return self.pointer.name

    def _get_hash_map_mem_name(self) -> bytes:
        return bytes(self.pointer.buf)[:32]

    def _get_key_value_list_mem_name(self) -> bytes:
        return bytes(self.pointer.buf)[-32:]

    def _create_hash_map_node_name(self, key: t.Hashable) -> str:
        hash_result = hash(key) % 10 ** 12
        # print('create hash', f'hm{hash_result}')
        return f'hm{hash_result}'

    def _get_or_create_hash_pointer(self,
                                    name: t.Optional[str],
                                    create: bool = False) -> SharedLinkedList:
        if name in self._key_value_map:
            return self._key_value_map[name]

        sll = SharedLinkedList(name, create=create)
        self._key_value_map[sll.name()] = sll
        return sll

    def _get_hash_linked_list(
            self,
            key: t.Hashable,
            get_only: bool = False
    ) -> t.Optional[SharedLinkedList]:
        # check if we have the hash already
        hash_node_name = self._create_hash_map_node_name(key)
        hash_node = self._hash_map.get_node(hash_node_name)
        if hash_node:
            # append value to the hash linked list
            hash_linked_list = self._get_or_create_hash_pointer(hash_node.value())
        else:
            # hash linked list is not created
            if get_only:
                # it get only, we should return None
                return None
            # create hash linked list
            hash_linked_list = self._get_or_create_hash_pointer(
                name=None, create=True)
            # hash node need to be create after that
            # and append to main hash map
            self._hash_map.append_node(
                data=hash_linked_list.name().encode(),
                name=hash_node_name)
        return hash_linked_list

    def set(self, key: t.Hashable, value):
        hash_linked_list = self._get_hash_linked_list(key)
        # set key, value to hash linked list
        data = construct_key_value(str(key), value)
        hash_linked_list.append_node(data)

    def get(self, key: t.Hashable) -> t.Union[t.Any, None]:
        hash_linked_list = self._get_hash_linked_list(key, get_only=True)
        if not hash_linked_list:
            return None
        node = hash_linked_list.get_first_node()
        while node:
            k, v = destruct_key_value(node.data())
            if k == key:
                return v
            node = hash_linked_list.next_node(node)
            if not node:
                break
        return None

    def remove(self, key: t.Hashable):
        hash_linked_list = self._get_hash_linked_list(key, get_only=True)
        if not hash_linked_list:
            return None
        node = hash_linked_list.get_first_node()
        res = None
        while node:
            k, v = destruct_key_value(node.data())
            if k == key:
                hash_linked_list.remove_node(node.name())
                res = node
                break
            node = hash_linked_list.next_node(node)
            if not node:
                break
        if hash_linked_list.is_empty():
            hash_linked_list.shutdown()
            self._hash_map.remove_node(hash_linked_list.name())
        return res

    def pop(self, key: str):
        pass

    def keys(self) -> t.List[str]:
        pass

    def values(self) -> list:
        pass

    def items(self) -> t.List[t.Tuple[str, t.Any]]:
        pass


def test_shared_dict():
    sd = SharedDictObject('test2'+"-"*29, create=True)
    sd.set('k1', 'First Value')
    assert sd.get('k1') == 'First Value'
    assert sd.get('not_in_dict') is None


def test_linked_list():
    stack = SharedLinkedList('test' + '-'*29, create=True)
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
    test_linked_list()
    # test_shared_dict()
