import typing as t
from multiprocessing import shared_memory


def _padding_name(n: bytes):
    cleaned_n = n.strip()
    return b' ' * max(14 - len(cleaned_n), 0) + cleaned_n


class Node:
    """Double Linked Node"""

    def __init__(self, name: t.Optional[str] = None, create: bool = False):
        self.size = 14 * 3 + 2
        self.create = create
        if create:
            self.pointer = shared_memory.SharedMemory(name, size=self.size, create=True)
        else:
            self.pointer = shared_memory.SharedMemory(name)

    def build(self, previous_node_name: bytes, key: bytes, next_node_name: bytes):
        if self.create:
            # build the buffer
            b = _padding_name(previous_node_name) + b'|' + _padding_name(key) + b'|' + _padding_name(
                next_node_name)
            self.pointer.buf[:] = b

    def _unpack(self):
        n = bytes(self.pointer.buf).split(b'|')
        return n[0], n[1], n[2]

    def name(self) -> str:
        return self.pointer.name

    def set_previous_node_name(self, name: bytes):
        self.pointer.buf[:14] = name

    def previous_node_name(self) -> str:
        p, _, _ = self._unpack()
        return p.decode()

    def key(self) -> str:
        _, p, _ = self._unpack()
        return p.decode()

    def next_node_name(self) -> str:
        _, _, p = self._unpack()
        return p.decode()


class SharedStackObject:

    def __init__(self, name: str, create: bool = False):
        self.name = name
        self.create = create
        self.size = 14  # name of the first node in stack
        if create:
            self.pointer = shared_memory.SharedMemory(name, size=self.size, create=True)
        else:
            self.pointer = shared_memory.SharedMemory(name)
        self.pointer.buf[:] = _padding_name(b'')
        self._node_cache_map: t.Dict[str, Node] = {}

    def _get_first_node(self):
        n = bytes(self.pointer.buf).decode()
        return self.get_node(n)

    def _is_first_node(self) -> bool:
        return len(bytes(self.pointer.buf).strip()) == 0

    def append_node(self, key: str) -> Node:
        # create new node
        node = Node(create=True)
        # check if it is the first node
        if not self._is_first_node():
            first_node = self._get_first_node()
            node.build(_padding_name(b''), key.encode(), first_node.name().encode())
            first_node.set_previous_node_name(node.name().encode())
        else:
            node.build(_padding_name(b''), key.encode(), _padding_name(b''))
        print(node.name().encode(), len(node.name().encode()))
        self.pointer.buf[:] = node.name().encode()
        self._node_cache_map[node.name()] = node
        return node

    def remove_node(self, node_name: str):
        pass

    def get_node(self, node_name: str) -> t.Optional[Node]:
        if not node_name:
            return None
        if node_name in self._node_cache_map:
            return self._node_cache_map[node_name]
        return Node(node_name)

    def get_all_nodes(self):
        res = []
        first_node = self._get_first_node()
        while first_node:
            res.append(first_node.key())
            first_node = self.get_node(first_node.next_node_name())
            if not first_node:
                break
        return res

    def shutdown(self):
        self.pointer.unlink()

if __name__ == '__main__':
    stack = SharedStackObject('test', create=True)
    stack.append_node('element1')
    stack.append_node('e_2')
    print(stack.get_all_nodes())
