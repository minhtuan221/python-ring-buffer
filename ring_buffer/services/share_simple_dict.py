import pickle
from multiprocessing import shared_memory

from ring_buffer.services.shared_dict_obj import SharedLinkedList
from ring_buffer.services.shared_obj import _padding_name, construct_key_value, destruct_key_value, _MAX_NAME_LENGTH


class SimpleSharedDict:
    def __init__(self, name: str, create: bool = False, prefix: str = 'SD'):
        self._memory_name_prefix = prefix
        self._size = _MAX_NAME_LENGTH
        if create:
            self.pointer = shared_memory.SharedMemory(
                name, size=self._size, create=True)
            self.pointer.buf[:] = _padding_name(b'')
            self._hash_map = SharedLinkedList(name=None, create=True)
        else:
            self.pointer = shared_memory.SharedMemory(name)
            self._hash_map = \
                SharedLinkedList(self._get_hash_map_mem_name().decode())

    def name(self) -> str:
        return self.pointer.name

    def _get_hash_map_mem_name(self) -> bytes:
        return bytes(self.pointer.buf)

    def _gen_name(self, s: str) -> str:
        return f"{self._memory_name_prefix}{s}"

    def write(self, key: str, value):
        data = pickle.dumps(value)
        node_name = self._gen_name(key)
        node = self._hash_map.append_node(data, name=node_name)
        node.close()

    def read(self, key: str):
        node_name = self._gen_name(key)
        node = self._hash_map.get_node(node_name)
        if not node:
            return None
        value = pickle.loads(node.data())
        node.close()
        return value

    def remove(self, key: str):
        node_name = self._gen_name(key)
        self._hash_map.remove_node(node_name)

    def get(self, key: str):
        return self.read(key)

    def set(self, key: str, value):
        v = self.read(key)
        if v:
            self.remove(key)
        self.write(key, value)

    def keys(self):
        pass


def test_simple_share_dict():
    smd = SimpleSharedDict('first_share_dict', create=True)
    smd.set("1", 'this is number one')
    smd.set("2", 'this is number two')
    smd.set("3", 'this is number five')
    print(smd.get('1'), smd.get('2'), smd.get('3'))

if __name__ == '__main__':
    test_simple_share_dict()

