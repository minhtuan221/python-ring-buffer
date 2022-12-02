import pickle
import multiprocessing as mp
from multiprocessing import shared_memory


_MAX_NAME_LENGTH = 32


class SimpleSharedDict:
    """Shared Simple Dict contain a list of shared memory:
     - each element in list is a name of a shared memory
     - read from shared memory
     - write/change use multiprocess simple queue
    """

    def __init__(
            self,
            name: str,
            sync_queue: mp.SimpleQueue,
            size: int,
            create: bool = False,
            prefix: str = 'SD'
    ):
        self._memory_name_prefix = prefix
        self._size = size
        self._queue = sync_queue
        self.is_manager: bool = create
        if create:
            # this is the main manage object
            self.pointer = shared_memory.SharedMemory(
                name,
                size=self._size * _MAX_NAME_LENGTH,
                create=True
            )
        else:
            self.pointer = shared_memory.SharedMemory(name)

    def name(self) -> str:
        return self.pointer.name

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
