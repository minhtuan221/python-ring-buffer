import hashlib
import string
import timeit
import typing as t
import random

b64 = hashlib.blake2b(key=b'k', digest_size=8)
b32 = hashlib.blake2b(key=b'k', digest_size=4)

MAX_HASH_B32 = 2 ** 32 - 1  # uint 32 - 4 bytes
MAX_HASH_B64 = 2 ** 64 - 1  # uint 64 - 8 bytes


def hash_md5(key: str):
    return int(hashlib.md5(key.encode()).hexdigest(), 16)


def hash_64_bit(key: str):
    b64.update(key.encode())
    return int(b64.hexdigest(), 16)


def hash_32_bit(key: str):
    b32.update(key.encode())
    return int(b32.hexdigest(), 16)


class RingNode:
    __slots__ = ('key', 'value', 'next')

    def __init__(self, key: int, value, _next=None):
        self.key = key
        self.value = value
        self.next = _next

    def __repr__(self):
        return f"RingNode({self.key}, {self.value}, {self.next.key})"


# Iterative Binary Search Function
# It returns index of x in given array arr if present,
# else returns -1
def binary_search_node(
        arr: t.List[RingNode],
        x: int,
) -> RingNode:
    low = 0
    high = len(arr)
    max_high = high - 1
    while low <= high:

        mid = (high + low) // 2
        if x == arr[mid].key:
            return arr[mid]
        elif x < arr[mid].key:
            if mid == 0:
                return arr[mid]
            elif arr[mid - 1].key < x:
                return arr[mid]
            else:
                # If x is smaller, ignore right half
                high = mid - 1
        else:  # arr[mid].key < x:
            if mid == max_high:
                return arr[0]
            elif x <= arr[mid + 1].key:
                return arr[mid + 1]
            else:
                # If x is greater, ignore left half
                low = mid + 1

    # If we reach here, then the element was not present
    raise ValueError('Cannot find value of x in array')


def test_search_ring_node():
    number_of_node = 20
    arr_node = [RingNode(100 * i, str(i)) for i in range(1, number_of_node + 1)]
    search_item = [random.randrange(0, number_of_node * 200) for i in range(10)]
    for x in search_item:
        res = binary_search_node(arr_node, x)
        print(f"{x} =>", res)


def test_search_ring_performance(n=1000):
    setup_ring_test_code = """
import typing as t
import random

class RingNode:
    __slots__ = ('key', 'value')

    def __init__(self, key: int, value):
        self.key = key
        self.value = value

    def __repr__(self):
        return f"RingNode({self.key}, {self.value})"


# Iterative Binary Search Function
# It returns index of x in given array arr if present,
# else returns -1
def binary_search_node(
        arr: t.List[RingNode],
        x: int,
) -> RingNode:
    low = 0
    high = len(arr)
    max_high = high - 1
    while low <= high:

        mid = (high + low) // 2
        if x <= arr[mid].key:
            if mid == 0:
                return arr[mid]
            elif arr[mid - 1].key < x:
                return arr[mid]
            else:
                # If x is smaller, ignore right half
                high = mid - 1
        else:  # arr[mid].key < x:
            if mid == max_high:
                return arr[0]
            elif x <= arr[mid + 1].key:
                return arr[mid + 1]
            else:
                # If x is greater, ignore left half
                low = mid + 1

    # If we reach here, then the element was not present
    raise ValueError('Cannot find value of x in array')

number_of_node = 64
max_int = (2**64-1)
one_distance = max_int // 64
arr_node = [RingNode(one_distance * i, str(i)) for i in range(1, number_of_node+1)]
"""
    test_ring_code = """
binary_search_node(arr_node, random.randrange(0, max_int))
"""
    print(f'run {n} binary search  Node in:', timeit.timeit(
        test_ring_code, setup=setup_ring_test_code, number=n))


class RingHash:

    def __init__(
            self,
            node_values: t.List[t.Any],
            n_node: int = 64,
            max_hash: int = 0,
            hash_fn=None,
    ):
        self._n_node = n_node
        if hash_fn:
            self._hash_fn = hash_fn
        else:
            self._hash_fn = hash_32_bit
        if max_hash:
            self._max_hash = max_hash
        else:
            self._max_hash = MAX_HASH_B32
        self._distance = self._max_hash // self._n_node
        self._ring_nodes = [RingNode(self._distance * i, None)
                            for i in range(1, self._n_node + 1)]
        # the same code as update code
        self._nodes = node_values
        self._distribute_nodes(self._nodes)

    def _distribute_nodes(self, nodes: list):
        len_nodes = len(nodes)
        last_node = self._ring_nodes[0]
        for i in range(len(self._ring_nodes) - 1, -1, -1):
            self._ring_nodes[i].value = nodes[i % len_nodes]
            self._ring_nodes[i].next = last_node
            last_node = self._ring_nodes[i]
            print(last_node)

    def add_node(self, node) -> bool:
        if node in self._nodes:
            return False
        self._nodes.append(node)
        self._distribute_nodes(self._nodes)
        return True

    def remove_node(self, node):
        if node not in self._nodes:
            return False
        self._nodes.remove(node)
        self._distribute_nodes(self._nodes)
        return True

    def update_nodes(self, nodes: list):
        self._nodes = nodes
        self._distribute_nodes(self._nodes)

    def get_all_node(self) -> list:
        return self._nodes

    def get_ring(self):
        return self._ring_nodes

    def get_node(self, key: t.Union[int, str]) -> RingNode:
        if isinstance(key, int):
            h = key
        else:
            h = self._hash_fn(key)
        return binary_search_node(self._ring_nodes, h)

    def get_value(self, key: t.Union[int, str]):
        return self.get_node(key).value

    def next_node(self, node: RingNode) -> RingNode:
        return node.next


def test_ring_hash(n=10):
    ring = RingHash([f"0.0.0.0:{i}" for i in (5001, 5002, 5003, 5004, 5005)], n_node=8)
    print(ring.get_all_node())
    for r in ring.get_ring():
        print(r)
    for i in range(n):
        random_key = ''.join(random.sample(string.ascii_letters, 8))
        v = ring.get_value(random_key)
        print(i, random_key, '=>', v)


def test_hash_fn_performance(n=1000000):
    import_code = """
import hashlib

b64 = hashlib.blake2s(digest_size=8)
b32 = hashlib.blake2s(digest_size=4)


def hash_md5(key: str):
    return int(hashlib.md5(key.encode()).hexdigest(), 16)


def hash_64_bit(key: str):
    b64.update(key.encode())
    return int(b64.hexdigest(), 16)


def hash_32_bit(key: str):
    b32.update(key.encode())
    return int(b32.hexdigest(), 16)
"""
    test_word = 'ablaaldfa adfasfasdfrwqeqfas123179173249857324'

    print(int.from_bytes(b'\xff\xff\xff\xff', 'big', signed=False))
    print(int.to_bytes(2 ** 32 - 1, 4, 'big', signed=False))

    test_md5_code = f"""
    hash_md5('{test_word}')
    """
    print(timeit.timeit(test_md5_code, setup=import_code, number=n))
    test_blake_64_code = f"""
    hash_64_bit('{test_word}')
    """
    print(timeit.timeit(test_blake_64_code, setup=import_code, number=n))
    test_blake_32_code = f"""
    hash_32_bit('{test_word}')
    """
    print(timeit.timeit(test_blake_32_code, setup=import_code, number=n))
    test_hash_code = f"""
    hash('{test_word}')
    """
    print(timeit.timeit(test_hash_code, setup=import_code, number=n))
    print(hash_64_bit(test_word))
    print(hash_32_bit(test_word))
    print(hash(test_word))


if __name__ == '__main__':
    # test_hash_fn_performance()
    # test_search_ring_node()
    # test_search_ring_performance(10**6)
    test_ring_hash()
