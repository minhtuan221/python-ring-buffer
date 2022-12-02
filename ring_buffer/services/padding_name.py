from multiprocessing import shared_memory


def unpad_name(n: bytes):
    return n.strip(b'\x00')


def set_name(buf, n: bytes, start: int = 0):
    buf[start:start + len(n)] = n


def get_name(buf, start: int, length: int):
    return bytes(buf[start:start + length])


def test_unpad_name():
    assert unpad_name(b'name\x00\x00\x00\x00') == b'name'
    assert unpad_name(b'\x00\x00\x00\x00 name') == b' name'
    assert unpad_name(b'\x00\x00name \x00\x00\x00') == b'name '


def test_set_name():
    smm = shared_memory.SharedMemory('test_set_name', create=True, size=10)
    print('start test')

    t1 = b'name'
    set_name(smm.buf, t1)
    assert unpad_name(bytes(smm.buf)) == t1

    t2 = b'1234'
    set_name(smm.buf, t2)
    print(unpad_name(bytes(smm.buf)))
    assert unpad_name(bytes(smm.buf)) == t2

    t3 = b' 5678 '
    set_name(smm.buf, t3, start=len(t2))
    print(unpad_name(bytes(smm.buf)))
    assert unpad_name(bytes(smm.buf)) == t2 + t3

    print(get_name(smm.buf, 5, 3))
    print(get_name(smm.buf, 0, 3))
    print(get_name(smm.buf, 1, 3))
    print(get_name(smm.buf, 2, 3))

    smm.unlink()


if __name__ == '__main__':
    test_unpad_name()
    test_set_name()
