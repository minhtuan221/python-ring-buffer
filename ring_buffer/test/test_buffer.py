"""Module for testing ring buffer"""
from ring_buffer.services import buffer
from ring_buffer.model import event


def create_test_ring_buffer() -> buffer.RingBuffer:
    """Create and return a test ring buffer instance

    Returns:
        buffer.RingBuffer: Test Instance of RingBuffer
    """
    return buffer.RingBuffer(10)


def test_ring_is_full():
    ring = create_test_ring_buffer()
    assert ring.is_empty() is True
    for i in range(10):
        print('put event', i)
        ring.put(event.Event('test', f'message number {i}'))

    assert ring.is_full() is True

    for i in range(9):
        ring.get()

    assert ring.is_full() is False
    assert ring.is_empty() is False
    assert ring.qsize() == 1
    assert ring.count_none_pointer() == 9
    ring.get()
    assert ring.qsize() == 0
    assert ring.is_empty() is True
