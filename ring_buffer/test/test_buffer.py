"""Module for testing ring buffer"""
from ring_buffer.services import buffer
from ring_buffer.model import event


def create_test_ring_buffer() -> buffer.RingBuffer:    
    return buffer.RingBuffer(10)


def test_ring_is_full():
    ring = create_test_ring_buffer()
    assert ring.is_empty() == True
    for i in range(10):
        print('put event', i)
        ring.put(event.Event('test', f'message number {i}'))
    
    assert ring.is_full() == True