from ring_buffer.services import shared_list_object as sl
import time


def test_get_set():
    smm = sl.SharedListObject('test1')
    print(smm.get(0))
    print(time.time(), smm.set(0, '200'))

