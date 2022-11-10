import typing as t
import json

from ring_buffer.services import buffer
from ring_buffer.services import consumer
from ring_buffer.model import event


def _get_total_quantity(total_quantity, transaction: dict):
    total_quantity += transaction.get('qty', 0)
    return total_quantity


def _create_test_tx_message(_id: int,
                            symbol: str,
                            quantity: int,
                            price: float) -> str:
    return json.dumps({'id': _id,
                       'symbol': symbol,
                       'qty': quantity,
                       'price': price
                       })


def process_test_message(e: event.Event):

    return


def create_test_messages(n: int = 1000) -> t.List[str]:
    message_array = []
    for i in range(1000):
        message_array.append(_create_test_tx_message(i, 'stock', i*10, 2.0))
    return message_array


def test_consumer():
    _ring = buffer.RingBuffer(100)
    _consumer = consumer.Consumer('test', _ring)
    total_quantity = 0
