"""This Producer is use for testing"""
import typing as t
import threading
import time

from ring_buffer.interface.producer import ProducerInterface
from ring_buffer.services import buffer
from ring_buffer.model import event


class TestProducer(ProducerInterface):
    """This producer is used only for testing

    Args:
        ProducerInterface: Producer interface
    """

    def __init__(self,
                 name: str,
                 message_array: t.List[str],
                 ring_buffer: buffer.RingBuffer):
        """Init tesing producer

        Args:
            name (str): name of the producer thread
            message_array (t.List[str]): _description_
            ring_buffer (buffer.RingBuffer): _description_
        """
        self.name = name
        self._message_array = message_array
        self._ring_buffer = ring_buffer
        self._thread = None

    def produce(self, e: event.Event) -> bool:
        self._ring_buffer.put(e)

    def _produce_loop(self):
        """run loop which producer message into ring buffer
        """
        while True:
            message = self._message_array.pop(0)
            self.produce(event.Event('test', message))
            time.sleep(0.00001)

    def start(self):
        self._thread = threading.Thread(name=self.name,
                                        target=self._produce_loop,
                                        daemon=True)
        self._thread.start()
