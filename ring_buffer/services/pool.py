"""Pool contains producer and consumers, where they co-opreating
"""
from ring_buffer.interface import producer as p
from ring_buffer.services import consumer as c


class Pool:
    """simple pool
    """

    def __init__(self,
                 producer: p.ProducerInterface,
                 consumer: c.Consumer):
        """Init Pool instance

        Args:
            producer (p.ProducerInterface):
                implementation of producer interface
            consumer (c.Consumer): Consumer
        """
        self._producer = producer
        self._consumer = consumer

    def start(self):
        """Start producer and consumer
        """
        self._consumer.start()
        self._producer.start()
