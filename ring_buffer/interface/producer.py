"""This module is about interface of producer"""
import abc

from ring_buffer.model import event


class ProducerInterface(abc.ABC):
    """Interface for all producer"""

    @abc.abstractmethod
    def produce(self, e: event.Event) -> bool:
        """produce/put message in to queue or consumer

        Args:
            e (event.Event): Event model

        Returns:
            bool: False if cannot put message

        Raises:
            NotImplementedError: raise if not implement
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def start(self):
        """Start producer thread or process

        Raises:
            NotImplementedError: raise if not implement
        """
        raise NotImplementedError()
