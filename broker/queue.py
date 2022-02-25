from abc import ABCMeta, abstractmethod
from typing import Callable

from broker import Subscription


class Queue(metaclass=ABCMeta):

    @abstractmethod
    def publish(self, message: bytes) -> None:
        pass

    @abstractmethod
    def subscribe(self, callback: Callable[[str, bytes], None]) -> Subscription:
        pass
