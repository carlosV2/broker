from abc import ABCMeta, abstractmethod
from importlib import import_module
from typing import Callable

from broker import Message, Sink, Subscription


class Broker(metaclass=ABCMeta):

    @abstractmethod
    def publish(self, message: Message) -> None:
        pass

    @abstractmethod
    def subscribe(self, sink: Sink, callback: Callable[[Message], None]) -> Subscription:
        pass

    @abstractmethod
    def consume_forever(self) -> None:
        pass

    @classmethod
    def from_dsn(cls, dsn) -> 'Broker':
        scheme, *_ = dsn.split(':', 1)
        return getattr(import_module('broker.{scheme}.broker'.format(scheme=scheme)), 'Broker').from_dsn(dsn)
