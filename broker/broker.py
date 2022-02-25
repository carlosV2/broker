from abc import ABCMeta, abstractmethod
from importlib import import_module
from typing import Callable

from broker import Subscription, Queue


class Broker(metaclass=ABCMeta):

    @abstractmethod
    def queue(self, name: str, **kwargs) -> Queue:
        pass

    def publish(self, queue: str, message: bytes):
        self.queue(name=queue).publish(message=message)

    def subscribe(self, queue: str, callback: Callable[[str, bytes], None]) -> Subscription:
        return self.queue(name=queue).subscribe(callback=callback)

    @abstractmethod
    def loop(self) -> None:
        pass

    @classmethod
    def from_dsn(cls, dsn) -> 'Broker':
        scheme, *_ = dsn.split(':', 1)
        return getattr(import_module('broker.{scheme}.broker'.format(scheme=scheme)), 'Broker').from_dsn(dsn)
