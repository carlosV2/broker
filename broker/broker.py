from abc import ABCMeta, abstractmethod
from re import fullmatch
from typing import Callable
from urllib.parse import unquote


class Message(metaclass=ABCMeta):

    def __init__(self, topic: str, payload: bytes):
        self._topic = topic
        self._payload = payload

    def get_topic(self) -> str:
        return self._topic

    def get_payload_as_bytes(self) -> bytes:
        return self._payload


class Broker(metaclass=ABCMeta):

    @abstractmethod
    def publish(self, message: Message) -> None:
        pass

    @abstractmethod
    def subscribe(self, queue: str, callback: Callable[[Message], None]) -> None:
        pass

    @abstractmethod
    def unsubscribe(self, queue: str) -> None:
        pass

    @abstractmethod
    def consume_forever(self) -> None:
        pass

    @staticmethod
    def from_dsn(dsn) -> 'Broker':
        from broker import Amqp
        from broker import Mqtt

        protocols = {'mqtt': Mqtt, 'amqp': Amqp}

        result = fullmatch('^([^:]+)://(?:([^:]+):([^@]+)@)?([^:@/]+)(?::(\d+))?(?:/([^/]+))?$', dsn)
        if result is None:
            raise Exception('Dsn `{dsn}` is invalid'.format(dsn=dsn))

        protocol = result.group(1)
        if protocol not in protocols:
            raise Exception('Protocol `{protocol}` is unsupported'.format(protocol=protocol))

        credentials = None
        if result.group(2) is not None and result.group(3) is not None:
            credentials = (unquote(result.group(2)), unquote(result.group(3)))

        host = result.group(4)
        port = None if result.group(5) is None else int(result.group(5))
        division = result.group(6)

        return protocols[protocol](host, port, credentials, division)
