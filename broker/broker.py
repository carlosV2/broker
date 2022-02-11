from abc import ABCMeta, abstractmethod
from json import loads, dumps
from re import fullmatch
from typing import Callable, Any
from urllib.parse import unquote


class Message(metaclass=ABCMeta):

    def __init__(self, topic: str, payload: bytes):
        self._topic = topic
        self._payload = payload

    def get_topic(self) -> str:
        return self._topic

    def get_raw_payload(self) -> bytes:
        return self._payload

    def get_encoded_payload(self) -> str:
        return self.get_raw_payload().decode()

    def get_hydrated_json_payload(self) -> Any:
        return loads(self.get_encoded_payload())

    @classmethod
    def from_raw_payload(cls, topic: str, payload: bytes) -> 'Message':
        return Message(topic=topic, payload=payload)

    @classmethod
    def from_decoded_payload(cls, topic: str, payload: str) -> 'Message':
        return Message.from_raw_payload(topic=topic, payload=payload.encode())

    @classmethod
    def from_dehydrated_json_payload(cls, topic: str, payload: Any) -> 'Message':
        return Message.from_decoded_payload(topic=topic, payload=dumps(payload))


class Broker(metaclass=ABCMeta):

    @abstractmethod
    def publish(self, message: Message) -> None:
        pass

    @abstractmethod
    def subscribe(self, topic: str, callback: Callable[[Message], None]) -> None:
        pass

    @abstractmethod
    def unsubscribe(self, topic: str) -> None:
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
