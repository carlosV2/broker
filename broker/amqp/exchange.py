from pika.adapters.blocking_connection import BlockingChannel

from broker.amqp import Route


class Exchange(object):

    def __init__(self, channel: BlockingChannel, name: str, **kwargs):
        self._channel = channel
        self._name = name

        self._init(**kwargs)

    def _init(self, **kwargs) -> None:
        self._channel.exchange_declare(exchange=self._name, exchange_type='topic', **kwargs)

    def get_name(self) -> str:
        return self._name

    def publish(self, topic: str, message: bytes) -> None:
        self._channel.basic_publish(exchange=self._name, routing_key=topic, body=message)

    def route(self, topic: str) -> Route:
        return Route(channel=self._channel, exchange=self._name, topic=topic)
