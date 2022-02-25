from typing import Callable

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from broker import Queue as BaseQueue
from broker.amqp import Subscription


class Queue(BaseQueue):

    def __init__(self, channel: BlockingChannel, name: str, **kwargs):
        self._channel = channel
        self._name = name

        self._init(**kwargs)

    def _init(self, **kwargs) -> None:
        self._channel.queue_declare(queue=self._name, **kwargs)

    def get_name(self) -> str:
        return self._name

    def publish(self, message: bytes) -> None:
        self._channel.basic_publish(exchange='', routing_key=self._name, body=message)

    def subscribe(self, callback: Callable[[str, bytes], None]) -> Subscription:
        def _on_message(channel: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
            try:
                callback(method.routing_key, body)
            finally:
                self._channel.basic_ack(delivery_tag=method.delivery_tag)

        tag = self._channel.basic_consume(queue=self._name, on_message_callback=_on_message)
        return Subscription(channel=self._channel, tag=tag)
