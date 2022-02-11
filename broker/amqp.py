from typing import Tuple, Callable

from pika import ConnectionParameters, PlainCredentials
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import Basic, BasicProperties

from broker import Broker, Message


class Amqp(Broker):

    def __init__(self, host: str, port: int = None, credentials: Tuple[str, str] = None, exchange: str = None):
        parameters = {'host': host}

        if port is not None:
            parameters['port'] = port
        if credentials is not None:
            parameters['credentials'] = PlainCredentials(username=credentials[0], password=credentials[1])

        self._consumers = {}
        self._exchange = exchange or ''
        self._connection = BlockingConnection(ConnectionParameters(**parameters))
        self._channel = self._connection.channel()

    def __del__(self):
        if hasattr(self, '_connection'):
            if self._connection:
                self._connection.close()

    def publish(self, message: Message) -> None:
        self._channel.basic_publish(
            exchange=self._exchange,
            routing_key=message.get_topic(),
            body=message.get_raw_payload()
        )

    def subscribe(self, topic: str, callback: Callable[[Message], None]) -> None:
        def _on_message(channel: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
            callback(Message(topic=method.routing_key, payload=body))

        tag = self._channel.basic_consume(queue=topic, on_message_callback=_on_message, auto_ack=True)
        self._consumers[topic] = tag

    def unsubscribe(self, topic: str) -> None:
        if topic in self._consumers:
            self._channel.basic_cancel(self._consumers.pop(topic))

    def consume_forever(self) -> None:
        self._channel.start_consuming()
