from typing import Tuple, Callable
from urllib.parse import unquote

from dsnparse import parse
from pika import ConnectionParameters, PlainCredentials
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel
from pika.spec import Basic, BasicProperties

from broker import Broker as BaseBroker, Message, Sink
from broker.amqp import Subscription


class Broker(BaseBroker):

    def __init__(self, host: str, port: int = None, credentials: Tuple[str, str] = None, exchange: str = None):
        parameters = {'host': host}

        if port is not None:
            parameters['port'] = port
        if credentials is not None:
            parameters['credentials'] = PlainCredentials(username=credentials[0], password=credentials[1])

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

    def subscribe(self, sink: Sink, callback: Callable[[Message], None]) -> Subscription:
        def _on_message(channel: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body: bytes):
            callback(Message(topic=method.routing_key, payload=body))

        binding = sink.get_binding(self._channel, self._exchange)
        tag = self._channel.basic_consume(queue=binding, on_message_callback=_on_message, auto_ack=True)

        return Subscription(channel=self._channel, tag=tag)

    def consume_forever(self) -> None:
        self._channel.start_consuming()

    @classmethod
    def from_dsn(cls, dsn) -> 'Amqp':
        result = parse(dsn)

        credentials = None
        if result.user is not None and result.password is not None:
            credentials = (unquote(result.user), unquote(result.password))

        exchange = result.path.lstrip('/')
        if len(exchange) == 0:
            exchange = None

        return cls(host=result.host, port=result.port, credentials=credentials, exchange=exchange)
