from abc import ABCMeta, abstractmethod

from pika.adapters.blocking_connection import BlockingChannel

from broker import Sink


class AmqpSink(Sink, metaclass=ABCMeta):

    @abstractmethod
    def get_binding(self, channel: BlockingChannel, exchange: str) -> str:
        pass


class Queue(AmqpSink):

    def __init__(self,
                 name: str,
                 passive: bool = False,
                 durable: bool = False,
                 exclusive: bool = False,
                 auto_delete: bool = False):
        self._name = name
        self._passive = passive
        self._durable = durable
        self._exclusive = exclusive
        self._auto_delete = auto_delete

    def get_binding(self, channel: BlockingChannel, exchange: str) -> str:
        return channel.queue_declare(
            self._name,
            passive=self._passive,
            durable=self._durable,
            exclusive=self._exclusive,
            auto_delete=self._auto_delete
        ).method.queue


class UnamedQueue(Queue):

    def __init__(self, passive: bool = False, durable: bool = False, auto_delete: bool = False):
        super(UnamedQueue, self).__init__(
            name='',
            exclusive=True,
            passive=passive,
            durable=durable,
            auto_delete=auto_delete
        )


class Routing(AmqpSink):

    def __init__(self, topic: str, queue: Queue):
        self._topic = topic
        self._queue = queue

    def get_binding(self, channel: BlockingChannel, exchange: str) -> str:
        queue = self._queue.get_binding(channel, exchange)
        channel.queue_bind(exchange=exchange, queue=queue, routing_key=self._topic)

        return queue
