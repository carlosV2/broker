from pika.adapters.blocking_connection import BlockingChannel


class Route(object):

    def __init__(self, channel: BlockingChannel, exchange: str, topic: str):
        self._channel = channel
        self._exchange = exchange
        self._topic = topic

    def to_queue(self, name: str, **kwargs) -> 'Queue':
        from broker.amqp import Queue

        queue = Queue(channel=self._channel, name=name, **kwargs)
        self._channel.queue_bind(queue=name, exchange=self._exchange, routing_key=self._topic)

        return queue

    def to_exchange(self, name: str, **kwargs) -> 'Exchange':
        from broker.amqp import Exchange

        exchange = Exchange(channel=self._channel, name=name, **kwargs)
        self._channel.exchange_bind(destination=name, source=self._exchange, routing_key=self._topic)

        return exchange
