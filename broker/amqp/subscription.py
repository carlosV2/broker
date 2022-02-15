from pika.adapters.blocking_connection import BlockingChannel

from broker import Subscription as BaseSubscription


class Subscription(BaseSubscription):

    def __init__(self, channel: BlockingChannel, tag: str):
        self._channel = channel
        self._tag = tag

    def unsubscribe(self) -> None:
        self._channel.basic_cancel(self._tag)
