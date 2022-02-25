from re import compile
from typing import Callable

from paho.mqtt.client import Client

from broker import Subscription as BaseSubscription


class Subscription(BaseSubscription):

    def __init__(self, client: Client, queue: str, callback: Callable[[str, bytes], None]):
        self._client = client
        self._queue = queue
        self._callback = callback
        self._regex = compile('^{topic}$'.format(topic=queue.replace('+', '[^/]+').replace('#', '.+')))

    def get_queue(self) -> str:
        return self._queue

    def process(self, topic: str, message: bytes):
        if self._regex.match(topic):
            self._callback(topic, message)

    def unsubscribe(self) -> None:
        self._client.on_message.unsubscribe(subscription=self)
