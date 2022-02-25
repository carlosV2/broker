from typing import Callable

from paho.mqtt.client import Client

from broker import Queue as BaseQueue, Subscription


class Queue(BaseQueue):

    def __init__(self, client: Client, name: str):
        self._client = client
        self._name = name

    def publish(self, message: bytes) -> None:
        self._client.publish(topic=self._name, payload=message)

    def subscribe(self, callback: Callable[[str, bytes], None]) -> Subscription:
        return self._client.on_message.subscribe(queue=self._name, callback=callback)
