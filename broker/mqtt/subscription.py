from paho.mqtt.client import Client

from broker import Subscription as BaseSubscription


class Subscription(BaseSubscription):

    def __init__(self, client: Client, topic: str):
        self._client = client
        self._topic = topic

    def unsubscribe(self) -> None:
        self._client.unsubscribe(self._topic)
