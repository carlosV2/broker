from typing import Any, Callable

from paho.mqtt.client import Client, MQTTMessage

from broker.mqtt import Subscription


class Router(object):

    def __init__(self, client: Client):
        self._client = client
        self._subscriptions = []

    def subscribe(self, queue: str, callback: Callable[[str, bytes], None]):
        subscription = Subscription(client=self._client, queue=queue, callback=callback)
        self._subscriptions.append(subscription)

        self._client.subscribe(topic=queue)
        return subscription

    def unsubscribe(self, subscription: Subscription):
        self._subscriptions = [current for current in self._subscriptions if current != subscription]
        self._client.unsubscribe(subscription.get_queue())

    def __call__(self, client: Client, userdata: Any, message: MQTTMessage) -> None:
        topic = message.topic
        payload = message.payload

        for subscription in self._subscriptions:
            subscription.process(topic, payload)
