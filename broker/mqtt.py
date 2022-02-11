from re import compile
from typing import Tuple, Callable, Any

from paho.mqtt.client import Client, MQTTMessage

from broker import Broker, Message


class Mqtt(Broker):

    def __init__(self, host: str, port: int = None, credentials: Tuple[str, str] = None, name: str = None):
        self._client = Client(client_id=name, clean_session=name is None)
        self._client.on_message = self._on_message

        if credentials is not None:
            self._client.username_pw_set(username=credentials[0], password=credentials[1])

        parameters = {'host': host}
        if port is not None:
            parameters['port'] = port

        self._subscriptions = {}
        self._client.connect(**parameters)

    def __del__(self):
        if self._client:
            self._client.disconnect()

    def _on_message(self, client: Client, userdata: Any, message: MQTTMessage) -> None:
        message = Message(topic=message.topic, payload=message.payload)

        for regex, callback in self._subscriptions.values():
            if regex.match(message.get_topic()):
                callback(message)

    def publish(self, message: Message) -> None:
        self._client.publish(topic=message.get_topic(), payload=message.get_payload_as_bytes())

    def subscribe(self, queue: str, callback: Callable[[Message], None]) -> None:
        self._client.subscribe(topic=queue)

        regex = '^{topic}$'.format(topic=queue.replace('+', '[^/]+').replace('#', '.+'))
        self._subscriptions[queue] = (compile(regex), callback)

    def unsubscribe(self, queue: str) -> None:
        self._client.unsubscribe(topic=queue)
        self._subscriptions.pop(queue)

    def consume_forever(self) -> None:
        self._client.loop_forever()
