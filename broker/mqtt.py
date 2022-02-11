from re import compile
from typing import Tuple, Callable, Any

from paho.mqtt.client import Client, MQTTMessage

from broker import Broker, Message


class Mqtt(Broker):

    def __init__(self, host: str, port: int = None, credentials: Tuple[str, str] = None, name: str = None):
        self._client = Client(client_id=name, clean_session=name is None or len(name) == 0)
        self._client.on_message = self._on_message

        if credentials is not None:
            self._client.username_pw_set(username=credentials[0], password=credentials[1])

        parameters = {'host': host}
        if port is not None:
            parameters['port'] = port

        self._subscriptions = {}
        self._client.connect(**parameters)

    def __del__(self):
        if hasattr(self, '_client'):
            if self._client:
                self._client.disconnect()

    def _on_message(self, client: Client, userdata: Any, message: MQTTMessage) -> None:
        message = Message(topic=message.topic, payload=message.payload)

        for regex, callback in self._subscriptions.values():
            if regex.match(message.get_topic()):
                callback(message)

    def publish(self, message: Message) -> None:
        self._client.publish(topic=message.get_topic(), payload=message.get_raw_payload())

    def subscribe(self, topic: str, callback: Callable[[Message], None]) -> None:
        self._client.subscribe(topic=topic)

        regex = '^{topic}$'.format(topic=topic.replace('+', '[^/]+').replace('#', '.+'))
        self._subscriptions[topic] = (compile(regex), callback)

    def unsubscribe(self, topic: str) -> None:
        self._client.unsubscribe(topic=topic)
        self._subscriptions.pop(topic)

    def consume_forever(self) -> None:
        self._client.loop_forever()
