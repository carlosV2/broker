from json import dumps, loads
from typing import Any


class Message(object):

    def __init__(self, topic: str, payload: bytes):
        self._topic = topic
        self._payload = payload

    def get_topic(self) -> str:
        return self._topic

    def get_raw_payload(self) -> bytes:
        return self._payload

    def get_encoded_payload(self) -> str:
        return self.get_raw_payload().decode()

    def get_hydrated_json_payload(self) -> Any:
        return loads(self.get_encoded_payload())

    @classmethod
    def from_raw_payload(cls, topic: str, payload: bytes) -> 'Message':
        return Message(topic=topic, payload=payload)

    @classmethod
    def from_decoded_payload(cls, topic: str, payload: str) -> 'Message':
        return Message.from_raw_payload(topic=topic, payload=payload.encode())

    @classmethod
    def from_dehydrated_json_payload(cls, topic: str, payload: Any) -> 'Message':
        return Message.from_decoded_payload(topic=topic, payload=dumps(payload))
