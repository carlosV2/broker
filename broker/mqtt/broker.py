from typing import Tuple
from urllib.parse import unquote

from dsnparse import parse
from paho.mqtt.client import Client

from broker import Broker as BaseBroker
from broker.mqtt import Queue, Router


class Broker(BaseBroker):

    def __init__(self, host: str, port: int = None, credentials: Tuple[str, str] = None, name: str = None):
        self._client = Client(client_id=name, clean_session=name is None or len(name) == 0)
        self._client.on_message = Router(self._client)

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

    def queue(self, name: str, **kwargs) -> Queue:
        return Queue(client=self._client, name=name)

    def loop(self) -> None:
        self._client.loop_forever()

    @classmethod
    def from_dsn(cls, dsn) -> 'Broker':
        result = parse(dsn)

        credentials = None
        if result.user is not None and result.password is not None:
            credentials = (unquote(result.user), unquote(result.password))

        name = result.path.lstrip('/')
        if len(name) == 0:
            name = None

        return cls(host=result.host, port=result.port, credentials=credentials, name=name)
