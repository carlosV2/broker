from typing import Tuple
from urllib.parse import unquote

from dsnparse import parse
from pika import ConnectionParameters, PlainCredentials
from pika.adapters.blocking_connection import BlockingConnection

from broker import Broker as BaseBroker
from broker.amqp import Queue, Exchange


class Broker(BaseBroker):

    def __init__(self, host: str, port: int = None, credentials: Tuple[str, str] = None, vhost: str = None):
        parameters = {'host': host}

        if port is not None:
            parameters['port'] = port
        if vhost is not None:
            parameters['virtual_host'] = vhost
        if credentials is not None:
            parameters['credentials'] = PlainCredentials(username=credentials[0], password=credentials[1])

        self._connection = BlockingConnection(ConnectionParameters(**parameters))
        self._channel = self._connection.channel()

    def __del__(self):
        if hasattr(self, '_connection'):
            if self._connection:
                self._connection.close()

    def exchange(self, name: str, **kwargs) -> Exchange:
        return Exchange(channel=self._channel, name=name, **kwargs)

    def queue(self, name: str, **kwargs) -> Queue:
        return Queue(channel=self._channel, name=name, **kwargs)

    def loop(self) -> None:
        self._channel.start_consuming()

    @classmethod
    def from_dsn(cls, dsn) -> 'Broker':
        result = parse(dsn)

        credentials = None
        if result.user is not None and result.password is not None:
            credentials = (unquote(result.user), unquote(result.password))

        vhost = result.path.lstrip('/')
        if len(vhost) == 0:
            vhost = None

        return cls(host=result.host, port=result.port, credentials=credentials, vhost=vhost)
