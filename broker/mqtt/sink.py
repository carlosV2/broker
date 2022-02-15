from broker import Sink


class Topic(Sink):

    def __init__(self, topic: str):
        self._topic = topic

    def get_binding(self) -> str:
        return self._topic
