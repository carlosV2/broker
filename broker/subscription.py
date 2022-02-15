from abc import ABCMeta, abstractmethod


class Subscription(metaclass=ABCMeta):

    @abstractmethod
    def unsubscribe(self) -> None:
        pass
