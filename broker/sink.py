from abc import ABCMeta, abstractmethod


class Sink(metaclass=ABCMeta):

    @abstractmethod
    def get_binding(self, *args, **kwargs) -> str:
        pass
