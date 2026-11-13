from abc import ABC, abstractmethod
from typing import Any, Optional


class Poller(ABC):
    def __init__(self, end_point: str, producer: Optional[Any] = None):
        self.producer = producer
        self.end_point = end_point

    @abstractmethod
    def poll(self) -> None:
        pass

    @abstractmethod
    def publish(self) -> None:
        pass


class EventTransformer(ABC):
    def __init__(self, event: Any):
        self.event = event

    @abstractmethod
    def transform(self) -> Any:
        pass


class KafkaTopic(ABC):
    def __init__(self, topic_name: str, topic_type: Optional[str] = None):
        self.topic_name = topic_name
        self.topic_type = topic_type

    @abstractmethod
    def produce(self, event: Any, topic_name: str) -> None:
        pass
