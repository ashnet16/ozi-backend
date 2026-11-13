from abc import ABC, abstractmethod
from typing import Any, List, Optional


class Consumer(ABC):
    def __init__(
        self,
        end_point: str,
        topics: Optional[List[str]] = None,
        consumer: Optional[Any] = None,
        consumer_group: Optional[str] = None,
    ):
        self.consumer = consumer
        self.end_point = end_point
        self.topics = topics or []
        self.consumer_group = consumer_group

    @abstractmethod
    def consume(self) -> None:
        pass

    @abstractmethod
    def process(self, msg: dict, topic_name: Optional[str] = None) -> None:
        pass
