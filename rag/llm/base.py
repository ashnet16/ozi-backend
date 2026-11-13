from abc import ABC, abstractmethod
from typing import Any, List


class BaseLLM(ABC):
    @abstractmethod
    def connect(self, connection_info: Any):
        pass

    @abstractmethod
    async def summarize(self, user_query: str, documents: List[str]) -> str:
        pass

    @abstractmethod
    async def ask(self, prompt: str) -> str:
        pass
