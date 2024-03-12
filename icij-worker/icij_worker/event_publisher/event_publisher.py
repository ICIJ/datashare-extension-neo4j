from abc import ABC, abstractmethod

from icij_worker import TaskEvent


class EventPublisher(ABC):
    @abstractmethod
    async def publish_event(self, event: TaskEvent, project: str):
        pass
