from abc import ABC, abstractmethod
from typing import List, Optional, Union

from neo4j_app.icij_worker import Task, TaskError, TaskResult, TaskStatus


class TaskStore(ABC):
    @abstractmethod
    async def get_task(self, *, project: str, task_id: str) -> Task:
        pass

    @abstractmethod
    async def get_task_errors(self, project: str, task_id: str) -> List[TaskError]:
        pass

    @abstractmethod
    async def get_task_result(self, project: str, task_id: str) -> TaskResult:
        pass

    @abstractmethod
    async def get_tasks(
        self,
        project: str,
        task_type: Optional[str] = None,
        status: Optional[Union[List[TaskStatus], TaskStatus]] = None,
    ) -> List[Task]:
        pass
