from abc import ABC, abstractmethod
from typing import List, Optional, Union, final

from neo4j_app.icij_worker import Task, TaskError, TaskResult, TaskStatus


class TaskStore(ABC):
    @final
    async def enqueue(self, task: Task, project: str) -> Task:
        if task.status is not TaskStatus.CREATED:
            msg = f"invalid status {task.status}, expected {TaskStatus.CREATED}"
            raise ValueError(msg)
        task = await self._enqueue(task, project)
        if task.status is not TaskStatus.QUEUED:
            msg = f"invalid status {task.status}, expected {TaskStatus.QUEUED}"
            raise ValueError(msg)
        return task

    @final
    async def cancel(self, task: Task, project: str) -> Task:
        task = await self._cancel(task, project)
        if task.status is not TaskStatus.CANCELLED:
            msg = f"invalid status {task.status}, expected {TaskStatus.CANCELLED}"
            raise ValueError(msg)
        return task

    @abstractmethod
    async def _enqueue(self, task: Task, project: str) -> Task:
        pass

    @abstractmethod
    async def _cancel(self, task: Task, project: str) -> Task:
        pass

    @abstractmethod
    async def get_task(self, *, task_id: str, project: str) -> Task:
        pass

    @abstractmethod
    async def get_task_errors(self, task_id: str, project: str) -> List[TaskError]:
        pass

    @abstractmethod
    async def get_task_result(self, task_id: str, project: str) -> TaskResult:
        pass

    @abstractmethod
    async def get_tasks(
        self,
        project: str,
        task_type: Optional[str] = None,
        status: Optional[Union[List[TaskStatus], TaskStatus]] = None,
    ) -> List[Task]:
        pass
