from abc import ABC, abstractmethod
from typing import Any

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.physical_operator import OpTask


class OperatorExecutor(ABC):
    """This is an abstract base class for concrete asynchronous executors for a given physical operator."""

    def __init__(self, op: PhysicalOperator):
        self._op = op

    @abstractmethod
    def start(self) -> None:
        ...

    @abstractmethod
    def submit(self, task: OpTask, *args: Any, **kwargs: Any) -> Any:
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a ObjectRef instance representing the execution of the callable.

        Returns:
            ObjectRef representing the given call.
        """
        ...

    @abstractmethod
    def shutdown(self, wait: bool) -> None:
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other
        methods can be called after this one.

        Args:
         wait: If True then shutdown will not return until all running
             task have finished executing and the resources used by the
             executor have been reclaimed.
        """
        ...

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(wait=True)
        return False
