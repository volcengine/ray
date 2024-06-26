from abc import ABC, abstractmethod

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.streaming_executor_state import OpState


class OperatorParallelismDecider(ABC):
    def __init__(self, op: PhysicalOperator):
        self._op = op

    @abstractmethod
    def decide(self, current_parallelism):
        ...
