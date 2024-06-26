import math

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.streaming_executor_state import OpState
from ray.data._internal.execution.v2.operators.parallelism.operator_parallelism_decider import (
    OperatorParallelismDecider,
)


class ExponentialParallelismDecider(OperatorParallelismDecider):
    def __init__(self, op: PhysicalOperator, factor: float):
        super().__init__(op)
        self._factor = factor

    def decide(self, current_parallelism) -> int:
        return max(1, int(math.floor(current_parallelism * self._factor)))
