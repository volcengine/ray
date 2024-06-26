from typing import Optional

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.v2.operators.parallelism.operator_parallelism_decider import (
    OperatorParallelismDecider,
)

DEFAULT_PARALLELISM_INCREMENT = 8


class LinearParallelismDecider(OperatorParallelismDecider):
    def __init__(
        self, op: PhysicalOperator, increment: int = DEFAULT_PARALLELISM_INCREMENT
    ):
        super().__init__(op)
        self._increment = increment

    def decide(self, current_parallelism: int) -> int:
        return current_parallelism + self._increment
