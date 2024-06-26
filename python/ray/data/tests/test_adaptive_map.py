import math
import random
import time

import numpy as np
import ray
from ray.data import DataContext, ActorPoolStrategy


def test_adaptive_map(shutdown_only):
    ray.init(num_cpus=100)
    ts = time.time()
    DataContext.get_current().enable_adaptive_execute = True
    DataContext.get_current().log_internal_stack_trace_to_stdout = True
    DataContext.get_current().task_backlog_timeout_secs = 0.1
    DataContext.get_current().actor_kill_timeout_secs = 1
    n = 1000

    ds = ray.data.range(n)

    class StatefulFn:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            for i in range(10000):
                for j in range(10000):
                    i * j
            time.sleep(1.5)
            self.num_reuses += 1
            return {"id": np.array([r])}

    class StatefulFn2:
        def __init__(self):
            self.num_reuses = 0

        def __call__(self, x):
            r = self.num_reuses
            for i in range(10000):
                for j in range(10000):
                    i * j
            time.sleep(1.5)
            self.num_reuses += 1
            return {"id": np.array([r])}

    # map
    actor_reuse = ds.map(StatefulFn, num_cpus=1, concurrency=(10, 100)).map(StatefulFn2, num_cpus=1,
                                                                            concurrency=(10, 100)).take_all()
    print(ds.stats())
    assert len(actor_reuse) == n
    print(f"cost time: {time.time() - ts}")
