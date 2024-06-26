import collections
import functools
import logging
from typing import Any, Callable, Dict, List, Optional

import ray
from ray._raylet import ObjectRef, ObjectRefGenerator
from ray.actor import ActorHandle
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data._internal.execution.interfaces.physical_operator import (
    DataFutureOpTask,
    OpTask,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    DEFAULT_MAX_TASKS_IN_FLIGHT,
    ActorPoolMapOperator,
    _MapWorker,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import MapTransformer
from ray.data._internal.execution.v2.operators.executor.actor_pool_operator_executor import (
    ActorPoolOperatorExecutor,
)
from ray.data.context import DataContext
from ray.util.placement_group import PlacementGroup


class AdaptiveMapOperator(ActorPoolMapOperator):
    """A MapOperator implementation that executes tasks on a size-adaptive pool."""

    def __init__(
        self,
        map_transformer: MapTransformer,
        input_op: PhysicalOperator,
        target_max_block_size: Optional[int],
        compute_strategy: ActorPoolStrategy,
        name: str = "AdaptiveMap",
        min_rows_per_bundle: Optional[int] = None,
        supports_fusion: bool = True,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            map_transformer,
            input_op,
            target_max_block_size,
            compute_strategy,
            name,
            min_rows_per_bundle,
            supports_fusion=supports_fusion,
            ray_remote_args_fn=ray_remote_args_fn,
            ray_remote_args=ray_remote_args,
        )

        del self._actor_pool
        self._actor_pool = None

        self._data_future_tasks: Dict[int, DataFutureOpTask] = {}

        self._op_executor = ActorPoolOperatorExecutor(
            self,
            min_pool_size=compute_strategy.min_size,
            max_pool_size=compute_strategy.max_size,
            create_actor_fn=self._start_actor,
            max_tasks_in_flight_per_actor=(
                compute_strategy.max_tasks_in_flight_per_actor
                or DEFAULT_MAX_TASKS_IN_FLIGHT
            ),
            ray_remote_args=self._ray_remote_args,
            ctx=DataContext.get_current(),
        )

    def internal_queue_size(self) -> int:
        return len(self._bundle_queue)

    def get_active_tasks(self) -> List[OpTask]:
        """Get a list of the active tasks of this operator."""
        return list(self._data_future_tasks.values())

    def start(self, options: ExecutionOptions):
        MapOperator.start(self, options=options)
        self._cls = ray.remote(**self._ray_remote_args)(_MapWorker)
        assert self._op_executor is not None
        self._op_executor.start()

    def should_add_input(self) -> bool:
        return self._op_executor.free_task_slot_size() > 0

    def _start_actor(self, **kwargs):
        if self._cls is None:
            self._cls = ray.remote(**self._ray_remote_args)(_MapWorker)
        ctx = DataContext.get_current()
        if self._ray_remote_args_fn:
            self._refresh_actor_cls()
        if kwargs:
            actor = self._cls.options(**kwargs).remote(
                ctx, src_fn_name=self.name, map_transformer=self._map_transformer
            )
        else:
            actor = self._cls.remote(
                ctx, src_fn_name=self.name, map_transformer=self._map_transformer
            )
        res_ref = actor.get_location.remote()
        return actor, res_ref

    def _dispatch_tasks(self):
        """Try to dispatch tasks from the bundle buffer to the actor pool.

        This is called when:
            * a new input bundle is added,
            * a task finishes,
            * a new worker has been created.
        """
        while self._bundle_queue:
            bundle = self._bundle_queue.popleft()
            self._metrics.on_input_dequeued(bundle)
            input_blocks = [block for block, _ in bundle.blocks]
            task_ctx = TaskContext(
                task_idx=self._next_data_task_idx,
                target_max_block_size=self.actual_target_max_block_size,
            )

            def _task_run_callback(actor, *args, **kwargs):
                streaming_generator = actor.submit.options(**kwargs).remote(*args)
                return actor, streaming_generator

            future = self._op_executor.submit(
                _task_run_callback,
                DataContext.get_current(),
                task_ctx,
                *input_blocks,
                **self._ray_actor_task_remote_args,
                **{"num_returns": "streaming", "name": self.name},
            )

            task_index = self._next_data_task_idx
            self._next_data_task_idx += 1
            self._metrics.on_task_submitted(task_index, bundle)

            def _output_ready_callback(task_index, output: RefBundle):
                # Since output is streamed, it should only contain one block.
                assert len(output) == 1
                self._metrics.on_task_output_generated(task_index, output)

                # Notify output queue that the task has produced an new output.
                self._output_queue.notify_task_output_ready(task_index, output)
                self._metrics.on_output_queued(output)

            def _task_done_callback(
                task_index: int, actor: ActorHandle, exception: Optional[Exception]
            ):
                self._metrics.on_task_finished(task_index, exception)
                # Estimate number of tasks from inputs received and tasks submitted so far
                # Estimate number of tasks from inputs received and tasks submitted so far
                upstream_op_num_outputs = self.input_dependencies[0].num_outputs_total()
                if upstream_op_num_outputs:
                    estimated_num_tasks = (
                        upstream_op_num_outputs
                        / self._metrics.num_inputs_received
                        * self._next_data_task_idx
                    )
                    self._estimated_num_output_bundles = round(
                        estimated_num_tasks
                        * self._metrics.num_outputs_of_finished_tasks
                        / self._metrics.num_tasks_finished
                    )

                self._data_future_tasks.pop(task_index)
                # Notify output queue that this task is complete.
                self._output_queue.notify_task_completed(task_index)
                self._op_executor.return_actor(actor)
                self._dispatch_tasks()

            self._data_future_tasks[task_index] = DataFutureOpTask(
                task_index,
                future,
                lambda output: _output_ready_callback(task_index, output),
                functools.partial(_task_done_callback, task_index),
            )

    def _submit_data_task(
        self,
        gen: ObjectRefGenerator,
        inputs: RefBundle,
        task_done_callback: Optional[Callable[[], None]] = None,
    ):
        raise NotImplementedError("_submit_data_task is not implemented yet")

    def _submit_metadata_task(
        self, result_ref: ObjectRef, task_done_callback: Callable[[], None]
    ):
        raise NotImplementedError("_submit_metadata_task is not implemented yet")

    def shutdown(self):
        self._op_executor.shutdown()

    def progress_str(self) -> str:
        base = f"[ready actors: {self._op_executor.ready_pool_size()}"
        pending = self._op_executor.pending_pool_size()
        base += f", pending actors: {pending}]"
        return base

    def base_resource_usage(self) -> ExecutionResources:
        min_workers = self._op_executor.min_pool_size()
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0) * min_workers,
            gpu=self._ray_remote_args.get("num_gpus", 0) * min_workers,
        )

    def num_active_tasks(self) -> int:
        return len(self._data_future_tasks)

    def current_processor_usage(self) -> ExecutionResources:
        # Both pending and running actors count towards our current resource usage.
        num_active_workers = self._op_executor.current_pool_size()
        return ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus", 0) * num_active_workers,
            gpu=self._ray_remote_args.get("num_gpus", 0) * num_active_workers,
        )

    def _extra_metrics(self) -> Dict[str, Any]:
        res = {}
        return res

    def get_core_resource_bundles(self):
        bundle = self._op_executor.resource_bundle_per_actor()
        return [bundle] * self._op_executor.min_pool_size()

    def set_core_placement_group(self, placement_group: PlacementGroup):
        self._op_executor._actor_pool.set_core_placement_group(placement_group)

    def min_pool_size(self) -> int:
        return self._op_executor.min_pool_size()
