import logging
import threading
import time
import uuid
from collections import defaultdict
from typing import Dict, Iterator, List, Optional, Tuple

import ray
from ray._raylet import ObjectRefGenerator
from ray.data import DataContext
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
)
from ray.data._internal.execution.backpressure_policy import (
    BackpressurePolicy,
    get_backpressure_policies,
)
from ray.data._internal.execution.interfaces import (
    Executor,
    OutputIterator,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionOptions
from ray.data._internal.execution.interfaces.physical_operator import (
    DataFutureOpTask,
    OpTask,
    Waitable,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor import (
    DEBUG_LOG_INTERVAL_SECONDS,
    PROGRESS_BAR_UPDATE_INTERVAL,
    StreamingExecutor,
    _debug_dump_topology,
    _log_op_metrics,
)
from ray.data._internal.execution.streaming_executor_state import (
    OpState,
    Topology,
    build_streaming_topology,
    select_operator_to_run,
    update_operator_states,
)
from ray.data._internal.execution.v2.operators.adaptive_actor_pool_map_operator import (
    AdaptiveMapOperator,
)
from ray.data._internal.logging import get_log_directory
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import DatasetStats, StatsManager
from ray.util import placement_group
from ray.util.placement_group import PlacementGroup, remove_placement_group

logger = logging.getLogger(__name__)


class AdaptiveStreamingExecutor(StreamingExecutor):
    """An adaptive streaming Dataset executor.

    This implementation executes Dataset DAGs as the base StreamingExecutor.
    And it provides the capability to adjust the individual operator parallelism adaptively.
    """

    def __init__(self, options: ExecutionOptions, dataset_tag: str = "unknown_dataset"):
        super().__init__(options, dataset_tag)

        thread_name = f"AdaptiveStreamingExecutor-{self._execution_id}"
        threading.Thread.__init__(self, daemon=True, name=thread_name)

    def execute(
        self, dag: PhysicalOperator, initial_stats: Optional[DatasetStats] = None
    ) -> Iterator[RefBundle]:
        self._initial_stats = initial_stats
        self._start_time = time.perf_counter()

        if not isinstance(dag, InputDataBuffer):
            context = DataContext.get_current()
            if context.print_on_execution_start:
                message = "Starting execution of Dataset."
                log_path = get_log_directory()
                if log_path is not None:
                    message += f" Full logs are in {log_path}"
                logger.info(message)
                logger.info(f"Execution plan of Dataset: {dag}")

            logger.debug("Execution config: %s", self._options)

        self._topology, self.core_placement_group = build_adaptive_streaming_executor(
            dag, self._options
        )
        self._resource_manager = ResourceManager(self._topology, self._options)
        self._backpressure_policies = get_backpressure_policies(self._topology)

        self._has_op_completed = {op: False for op in self._topology}
        if not isinstance(dag, InputDataBuffer):
            # Note: DAG must be initialized in order to query num_outputs_total.
            self._global_info = ProgressBar("Running", dag.num_outputs_total(), unit="bundle")

        self._output_node: OpState = self._topology[dag]

        StatsManager.register_dataset_to_stats_actor(
            self._dataset_tag,
            self._get_operator_tags(),
        )
        self.start()
        self._execution_started = True

        class StreamIterator(OutputIterator):
            def __init__(self, outer: Executor):
                self._outer = outer

            def get_next(self, output_split_idx: Optional[int] = None) -> RefBundle:
                try:
                    item = self._outer._output_node.get_output_blocking(
                        output_split_idx
                    )
                    if self._outer._global_info:
                        self._outer._global_info.update(1, dag.num_outputs_total())
                    return item
                # Needs to be BaseException to catch KeyboardInterrupt. Otherwise we
                # can leave dangling progress bars by skipping shutdown.
                except BaseException as e:
                    self._outer.shutdown(isinstance(e, StopIteration))
                    raise

            def __del__(self):
                self._outer.shutdown()

        return StreamIterator(self)

    def __del__(self):
        self.shutdown()

    def shutdown(self, execution_completed: bool = True):
        context = DataContext.get_current()

        with self._shutdown_lock:
            if not self._execution_started or self._shutdown:
                return
            self._shutdown = True
            self.join(timeout=2.0)
            self._update_stats_metrics(
                state="FINISHED" if execution_completed else "FAILED",
                force_update=True,
            )
            # Clears metrics for this dataset so that they do
            # not persist in the grafana dashboard after execution
            StatsManager.clear_execution_metrics(
                self._dataset_tag, self._get_operator_tags()
            )
            # Freeze the stats and save it.
            self._final_stats = self._generate_stats()
            stats_summary_string = self._final_stats.to_summary().to_string(
                include_parent=False
            )
            if context.enable_auto_log_stats:
                logger.info(stats_summary_string)
            # Close the progress bars from top to bottom to avoid them jumping
            # around in the console after completion.
            if self._global_info:
                self._global_info.close()
            for op, state in self._topology.items():
                op.shutdown()
                state.close_progress_bars()
            if self.core_placement_group:
                remove_placement_group(self.core_placement_group)

    def run(self):
        try:
            # Run scheduling loop until complete.
            while True:
                t_start = time.process_time()
                continue_sche = self._scheduling_loop_step(self._topology)
                if self._initial_stats:
                    self._initial_stats.streaming_exec_schedule_s.add(
                        time.process_time() - t_start
                    )
                if not continue_sche or self._shutdown:
                    break
        except Exception as e:
            # Propagate it to the result iterator.
            self._output_node.mark_finished(e)
        finally:
            self._output_node.mark_finished()

    def _scheduling_loop_step(self, topology: Topology) -> bool:
        self._resource_manager.update_usages()
        num_errored_blocks = process_data_tasks(
            topology,
            self._resource_manager,
            self._max_errored_blocks,
        )
        if self._max_errored_blocks > 0:
            self._max_errored_blocks -= num_errored_blocks
        self._num_errored_blocks += num_errored_blocks

        self._resource_manager.update_usages()

        self._report_current_usage()

        op = select_operator_to_run(
            topology,
            self._resource_manager,
            self._backpressure_policies,
            None,
            ensure_at_least_one_running=self._consumer_idling(),
        )

        i = 0
        while op is not None:
            i += 1
            if i > PROGRESS_BAR_UPDATE_INTERVAL:
                break
            topology[op].dispatch_next_task()
            self._resource_manager.update_usages()
            op = select_operator_to_run(
                topology,
                self._resource_manager,
                self._backpressure_policies,
                None,
                ensure_at_least_one_running=self._consumer_idling(),
            )

        update_operator_states(topology)

        # Update the progress bar to reflect scheduling decisions.
        for op_state in topology.values():
            op_state.refresh_progress_bar(self._resource_manager)

        self._update_stats_metrics(state="RUNNING")
        if time.time() - self._last_debug_log_time >= DEBUG_LOG_INTERVAL_SECONDS:
            _log_op_metrics(topology)
            _debug_dump_topology(topology, self._resource_manager)
            self._last_debug_log_time = time.time()

        # Log metrics of newly completed operators.
        for op in topology:
            if op.completed() and not self._has_op_completed[op]:
                log_str = (
                    f"Operator {op} completed. "
                    f"Operator Metrics:\n{op._metrics.as_dict()}"
                )
                logger.debug(log_str)
                self._has_op_completed[op] = True

        # Keep going until all operators run to completion.
        return not all(op.completed() for op in topology)


def process_data_tasks(
    topology: Topology, resource_manager: ResourceManager, max_errored_blocks: int
) -> int:
    active_tasks: Dict[ObjectRefGenerator, Tuple[OpState, OpTask]] = {}
    for op, state in topology.items():
        if isinstance(op, AdaptiveMapOperator):
            for task in op.get_active_tasks():
                if task.get_waitable().done():
                    assert not task.get_waitable().cancelled()
                    _, generator_ref = task.get_waitable().result()
                    active_tasks[generator_ref] = (state, task)
        else:
            for task in op.get_active_tasks():
                active_tasks[task.get_waitable()] = (state, task)

    max_bytes_to_read_per_op: Dict[OpState, int] = {}
    if resource_manager.op_resource_allocator_enabled():
        for op, state in topology.items():
            max_bytes_to_read = (
                resource_manager.op_resource_allocator.max_task_output_bytes_to_read(op)
            )
            if max_bytes_to_read is not None:
                max_bytes_to_read_per_op[state] = max_bytes_to_read

    num_errored_blocks = 0
    if active_tasks:
        ready, _ = ray.wait(
            list(active_tasks.keys()),
            num_returns=len(active_tasks),
            fetch_local=False,
            timeout=0.1,
        )

        # Organize tasks by the operator they belong to, and sort them by task index.
        # So that we'll process them in a deterministic order.
        # This is because OpResourceAllocator may limit the number of blocks to read
        # per operator. In this case, we want to have fewer tasks finish quickly and
        # yield resources, instead of having all tasks output blocks together.
        ready_tasks_by_op = defaultdict(list)
        for ref in ready:
            state, task = active_tasks[ref]
            ready_tasks_by_op[state].append(task)

        for state, ready_tasks in ready_tasks_by_op.items():
            ready_tasks = sorted(ready_tasks, key=lambda t: t.task_index())
            for task in ready_tasks:
                try:
                    bytes_read = task.on_data_ready(
                        max_bytes_to_read_per_op.get(state, None)
                    )
                    if state in max_bytes_to_read_per_op:
                        max_bytes_to_read_per_op[state] -= bytes_read
                except Exception as e:
                    num_errored_blocks += 1
                    should_ignore = (
                        max_errored_blocks < 0
                        or max_errored_blocks >= num_errored_blocks
                    )
                    error_message = (
                        "An exception was raised from a task of "
                        f'operator "{state.op.name}".'
                    )
                    if should_ignore:
                        remaining = (
                            max_errored_blocks - num_errored_blocks
                            if max_errored_blocks >= 0
                            else "unlimited"
                        )
                        error_message += (
                            " Ignoring this exception with remaining"
                            f" max_errored_blocks={remaining}."
                        )
                        logger.error(error_message, exc_info=e)
                    else:
                        error_message += (
                            " Dataset execution will now abort."
                            " To ignore this exception and continue, set"
                            " DataContext.max_errored_blocks."
                        )
                        logger.error(error_message)
                        raise e from None

    # Pull any operator outputs into the streaming op state.
    for op, op_state in topology.items():
        while op.has_next():
            op_state.add_output(op.get_next())

    return num_errored_blocks


def build_adaptive_streaming_executor(
    dag: PhysicalOperator, options: ExecutionOptions
) -> Tuple[Topology, PlacementGroup]:
    topology: Topology = {}
    bundles: List[Dict] = []

    # DFS walk to wire up operator states.
    def setup_state(op: PhysicalOperator) -> OpState:
        if op in topology:
            raise ValueError("An operator can only be present in a topology once.")

        # Wire up the input outqueues to this op's inqueues.
        inqueues = []
        for i, parent in enumerate(op.input_dependencies):
            parent_state = setup_state(parent)
            inqueues.append(parent_state.outqueue)

        if isinstance(op, AdaptiveMapOperator):
            bundles.extend(op.get_core_resource_bundles())

        # Create state.
        op_state = OpState(op, inqueues)
        topology[op] = op_state
        # op.start(options)
        return op_state

    setup_state(dag)

    pg = None

    if bundles:
        pg = placement_group(bundles, strategy="PACK")
        ray.get(pg.ready(), timeout=DataContext.get_current().wait_for_min_actors_s)
        logger.info(f"Core placement group ready, bundles = {len(bundles)}")

    for op, state in topology.items():
        if isinstance(op, AdaptiveMapOperator):
            op.set_core_placement_group(pg)
        op.start(options)
    # Create the progress bars starting from the first operator to run.
    # Note that the topology dict is in topological sort order. Index zero is reserved
    # for global progress information.
    i = 1
    for op_state in list(topology.values()):
        if not isinstance(op_state.op, InputDataBuffer):
            i += op_state.initialize_progress_bars(i, options.verbose_progress)

    return (topology, pg)
