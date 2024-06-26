import logging
import math
import queue
import threading
import time
import uuid
from collections import OrderedDict, defaultdict
from concurrent.futures import Future
from typing import Any, Callable, Dict, List, Optional, Tuple

import ray
from ray._raylet import ObjectRef
from ray.actor import ActorHandle
from ray.data import DataContext, NodeIdStr
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.v2.operators.executor.operator_executor import (
    OperatorExecutor,
)
from ray.data._internal.execution.v2.operators.parallelism.linear_parallelism_decider import (
    LinearParallelismDecider,
)
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

logger = logging.getLogger(__name__)


class ActorInfo:
    def __init__(self, max_slot_size, location):
        self.max_slot_size = max_slot_size
        self.used_slot_size = 0
        self.timestamp = time.time()
        # NodeID actor located on.
        self.location = location

    def use_slot(self):
        if self.used_slot_size < self.max_slot_size:
            self.used_slot_size += 1
            self.timestamp = time.time()

    def free_slot(self):
        if self.used_slot_size > 0:
            self.used_slot_size -= 1

    @property
    def free_slot_size(self):
        return self.max_slot_size - self.used_slot_size

    def __repr__(self) -> str:
        return "ActorUsage(max_slot_size={}, used_slot_size={}, timestamp={}, location)".format(
            self.max_slot_size, self.used_slot_size, self.timestamp, self.location
        )


class _ActorPool:
    def __init__(self, name, max_tasks_in_flight_per_actor: int):
        if max_tasks_in_flight_per_actor <= 0:
            raise ValueError("_max_tasks_in_flight must be > 0")

        self._repr = name

        self._max_tasks_in_flight_per_actor = max_tasks_in_flight_per_actor

        self._pending_actors: OrderedDict[ObjectRef, ActorHandle] = OrderedDict()
        self._ready_actors: Dict[ActorHandle, ActorInfo] = {}

        self._locality_hits: int = 0
        self._locality_misses: int = 0

        self._lock = threading.RLock()

    def add_pending_actor(self, actor: ActorHandle, ready_ref: ObjectRef):
        with self._lock:
            self._pending_actors[ready_ref] = actor

    def pending_to_ready(self, ready_ref: ObjectRef) -> bool:
        ready_obj = ray.get(ready_ref)
        with self._lock:
            if ready_ref not in self._pending_actors:
                return False
            actor = self._pending_actors.pop(ready_ref)
            self._ready_actors[actor] = ActorInfo(
                self._max_tasks_in_flight_per_actor, ready_obj
            )

    def num_pending_actors(self) -> int:
        return len(self._pending_actors)

    def num_ready_actors(self) -> int:
        return len(self._ready_actors)

    def pick_actor(self) -> Optional[ActorHandle]:
        """Picks an actor for task submission based on busyness and locality.

        None will be returned if all actors are either at capacity (according to
        max_tasks_in_flight) or are still pending.

        """

        with self._lock:
            def _free(info):
                return info.free_slot_size > 0

            picked_actor, info = _pick_actor_to_use(self._ready_actors, _free)
            if info is not None:
                info.use_slot()

            return picked_actor

    def return_actor(self, actor: ActorHandle):
        with self._lock:
            assert actor in self._ready_actors
            assert self._ready_actors[actor].used_slot_size > 0
            self._ready_actors[actor].free_slot()

    def locality_metrics(self):
        return self._locality_hits, self._locality_misses

    def num_free_task_slots(self):
        if not self._ready_actors:
            return 0
        return sum(usage.free_slot_size for usage in self._ready_actors.values())

    def _remove_ready_actor(self, actor: ActorHandle):
        with self._lock:
            v = self._ready_actors.pop(actor, None)
            return v is not None

    def _remove_pending_actor(self, ready_ref: ObjectRef):
        with self._lock:
            v = self._pending_actors.pop(ready_ref, None)
            return v


class _ScaleEvent:
    def __init__(self, delta):
        self._delta = delta

    @property
    def delta(self):
        return self._delta


class _ScalableActorPool(_ActorPool, threading.Thread):
    def __init__(
        self,
        name: str,
        min_size: int,
        max_size: int,
        create_actor_fn: Callable,
        max_tasks_in_flight_per_actor: int,
        resource_bundle_per_actor: Dict,
        ctx: DataContext,
    ):
        super().__init__(name, max_tasks_in_flight_per_actor)
        if min_size < 0:
            raise ValueError("min_size must be greater than or equal to 0")
        if max_size < 0:
            raise ValueError("max_size must be greater than or equal to 0")
        if min_size > max_size:
            raise ValueError("min_size must be less than or equal to max_size")

        self._min_size: int = min_size
        self._max_size: int = max_size
        self._create_actor_fn: Callable = create_actor_fn
        self._resource_bundle_per_actor = resource_bundle_per_actor
        self._ctx = ctx
        self._repr = name

        self._queue = queue.SimpleQueue()
        self._shutdown = True
        self._shutdown_lock = threading.Lock()

        thread_name = f"ScalableActorPool-{uuid.uuid4().hex}"
        threading.Thread.__init__(self, daemon=True, name=thread_name)

        self._core_placement_group = None

    def set_core_placement_group(self, placement_group):
        self._core_placement_group = placement_group

    def __str__(self):
        return f"[{self._repr}]"

    def _create_actor(self, **kwargs) -> Tuple[ActorHandle, ObjectRef]:
        actor, ready_ref = self._create_actor_fn(**kwargs)
        self.add_pending_actor(actor, ready_ref)
        return actor, ready_ref

    def _adjust_actor_count(self):
        def _scale_down(num_actors: int) -> int:
            num_killed = 0
            current_size = self.size()

            real_delta = min(current_size - self.min_size(), num_actors)
            if real_delta > 0:
                for _ in range(real_delta):
                    if self._kill_inactive_actor(hard=True):
                        num_killed += 1
            return num_killed

        def _scale_up(event: _ScaleEvent) -> int:
            current_size = self.size()
            real_delta = min(self._max_size - current_size, event.delta)
            if real_delta > 0:
                for _ in range(real_delta):
                    self._create_actor()
            return real_delta

        while True:
            if self._shutdown:
                break

            pending_actor_refs = list(self._pending_actors.keys())
            pending_size = len(pending_actor_refs)
            if pending_size > 0:
                ready_refs, _ = ray.wait(
                    pending_actor_refs,
                    num_returns=pending_size,
                    fetch_local=False,
                    timeout=0.1,
                )
                if len(ready_refs) > 0:
                    for ready_ref in ready_refs:
                        self.pending_to_ready(ready_ref)
                    logger.debug(
                        f"Add {len(ready_refs)} actors to the ready pool={self}."
                    )

            try:
                scale_event = self._queue.get_nowait()
                if scale_event is not None and scale_event.delta > 0:
                    _scale_up(scale_event)
            except queue.Empty:
                # If any alive actor is idle, we kill it and kill one pending actor more.
                self._kill_inactive_actor()
                time.sleep(1)

        # kill all actors on shutdown
        actor_num = self.size()
        logger.info(f"Shutting down actor pool={self} and kill all {actor_num} actors.")
        _scale_down(actor_num)

    def _kill_inactive_actor(self, hard: bool = False) -> bool:
        """Kills a single pending or idle ready actor if any actor is pending or idle."""
        with self._lock:

            def _kill_one():
                killed = self._try_kill_pending_actor()
                if not killed:
                    killed = self._try_kill_idle_actor()
                return killed

            if hard or (self.size() > self.min_size() and self._idle_expired_actors()):
                return _kill_one()
            return False

    def _try_kill_pending_actor(self) -> bool:
        if self._pending_actors:
            self._kill_pending_actor(next(iter(reversed(self._pending_actors.keys()))))
            return True
        return False

    def _idle_expired_actors(self):
        now = time.time()

        def _check(info):
            free = info.used_slot_size == 0
            expired = now - info.timestamp > self._ctx.actor_kill_timeout_secs
            return free and expired

        with self._lock:
            actor_info_tuples = [
                (actor, info)
                for actor, info in self._ready_actors.items()
                if _check(info)
            ]
            return actor_info_tuples

    def _try_kill_idle_actor(self) -> bool:
        actor_info_tuples = self._idle_expired_actors()
        actor, info = _pick_actor_to_kill(actor_info_tuples, self._ready_actors)
        # Kill an alive actor and restart is an expensive operation. So we only pick one and kill it.
        if actor is not None and info.used_slot_size == 0:
            return self._kill_ready_actor(actor)
        return False

    def _kill_pending_actor(self, ready_ref: ObjectRef) -> bool:
        actor = self._remove_pending_actor(ready_ref)
        if actor:
            logger.debug(f"Kill a pending actor, actor={actor}, pool={self}")
            ray.kill(actor)
            del actor
            return True
        return False

    def _kill_ready_actor(self, actor: ActorHandle) -> bool:
        if self._remove_ready_actor(actor):
            logger.debug(f"Kill a ready actor, actor={actor}, pool={self}")
            ray.kill(actor)
            del actor
            return True
        return False

    def run(self):
        with self._shutdown_lock:
            self._shutdown = False
        self._adjust_actor_count()

    def min_size(self) -> int:
        return self._min_size

    def max_size(self) -> int:
        return self._max_size

    def size(self) -> int:
        return self.num_pending_actors() + self.num_ready_actors()

    def scale_up(self, num_actors: int):
        event = _ScaleEvent(num_actors)
        self._queue.put(event)

    def init_core_actor_pool(self):
        try:
            for _ in range(self._min_size):
                self._create_actor(
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=self._core_placement_group
                    )
                )
        except Exception as e:
            raise TimeoutError(f"Init pool={self} timeout, exception={e}")

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        with self._shutdown_lock:
            if not self._shutdown:
                self._shutdown = True
                self.join(1)


class _WorkItem:
    def __init__(self, future, fn, args, kwargs):
        self._future = future
        self._fn = fn
        self._args = args
        self._kwargs = kwargs
        self.submit_ts = time.time()

    def run_with_actor(self, actor):
        if not self._future.set_running_or_notify_cancel():
            return

        try:
            result = self._fn(actor, *self._args, **self._kwargs)
        except BaseException as e:
            self._future.set_exception(e)
            self = None
        else:
            self._future.set_result(result)


class ActorPoolOperatorExecutor(OperatorExecutor):
    def __init__(
        self,
        op: PhysicalOperator,
        min_pool_size: int,
        max_pool_size: int,
        create_actor_fn: Callable,
        max_tasks_in_flight_per_actor: int,
        ray_remote_args: Optional[Dict[str, Any]],
        ctx: DataContext,
    ):
        super().__init__(op)

        def _to_bundle() -> Dict:
            req = {}
            if "num_cpus" in ray_remote_args:
                req["CPU"] = math.ceil(ray_remote_args["num_cpus"])
            else:
                req["CPU"] = 1
            if "num_gpus" in ray_remote_args:
                req["GPU"] = math.ceil(ray_remote_args["num_gpus"])
            return req

        self._resource_bundle_per_actor = _to_bundle()

        self._actor_pool = _ScalableActorPool(
            op.name,
            min_pool_size,
            max_pool_size,
            create_actor_fn,
            max_tasks_in_flight_per_actor,
            self._resource_bundle_per_actor,
            ctx,
        )
        self._ctx = ctx
        self._op_parallelism_decider = LinearParallelismDecider(op)

        self._work_queue = queue.SimpleQueue()

        self._shutdown = False
        self._shutdown_lock = threading.Lock()

        self._consume_thread: threading.Thread = None

    def start(self):
        self._actor_pool.init_core_actor_pool()
        self._actor_pool.start()

        def _consume():
            while True:
                if self._shutdown:
                    logger.info(
                        f"Shutting down the executor={self._op.name}, task size={self._work_queue.qsize()}"
                    )
                    break
                try:
                    work_item = self._work_queue.get_nowait()
                except queue.Empty:
                    work_item = self._work_queue.get(block=True)

                if not work_item:
                    logger.warning("Empty work item, signal to stop the consume")
                    break

                scaled = False

                while True:
                    actor = self._actor_pool.pick_actor()
                    if actor is not None:
                        work_item.run_with_actor(actor)
                        del work_item
                        break
                    else:
                        current_size = self._actor_pool.size()
                        core_pool_ready = current_size >= self.min_pool_size()
                        under_upper_limit = current_size < self.max_pool_size()

                        task_wait_secs = time.time() - work_item.submit_ts
                        if (
                            not scaled
                            and core_pool_ready
                            and under_upper_limit
                            and task_wait_secs > self._ctx.task_backlog_timeout_secs
                        ):
                            decided_parallelism = self._op_parallelism_decider.decide(
                                current_size
                            )
                            delta_size = decided_parallelism - current_size
                            logger.info(
                                f"Task backlog timeout after {self._ctx.task_backlog_timeout_secs} secs. We try to scale up the pool=[{self._actor_pool}] with incremental {delta_size} actors and current size is {current_size}"
                            )
                            self._actor_pool.scale_up(delta_size)
                            scaled = True
                        time.sleep(0.01)

        self._consume_thread: threading.Thread = threading.Thread(
            target=_consume,
            daemon=True,
            name=f"OperatorExecutor-consume-thread-{uuid.uuid4().hex}",
        )
        self._consume_thread.start()

    def submit(self, fn: Callable, *args: Any, **kwargs: Any) -> Future:
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("ActorPoolOperator has already been shutdown")

            f = Future()
            w = _WorkItem(f, fn, args, kwargs)
            self._work_queue.put(w)
            return f

    # TODO(mzs): remove the temp implementation
    def return_actor(self, actor: ActorHandle):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("ActorPoolOperator has already been shutdown")
        self._actor_pool.return_actor(actor)

    def shutdown(self, wait: bool = True) -> None:
        with self._shutdown_lock:
            self._actor_pool.shutdown()
            self._work_queue.put(None)
            self._shutdown = True
            self._consume_thread.join(1)

    def min_pool_size(self):
        return self._actor_pool.min_size()

    def max_pool_size(self):
        return self._actor_pool.max_size()

    def current_pool_size(self):
        return self._actor_pool.size()

    def ready_pool_size(self):
        return self._actor_pool.num_ready_actors()

    def pending_pool_size(self):
        return self._actor_pool.num_pending_actors()

    def free_task_slot_size(self):
        return self._actor_pool.num_free_task_slots()

    def resource_bundle_per_actor(self):
        return self._resource_bundle_per_actor


def _group_by_nodes(actors) -> Dict[NodeIdStr, List[Tuple[ActorHandle, ActorInfo]]]:
    ret = defaultdict(list)
    for actor, info in actors:
        ret[info.location].append((actor, info))
    return ret


def _pick_actor_to_kill(
    candidates: List[Tuple[ActorHandle, ActorInfo]], total: Dict[ActorHandle, ActorInfo]
) -> Tuple[ActorHandle, ActorInfo]:
    candidates_group_by_node = _group_by_nodes(candidates)

    # sort by node id
    actors_group_by_node = _group_by_nodes(total.items())
    actors_sorted_by_node = sorted(
        actors_group_by_node.items(), key=lambda item: item[0]
    )

    actors_sorted_by_load = sorted(actors_sorted_by_node, key=lambda x: len(x[1]))
    for node, _ in actors_sorted_by_load:
        actors = candidates_group_by_node.get(node, None)
        if actors:
            selected = max(actors, key=lambda x: x[1].timestamp)
            return selected
    return None, None


def _pick_actor_to_use(
    total: Dict[ActorHandle, ActorInfo], condition: Callable[[ActorInfo], bool]
) -> Tuple[ActorHandle, ActorInfo]:
    actors_group_by_node = _group_by_nodes(total.items())
    # sort by node id
    actors_sorted_by_node = sorted(
        actors_group_by_node.items(), key=lambda item: item[0]
    )
    # reverse sort by load, try to pack the task.
    actors_sorted_by_load = sorted(
        actors_sorted_by_node, key=lambda x: len(x[1]), reverse=True
    )

    for node, actors in actors_sorted_by_load:
        satisfied_actors = [(actor, info) for actor, info in actors if condition(info)]
        if satisfied_actors:
            selected = min(
                satisfied_actors, key=lambda x: (x[1].used_slot_size, x[1].timestamp)
            )
            return selected
    return None, None
