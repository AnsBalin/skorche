# package imports
from .node import Node, NodeType
from .op import SplitOp, MergeOp, BatchOp, UnbatchOp, FilterOp, Op
from .queue import Queue
from .task import Task


# standard library imports
from collections import deque
import concurrent.futures
from typing import Callable, List, Tuple


# dependency imports
from graphviz import Digraph


class PipelineManager:
    """Manager for entire pipeline state"""

    def __init__(self):
        self.task_table = {}
        self.ops = []
        self.op_table = {}
        self.pool_table = {}

        # this should only ever be touched with new_qid()
        self._queue_counter = 0

    def new_qid(self) -> int:
        """return new queue id"""
        self._queue_counter += 1
        return self._queue_counter

    def map(self, task: Task, queue_in: Queue, queue_out: Queue = None) -> Queue:
        if queue_out == None:
            queue_out = Queue(id=self.new_qid())

        self.task_table[task] = {"queue_in": queue_in, "queue_out": queue_out}

        queue_in.children.add(task)
        task.children.add(queue_out)

        return queue_out

    def chain(
        self, task_list: List[Task], queue_in: Queue, queue_out: Queue = None
    ) -> Queue:
        # TODO: fix bug here where queue_out is never used

        if len(task_list) == 1:
            self.map(task_list[0], queue_in, queue_out)
            return

        # Construct intermediate queue to join between chained tasks
        queue_int = Queue(id=self.new_qid())
        queue_int = self.map(task_list[0], queue_in, queue_int)

        for task in task_list[1:]:
            queue_int = self.map(task, queue_int, Queue(id=self.new_qid()))

        return queue_int

    def split(
        self,
        predicate_fn: Callable,
        queue_in: Queue,
        predicate_values: Tuple = (True, False),
    ) -> Tuple[Queue]:
        out_queue_map = {
            value: Queue(name=str(value), id=self.new_qid())
            for value in predicate_values
        }
        op = SplitOp(predicate_fn, queue_in, out_queue_map)
        self.ops.append(op)
        self.op_table[op] = {
            "queues_in": [queue_in],
            "queues_out": list(out_queue_map.values()),
        }

        queue_in.children.add(op)
        for out_queue in out_queue_map.values():
            op.children.add(out_queue)

        return (out_queue_map[value] for value in predicate_values)

    def merge(self, queues_in: Tuple[Queue], queue_out: Queue = None) -> Queue:
        if queue_out == None:
            queue_out = Queue(id=self.new_qid())

        op = MergeOp(queues_in, queue_out)
        self.ops.append(op)
        self.op_table[op] = {"queues_in": list(queues_in), "queues_out": [queue_out]}

        for in_queue in queues_in:
            in_queue.children.add(op)
        op.children.add(queue_out)

        return queue_out

    def batch(
        self,
        queue_in: Queue,
        queue_out: Queue = None,
        batch_size: int = 1,
        fill_batch: bool = False,
    ):
        if queue_out == None:
            queue_out = Queue(id=self.new_qid())

        op = BatchOp(queue_in, queue_out, batch_size, fill_batch)
        self.ops.append(op)
        self.op_table[op] = {"queues_in": [queue_in], "queues_out": [queue_out]}

        queue_in.children.add(op)
        op.children.add(queue_out)

        return queue_out

    def unbatch(self, queue_in: Queue, queue_out: Queue = None):
        if queue_out == None:
            queue_out = Queue(id=self.new_qid())

        op = UnbatchOp(queue_in, queue_out)
        self.ops.append(op)
        self.op_table[op] = {"queues_in": [queue_in], "queues_out": [queue_out]}

        queue_in.children.add(op)
        op.children.add(queue_out)

        return queue_out

    def filter(self, predicate_fn, queue_in: Queue, queue_out: Queue = None):
        if queue_out == None:
            queue_out = Queue(id=self.new_qid())

        op = FilterOp(predicate_fn, queue_in, queue_out)
        self.ops.append(op)
        self.op_table[op] = {"queues_in": [queue_in], "queues_out": [queue_out]}

        queue_in.children.add(op)
        op.children.add(queue_out)

        return queue_out

    def run(self):
        for task, queue_dict in self.task_table.items():
            queue_in = queue_dict["queue_in"]
            queue_out = queue_dict["queue_out"]

            # TODO: Rather than one pool per task, have one centrally managed pool
            process_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers=task.max_workers
            )

            self.pool_table[task] = process_pool

            for worker_id in range(task.max_workers):
                process_pool.submit(task.handle_task, worker_id, queue_in, queue_out)

        # TODO: Make this non-blocking in the main thread
        self.op_worker(self.ops)

    def op_worker(self, ops: List[Op]):
        """Run op loop on the pool so it doesn't block the main thread"""
        while len(ops):
            for op in ops:
                # Op node returns a shutdown signal when it handles the queue sentinel.
                shutdown = op.handle_op()

                if shutdown:
                    ops.remove(op)

    def shutdown(self):
        for pool in self.pool_table.values():
            pool.shutdown(wait=True)

        self.__init__()

    def graph_analyzer(self) -> None:
        """To be implemented. For now here is a running list of asserts we should make in the future
        * TODO: Assert each Op node is the only consumer for it's queue (they rely on q.empty())
        """
        pass

    def render_pipeline(
        self, filename="pipeline", root=None, skip_anon_ques=True
    ) -> None:
        """Render pipeline to png"""
        dot = Digraph("Pipeline", format="svg", graph_attr={"rankdir": "LR"})

        visited = set()
        q = deque()
        q.append(root)
        dot.node(name=str(root))
        visited.add(root)

        while len(q):
            node = q.popleft()
            for child in node.children:
                # To skip anonymous queues if the current node's child is a queue, we
                # want to draw an edge to the child of the queue, if any exist
                if (
                    skip_anon_ques
                    and child.type == NodeType.QUEUE
                    and len(child.children)
                ):
                    child = list(child.children)[0]

                if child.type == NodeType.OP:
                    attr = {
                        "shape": "rectangle",
                        "style": "filled",
                        "color": "lightgrey",
                    }
                elif child.type == NodeType.TASK:
                    attr = {"shape": "rectangle"}
                else:
                    attr = {}

                dot.node(str(child), **attr)
                dot.edge(str(node), str(child))
                if child not in visited:
                    visited.add(child)
                    q.append(child)

        dot.render(directory="graphviz", filename=filename)


_global_pipeline = PipelineManager()
