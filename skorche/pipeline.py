# package imports
from .node import Node, NodeType
from .op import SplitOp, MergeOp, BatchOp, UnbatchOp, Op
from .queue import Queue
from .task import Task


# standard library imports
import concurrent.futures
from typing import Callable, List, Tuple

# dependency imports
from graphviz import Digraph

class PipelineManager:
    """Manager for entire pipeline state"""

    def __init__(self):
        self.task_table = {}
        self.ops = []
        self.node_table = {}
        self.pool_table = {}

    def map(self, task: Task, queue_in: Queue, queue_out=Queue()) -> Queue: 
        #TODO: just use the objects are keys
        self.node_table[id(task)] = task
        self.node_table[id(queue_in)] = queue_in
        self.node_table[id(queue_out)] = queue_out

        self.task_table[id(task)] = {'queue_in': id(queue_in), 'queue_out': id(queue_out)}
        return queue_out


    def chain(self, task_list: List[Task], queue_in: Queue, queue_out:Queue = Queue() ) -> Queue:

        if len(task_list) == 1:
            self.map(task_list[0], queue_in, queue_out)
            return

        # Construct intermediate queue to join between chained tasks 
        queue_out = Queue()
        queue_out = self.map(task_list[0], queue_in, queue_out)

        for task in task_list[1:]:
            queue_out = self.map(task, queue_out, Queue())

        return queue_out

    def split(self, predicate_fn: Callable, queue_in: Queue, predicate_values: Tuple = (True, False)) -> Tuple[Queue]:

        out_queue_map = {value: Queue() for value in predicate_values}
        op = SplitOp(predicate_fn, queue_in, out_queue_map)
        self.ops.append(op)

        return (out_queue_map[value] for value in predicate_values)

    def merge(self, queues_in: Tuple[Queue], queue_out: Queue = Queue()) -> Queue:

        op = MergeOp(queues_in, queue_out)
        self.ops.append(op)

        return queue_out

    def batch(self, queue_in: Queue, queue_out: Queue = Queue(), batch_size: int = 1, fill_batch: bool = False):
        
        op = BatchOp(queue_in, queue_out, batch_size, fill_batch)
        self.ops.append(op)

        return queue_out

    def unbatch(self, queue_in: Queue, queue_out: Queue = Queue()):
        op = UnbatchOp(queue_in, queue_out)
        self.ops.append(op)

        return queue_out

    def run(self):

        for (task_id, queue_dict) in self.task_table.items():
            task = self._get(task_id)
            queue_in = self._get(queue_dict['queue_in'])
            queue_out = self._get(queue_dict['queue_out'])

            #TODO: Rather than one pool per task, have one centrally managed pool
            process_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers = task.max_workers
            )

            self.pool_table[task_id] = process_pool

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


    def _get(self, id: int) -> Node:    
        """Return Node object from table"""
        return self.node_table[id]

    def graph_analyzer(self) -> None:
        """To be implemented. For now here is a running list of asserts we should make in the future
            * TODO: Assert each Op node is the only consumer for it's queue (they rely on q.empty())
        """
        pass
    
    def render_pipeline(self) -> None:
        """Render pipeline to png"""
        dot = Digraph("Pipeline", format="svg", graph_attr={'rank_dir':'LR'})

        for (task, queues) in self.task_table.items():
            queue_in = queues['queue_in']
            queue_out = queues['queue_out']

            task_name = f"{self._get(task).name} {task}"
            queue_in_name = f"{self._get(queue_in).name} {queue_in}"
            queue_out_name = f"{self._get(queue_out).name} {queue_out}"
            dot.node(name=task_name)
            dot.node(name=queue_in_name)
            dot.node(name=queue_out_name)
            dot.edge(queue_in_name, task_name)
            dot.edge(task_name, queue_out_name )

        dot.render(directory="graphviz")
            

        


_global_pipeline = PipelineManager()


