# package imports
from .node import Node, NodeType
from .queue import Queue
from .task import Task

# standard library imports
import concurrent.futures
from typing import List

# dependency imports
from graphviz import Digraph

class PipelineManager:
    """Manager for entire pipeline state"""

    def __init__(self):
        self.task_table = {}
        self.node_table = {}
        self.pool_table = {}

    def map(self, task: Task, queue_in: Queue, queue_out=Queue()) -> Queue: 

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
            

    def run(self):

        for (task_id, queue_dict) in self.task_table.items():
            task = self._get(task_id)
            queue_in = self._get(queue_dict['queue_in'])
            queue_out = self._get(queue_dict['queue_out'])
            
            process_pool = concurrent.futures.ThreadPoolExecutor(
                max_workers = task.max_workers
            )

            self.pool_table[task_id] = process_pool

            for worker_id in range(task.max_workers):
                process_pool.submit(task.handle_task, worker_id, queue_in, queue_out)
        
    def shutdown(self):
        for pool in self.pool_table.values():
            pool.shutdown(wait=True)

        self.__init__()


    def _get(self, id: int) -> Node:    
        """Return Node object from table"""
        return self.node_table[id]
    
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


