from .node import Node, NodeType
from .queue import Queue
from .task import Task

from graphviz import Digraph

class PipelineManager:
    """Manager for entire pipeline state"""

    def __init__(self):
        self.task_table = {}
        self.node_table = {}

    def map(self, task: Task, queue_in: Queue, queue_out=Queue()) -> Queue: 

        self.node_table[id(task)] = task
        self.node_table[id(queue_in)] = queue_in
        self.node_table[id(queue_out)] = queue_out

        self.task_table[id(task)] = {'queue_in': id(queue_in), 'queue_out': id(queue_out)}
        return queue_out

    def run(self):
        for (task, queue_dict) in self.task_table.items():
            task = self._get(task)
            queue_in = self._get(queue_dict['queue_in'])
            queue_out = self._get(queue_dict['queue_out'])
            while not queue_in.empty():
                x = queue_in.get()
                queue_out.put(task(x))

    def _get(self, id: int) -> Node:    
        """Return Node object from table"""
        return self.node_table[id]
    
    def render_pipeline(self) -> None:
        """Render pipeline to png"""
        dot = Digraph("Pipeline", format="svg", graph_attr={'rank_dir':'LR'})

        for (task, queues) in self.task_table.items():
            queue_in = queues['queue_in']
            queue_out = queues['queue_out']

            task_name = self._get(task).name
            queue_in_name = self._get(queue_in).name
            queue_out_name = self._get(queue_out).name
            dot.node(name=task_name)
            dot.node(name=queue_in_name)
            dot.node(name=queue_out_name)
            dot.edge(queue_in_name, task_name)
            dot.edge(task_name, queue_out_name )

        dot.render(directory="graphviz")
            

        


_global_pipeline = PipelineManager()


