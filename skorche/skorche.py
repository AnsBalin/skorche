from .constants import *
from .pipeline import _global_pipeline
from .queue import Queue
from .task import Task

from typing import List

"""skorche API"""

def map(task: Task, queue_in: Queue, queue_out: Queue = Queue()):
    """Maps a task performing function over an input queue and binds it to an output queue"""
    queue_out = _global_pipeline.map(task, queue_in, queue_out=queue_out)
    return queue_out

def chain(task_list: List[Task], queue_in: Queue, queue_out: Queue = Queue()):
    """
    Chains together a list of tasks between an input queue and output queue.
    
    The list is left-associated, in other words [f, g, h] is interpreted h(g(f(input))).
    """
    queue_out = _global_pipeline.chain(task_list, queue_in, queue_out=queue_out)
    return queue_out


def run():
    """Run pipeline"""
    _global_pipeline.run()

def shutdown():
    """Shutdown pipeline"""
    _global_pipeline.shutdown()

def push_to_queue(task_list: list, queue: Queue):
    for task in task_list:
        queue.put(task)
    queue.put(QUEUE_SENTINEL)


def init():
    _global_pipeline.__init__()

