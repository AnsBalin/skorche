from .constants import *
from .pipeline import _global_pipeline
from .queue import Queue
from .task import Task

from typing import Callable, List, Tuple

"""skorche API"""

def map(task: Task, queue_in: Queue, queue_out: Queue = Queue()) -> Queue:
    """Maps a task performing function over an input queue and binds it to an output queue"""
    queue_out = _global_pipeline.map(task, queue_in, queue_out=queue_out)
    return queue_out

def chain(task_list: List[Task], queue_in: Queue, queue_out: Queue = Queue()) -> Queue:
    """
    Chains together a list of tasks between an input queue and output queue.
    
    The list is left-associated, in other words [f, g, h] is interpreted h(g(f(input))).
    """
    queue_out = _global_pipeline.chain(task_list, queue_in, queue_out=queue_out)
    return queue_out

def split(predicate_fn: Callable, queue_in: Queue, predicate_values: Tuple = (True, False)) -> Tuple[Queue]:
    """
    Splits a queue by a predicate function. Each predicate value (by default: True, False) will 
    have associated with it an output queue. The user must ensure that predicate_fn will only
    ever return one of the predicate values.
    """
    queue_out_tuple = _global_pipeline.split(predicate_fn, queue_in, predicate_values)
    return queue_out_tuple
    

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

