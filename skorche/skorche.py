from .constants import *
from .pipeline import _global_pipeline
from .queue import Queue
from .task import Task

from typing import Callable, List, Tuple

"""skorche API"""

def map(task: Task, queue_in: Queue, queue_out: Queue = None) -> Queue:
    """Maps a task performing function over an input queue and binds it to an output queue"""
    queue_out = _global_pipeline.map(task, queue_in, queue_out=queue_out)
    return queue_out

def chain(task_list: List[Task], queue_in: Queue, queue_out: Queue = None) -> Queue:
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

def merge(queues_in: Tuple[Queue], queue_out: Queue = None) -> Queue:
    """
    Merges multiple queues into one.
    The order in which input queues are popped is not specified.
    """
    queue_out = _global_pipeline.merge(queues_in, queue_out=queue_out)
    return queue_out
    
def batch(queue_in: Queue, queue_out: Queue = None, batch_size: int = 1, fill_batch: bool = True) -> Queue:
    """
    Batches items from an input queue together and pushes a list to the output queue.

    Args:
        queue_in (:obj: Queue`): The input queue.
        queue_out (:obj:`Queue, optional): The output queue.
        batch_size (int, optional): Maximum number of task items to batch together. Default=1.
        fill_batch (bool, optional): if False, send a batch smaller than batch_size if queue is empty. Default = True.
    Returns:
        queue_out (:obj:`Queue`): The output queue.
    """

    queue_out = _global_pipeline.batch(queue_in, queue_out=queue_out, batch_size=batch_size, fill_batch=fill_batch)
    return queue_out

def unbatch(queue_in: Queue, queue_out: Queue = None) -> Queue:
    """Unbatches tasks in a queue.

    Args:
        queue_in (:obj: Queue`): The input queue.
        queue_out (:obj:`Queue, optional): The output queue.
    Returns:
        queue_out (:obj:`Queue`): The output queue.
    """
    queue_out = _global_pipeline.unbatch(queue_in, queue_out=queue_out)
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

