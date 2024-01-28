from .pipeline import _global_pipeline
from .queue import Queue
from .task import Task


"""skorche API"""

def map(func: Task, queue_in: Queue, queue_out: Queue):
    """Maps a task performing function"""
    queue_out = _global_pipeline.map(func, queue_in, queue_out=queue_out)
    return queue_out


