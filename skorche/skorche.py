from .pipeline import _global_pipeline
from .queue import Queue
from .task import Task


"""skorche API"""

def map(task: Task, queue_in: Queue, queue_out: Queue = Queue()):
    """Maps a task performing function over an input queue and binds it to an output queue"""
    queue_out = _global_pipeline.map(task, queue_in, queue_out=queue_out)
    return queue_out


def run():
    """Run pipeline"""
    _global_pipeline.run()