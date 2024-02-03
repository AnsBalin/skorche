from .constants import *
from .node import NodeType, Node
from .queue import Queue
import logging

class Task(Node):
    """Base class for task"""

    def __init__(self, func, name=TASK_DEFAULT_NAME, max_workers=1, logger=logging.getLogger()):
        super().__init__(NodeType.TASK)
        self.perform_task = func
        self.name = name
        self.max_workers = max_workers

    def __call__(self, *args, **kwargs):
        result = self.perform_task(*args, **kwargs)
        return result

    def __str__(self):
       return self.name 

    def handle_task(self, worker_id: int, queue_in: Queue, queue_out: Queue):

        sentinel_reached = False
        
        while not sentinel_reached:
            try:
                task = queue_in.get()

                if task is not QUEUE_SENTINEL:
                    result = self.perform_task(task)

            except Exception as e:
                pass

            else:
                if task is not QUEUE_SENTINEL:
                    queue_out.put(result)
            
            finally:
                queue_in.task_done()
                if task is QUEUE_SENTINEL:
                    queue_out.put(QUEUE_SENTINEL)
                    sentinel_reached = True


def task(name=TASK_DEFAULT_NAME, max_workers=1, logger=logging.getLogger()):
    """
    @task decorator which wraps a user function into a Task instance.

    Usage:
    -Decorate with no arguments. Will instantiate Task() with default parameters.
        @task
        def my_fun():
            pass 

    -Decorate with arguments. Arguments are passed to Task() constructor
        @task(name="My Task", max_workers=2)
        def my_fun():
            pas

    """
    if callable(name):
        # pattern where user decorated function with @task

        func = name
        return Task(func, name=func.__name__)

    else:
        # pattern where user decorated with @task(name=...)

        def decorator(func):
            task_instance = Task(func, name, max_workers, logger)
            return task_instance
        return decorator
