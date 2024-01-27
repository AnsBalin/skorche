
import logging

TASK_DEFAULT_NAME = "skorche task" 

LOG_SENTINEL = "EOF"

class Task:
    """Base class for task"""

    def __init__(self, func, task_name=TASK_DEFAULT_NAME, max_workers=1, logger=logging.getLogger()):
        self.perform_task = func
        self.task_name = task_name
        self.max_workers = max_workers
        self.logger = logger 

    def __call__(self, *args, **kwargs):
        result = self.perform_task(*args, **kwargs)
        return result

    def handle_task(self, id, queue_in, queue_out):
        self.logger(f"Handling task {id}")

        sentinel_reached = False
        
        while not sentinel_reached:
            try:
                task = queue_in.get()

                if task is not None:
                    result = self.perform_task(task)
                    self.logger.info("Task complete")

            except Exception as e:
                self.logger.error("Task error")

            else:
                
                if task is not None:
                    queue_out.put(result)
            
            finally:
                queue_in.task_done()
                if task is None:
                    self.logger.info(LOG_SENTINEL)

                    sentinel_reached = True

def task(task_name=TASK_DEFAULT_NAME, max_workers=1, logger=logging.getLogger()):
    """
    @task decorator which wraps a user function into a Task instance.

    Usage:
    -Decorate with no arguments. Will instantiate Task() with default parameters.
        @task
        def my_fun():
            pass 

    -Decorate with arguments. Arguments are passed to Task() constructor
        @task(task_name="My Task", max_workers=2)
        def my_fun():
            pas

    """
    if callable(task_name):
        # pattern where user decorated function with @task

        func = task_name
        return Task(func)

    else:
        # pattern where user decorated with @task(task_name=...)

        def decorator(func):
            task_instance = Task(func, task_name, max_workers, logger)
            return task_instance
        return decorator

class Pipeline:
    def put():
        pass
