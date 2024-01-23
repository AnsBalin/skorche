
import logging

class Task:
    """Base class for task"""

    def __init__(self, task_name, max_workers, logger):
        self.task_name = task_name
        self.max_workers = max_workers
        self.logger = logger 

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
                    self.logger.info(f"EOF")

                    sentinel_reached = True

    def perform_task(self, task):
        pass


def task(func, task_name = "Task", max_workers = 1, logger = logging.getLogger()):


    def wrapper(*args, **kwargs):
        _task = Task(task_name=task_name, max_workers=max_workers, logger=logger)
        func(*args, **kwargs)

    return wrapper



def run(task_func, *args, **kwargs):
    task_func(*args, **kwargs)


class Pipeline:
    def put()