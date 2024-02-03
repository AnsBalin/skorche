from .constants import QUEUE_SENTINEL
from .node import NodeType, Node
from .resources import get_queue


class Queue(Node):
    """Wrapper interface for multiprocessing.Manager().Queue()"""
    def __init__(self, name="Queue", id=None, fixed_inputs= None):
        """Constructs a Queue instance
        
        Args:
            name (string): Optional name for queue
            fixed_inputs (List): Optional list of task items to enqueue. 
                It is 'fixed' meaning QUEUE_SENTINEL will be enqued 
                at the end, terminating the queue. To enque a list without
                the sentinel, use skorche.push_to_queue instead.
        """
        super().__init__(NodeType.QUEUE)
        self.name = name
        self.id = id
        self.queue = get_queue()

        if fixed_inputs:

            for input in fixed_inputs:
                self.put(input)

            self.put(QUEUE_SENTINEL)
    
    def __str__(self):
        if self.id:
            return f"{self.name} {self.id}"
        return self.name

    # ---- Queue interface BEGIN
    def empty(self):
        return self.queue.empty()

    def put(self, item):
        self.queue.put(item)

    def get(self):  
        # TODO: handle self.queue.task_done() here so we dont have to everywhere else
        return self.queue.get()

    def task_done(self):
        self.queue.task_done()  
    # ---- Queue interface END

    def nameit(self, name: str = "Queue", id: int = None):
        """
        Gives the queue a user specified name.
        
        Warning: this is a helper function to be used outside the context
        of skorche.run(). Call it before rendering the pipeline so the 
        output queue isn't anonymously skipped
        """
        self.name = name
        self.id = id

    def flush(self) -> list:
        """
        Flushes queue into a list excluding any sentinels.
        
        Warning: This is a helper function intended to be used outside 
        the context of skorche.run() and it is expected that the queue
        is non-empty and sentinel-terminated. Otherwise it will block.
        """
        buffer = []
        while not self.queue.empty():
            task_item = self.queue.get()
            self.queue.task_done()
            
            if task_item == QUEUE_SENTINEL:
                break
            
            else:
                buffer.append(task_item)

        return buffer

    




