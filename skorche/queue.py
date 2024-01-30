from .constants import QUEUE_SENTINEL
from .node import NodeType, Node
from .resources import get_queue


class Queue(Node):
    """Wrapper interface for multiprocessing.Manager().Queue()"""
    def __init__(self, name="Queue", fixed_inputs= None):
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
        self.queue = get_queue()

        if fixed_inputs:

            for input in fixed_inputs:
                self.put(input)

            self.put(QUEUE_SENTINEL)

    def empty(self):
        return self.queue.empty()

    def put(self, item):
        self.queue.put(item)

    def get(self):
        return self.queue.get()

    def task_done(self):
        self.queue.task_done()  




