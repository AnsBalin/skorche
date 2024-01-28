from .node import NodeType, Node
from .resources import get_queue

class Queue(Node):
    """Wrapper interface for multiprocessing.Manager().Queue()"""
    def __init__(self, name="Queue"):
        super().__init__(NodeType.QUEUE)
        self.name = name
        self.queue = get_queue()

    def empty(self):
        return self.queue.empty()

    def put(self, item):
        self.queue.put(item)

    def get(self):
        return self.queue.get()




