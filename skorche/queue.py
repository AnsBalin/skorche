from .node import NodeType, Node

class Queue(Node):
    """Wrapper interface for multiprocessing.Manager().Queue()"""
    def __init__(self, name="Queue"):
        super().__init__(NodeType.QUEUE)
        self.name = name

        # TODO: replace temporary list implementation of queue.
        self.queue = []
        self.qptr = 0

    def empty(self):
        return self.qptr >= len(self.queue)  

    def put(self, item):
        self.queue.append(item)

    def get(self):
        if not self.empty():
            item = self.queue[self.qptr]
            self.qptr += 1
            return item
        else:
            raise Exception("Cannot get item from empty queue") 




