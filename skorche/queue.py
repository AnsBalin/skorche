from .node import NodeType, Node

class Queue(Node):
    """Wrapper interface for multiprocessing.Manager().Queue()"""
    def __init__(self, name="Queue"):
        super().__init__(NodeType.QUEUE)
        self.name = name


