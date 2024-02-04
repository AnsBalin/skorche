from enum import Enum


class NodeType(Enum):
    TASK = "Task"
    QUEUE = "Queue"
    OP = "Op"


class Node:
    def __init__(self, type: NodeType):
        self.type = type

        self.children = set()
