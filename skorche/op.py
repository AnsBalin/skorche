from .constants import QUEUE_SENTINEL
from .node import Node, NodeType
from .queue import Queue

from typing import Callable, Dict, Tuple


class Op(Node):
    """Op node base class"""

    def __init__(self):
        super().__init__(NodeType.OP)


class SplitOp(Op):
    def __init__(self, predicate_fn: Callable, queue_in: Queue, queue_out_dict: Dict):
        """Op node for splitting a queue based on a predicate function"""
        super().__init__()

        self.predicate_fn = predicate_fn
        self.queue_in = queue_in
        self.queue_out_dict = queue_out_dict
        self.shutdown = False

    def __str__(self):
        return f"SplitOp({self.predicate_fn.__name__})"

    def handle_op(self):
        """
        Gets task item, evaluates a predicate function, and pushes item
        on to appropriate output queue.
        """

        if not self.queue_in.empty():
            task_item = self.queue_in.get()
            self.queue_in.task_done()

            if task_item == QUEUE_SENTINEL:
                # push sentinel to output queues and set shutdown flag
                self.handle_sentinel()

            else:
                predicate_value = self.predicate_fn(task_item)
                queue_to_push = self.queue_out_dict[predicate_value]
                queue_to_push.put(task_item)

        return self.shutdown

    def handle_sentinel(self):
        """Push the sentinel to all consumers"""

        for queue_out in self.queue_out_dict.values():
            queue_out.put(QUEUE_SENTINEL)

        self.shutdown = True


class MergeOp(Op):
    def __init__(self, queues_in: Tuple[Queue], queue_out: Queue):
        """Op node for merging a number of input queues"""
        super().__init__()
        self.queues_in = queues_in
        self.queue_out = queue_out
        self.shutdown = False

        # for N input queues, expect N sentinels, but only push sentinel
        # to output when N sentinels have been reached
        self.sentinels_reached = 0
        self.sentinels_expected = len(self.queues_in)

    def __str__(self):
        return "MergeOp"

    def handle_op(self):
        """
        Pops a value from any input queue and pushes to the output queue
        """
        for q_in in self.queues_in:
            if not q_in.empty():
                task_item = q_in.get()
                q_in.task_done()

                if task_item == QUEUE_SENTINEL:
                    self.handle_sentinel()

                else:
                    self.queue_out.put(task_item)

        return self.shutdown

    def handle_sentinel(self):
        """if expected number of sentinels have been encountered, push sentinel to output"""

        self.sentinels_reached += 1
        if self.sentinels_reached == self.sentinels_expected:
            self.queue_out.put(QUEUE_SENTINEL)

            self.shutdown = True


class BatchOp(Op):
    def __init__(
        self, queue_in: Queue, queue_out: Queue, batch_size: int, fill_batch: bool
    ):
        """Op node for batching"""
        super().__init__()
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.batch_size = batch_size
        self.fill_batch = fill_batch

        # Buffer for collecting tasks
        self.buffer = []

        self.shutdown = False

    def __str__(self):
        return f"BatchOp(batch_size={self.batch_size})"

    def handle_op(self):
        """
        Handles task batching
        """
        while not self.queue_in.empty():
            task_item = self.queue_in.get()
            self.queue_in.task_done()

            if task_item == QUEUE_SENTINEL:
                # send whatever is currently in buffer as a batch
                self.send_batch()

                # send sentinel value
                self.queue_out.put(QUEUE_SENTINEL)
                self.shutdown = True
                break

            else:
                # Add task to buffer and send if batch_size reached
                self.buffer.append(task_item)
                if len(self.buffer) == self.batch_size:
                    self.send_batch()

        # send whatever else is left in the buffer
        if not self.fill_batch and len(self.buffer):
            self.send_batch()

        return self.shutdown

    def send_batch(self):
        """Sends buffer into output queue and clear buffer"""
        self.queue_out.put(self.buffer)
        self.buffer = []


class UnbatchOp(Op):
    def __init__(self, queue_in: Queue, queue_out: Queue):
        """Op node for unbatching"""
        super().__init__()
        self.queue_in = queue_in
        self.queue_out = queue_out

        self.shutdown = False

    def __str__(self):
        return "UnbatchOp"

    def handle_op(self):
        """
        Handles task unbatching
        """

        while not self.queue_in.empty():
            task_batch = self.queue_in.get()
            self.queue_in.task_done()

            if task_batch == QUEUE_SENTINEL:
                # send sentinel value
                self.queue_out.put(QUEUE_SENTINEL)
                self.shutdown = True
                break

            else:
                for task_item in task_batch:
                    self.queue_out.put(task_item)

        return self.shutdown


class FilterOp(Op):
    def __init__(self, predicate_fn: Callable, queue_in: Queue, queue_out: Queue):
        """Op node for unbatching"""
        super().__init__()
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.predicate_fn = predicate_fn

        self.shutdown = False

    def __str__(self):
        return "FilterOp"

    def handle_op(self):
        """
        Handles task unbatching
        """

        while not self.queue_in.empty():
            task_item = self.queue_in.get()
            self.queue_in.task_done()

            if task_item == QUEUE_SENTINEL:
                # send sentinel value
                self.queue_out.put(QUEUE_SENTINEL)
                self.shutdown = True
                break

            else:
                if self.predicate_fn(task_item):
                    self.queue_out.put(task_item)

        return self.shutdown
