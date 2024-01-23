import time
import pytest

from skorch import skorch


def test_skorch(capsys):
    @skorch.task
    def my_task():
        time.sleep(1)
        print("Task done")

    skorch.run(my_task)
    captured = capsys.readouterr()
    assert captured.out == "Task done\n"


def test_skorch_pipline(capsys):
    @skorch.task
    def task_one(n: int) -> int:
        return n + 1

    @skorch.task
    def task_two(n: int) -> int:
        return n * n

    pipeline = skorch.Pipeline()
    pipeline.put(task_one)
    pipeline.put(task_two)

    queue = skorch.Queue()
    queue.put(1)
    queue.put(2)

    pipeline.run(queue)
