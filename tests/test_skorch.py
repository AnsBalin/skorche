import logging
import pytest
import time

from skorch import skorch

def test_task_can_be_called_like_function():
    """Test that decorated functions can still be called like normal functions"""
    @skorch.task
    def add_two(x: float):
        return x + 2.0 

    @skorch.task
    def square(x: float):
        return x * x 
    
    inputs = [ 1.0, 2.5, 3.5, 4.0, 10.1]

    for input in inputs:
        output = add_two(input)
        output = square(output)

        assert output == (input + 2.0)**2
    
def test_task_has_task_name():
    """Test default decorator gives a task name, or that task name can be provided"""

    @skorch.task
    def my_default_task():
        return True
    
    @skorch.task(task_name="Example task")
    def my_example_task():
        return True

    assert my_default_task.task_name == skorch.TASK_DEFAULT_NAME
    assert my_example_task.task_name == "Example task"

