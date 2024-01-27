import logging
import pytest
import time

from skorche import skorche

def test_task_can_be_called_like_function():
    """Test that decorated functions can still be called like normal functions"""
    @skorche.task
    def add_two(x: float):
        return x + 2.0 

    @skorche.task
    def square(x: float):
        return x * x 
    
    inputs = [ 1.0, 2.5, 3.5, 4.0, 10.1]

    for input in inputs:
        output = add_two(input)
        output = square(output)

        assert output == (input + 2.0)**2
    
def test_task_has_task_name():
    """Test default decorator gives a task name, or that task name can be provided"""

    @skorche.task
    def my_default_task():
        return True
    
    @skorche.task(task_name="Example task")
    def my_example_task():
        return True

    assert my_default_task.task_name == skorche.TASK_DEFAULT_NAME
    assert my_example_task.task_name == "Example task"
