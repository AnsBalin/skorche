import logging
import pytest
import time

import skorche

def test_task_can_be_called_like_function():
    """Test that decorated functions can still be called like normal functions."""
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
    """Test default decorator gives a task name, or that task name can be provided."""

    @skorche.task
    def my_default_task():
        return True
    
    @skorche.task(name="Example task")
    def my_example_task():
        return True

    assert my_default_task.name == skorche.TASK_DEFAULT_NAME
    assert my_example_task.name == "Example task"

def test_global_pipeline_exists():
    """When skorche is imported a singleton pipeline manager is intantiated."""
    assert "_global_pipeline" in dir(skorche)

def test_render_pipeline():
    """Tests pipeline rendering using graphviz works as intended."""

    queue_in = skorche.Queue("inputs")
    queue_out = skorche.Queue("outputs")

    @skorche.task
    def add_one(x: float):
        return x + 1.0

    queue_out = skorche.map(add_one, queue_in, queue_out=queue_out)

    skorche._global_pipeline.render_pipeline()