import logging
import pytest
import time

import skorche

@pytest.fixture(autouse=True)
def skorche_init():
    skorche.init()
    time.sleep(0.0)
    yield
    

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

def test_map_and_run():
    """Creates a simple pipeline consisting of a function mapped to an input queue"""

    @skorche.task
    def add_two(x: int):
        return x + 2

    inputs = [1, 2, 3, 12, 99, -1]
    expected = [x+2 for x in inputs]
    queue_in = skorche.Queue("inputs")
    queue_out = skorche.map(add_two, queue_in)

    for i in inputs:
        queue_in.put(i)
    queue_in.put(skorche.QUEUE_SENTINEL)

    skorche.run()

    skorche.shutdown()
    results = queue_out.flush()

    assert results == expected

def test_init_fixed_queue():
    """Test queue with fixed_inputs is initialized correctly"""
    q = skorche.Queue(fixed_inputs=[1, 2, 3])
    assert [q.get() for _ in range(4)] == [1, 2, 3, skorche.QUEUE_SENTINEL]
    

def test_push_to_queue():
    """Unit test for skorche.push_to_queue()"""

    inputs = [1, 5, 6, 7]
    expected = inputs

    queue = skorche.Queue()
    skorche.push_to_queue(inputs, queue)

    results = queue.flush()

    assert results == expected

def test_chain():
    """Creates a chained pipeline of three tasks"""

    @skorche.task(name="add_one")
    def add_one(x: int):
        return x + 1
    
    @skorche.task(name="multiply_two")
    def multiply_two(x: int):
        return 2 * x

    @skorche.task(name="square")
    def square(x: int):
        return x * x

    inputs = [1, 5, -2, 12, 100]
    expected = [(2 * (x + 1))**2 for x in inputs]

    queue_in = skorche.Queue("inputs")
    skorche.push_to_queue(inputs, queue_in)

    queue_out = skorche.chain([add_one, multiply_two, square], queue_in)
    skorche._global_pipeline.render_pipeline()
    skorche.run()
    skorche.shutdown()

    results = queue_out.flush()

    assert results == expected

def test_split():
    """Split a queue and test task items exist and are not duplicated"""

    def predicate_fn(x):
        return x > 0

    q = skorche.Queue(fixed_inputs=[-2, 1, 4, -1, 7])

    (q_pos, q_neg) = skorche.split(predicate_fn, q)


    skorche.run()

    pos_out = q_pos.flush()
    neg_out = q_neg.flush()

    assert pos_out == [1, 4, 7]
    assert neg_out == [-2, -1]

def test_merge():
    """Merge two queues into one"""

    q1 = skorche.Queue(fixed_inputs=[1,3,5,7])
    q2 = skorche.Queue(fixed_inputs=[0,2,4,6])
    
    q_out = skorche.merge((q1, q2))

    skorche.run()

    results = []

    results = q_out.flush()

    assert all([i in results for i in range(8)])

    