import functools
import logging
import pytest
import time

import multiprocessing

import skorche


@pytest.fixture(autouse=True)
def skorche_init():
    skorche.init()
    yield




def test_task_can_be_called_like_function():
    """Test that decorated functions can still be called like normal functions."""

    @skorche.task
    def add_two(x: float):
        return x + 2.0

    @skorche.task
    def square(x: float):
        return x * x

    inputs = [1.0, 2.5, 3.5, 4.0, 10.1]

    for input in inputs:
        output = add_two(input)
        output = square(output)

        assert output == (input + 2.0) ** 2


def test_task_has_task_name():
    """Test default decorator gives a task name, or that task name can be provided."""

    @skorche.task
    def my_func():
        return True

    @skorche.task(max_workers=5)
    def default_task_name():
        return True

    @skorche.task(name="Example task")
    def my_example_task():
        return True

    assert my_func.name == "my_func"
    assert default_task_name.name == skorche.TASK_DEFAULT_NAME
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
    skorche._global_pipeline.render_pipeline(filename="test_render", root=queue_in)


def test_map_and_run():
    """Creates a simple pipeline consisting of a function mapped to an input queue"""

    @skorche.task
    def add_two(x: int):
        return x + 2

    inputs = [1, 2, 3, 12, 99, -1]
    expected = [x + 2 for x in inputs]
    queue_in = skorche.Queue("inputs")
    queue_out = skorche.map(add_two, queue_in)

    skorche.run()

    for i in inputs:
        queue_in.put(i)
    queue_in.put(skorche.QUEUE_SENTINEL)

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

    skorche.run()
    skorche.shutdown()

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
    expected = [(2 * (x + 1)) ** 2 for x in inputs]

    queue_in = skorche.Queue("inputs")
    skorche.push_to_queue(inputs, queue_in)

    queue_out = skorche.chain([add_one, multiply_two, square], queue_in)
    skorche._global_pipeline.render_pipeline(filename="chain", root=queue_in)
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

    skorche.shutdown()

    pos_out = q_pos.flush()
    neg_out = q_neg.flush()

    assert pos_out == [1, 4, 7]
    assert neg_out == [-2, -1]


def test_merge():
    """Merge two queues into one"""

    q1 = skorche.Queue(fixed_inputs=[1, 3, 5, 7])
    q2 = skorche.Queue(fixed_inputs=[0, 2, 4, 6])

    q_out = skorche.merge((q1, q2))

    skorche.run()

    skorche.shutdown()
    results = []

    results = q_out.flush()

    assert all([i in results for i in range(8)])


def test_batch():
    """Test for skorche.batch()"""

    # Each item in q will be an int
    q = skorche.Queue(fixed_inputs=list(range(15)))
    expected = [
        list(range(0, 4)),
        list(range(4, 8)),
        list(range(8, 12)),
        list(range(12, 15)),  # last one will be length 3
    ]

    # Each item in q_out will be list of ints with max size 4
    q_out = skorche.batch(q, batch_size=4)

    skorche.run()
    skorche.shutdown()

    batched = q_out.flush()

    assert batched == expected


def test_fill_batch_false():
    """
    Tests that with fill_batch=False, skorche.batch() will not wait for full buffer

    put_four_at_a_time will ensure with high likelihood the queue size remains
    less than four, so expect batch to just collect whatever is there.

    TODO: Make this test a little less brittle. It assumes the queue will be
    flushed every 0.1s which is only true most of the time
    """

    @skorche.task
    def put_four_at_a_time(i: int):
        """force task to sleep every 4 items"""
        if (i) % 4 == 0:
            time.sleep(0.1)

        return i

    q = skorche.Queue(fixed_inputs=list(range(10)))
    q = skorche.map(put_four_at_a_time, q)
    q_out = skorche.batch(q, batch_size=10, fill_batch=False)

    skorche.run()
    skorche.shutdown()

    results = q_out.flush()

    flat_results = functools.reduce(lambda x, y: x + y, results, [])
    assert all([result in list(range(10)) for result in flat_results])
    assert all([len(result) <= 4 for result in results])


def test_batch_and_unbatch():
    """Tests that batched tasks can be unbatched"""

    init_list = list(range(20))
    q = skorche.Queue(fixed_inputs=init_list)
    q = skorche.batch(q, batch_size=7)
    q_out = skorche.unbatch(q)

    skorche.run()
    skorche.shutdown()

    results = q_out.flush()
    assert results == init_list


def test_filter():
    """Test for skorche.filter()"""

    def is_positive(x: int) -> bool:
        return x > 0

    q = skorche.Queue(fixed_inputs=[-1, 1, 2, -4, 1, 9, -2, -3])
    q = skorche.filter(is_positive, q)

    skorche.run()
    skorche.shutdown()

    results = q.flush()
    assert [1, 2, 1, 9] == results


def test_end_to_end():
    """
    End to end test for multistage pipeline
    """

    inputs = [1, 4, -2, -4, 5, -1, 3, 9, 12, -7, -6, 4, -3, 1, 4, 7, 1]

    @skorche.task
    def add_one(x: int):
        return x + 1

    @skorche.task
    def multiply_two(x: int):
        return 2 * x

    def is_positive(x: int):
        return x > 0

    @skorche.task
    def square(x: int):
        return x * x

    @skorche.task
    def add_three(x: int):
        return x + 3

    q_in = skorche.Queue(name="inputs", fixed_inputs=inputs)
    q = skorche.chain([add_one, multiply_two], q_in)

    q_pos, q_neg = skorche.split(is_positive, q)

    q_pos = skorche.map(square, q_pos)
    q_neg = skorche.map(add_three, q_neg)
    q_out = skorche.merge((q_pos, q_neg))
    q_out.nameit(name="outputs")

    skorche._global_pipeline.render_pipeline(filename="e2e", root=q_in)

    skorche.run()
    skorche.shutdown()

    results = q_out.flush()

    # Generate expected results
    expected = []
    for input in inputs:
        input = add_one(input)
        input = multiply_two(input)

        if is_positive(input):
            expected.append(square(input))
        else:
            expected.append(add_three(input))

    assert all([result in expected for result in results])
