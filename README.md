# _skorche_

_skorche_ is (or will be!) a lightweight python library for simple task orchestration and pipeline management. It provides a declarative API for constructing workflows and pipelines out of existing functions and allows different parts of the pipeline to operate asynchronously and scale independently.

```python
input_files = ['cat1.zip', 'cat2.zip', 'dog1.zip']

q_in = skorche.Queue(input_files)
q = skorche.chain([download_file, unzip_file], q_in)

(q_cats, q_dogs) = skorche.split(is_cat_or_dog, q)
q_cats = skorche.map(meow, q_cats)
q_dogs = skorche.map(woof, q_dogs)

q_out = skorche.merge([q_cats, q_dogs])

skorche.run()

while True:
    result = q_out.get()
```

## Features

- **Declarative API**: `skorche` provides an intuitive and straightforward API for defining pipelines.
- **Pipeline Semantics**: `map` tasks to queues, `chain` together multiple tasks, and `split` and `merge` pipelines to compose complex computational graphs.
- **Asynchronous Execution**: `skorche` manages thread and process pools allowing tasks to operate in parallel and scale.
- **Pipeline rendering**: Use `skorche`'s built-in graph renderer to visualise your pipelines.
- **Graph Analyzer**: (Planned) Profile your pipeline in realtime to identify hotspots or let `skorche` manage load balancing entirely.

## Example

Pipelines are constructed from instances of `Task`, `Queue` and `Op`. In the following example we will build a pipeline for processing images and documents.

### Tasks

Decorate existing functions with `@skorche.task` to turn them into `Task` instances:

```python
@skorche.task(max_workers=4)
def download_file(fname):
    pass

@skorche.task
def unzip_file(fname):
    pass

@skorche.task
def process_images(fname_list):
    # Processes multiple images in one batch
    pass

@skorche.task
def process_doc(fname):
    pass
```

### Queues

A pipeline can be thought of as a computational graph in which each `Queue` is a directed edges connecting nodes, and where each node is a `Tasks` or `Op`. First, instantiate a `Queue` which will act as the input into the whole system:

```python
input_files = ['file1.zip', 'file2.zip', 'file3.zip']
queue_in = skorche.Queue(input_files)
```

### `map` and `chain`

The simplest example of a pipeline consists of a function that transforms some input queue into an output queue. `skorche` provides this as a `map`:

```python
queue_out = skorche.map(download_file, queue_in)
```

This line is a purely declarative statement that tells `skorche` to bind the task to the input and output queues. Actual execution of this task is deferred until later.

In this case, we have a series of composable functions `download_file`, `unzip_file`, so we could write out a series of map bindings:

```python
q_zipped = skorch.map(download_file, queue_in)
q_unzipped = skorch.map(unzip_file, q_zipped)
```

`skorche` provides the `chain` function to make this simpler:

```python
q_unzipped = skorch.chain([download_file, unzip_file], queue_in)
```

### `split`

We now have a queue of unzipped folders, each of which either contains an image or a doc, but we have separate functions for processing these: `process_image` and `process_doc`. In this case, we want to split the pipeline:

```python
def image_or_doc(fname):
    # Returns True if fname is an image or False if doc
    pass

(q_img, q_doc) = skorche.split(image_or_doc, q_in)
```

We now have two queues which are ready to be mapped over by different tasks.

### `batch`

`process_image` defined above expects a batch of images to process in one go. `skorche` achieves this like so:

```python
q_img_batch = skorche.batch(q_img, batch_size=10)
```

### `filter`

Maybe some documents are irrelevant to us and we need not process them. `skorche` provides a `filter` to remove these from the pipeline.

```python
q_doc_filtered = skorche.filter(filter_fn, q_doc)
```

We are now ready to map over these two queues:

```python
q_doc_out = skorche.map(process_doc, q_doc_filtered)
q_img_out = skorche.map(process_images, q_img_batch)
q_img_out = skorche.unbatch(q_img_out)
```

and finally merge the two queues back again:

```python
q_out = skorche.merge((q_img_out, q_doc_out))
```

### Pipeline rendering

All we have done so far is declare our pipeline. None of the tasks have executed any code yet, but `skorche` has built the pipeline, and can render it:

```python
skorche.render_pipeline()
```

### `run`

Now time to run it:

```python
skorche.run()
```

### Putting this together

Our complete program looks like this:

```python
import skorche

@skorche.task(max_workers=4)
def download_file(fname):
    pass

@skorche.task
def unzip_file(fname):
    pass

@skorche.task
def process_images(fname_list):
    # Processes multiple images in one batch
    pass

@skorche.task
def process_doc(fname):
    pass


input_files = ['file1.zip', 'file2.zip', 'file3.zip']
queue_in = skorche.Queue(input_files)

q_unzipped = skorche.chain([download_file, unzip_file], queue_in)

(q_img, q_doc) = skorche.split(image_or_doc, q_unzipped)
q_img_batch = skorche.batch(q_img, batch_size=10)
q_doc_filtered = skorche.filter(filter_fn, q_doc)
q_doc_out = skorche.map(process_doc, q_doc_filtered)
q_img_out = skorche.map(process_images, q_img_batch)
q_img_out = skorche.unbatch(q_img_out)
q_out = skorche.merge((q_img_out, q_doc_out))
skorche.render_pipeline()
skorche.run()
```
