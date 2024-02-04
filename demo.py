import skorche
import multiprocessing


@skorche.task
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


def is_image(fname):
    pass


def filter_fn(fname):
    pass


if __name__ == "__main__":
    # Initialises multiprocessing manager
    skorche.init()
    input_files = ["file1.zip", "file2.zip", "file3.zip"]

    # TODO: Make it so we dont need to do this
    mp_manager = multiprocessing.Manager()

    # input and output queues
    queue_in = skorche.Queue(
        name="inputs", fixed_inputs=input_files, mp_manager=mp_manager
    )
    queue_out = skorche.Queue(name="outputs", mp_manager=mp_manager)

    # chain together two tasks sequentially
    q_unzipped = skorche.chain([download_file, unzip_file], queue_in)

    # split based on file type
    (q_img, q_doc) = skorche.split(is_image, q_unzipped)

    # image processing branch
    q_img_batch = skorche.batch(q_img, batch_size=10)
    q_img_out = skorche.map(process_images, q_img_batch)
    q_img_out = skorche.unbatch(q_img_out)

    # doc processing branch
    q_doc_filtered = skorche.filter(filter_fn, q_doc)
    q_doc_out = skorche.map(process_doc, q_doc_filtered)

    # merge branches to output queue
    queue_out = skorche.merge((q_img_out, q_doc_out), queue_out=queue_out)

    # Visualise pipeline. Writes to ./graphviz/demo.svg
    skorche.render_pipeline(filename="demo", root=queue_in)

    # Run pipeline
    # skorche.run()
