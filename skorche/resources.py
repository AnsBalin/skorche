from multiprocessing import Manager

"""
Global resource for acquiring a multiprocessing queue.
TODO: migrate this somewhere where it need not be global
"""
_mp_manager = Manager()


def get_queue():
    return _mp_manager.Queue()
