"""
"""

import math
import multiprocessing


class Workgroup(object):

    def __init__(self, size, on_task):
        self._size = size
        self._on_task = on_task
        self._processes = []

    def work(self, context, start, end):
        if len(self._processes) != 0:
            return None
        n = math.ceil((end - start) / self._size)
        queue = multiprocessing.Queue()
        for i in range(self._size):
            _start = start + i * n
            _end = start + (i + 1) * n
            if _end > end:
                _end = end
            if _start >= _end:
                break
            p = multiprocessing.Process(
                target=self._on_task,
                args=(i, queue, context, _start, _end)
            )
            p.start()
            self._processes.append(p)
        return queue

    def wait(self):
        for p in self._processes:
            p.join()
        self._processes = []
