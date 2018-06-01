"""
MIT License

Copyright (c) 2017 AlessioNetti

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from collections import deque
from threading import Thread, Semaphore
from multiprocessing import Process


class AsyncWriter:
    """
    This class handles asynchronous IO of files, using a thread and queue-based implementation.
    """

    def __init__(self, path, pre_process_fun=None, buffer_size=10000):
        """
        Constructor for the class

        :param path: Path to the output file
        :param pre_process_fun: A pre-processing function for objects pushed to the queue. It MUST be a function that
            receives an object as input, and returns a string representation of it
        """
        self._path = path
        self._outfile = None
        self._toTerminate = False
        self._thread = None
        self._deque = deque()
        self._sem = Semaphore(value=0)
        self._buffer_size = buffer_size
        self._buf_counter = 0
        if pre_process_fun is None:
            self._pre_processor = AsyncWriter._dummy_pre_process
        else:
            self._pre_processor = pre_process_fun

    def push(self, data_obj):
        """
        Writes to an output file the data object specified as input asynchronously, after pre-processing it.

        :param data_obj: The object to be pre-processed to string format, and written to output
        """
        self._deque.append(data_obj)
        self._buf_counter += 1
        if self._buf_counter >= self._buffer_size:
            self._buf_counter = 0
            self._sem.release()

    def start(self):
        """
        Starts up the worker thread handling file IO
        """
        self._thread = Thread(target=self._working_loop)
        self._thread.start()

    def stop(self):
        """
        Stops the worker thread handling file IO
        """
        if self._thread is not None:
            self._toTerminate = True
            self._sem.release()
            self._thread.join()
            self._thread = None

        if self._outfile is not None:
            self._outfile.close()
            self._outfile = None
        self._deque.clear()

    def _working_loop(self):
        self._outfile = open(self._path, 'a')
        while not self._toTerminate or len(self._deque) > 0:
            self._sem.acquire()
            process = Process(target=self._flush_queue)
            process.start()
            process.join()
            counter_pop = 0
            while counter_pop < self._buffer_size and len(self._deque) > 0:
                self._deque.popleft()
                counter_pop += 1
        self._outfile.close()
        self._outfile = None

    def _flush_queue(self):
        counter_flushed = 0
        buffer = ''
        while counter_flushed < self._buffer_size and len(self._deque) > 0:
            entry = self._deque.popleft()
            str_out = self._pre_processor(entry)
            if isinstance(str_out, (list, tuple)) and len(str_out) > 1:
                for str_el in str_out:
                    buffer += str_el
            else:
                buffer += str_out
            counter_flushed += 1
        if len(buffer) > 0:
            self._outfile.write(buffer)

    @staticmethod
    def _dummy_pre_process(data_obj):
        return str(data_obj)
