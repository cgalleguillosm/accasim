from collections import deque
from threading import Thread, Semaphore


class async_writer:
    """
    This class handles asynchronous IO of files, using a thread and queue-based implementation.
    """

    def __init__(self, path, pre_process_fun=None):
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
        if pre_process_fun is None:
            self._pre_processor = async_writer._dummy_pre_process
        else:
            self._pre_processor = pre_process_fun

    def push(self, data_obj):
        """
        Writes to an output file the data object specified as input asynchronously, after pre-processing it.

        :param data_obj: The object to be pre-processed to string format, and written to output
        """
        self._deque.append(data_obj)
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
            if len(self._deque) > 0:
                entry = self._deque.popleft()
                str_out = self._pre_processor(entry)
                if isinstance(str_out, (list, tuple)) and len(str_out) > 1:
                    for str_el in str_out:
                        self._outfile.write(str_el)
                else:
                    self._outfile.write(str_out)
        self._outfile.close()
        self._outfile = None

    @staticmethod
    def _dummy_pre_process(data_obj):
        return str(data_obj)
