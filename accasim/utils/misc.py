"""
MIT License

Copyright (c) 2017 cgalleguillosm, AlessioNetti

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
import re
import os
import logging
import json
from datetime import datetime
import time
from collections import Mapping
from bisect import bisect, bisect_left, bisect_right
import socket
import threading
from _functools import reduce
from itertools import islice
from builtins import int, str
from weakref import WeakValueDictionary as _WeakValueDictionary
from inspect import getmembers as _getmembers, isclass as _isclass
from sys import modules as _modules

# ===============================================================================
# Patterns for SWF files
# ===============================================================================
_swf_int_pattern = ('\s*(?P<{}>[-+]?\d+)', int)
_swf_float_pattern = ('\s*(?P<{}>[-+]?\d+\.\d+|[-+]?\d+)', float)
_swf_avoid_regexps = [r'^;.*']
default_swf_parse_config = (
    {
        'job_number': _swf_int_pattern,
        'queued_time': _swf_int_pattern,
        'wait_time': _swf_int_pattern,
        'duration': _swf_int_pattern,
        'allocated_processors': _swf_int_pattern,
        'avg_cpu_time': _swf_float_pattern,
        'used_memory': _swf_int_pattern,
        'requested_number_processors': _swf_int_pattern,
        'requested_time': _swf_int_pattern,
        'requested_memory': _swf_int_pattern,
        'status': _swf_int_pattern,
        'user_id': _swf_int_pattern,
        'group_id': _swf_int_pattern,
        'executable_number': _swf_int_pattern,
        'queue_number': _swf_int_pattern,
        'partition_number': _swf_int_pattern,
        'preceding_job_number': _swf_int_pattern,
        'think_time_prejob': _swf_int_pattern
    }, _swf_avoid_regexps)

default_swf_mapper = {
    'job_number': 'job_id',
    'requested_time': 'expected_duration'
}


def default_sorting_function(obj1, obj2, avoid_data_tokens=[';']):
    """
    
    Function for sorting the swf files in ascending order. If one of the object belongs to avoid_data_tokens, the same order is maintained by returning 1.
    
    :param obj1: Object 1
    :param obj2: Object 2
    :param avoid_data_tokens: Tokens to avoid
    
    :return: return a positive number for maintaing the order, or a negative one to change the order.
    
    """
    if obj1[0] in avoid_data_tokens or obj2[0] in avoid_data_tokens:
        return 1
    return default_sorted_attribute(obj1) - default_sorted_attribute(obj2)


def default_sorted_attribute(workload_line, attr='submit_time', converter=None):
    """
    
    :param workload_line: A line readed from the file.
    :param attr: Attribute of the line for sorting.
    :param converter: Converter function to cast the attribute.   
    
    :return: Returns the attribute of the line. Casted if it's required. 
    
    """
    value = workload_parser(workload_line, attr)[attr]
    if converter:
        return converter(value)
    return value


def workload_parser(workload_line, attrs=None, avoid_data_tokens=[';']):
    """ 
    
        Attributes of each workload line in a SWF format (separated by space):
        
        1. job_number -- a counter field, starting from 1.
        2. submit_time -- in seconds. The earliest time the log refers to is zero, and is usually the submittal time of the first job. The lines in the log are sorted by ascending submittal times. It makes sense for jobs to also be numbered in this order.
        3. wait_time -- in seconds. The difference between the job's submit time and the time at which it actually began to run. Naturally, this is only relevant to real logs, not to models.
        4. duration -- in seconds. The wall clock time the job was running (end time minus start time).
        5. allocated_processors -- an integer. In most cases this is also the number of processors the job uses; if the job does not use all of them, we typically don't know about it.
        6. avg_cpu_time -- Time Used for both user and system, in seconds. This is the average over all processors of the CPU time used, and may therefore be smaller than the wall clock runtime. If a log contains the total CPU time used by all the processors, it is divided by the number of allocated processors to derive the average.
        7. used_memory -- in kilobytes. This is again the average per processor.
        8. requested_number_processors --- Requested Number of Processors.
        9. requested_time -- This can be either runtime (measured in wallclock seconds), or average CPU time per processor (also in seconds) -- the exact meaning is determined by a header comment. In many logs this field is used for the user runtime estimate (or upper bound) used in backfilling. If a log contains a request for total CPU time, it is divided by the number of requested processors.
        10. requested_memory -- Requested memory in kilobytes per processor.
        11. status -- 1 if the job was completed, 0 if it failed, and 5 if cancelled. If information about chekcpointing or swapping is included, other values are also possible. See usage note below. This field is meaningless for models, so would be -1.
        12. user_id -- a natural number, between one and the number of different users.
        13. group_id -- a natural number, between one and the number of different groups. Some systems control resource usage by groups rather than by individual users.
        14. executable_number -- a natural number, between one and the number of different applications appearing in the workload. in some logs, this might represent a script file used to run jobs rather than the executable directly; this should be noted in a header comment.
        15. queue_number -- a natural number, between one and the number of different queues in the system. The nature of the system's queues should be explained in a header comment. This field is where batch and interactive jobs should be differentiated: we suggest the convention of denoting interactive jobs by 0.
        16. partition_number -- a natural number, between one and the number of different partitions in the systems. The nature of the system's partitions should be explained in a header comment. For example, it is possible to use partition numbers to identify which machine in a cluster was used.
        17. preceding_job_number -- this is the number of a previous job in the workload, such that the current job can only start after the termination of this preceding job. Together with the next field, this allows the workload to include feedback as described below.
        18. think_time_prejob -- this is the number of seconds that should elapse between the termination of the preceding job and the submittal of this one.
        
        :param workload_line: A Line of the workload file
        :param attrs: List of attributes to be considered. Default None, all attributes will be considered.
        :param avoid_data_tokens: List of tokens to avoid the line
        
        :return: A dictionary with all the attributes requested. If the line is returned it means that the line has the token to avoid.     
    
    """
    if workload_line[0] in avoid_data_tokens:
        return workload_line
    _common_int_pattern = ('\s*(?P<{}>[-+]?\d+)', int)
    _common_float_pattern = ('\s*(?P<{}>[-+]?\d+\.\d+|[-+]?\d+)', float)
    _dict = {
        'job_number': _common_int_pattern,
        'submit_time': _common_int_pattern,
        'wait_time': _common_int_pattern,
        'duration': _common_int_pattern,
        'allocated_processors': _common_int_pattern,
        'avg_cpu_time': _common_float_pattern,
        'used_memory': _common_int_pattern,
        'requested_number_processors': _common_int_pattern,
        'requested_time': _common_int_pattern,
        'requested_memory': _common_int_pattern,
        'status': _common_int_pattern,
        'user_id': _common_int_pattern,
        'group_id': _common_int_pattern,
        'executable_number': _common_int_pattern,
        'queue_number': _common_int_pattern,
        'partition_number': _common_int_pattern,
        'preceding_job_number': _common_int_pattern,
        'think_time_prejob': _common_int_pattern
    }
    _sequence = _dict.keys() if not attrs else ((attrs,) if isinstance(attrs, str) else attrs)
    reg_exp = r''
    for _key in _sequence:
        reg_exp += _dict[_key][0].format(_key)
    p = re.compile(reg_exp)
    _matches = p.match(workload_line)
    _dict_line = _matches.groupdict()
    return {key: _dict[key][1](_dict_line[key]) for key in _sequence}


def sort_file(input_filepath, lines=None, sort_function=default_sorting_function, avoid_data_tokens=[';'],
              output_filepath=None):
    """
    
    The input file for the simulator must be sorted by submit time. It modifies the file input file, 
    or also can be saved to a new one if the output_filepath arg is defined.
      
    :param input_filepath: Input workload file
    :param lines: Number of lines to be read. It includes all lines from the begining of the file. 
    :param sort_function: (Optional) The function that sorts the file by submit time. The user is responsable to define the correct function. If a workload with SWF format is used, by default default_sorting_function (SWF workload) is used.
    :param avoid_data_tokens: (Optional) By default avoid to modify comment lines of SWF workload.      
    :param output_filepath: (Optional) The sorted data is saves into another file (this filepath). It will not content the lines that begin with tokens of the avoid_data_tokens var.
    
    :return: A list of queued time points.  

    """
    assert (callable(sort_function))
    logging.debug('Sorting File: %s ' % (input_filepath))
    with open(input_filepath) as f:
        sorted_file = list(f if not lines else islice(f, lines))
        sorted_file.sort(
            key=cmp_to_key(sort_function)
        )
    if output_filepath is None:
        output_filepath = input_filepath
    logging.debug("Writing sorted file to %s" % (output_filepath))
    queued_times = sorted_list()
    with open(output_filepath, 'w') as f:
        for line in sorted_file:
            if line[0] in avoid_data_tokens:
                f.write(line)
                continue
            _line = workload_parser(line)
            if int(_line['requested_number_processors']) == -1 and int(_line['allocated_processors']) == -1 or int(
                    _line['requested_memory']) == -1 and int(_line['used_memory']) == -1:
                continue
            qtime = default_sorted_attribute(line, 'submit_time')
            queued_times.add(qtime)
            f.write(line)
    return queued_times.get_list()


def cmp_to_key(mycmp):
    """
    
    Convert a cmp= function into a key= function
    
    """

    class k(object):
        def __init__(self, obj, *args):
            self.obj = obj

        def __lt__(self, other):
            return mycmp(self.obj, other.obj) < 0

        def __gt__(self, other):
            return mycmp(self.obj, other.obj) > 0

        def __eq__(self, other):
            return mycmp(self.obj, other.obj) == 0

        def __le__(self, other):
            return mycmp(self.obj, other.obj) <= 0

        def __ge__(self, other):
            return mycmp(self.obj, other.obj) >= 0

        def __ne__(self, other):
            return mycmp(self.obj, other.obj) != 0

    return k


def from_isodatetime_2_timestamp(dtime):
    """
    
    Converts a ISO datetime to Unix Timestamp
    
    :param dtime: Datetime in YYYY-MM-DD HH:MM:SS format
    
    :return: Timestamp of the dtime 
    
    """
    p = re.compile(r'(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2})')
    m = p.search(dtime).groups()
    # year, month, day, hour, minute, second, microsecond
    t = datetime(year=int(m[0]), month=int(m[1]), day=int(m[2]), hour=int(m[3]), minute=int(m[4]), second=int(m[5]))
    return int(t.timestamp())


class watcher_daemon:
    """
    
    Wathcer Daemon allows to track the simulation process through command line querying.
    
    """
    MAX_LENGTH = 2048

    def __init__(self, port, functions):
        """
    
        Watcher daemon constructor
        
        :param port: Port of the watcher   
        :param functions: Available functions to call for data.
    
        """
        self.server_address = ('', port)
        af = socket.AF_INET
        self.sock = socket.socket(af, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(1)
        self.thread = None
        self.hastofinish = False
        self.const = CONSTANT()
        self.functions = functions

    def start(self):
        """
    
        Start the daemon
    
        """
        self.thread = threading.Thread(target=self.listenForRequests)
        self.hastofinish = False
        self.thread.start()

    def listenForRequests(self):
        """
    
        Listening for requests
    
        """
        # Listen for incoming connections
        # Reusing
        self.sock.bind(self.server_address)
        self.sock.listen(5)

        while not self.hastofinish:
            try:
                connection, client_address = self.sock.accept()
                with connection:
                    # print('connection from %s' % (client_address[0]))
                    data = json.loads(connection.recv(self.MAX_LENGTH).decode())
                    if isinstance(data, str):
                        response = {}
                        response['actual_time'] = str(str_datetime(self.call_inner_function('current_time_function')))
                        if data == 'progress':
                            response['input_filepath'] = self.const.input_filepath
                            response['progress'] = os.path.getsize(self.const.sched_output_filepath) / os.path.getsize(
                                self.const.input_filepath)
                            response['time'] = time.clock() - self.const.start_time
                        elif data == 'usage':
                            response['simulation_status'] = self.call_inner_function('simulated_status_function')
                            response['usage'] = self.call_inner_function('usage_function')
                        elif data == 'all':
                            response['input_filepath'] = self.const.input_filepath
                            response['progress'] = os.path.getsize(self.const.sched_output_filepath) / os.path.getsize(
                                self.const.input_filepath)
                            response['time'] = time.clock() - self.const.start_time
                            response['simulation_status'] = self.call_inner_function('simulated_status_function')
                            response['usage'] = self.call_inner_function('usage_function')
                        connection.sendall(json.dumps(response).encode())
                    connection.close()
            except socket.timeout:
                pass
        self.sock.close()

    def call_inner_function(self, name):
        """
    
        Call a function and retrives it results
    
        :param name: name of the function 
    
        """
        if name in self.functions:
            _func = self.functions[name]
            if callable(_func):
                return _func()
            else:
                return _func
        raise Exception('{} was no defined'.format(name))

    def stop(self):
        """
    
        Stop the daemon
    
        """
        self.hastofinish = True
        self.timedemon.stop()


def generate_config(config_fp, **kwargs):
    """
    
    Creates a config file.
    
    :param config_fp: Filepath to the config
    :param \*\*kwargs: Source for the config data  
    
    """
    _local = {}
    for k, v in kwargs.items():
        _local[k] = v
    with open(config_fp, 'w') as c:
        json.dump(_local, c, indent=2)


def hinted_tuple_hook(obj):
    """
    
    Decoder for specific object of json files, for preserving the type of the object.
    It's used with the json.load function.
    
    """
    if '__tuple__' in obj:
        return tuple(obj['items'])
    else:
        return obj


def load_config(config_fp):
    """
    
    Loads an specific config file in json format
    
    :param config_fp: Filepath of the config file.
    
    :return: Dictionary with the configuration. 
    
    """
    _dict = None
    with open(config_fp) as c:
        _dict = json.load(c, object_hook=hinted_tuple_hook)
    return _dict


class Singleton(object):
    """
    
    Singleton class
    
    """
    _instances = {}

    def __new__(class_, *args, **kwargs):
        if class_ not in class_._instances:
            class_._instances[class_] = super(Singleton, class_).__new__(class_, *args, **kwargs)
        return class_._instances[class_]


class CONSTANT(Singleton):
    """
    
    This class allows to load all config parameters into a :class:`.Singleton` Object. 
    This object will allow access to all the parameters. The parameters could be accessed as attribute name.
    
    New attrs could be passed as dict (:func:`load_constants`) or simply with (attr, value) (:func:`load_constant`)
    
    :Example:
          
        **Program**:
        
        >>> PATH = '/path/to/'
        >>> c = CONSTANT()
        >>> c.load('PATH', PATH)
        >>> print(c.PATH)
        >>> /path/to/

    :Note:
    
        It's loaded into all base class by default!
    
    """
    _constants = []

    def load_constants(self, _dict):
        """
        
        Loads an entire dictionary into the singleton.
        
        :param _dict: Dictionary with the new parameters to load. 
        
        """
        for k, v in _dict.items():
            self.load_constant(k, v)

    def load_constant(self, k, v):
        """
        
        Load an specific parameter.
        
        :param k: Name of the parameter
        :param v: Value of the parameter
        
        """
        assert (not hasattr(self, k)), '{} already exists as constant ({}={}). Choose a new name.'.format(k, k,
                                                                                                          getattr(self,
                                                                                                                  k))
        setattr(self, k, v)
        self._constants.append(k)

    def clean_constants(self):
        for _constant in self._constants:
            delattr(self, _constant)
        self._constants = []


# ===============================================================================
# Utils Types for the hinted tuple 
# ===============================================================================

class str_:
    def __init__(self, text):
        self.text = text

    def __str__(self):
        return self.text


class str_datetime:
    REGEX = '\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}'
    REGEX_GROUP = '(?P<{}>\d{{4}}-\d{{2}}-\d{{2}}\s\d{{2}}:\d{{2}}:\d{{2}})'

    def __init__(self, epoch_time):
        self.str_datetime = datetime.fromtimestamp(int(epoch_time)).strftime('%Y-%m-%d %H:%M:%S')

    def __format__(self, *args):
        return self.str_datetime

    def __str__(self):
        return self.str_datetime


class str_time:
    def __init__(self, secs):
        self.str_time = time.gmtime(int(secs))  # time.strftime('%H:%M:%S', time.gmtime(int(secs)))

    def __str__(self):
        return self.str_time


class str_resources:
    SEPARATOR = '#'
    REGEX = '[\d+;|' + SEPARATOR + ']+'
    REGEX_GROUP = '(?P<{}>[\d+;|' + SEPARATOR + ']+)'

    def __init__(self, nodes, resources):
        self.nodes = nodes
        self.resources = resources  # namedtuple('resources', [k for k in resources.keys()])(**resources)
        self.constants = CONSTANT()
        if not hasattr(self.constants, 'resource_order'):
            default_order = list(self.resources.keys())
            self.constants.load_constant('resource_order', default_order)
        self.order = self.constants.resource_order

    def __str__(self):
        return self.SEPARATOR.join(
            [';'.join([node.split('_')[1]] + [str(self.resources[_k]) for _k in self.order]) for node in
             self.nodes]) + self.SEPARATOR


class str_nodes:
    def __init__(self, nodes):
        self.nodes = nodes

    def __format__(self, format_spec):
        return self.__str__()

    def __str__(self):
        return ','.join([node.split('_')[1] for node in self.nodes])


class sorted_object_list():
    """
    
    Sorted Object list, with two elements for comparison, the main and the tie breaker. Each object must have an id for identification
    
    """

    def __init__(self, sorting_priority, _list=[]):
        """
    
        Sorted object list constructor. 
        
        :param sorting_priority: Dictionary with the 'main' and 'break_tie' keys for selecting the attributes for sorting. The value of the key corresponds to the object attribute.
        :param _list: Optional. Initial list  
    
        """
        assert (isinstance(sorting_priority, dict) and set(['main', 'break_tie']) <= set(sorting_priority.keys()))

        self.main_sort = sorting_priority['main']
        self.break_tie_sort = sorting_priority['break_tie']
        self.list = []
        self.main = []
        self.secondary = []
        self.map = {
            'pos': {},
            'id': {}
        }
        self.objects = {}
        # dict values, function or inner attributes of wrappred objs
        self._iter_func = lambda act, next: act.get(next) if isinstance(act, dict) else (
            getattr(act, next)() if callable(getattr(act, next)) else getattr(act, next))

        if _list:
            self.add(*_list)

    def add(self, *args):
        """
    
        Add new elements to the list
        
        :param \*args: List of new elements 
    
        """
        for arg in args:
            _id = getattr(arg, 'id')
            if _id in self.map['id']:
                continue
            self.objects[_id] = arg
            _main = reduce(self._iter_func, self.main_sort.split('.'), arg)
            _sec = reduce(self._iter_func, self.break_tie_sort.split('.'), arg)
            _pos = bisect_left(self.main, _main)
            main_pos_r = bisect_right(self.main, _main)
            if _pos == main_pos_r:
                self.list.insert(_pos, _id)
                self.main.insert(_pos, _main)
                self.secondary.insert(_pos, _sec)
            else:
                _pos = bisect_left(self.secondary[_pos:main_pos_r], _sec) + _pos
                self.list.insert(_pos, _id)
                self.main.insert(_pos, _main)
                self.secondary.insert(_pos, _sec)
            self.map_insert(self.map['id'], self.map['pos'], _pos, _id)

    def map_insert(self, ids_, poss_, new_pos, new_id):
        """
    
        Maps the new element to maintain the sorted list.
    
        :param ids_: Current id of the object
        :param poss_: Current position of the object
        :param new_pos: New position
        :param new_id: New id
    
        """
        n_items = len(ids_)
        if n_items > 0:
            if not (new_pos in poss_):
                poss_[new_pos] = new_id
                ids_[new_id] = new_pos
            else:
                self.make_map(ids_, poss_, new_pos)
        else:
            ids_[new_id] = new_pos
            poss_[new_pos] = new_id

    def make_map(self, ids_, poss_, new_pos=0, debug=False):
        """
    
        After a removal of a element the map must be reconstructed.
    
        """
        for _idx, _id in enumerate(self.list[new_pos:]):
            ids_[_id] = _idx + new_pos
            poss_[_idx + new_pos] = _id
        if len(ids_) == len(poss_):
            return
        for p in list(poss_.keys()):
            if p > _idx:
                del poss_[p]

    def remove(self, *args, **kwargs):
        """
    
        Removal of an element
    
        :param \*args: List of elements
    
        """
        for id in args:
            assert (id in self.objects)
            del self.objects[id]
            self._remove(self.map['id'][id], **kwargs)

    def _remove(self, _pos, **kwargs):
        """
        
        Removal of an element
        
        :param \*args: List of elements
        
        """
        del self.list[_pos]
        del self.secondary[_pos]
        del self.main[_pos]

        _id = self.map['pos'].pop(_pos)
        del self.map['id'][_id]
        self.make_map(self.map['id'], self.map['pos'], **kwargs)

    def get(self, pos):
        """
        
        Return an element in a specific position
        
        :param pos: Position of the object 
        
        :return: Object in the specified position
        
        """
        return self.list[pos]

    def get_object(self, id):
        """
        
        Return an element with a specific id.
        
        :param id: Id of the object 
        
        :return: Obect with the specific id
        
        """
        return self.objects[id]

    def get_list(self):
        """
        
        :return: The sorted list of ids of elements
        
        """
        return self.list

    def get_object_list(self):
        """
        
        :return: The sorted list of objects
        
        """
        return [self.objects[_id] for _id in self.list]

    def __len__(self):
        return len(self.list)

    # Return None if there is no coincidence
    def pop(self, id=None, pos=None):
        """
        
        Pop an element of the sorted list. 
        
        :param id: id to be poped
        :param pos: pos to be poped
        
        :return: Object
        
        """
        assert (not all([id, pos])), 'Pop only accepts one or zero arguments'
        if not self.list:
            return None
        elif id:
            return self._specific_pop_id(id)
        elif pos:
            return self._specific_pop_pos(pos)
        else:
            _id = self.list[0]
            self._remove(0)
            return self.objects.pop(_id)

    def _specific_pop_id(self, id):
        _obj = self.objects.pop(id, None)
        if _obj:
            self._remove(self.map['id'][id])
        return _obj

    def _specific_pop_pos(self, pos):
        _id = self.map['pos'].pop(pos, None)
        if _id:
            self.map['pos'][pos] = _id
            self._remove(pos)
        return self.objects.pop(_id, None)

    def __iter__(self):
        self.actual_index = 0
        return self

    def __next__(self):
        try:
            self.actual_index += 1
            return self.list[self.actual_index - 1]
        except IndexError:
            raise StopIteration

    def get_reversed_list(self):
        """
        
        :return:  Reversed list of ids
        
        """
        return list(reversed(self.list))

    def get_reversed_object_list(self):
        """
        
        :return: Reversed list of objects
        
        """
        return [self.objects[_id] for _id in reversed(self.list)]

    def __str__(self):
        return str(self.list)


class sorted_list:
    """
    
    Sorted list for single comparable objects (int, float, etc)
     
    """

    def __init__(self, _list=[]):
        assert (isinstance(_list, (list)))
        self.list = []
        if _list:
            self.add(*_list)

    def add(self, *args):
        """
    
        Add elements to the sorted list
        
        :param \*args: Array of elements 
    
        """
        for arg in args:
            if len(self.list) == 0:
                self.list.append(arg)
            else:
                _num = arg
                pos = bisect(self.list, _num)
                if self.list[pos - 1] != _num:
                    self.list.insert(pos, _num)

    def get_list(self):
        """
    
        :return: Return the sorted list
    
        """
        return self.list

    def find(self, _num):
        """
    
        Find the position of the element in the list
        
        :return: Position in the list
    
        """
        return bisect(self.list, _num) - 1

    def remove(self, *args):
        """
    
        Removes the elements from the list
        
        :param \*args: List of elements 
    
        """
        for arg in args:
            self.list.remove(arg)

    def get(self, pos):
        """
        
        :return: Return a element in a specific position.
        
        """
        return self.list[pos]

    def __len__(self):
        """
        
        List's size
        
        :return: Size of the sorted list
        
        """
        return len(self.list)

    def pop(self):
        """

        Pop the first element of the list
        
        :return: Return the first element of the list. None if it's empy.

        """
        if self.list:
            return self.list.pop(0)
        return None

    def __iter__(self):
        self.actual_index = 0
        return self

    def __next__(self):
        try:
            self.actual_index += 1
            return self.list[self.actual_index - 1]
        except IndexError:
            raise StopIteration

    def __str__(self):
        return str(self.list)

    def _check_sort(self):
        """

        Verifies the consistency of the list 

        """
        for i in range(len(self.list) - 1):
            if self.list[i] >= self.list[i + 1]:
                self.list[0:i + 1]
                raise Exception('Sorting problem!')


class FrozenDict(Mapping):
    """

    Inmutable dictionary useful for storing parameter that are dinamycally loaded

    """

    def __init__(self, *args, **kwargs):
        self._d = dict(*args, **kwargs)
        self._hash = None

    def __iter__(self):
        return iter(self._d)

    def __len__(self):
        return len(self._d)

    def __getitem__(self, key):
        return self._d[key]

    def __hash__(self):
        if self._hash is None:
            self._hash = 0
            for pair in self.iteritems():
                self._hash ^= hash(pair)
        return self._hash


def clean_results(*args):
    """

    Removes the filepaths passed as argument

    :param \*args: List of filepaths 

    """
    for fp in args:
        if os.path.isfile(fp) and os.path.exists(fp):
            os.remove(fp)


class DEFAULT_SIMULATION:
    """
    
    Default and base simulation parameters. The following parameters are loaded into the :class:`.CONSTANT`.
    This constants values can be overridden by passing as kwargs in the :class:`accasim.base.simulator_class.hpc_simulator` class instantiation.
    
    :Note:
    
        * CONFIG_FOLDER_NAME: Folder where the configuration files are.
            * "CONFIG_FOLDER_NAME": "config/"
        * RESULTS_FOLDER_NAME: Folder where the configuration files will be.
            * "RESULTS_FOLDER_NAME": "results/"
        * SCHEDULE_OUTPUT: Format of the dispatching plan file.
            * "SCHEDULE_OUTPUT": 
            
            .. code:: 

                {
                    "format": "{job_id};{user};{queue_time}__{assignations}__{start_time};{end_time};{total_nodes};{total_cpu};{total_mem};{expected_duration};",
                    "attributes": {
                        "job_id": ("id", "str"),
                        "user": ("user_id", "str"),
                        "queue_time": ("queued_time", "accasim.utils.misc.str_datetime"),
                        "start_time": ("start_time", "accasim.utils.misc.str_datetime"),
                        "end_time": ("end_time", "accasim.utils.misc.str_datetime"),
                        "assignations": ("assigned_nodes", "requested_resources", "accasim.utils.misc.str_resources"),
                        "total_nodes": ("requested_nodes", "int"),
                        "total_cpu": ("core", "int"),
                        "total_mem": ("mem", "int"),
                        "expected_duration": ("expected_duration", "int")      
                    }
                }
                
        * PPRINT_SCHEDULE_OUTPUT: Format of the dispatching plan file in pretty print version. (Human readable version).
            * "PPRINT_SCHEDULE_OUTPUT":
            
            .. code:: 
            
                {
                    "format": "{:>5} {:>15} {:^19} {:^19} {:>8} {:>8} {:>8} {:>5} {:>4} {:>10} {:<20}",
                    "order": ["n", "job_id", "start_time", "end_time", "wtime", "rtime", "slowdown", "nodes", "core", "mem", "assigned_nodes"],
                    "attributes":{
                        "n": ("end_order", "int"),
                        "job_id": ("id", "str"),
                        "start_time": ("start_time", "accasim.utils.misc.str_datetime"),
                        "end_time": ("end_time", "accasim.utils.misc.str_datetime"),
                        "wtime": ("waiting_time", "int"),
                        "rtime": ("running_time", "int"),
                        "slowdown": ("slowdown", "float"),
                        "nodes": ("requested_nodes", "int"),
                        "core": ("core", "int"),
                        "mem": ("mem", "int"),
                        "assigned_nodes": ("assigned_nodes", "accasim.utils.misc.str_nodes")
                    }
                }
                
        * SCHED_PREFIX: Prefix of the dispatching plan file.
            * "SCHED_PREFIX": "sched-"
        * PPRINT_PREFIX: Prefix of the pprint file.
            * "PPRINT_PREFIX": "pprint-"
        * STATISTICS_PREFIX: Prefix of the statistic file.
            * "STATISTICS_PREFIX": "stats-"
        * BENCHMARK_PREFIX: Prefix of the benchmark file.
            * "BENCHMARK_PREFIX": "bench-"
        * SUBMISSION_ERROR_PREFIX: Prefix of the submission error file.
            * "SUBMISSION_ERROR_PREFIX": "suberror-"
        * RESOURCE_ORDER: How resource are sorted for printing purposes.
            * "RESOURCE_ORDER": ["core", "mem"]
        * WATCH_PORT: Port used for the watcher daemon.
            * "WATCH_PORT": 8999
    
    """

    parameters = {
        "CONFIG_FOLDER_NAME": "config/",
        "RESULTS_FOLDER_NAME": "results/",
        "SCHEDULE_OUTPUT": {
            "format": "{job_id};{user};{queue_time}__{assignations}__{start_time};{end_time};{total_nodes};{total_cpu};{total_mem};{expected_duration};",
            "attributes": {
                "job_id": ("id", "str"),
                "user": ("user_id", "str"),
                "queue_time": ("queued_time", "accasim.utils.misc.str_datetime"),
                "start_time": ("start_time", "accasim.utils.misc.str_datetime"),
                "end_time": ("end_time", "accasim.utils.misc.str_datetime"),
                "assignations": ("assigned_nodes", "requested_resources", "accasim.utils.misc.str_resources"),
                "total_nodes": ("requested_nodes", "int"),
                "total_cpu": ("core", "int"),
                "total_mem": ("mem", "int"),
                "expected_duration": ("expected_duration", "int")
            }
        },
        "PPRINT_SCHEDULE_OUTPUT": {
            "format": "{:>5} {:>15} {:^19} {:^19} {:>8} {:>8} {:>8} {:>5} {:>4} {:>10} {:<20}",
            "order": ["n", "job_id", "start_time", "end_time", "wtime", "rtime", "slowdown", "nodes", "core", "mem",
                      "assigned_nodes"],
            "attributes": {
                "n": ("end_order", "int"),
                "job_id": ("id", "str"),
                "start_time": ("start_time", "accasim.utils.misc.str_datetime"),
                "end_time": ("end_time", "accasim.utils.misc.str_datetime"),
                "wtime": ("waiting_time", "int"),
                "rtime": ("running_time", "int"),
                "slowdown": ("slowdown", "float"),
                "nodes": ("requested_nodes", "int"),
                "core": ("core", "int"),
                "mem": ("mem", "int"),
                "assigned_nodes": ("assigned_nodes", "accasim.utils.misc.str_nodes")
            }
        },
        "SCHED_PREFIX": "sched-",
        "PPRINT_PREFIX": "pprint-",
        "STATISTICS_PREFIX": "stats-",
        "BENCHMARK_PREFIX": "bench-",
        "SUBMISSION_ERROR_PREFIX": "suberror-",
        "RESOURCE_ORDER": ["core", "mem"],
        "WATCH_PORT": 8999
    }
    """dict: Default Simulation parameters """


def obj_assertion(obj, class_type, error_msg=None, msg_args=None):
    """

    :param obj:
    :param class_type:
    :param error_msg:
    :param msg_args:
    :return:
    """
    if error_msg:
        assert (isinstance(obj, class_type)), error_msg.format(*msg_args)
        return
    assert (isinstance(obj, class_type))


def list_class_assertion(_list, class_type, allow_empty=False, error_msg=None, msg_args=None):
    """

    :param _list:
    :param class_type:
    :param allow_empty:
    :param error_msg:
    :param msg_args:
    :return:
    """
    assert (not allow_empty and len(_list) > 0), 'Empty list not allowed.'
    try:
        if error_msg:
            assert (
                isinstance(_list, list) and all(
                    [issubclass(_class, class_type) for _class in _list])), error_msg.format(
                msg_args)
            return
        assert (
            isinstance(_list, list) and all([issubclass(_class, class_type) for _class in _list]))
    except TypeError:
        if error_msg:
            raise Exception(error_msg.format(msg_args))


CUSTOM_TYPES = {
    'accasim.utils.misc.str_datetime': str_datetime.REGEX_GROUP,
    'accasim.utils.misc.str_resources': str_resources.REGEX_GROUP,
}


def type_regexp(_type, new_regexp={}):
    """

    :param _type:
    :param new_regexp:
    :return:
    """
    STR_TYPE = 'str'
    INT_TYPE = 'int'

    if _type == STR_TYPE:
        return '(?P<{}>[0-9a-zA-Z_\-\.@]+)'
    if _type == INT_TYPE:
        return '(?P<{}>\d+)'
    elif _type in CUSTOM_TYPES:
        return CUSTOM_TYPES[_type]
    elif _type in new_regexp:
        return new_regexp[_type]
    else:
        raise Exception('The regular expression for the {} type is not defined.')
