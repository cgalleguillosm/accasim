"""
MIT License

Copyright (c) 2017 cgalleguillosm

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
import logging
import re
import sys, os
import time
from abc import abstractmethod, ABC
from pydoc import locate
import asyncio
from builtins import str, filter
from inspect import signature
from _functools import reduce
from types import MethodType
from accasim.utils.misc import CONSTANT, sorted_list, default_swf_mapper
from accasim.base.resource_manager_class import resource_manager


class attribute_type:
    
    def __init__(self, name, type_class=None, optional=False):
        """
        @param name: Attribute name
        @param type_class: Class type of attribute (str, int, float, etc.) for casting. If value  is already casted it is not necesary.
        @param optional: False by default. If it is True, the default value will be None and it is not required to give any value to this.  
        """
        assert(isinstance(name, str))
        self.name = name
        self.type = type_class
        self.optional = optional  
                
class event(ABC):
    
    def __init__(self, job_id, queued_time, duration):
        self.constants = CONSTANT()
        self.id = job_id  # uuid.uuid1()
        self.queued_time = queued_time
        self.start_time = None
        self.end_time = None
        self.duration = duration
        self.end_order = 0
    
    def subattr(self, obj, attrs):
        if isinstance(attrs, tuple):
            values = []
            for attr in list(attrs):
                values.append(self.subattr(obj, attr))
            return values
        sp_attr = attrs.split('.')
        if len(sp_attr) > 1:
            tmp = getattr(obj, sp_attr[0])
            return self.subattr(tmp, ''.join(sp_attr[1:]))
        return getattr(obj, sp_attr[0])
    
    def schd_write_out(self):
        if not self.constants.SCHEDULING_OUTPUT:
            return 
        _dict = self.constants.SCHEDULE_OUTPUT
        _attrs = {}
        for a, av in _dict['attributes'].items():
            _attrs[a] = locate(av[-1])(*self.subattr(self, av[:-1])) 
        output_format = _dict['format']
        format_elements = re.findall('\{(\w+)\}', output_format)
        values = {k: v for k, v in _attrs.items() if k in format_elements}
        _filepath = os.path.join(self.constants.RESULTS_FOLDER_PATH, self.constants.SCHED_PREFIX + self.constants.WORKLOAD_FILENAME)
        with open(_filepath, 'a') as f:
            f.write(output_format.format(**values) + '\n')
    
    def schd_pprint_write_out(self):
        if not self.constants.PPRINT_OUTPUT:
            return 
        _dict = self.constants.PPRINT_SCHEDULE_OUTPUT
        _order = _dict['order']
        _attrs = {a: locate(av[-1])(*self.subattr(self, av[:-1])) for a, av in _dict['attributes'].items() if a in _order}
        output_format = _dict['format']
        format_elements = re.findall('\{(\w+)\}', output_format)
        values = [_attrs[k] for k in _order]
        _filepath = os.path.join(self.constants.RESULTS_FOLDER_PATH, self.constants.PPRINT_PREFIX + self.constants.WORKLOAD_FILENAME)
        with open(_filepath, 'a') as f:
            if self.end_order == 1:
                f.write(output_format.format(*_order) + '\n')
            f.write(output_format.format(*values) + '\n')
                        
class job_factory:
    def __init__(self, _resource_manager, job_class=event, job_attrs=[], job_mapper=default_swf_mapper):
        """
        @param _resource_manager: The resource manager of the simulator. It is required for creating the job requests.
        @param job_class: The class to be created by the Factory. By default it uses the Event class, but any subclass of it can be used (modified versions). 
        @param job_attrs: The extra attributes (attribute_type class) (already job_id, queued_time and duration are mandatory) to be set in the JobEvent class
        @param job_mapper: Rename the the old key to a new key (using the value of the mapper dictionary)
        """
        assert(isinstance(_resource_manager, resource_manager))
        assert(issubclass(job_class, event)), 'Only subclasses of event class are accepted. Received: {} class'.format(_class.__name__)
        if job_attrs:
            assert(isinstance(job_attrs, list) and all(isinstance(attr_type, attribute_type) for attr_type in job_attrs))
        else:
            attrs = self.default_job_description()
        # self.resource_manager = _resource_manager
        self.group_resources = _resource_manager.groups_available_resource()
        self.system_resources = _resource_manager.resources.system_resource_types
        self.obj_type = job_class
        self.obj_parameters = list(signature(self.obj_type).parameters)
        self.attrs_names = []
        self.mandatory_attrs = {}
        self.optional_attrs = {}
        self.mapper = job_mapper
        
        for attr in attrs:
            _attr_name = attr.name
            assert(_attr_name not in self.attrs_names + self.obj_parameters), '{} attribute name already set. Names must be unique'.format(_attr_name)
            if attr.optional:
                assert(_attr_name in self.obj_parameters), '{} attribute name is mandatory.'.format(_attr_name)
                self.optional_attrs[_attr_name] = attr
            else:
                self.mandatory_attrs[_attr_name] = attr
            self.attrs_names.append(_attr_name)
                 
    def factory(self, **kwargs):
        for _old, _new in self.mapper.items():
            value = kwargs.pop(_old)
            kwargs[_new] = value
        _missing = list(filter(lambda x:x not in kwargs, set(self.obj_parameters + list(self.mandatory_attrs))))
        assert(not _missing), 'Missing attributes: {}'.format(', '.join(_missing))
        _tmp = self.obj_type(**{k:kwargs[k] for k in self.obj_parameters})
        setattr(_tmp, '_dict', kwargs)
        self.add_attrs(_tmp, self.mandatory_attrs, kwargs)
        self.add_attrs(_tmp, self.optional_attrs, kwargs)
        self.add_request(_tmp)
        return _tmp
        
    def add_attrs(self, obj, reference, values):
        for _attr in reference:
            _type = reference[_attr].type
            _value = None
            if not reference[_attr].optional or (_attr in values and values[_attr]):
                _value = _type(values[_attr]) if _type else values[_attr] 
            setattr(obj, _attr, _value)
    
    def add_request(self, obj):
        # Calculate only if it is not present
        if not hasattr(obj, 'requested_nodes'):
            _partition = 0 
            for _res in self.system_resources:
                _total_request = getattr(obj, _res)
                assert(_total_request >= 0), 'The request for {} is no feasible ({}). Accepted values are equal or greater than 0. Job {} must be tweaked before re-run. See the example.'.format(_res, _total_request, obj.id)
                _partition = max([_partition] + [round(getattr(obj, _res) / _resources[_res]) for _resources in self.group_resources.values()])
            setattr(obj, 'requested_nodes', _partition)
        if not hasattr(obj, 'requested_resources'):
            _partition = getattr(obj, 'requested_nodes')
            setattr(obj, 'requested_resources', {_res: getattr(obj, _res) // _partition for _res in self.system_resources})
    
    def default_job_description(self):
        # Attribute to identify the user
        user_id = attribute_type('user_id', int)
        
        # New attributes required by the Dispatching methods.
        expected_duration = attribute_type('expected_duration', int)
        
        # Default system resources: core and mem.
        total_cores = attribute_type('core', int)
        total_mem = attribute_type('mem', int)
    
        # This attributes are required to be set, if not by default are calculated. As in this example (explained in tweak_dict function) 
        requested_nodes = attribute_type('requested_nodes', int)
        requested_resources = attribute_type('requested_resources', dict)
        
        return [total_cores, total_mem, requested_nodes, requested_resources, expected_duration, user_id ]
            
class event_mapper:
    """
        This class coordinates events submission, queueing and ending. 
        kwargs:
            - nothing by the moment
    """
    def __init__(self, _resource_manager, **kwargs):
        assert(isinstance(_resource_manager, resource_manager)), 'Wrong type for the resource_manager argument.'
        self.resource_manager = _resource_manager
        self.constants = CONSTANT()
        #=======================================================================
        # if 'resource_manager' in kwargs:            
        #     self.resource_manager = kwargs['resource_manager']
        #=======================================================================
        # Stats
        self.first_time_dispatch = None
        self.last_run_time = None
        self.slowdowns = []
        self.wtimes = []
        
        self.current_time = 0
        self.time_points = sorted_list()
        # self.ending_time_points = sorted_list()
        self.events = {}
        self.loaded = {}
        self.queued = []
        self.real_ending = {}
        self.running = []
        self.finished = []
            
    def load_events(self, es):
        if isinstance(es, list):
            for e in es:
                self.load_event(e)
        else:
            self.load_event(es)
                    
    def load_event(self, e):
        assert(isinstance(e, event)), 'Using %s, expecting a single %s' % (e.__class__, event.__name__)
        if self.current_time == e.queued_time:
            self.submit_event(e)
        elif self.current_time < e.queued_time:
            if e.queued_time not in self.loaded:
                 self.loaded[e.queued_time] = []
            self.loaded[e.queued_time].append(e.id)
            self.time_points.add(e.queued_time)
        else:
            raise Exception('Time sync problem, the actual event was loaded after the real submit time. This a programming error, must be checked.')
                        

    def move_to_finished(self, events_dict):
        """
            There are two time points for a job could ends, the expected one and the real one. 
            The job must run until the real one is reached, then if a job is waiting to finish but is less than the
            real ending time, this value must be updated with the real one.        
        """
        _es = []
        for e_id in self.real_ending.pop(self.current_time, []):
            if e_id in self.running:
                self.running.remove(e_id)
                e = events_dict[e_id]
                self.finish_event(e)  
                _es.append(e_id)
        self.last_run_time = self.current_time
        return _es

    def finish_event(self, e):
        e.end_time = self.current_time
        e.running_time = e.end_time - e.start_time
        e.waiting_time = e.start_time - e.queued_time
        e.slowdown = float("{0:.2f}".format((e.waiting_time + e.running_time) / e.running_time)) if e.running_time != 0 else e.waiting_time if e.waiting_time != 0 else 1.0
        self.slowdowns.append(e.slowdown)   
        self.wtimes.append(e.waiting_time)
        self.finished.append(e.id)
        e.end_order = len(self.finished)
        asyncio.create_subprocess_exec([e.schd_write_out(), e.schd_pprint_write_out()])
   
    def dispatch_event(self, _e, time, time_diff, _nodes): 
        id = _e.id
        start_time = time + time_diff
        assert(self.current_time == start_time), 'Start time is different to the current time'
        
        """
         Update job info
        """
        _e.start_time = start_time
        _e.assigned_nodes = _nodes


        # Used only for statistics
        if self.first_time_dispatch == None:
            self.first_time_dispatch = start_time

        if _e.duration == 0:
            logging.debug('%s: %s Dispatched and Finished at the same moment. Job Lenght 0' % (self.current_time, id))
            self.finish_event(_e)
            self.time_points.add(self.current_time)
            return False
        # Move to running jobs 
        self.running.append(id)
        """
            Setting the ending time as walltime
        """
        expected_end_time = _e.start_time + _e.expected_duration
        real_end_time = _e.start_time + _e.duration
        # self.ending_time_points.add(expected_end_time, real_end_time)
        self.time_points.add(expected_end_time)
        self.time_points.add(real_end_time)

        if real_end_time not in self.real_ending:
            self.real_ending[real_end_time] = [] 
        self.real_ending[real_end_time].append(id)
        logging.debug('%s: %s Dispatched! Init %s Ending %s' % (self.current_time, id, start_time, expected_end_time))
        return True
        #=======================================================================
        # elif self.current_time <= start_time:
        #     # Here must be reserved slots!!
        #     logging.debug('%s: %s moved to Queue! Will init %s' % (self.current_time, id, start_time))
        #     self.submit_event(_e, start_time)
        #     return False
        # else:
        #     logging.error('Error: ', self.current_time, time)
        #     raise Exception('Can\'t comeback to the past :( (%s->%s)' % (self.current_time, time))
        #=======================================================================
        
    def submit_event(self, e_id):  # (self, e, re_queued_time=None, debug=False):
        # assert(isinstance(e, event)) 
        # self.queued.append(e.id)
        self.queued.append(e_id)
    
    def next_events(self):
        self.current_time = self.time_points.pop()
        if self.current_time is None:
            return []
        submitted = self.loaded.pop(self.current_time, [])
        new_queue = self.queued + submitted 
        logging.debug('Next events: \n-Recently submited: %s\n-Already queued: %s' % (submitted, self.queued))
        self.queued.clear()
        return new_queue
        
    def has_events(self):
        return (self.loaded or self.queued or self.running)
    
    def show_map(self, events):
        i = 1
        print('#\tjob_id\t\twtime\trtime\tbsld\tnodes\tcore\tgpu\tmic\tmem\tnodes')
        self.finished.sort(key=lambda e: events[e].start_time, reverse=False)
        for e in self.finished:
            _e = events[e]
            _e_resources = _e.requested_resources
            _wtime = _e.start_time - _e.queued_time
            _rtime = _e.end_time - _e.start_time
            _bsld = (_wtime + _rtime) / _rtime if _rtime > 0 else 1.0
            print('%s\t%s\t%s\t%s\t%.2f\t%s\t%s\t%s\t%s\t%s\t%s' % (i, _e.id, _wtime, _rtime, _bsld, _e.requested_nodes, _e_resources['core'], _e_resources['gpu'], _e_resources['mic'], _e_resources['mem'], ','.join([n.split('_')[1] for n in _e.assigned_nodes])))
            i += 1        
 
    def dispatch_events(self, event_dict, to_dispatch, time_diff, _debug=False):
        for (_time, _id, _nodes) in to_dispatch:
            assert(_time is None or _time >= self.current_time), 'Receiving wrong schedules.'
            """
            Time must be equal or later than current time.
                Equals will be dispatched in the momement, instead later ones, which will be requeued with expected ending time of the job that release the resources. 
            If the expected ending is surpass, because the job takes more time to finish, the time tuple\'s element must be None. '
            """
            if not _nodes:
                # For blocked jobs
                if _time is not None and _time != self.current_time:
                    self.time_points.add(_time)
                """
                 Maintaining the event in the queue 
                """
                self.submit_event(_id)
                continue
            _e = event_dict[_id]
            # print(_e, _time, time_diff, _nodes)
            if self.dispatch_event(_e, _time, time_diff, _nodes):
                self.resource_manager.allocate_event(_e, _nodes)
                
    def release_ended_events(self, event_dict):
        _es = self.move_to_finished(event_dict)
        for _e in _es:
            self.resource_manager.remove_event(_e)
            # Freeing mem (finished events)
            event_dict.pop(_e)
        return _es
    
    def simulated_status(self):
        return ('Loaded {}, Queued {}, Running {}, and Finished {} Jobs'.format(len(self.loaded), len(self.queued), len(self.running), len(self.finished)))
    
    def availability(self):
        return self.resource_manager.availability()
    
    def usage(self):
        return self.resource_manager.resources.usage()
    
    def simulated_current_time(self):
        return self.current_time
    
    def __str__(self):
        return 'Loaded: %s\nQueued: %s\nRunning: %s\nExpected job finish: %s\nReal job finish on: %s,\nFinished: %s\nNext time events: %s' % (self.loaded, self.queued, self.running, None, self.real_ending, self.finished, self.time_points)
