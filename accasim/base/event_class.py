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

from os import path
from abc import ABC
from pydoc import locate
from builtins import str, filter
from inspect import signature
from sortedcontainers import SortedSet, SortedDict

from accasim.utils.misc import CONSTANT, FrozenDict
from accasim.base.resource_manager_class import ResourceManager
from accasim.utils.async_writer import AsyncWriter


class AttributeType:

    def __init__(self, name, type_class=None, optional=False):
        """

        Constructor for defining a new attribute type.

        :param name: Attribute name
        :param type_class: Class type of attribute (str, int, float, etc.) for casting. If value  is already casted it is not necesary.
        :param optional: False by default. If it is True, the default value will be None and it is not required to give any value to this.

        """
        assert(isinstance(name, str))
        self.name = name
        self.type = type_class
        self.optional = optional


class Event(ABC):

    def __init__(self, job_id, queued_time, duration, requested_nodes, requested_resources):
        """

        Constructor of the basic job event.

        :param job_id: Identification of the job.
        :param queued_time: Corresponding time to the submission time to the system in unix timestamp.
        :param duration: Real duration of the job in unix timestamp.
        :param requested_nodes: Number of requested nodes
        :param requested_resources: Dictionary with the requested resources for a single node.

        """
        self.constants = CONSTANT()
        self.id = str(job_id)
        self.queued_time = queued_time
        self._requested_nodes = requested_nodes        
        self._requested_resources = requested_resources
        self.start_time = None
        self.end_time = None
        self.duration = duration
        self.end_order = 0
        self._checked = False

    def subattr(self, obj, attrs):
        """

        Internal method that reads a description, and extract the value from the object itself and return it. It is used
        for genereting the output logs. (This method is candidate to be moved into utils package.)

        :param obj: Object to be analyzed
        :param attrs: Attributes to be extracted from the object

        :return: Value of the object.

        """
        if isinstance(attrs, tuple):
            values = []
            for attr in list(attrs):
                values.append(self.subattr(obj, attr))
            return values
        sp_attr = attrs.split('.')
        if len(sp_attr) > 1:
            tmp = getattr(obj, sp_attr[0])
            return self.subattr(tmp, ''.join(sp_attr[1:]))

        try:
            if isinstance(obj, dict):
                return obj.get(sp_attr[0], 'NA')
            return getattr(obj, sp_attr[0])
        except AttributeError as e:
            return 'NA'
        
    def set_checked(self, state):
        self._checked = state

    def get_checked(self):
        return self._checked

    def __str__(self):
        return 'Job_{}'.format(self.id)

    def __repr__(self):
        return 'Job_{}'.format(self.id)
    
    @property
    def requested_nodes(self):
        return self._requested_nodes

    @property
    def requested_resources(self):
        return self._requested_resources
    
    def override_requested_nodes(self, requested_nodes):
        self._requested_nodes = requested_nodes
        
    def override_requested_resources(self, type, value):
        self._requested_resources[type] = value


class JobFactory:

    def __init__(self, resource_manager=None, job_class=Event, job_attrs=[], job_mapper={}):
        """

        :param resource_manager: The resource manager of the simulator. It is required for creating the job requests.
        :param job_class: The class to be created by the Factory. By default it uses the Event class, but any subclass of it can be used (modified versions).
        :param job_attrs: The extra attributes (AttributeType class) (already job_id, queued_time and duration are mandatory) to be set in the JobEvent class
        :param job_mapper: Rename the the old key to a new key (using the value of the job_mapper dictionary)

        """
        self.resource_manager = None
        if resource_manager:
            assert(isinstance(resource_manager, ResourceManager)), 'Only subclases of :class:`.ResourceManager` are accepted.'
            self.resource_manager = resource_manager
            self.resource_manager_setup()

        assert(issubclass(job_class, Event)), 'Only subclasses of event class are accepted. Received: {} class'.format(_class.__name__)

        if job_attrs:
            assert(isinstance(job_attrs, list)), 'jobs_attrs must be a list'
            assert(all(isinstance(attr_type, AttributeType) for attr_type in job_attrs)), 'The elements of jobs_attrs must be of :class:`.AttributeType` class.'

        self.obj_type = job_class
        self.obj_parameters = list(signature(self.obj_type).parameters)
        self.attrs_names = []
        self.mandatory_attrs = {}
        self.optional_attrs = {}
        self.job_mapper = job_mapper
        self.checked = False
        self._logger = logging.getLogger('accasim')
        self.remove_req_resources = []

        for attr in job_attrs:
            _attr_name = attr.name
            assert(_attr_name not in self.attrs_names + self.obj_parameters), '{} attribute name already set. Names must be unique'.format(_attr_name)
            if attr.optional:
                # assert(_attr_name in self.obj_parameters), '{} attribute name is mandatory.'.format(_attr_name)
                self.optional_attrs[_attr_name] = attr
            else:
                self.mandatory_attrs[_attr_name] = attr
            self.attrs_names.append(_attr_name)

    def check_requested_resources(self, job_attrs):
        """
        Checks if the requested resources in the dict include all the system resources.

        :param job_attrs: Array of job attribute names
        """
        _req_resources = job_attrs['requested_resources']
        missing_res = {r for r in self.system_resources if r not in _req_resources.keys()}
        remove_req = set(_req_resources.keys()) - set(self.system_resources) 
        
        if missing_res:
            self._logger.info('Some resources has not been included in the parser, assigning 0 to the {} resources in the job request.'.format(missing_res))
            required = {'core'}  # , 'mem'}
            inter = missing_res & required
            if inter and len(inter) != len(required):
                self._logger.error('Some mandatory attributes are missing: {}. The simulation will stop.'.format(inter))
                exit()
            self.missing_resources = missing_res
            
        if remove_req:
            self._logger.info('Job requests contains {} resources, but there are only {} resources. All the requests will be adapted.'.format(list(_req_resources.keys()), self.system_resources))
            
        self.checked = True
        return remove_req

    def resource_manager_setup(self):
        """

        The groups and system resources types are set.

        """
        self.group_resources = self.resource_manager.groups_available_resource()
        self.system_resources = self.resource_manager.system_resource_types()

    def set_resource_manager(self, resource_manager):
        if resource_manager:
            assert(isinstance(resource_manager, ResourceManager)), 'Only subclases of :class:`.resource_manager` are accepted.'
            self.resource_manager = resource_manager
            self.resource_manager_setup()

    def factory(self, **kwargs):
        """

        Creates a job instance with the dictionary received as argument. It verifies that all attributes has been included in the kwargs.

        :param \*\*kwargs: Dictionary with the job attributes.

        :return: Returns a job instance.

        """
        assert(self.resource_manager), 'Missing resource_manager attribute. It must be added via :func:`.set_resource_manager`.'
        for _old, _new in self.job_mapper.items():
            value = kwargs.pop(_old)
            kwargs[_new] = value
        _missing = list(filter(lambda x:x not in kwargs, set(self.obj_parameters + list(self.mandatory_attrs))))
        assert(not _missing), 'Missing attributes: {}'.format(', '.join(_missing))

        _obj_attr = {k:kwargs[k] for k in self.obj_parameters}

        if not self.checked:
            self.remove_req_resources = self.check_requested_resources(_obj_attr)
        
        for r in self.remove_req_resources:
            _obj_attr['requested_resources'].pop(r)        
        
        if hasattr(self, 'missing_resources'):
            for r in self.missing_resources:
                _obj_attr['requested_resources'][r] = 0

        _tmp = self.obj_type(**_obj_attr)
        setattr(_tmp, '_dict', kwargs)
        self.add_attrs(_tmp, self.mandatory_attrs, kwargs)
        self.add_attrs(_tmp, self.optional_attrs, kwargs)

        return _tmp

    def add_attrs(self, obj, reference, values):
        """

        Sets the attributes to the job object.

        :param obj: Object to be updated
        :param reference: Attribute type of reference. It contains the name, optionality and type for casting.
        :param values: Values to be added to the object

        """
        for _attr in reference:
            _type = reference[_attr].type
            _value = None
            if not reference[_attr].optional or (_attr in values and values[_attr]):
                _value = _type(values[_attr]) if _type else values[_attr]
            setattr(obj, _attr, _value)

    def add_request(self, obj):
        """

        This method sets the request of the job, it uses the resources available of the system to define it.

        :param obj: Job object

        """
        # Calculate only if it is not present
        if not hasattr(obj, 'requested_nodes'):
            _times = 0
            for _res in self.system_resources:
                _total_request = getattr(obj, _res)
                assert(_total_request >= 0), 'The request for {} is no feasible ({}). Accepted values are equal or greater than 0. Job {} must be tweaked before re-run. See the example.'.format(_res, _total_request, obj.id)
                _times = max([_times] + [round(getattr(obj, _res) / _resources[_res]) for _resources in self.group_resources.values()])
            setattr(obj, 'requested_nodes', _times)
        if not hasattr(obj, 'requested_resources'):
            _times = getattr(obj, 'requested_nodes')
            setattr(obj, 'requested_resources', {_res: getattr(obj, _res) // _times for _res in self.system_resources})            


class EventManager:

    def __init__(self, resource_manager, dispatcher, additional_data, **kwargs):
        """

        This class coordinates events submission, queueing and ending.

        :param resource_manager: Resource manager instance
        :param \*\*kwargs: nothing for the moment.

        """
        assert(isinstance(resource_manager, ResourceManager)), 'Wrong type for the resource_manager argument.'
        self.resource_manager = resource_manager
        self.dispatcher = dispatcher
        self.additional_data = additional_data
        self.constants = CONSTANT()
        # self.debug = debug
        # Stats
        self.first_time_dispatch = None
        self.last_run_time = None
        self.slowdowns = []
        self.wtimes = []

        self.current_time = None
        self.time_points = SortedSet()
        self.events = SortedDict()
        self.loaded = SortedDict()
        self.queued = []
        self.real_ending = SortedDict()
        self.running = []
        self.finished = 0
        
        self._logger = logging.getLogger('accasim')
        
        self._writers = []
        if self.constants.SCHEDULING_OUTPUT:
            _sched_fp = path.join(self.constants.RESULTS_FOLDER_PATH, self.constants.SCHED_PREFIX + self.constants.WORKLOAD_FILENAME)
            self._sched_writer = AsyncWriter(path=_sched_fp, pre_process_fun=EventManager._schd_write_preprocessor)
            self._writers.append(self._sched_writer)
        
        if self.constants.PPRINT_OUTPUT:
            _pprint_fp = path.join(self.constants.RESULTS_FOLDER_PATH, self.constants.PPRINT_PREFIX + self.constants.WORKLOAD_FILENAME)
            self._pprint_writer = AsyncWriter(path=_pprint_fp, pre_process_fun=EventManager._schd_pprint_preprocessor)
            self._writers.append(self._pprint_writer)
            
        for ad in self.additional_data:
            ad.set_event_manager(self)

    def load_events(self, es):
        """

        Jobs are loaded to the system. This is the first step for a job simulation.

        :param es: List of jobs. Jobs must be subclass of event class.

        """
        for ad in self.additional_data:
            ad.exec_before_submission(es)
               
        if isinstance(es, list):
            for e in es:
                assert(isinstance(e, Event)), 'Only subclasses of event can be simulated.'
                self.load_event(e)
        else:
            assert(isinstance(es, Event)), 'Only subclasses of event can be simulated.'
            self.load_event(es)

        for ad in self.additional_data:
            ad.exec_after_submission()

    def load_event(self, e):
        """

        Internal method for job submission.

        :param e: Single job (event subclass).

        """
        assert(isinstance(e, Event)), 'Using {}, expecting a single {}'.format(e.__class__, Event.__name__)
        if not self.current_time:
            self.current_time = e.queued_time - 1
            self.time_points.add(self.current_time)
        if self.current_time == e.queued_time:
            self.submit_event(e.id)
        elif self.current_time < e.queued_time:
            if e.queued_time not in self.loaded:
                 self.loaded[e.queued_time] = []
            self.loaded[e.queued_time].append(e.id)
            self.time_points.add(e.queued_time)
        else:
            raise EventException('Time sync problem, the actual event {} was loaded after ({}) the real submit time ({}). This a programming error, must be checked.'.format(e.id, self.current_time, e.queued_time))

    def move_to_finished(self, events_dict):
        """

        There are two time points for a job could ends, the expected one and the real one.
        The job must run until the real one is reached, then if a job is waiting to finish but is less than the
        real ending time, this value must be updated with the real one.

        :param events_dict: Actual Loaded, queued and running jobs in a dictionary {id: job object}

        :return: Array of completed jobs

        """
        _es = []
        for e_id in self.real_ending.pop(self.current_time, []):
            if e_id in self.running:
                self.running.remove(e_id)
                e = events_dict[e_id]
                self.finish_event(e)
                _es.append(e_id)
        if (not self.loaded and not self.running and not self.queued) and _es:
            self.last_run_time = self.current_time
        return _es

    def finish_event(self, e):
        """

        Internal method for Job's completion. This method sets the ending time, and make some standard calculations for statistics, such as slowdown, waiting time.
        Finally it calls the methods for output.

        :param e: Job to be completed.

        """
        e.end_time = self.current_time
        e.running_time = e.end_time - e.start_time
        e.waiting_time = e.start_time - e.queued_time
        e.slowdown = float("{0:.2f}".format((e.waiting_time + e.running_time) / e.running_time)) if e.running_time != 0 else e.waiting_time if e.waiting_time != 0 else 1.0
        self.slowdowns.append(e.slowdown)
        self.wtimes.append(e.waiting_time)
        self.finished += 1
        e.end_order = self.finished
        if self.constants.SCHEDULING_OUTPUT:
            self._sched_writer.push(e)
        if self.constants.PPRINT_OUTPUT:
            self._pprint_writer.push(e)

    def dispatch_event(self, _job, _time, _time_diff, _nodes):
        """

        Internal method for Job's dispatching. This method updates the related attributes for allocation of the job.

        :param _job: Job object
        :param _time: Time of dispatching
        :param _time_diff: Time used if dispatching processing _time must be considered.
        :param _nodes: Nodes to be allocated.

        :return: True if the allocation must be performed, false otherwise. False for jobs that have duration equal to 0

        """
        _id = _job.id
        start_time = _time + _time_diff
        
        #=======================================================================
        # started, started_ended, postponed
        # state = (0, 0, 0)
        #=======================================================================
        assert(self.current_time == start_time), 'Start _time is different to the current _time'

        # Try to allocate
        done, msg = self.resource_manager.allocate_event(_job, _nodes)
        if not done:
            self._logger.error('{} Must be postponed. Reason: {}. If you see this message many times, probably you have to check your allocation heuristic.'.format(_id, msg))
            self.submit_event(_id)
            return (0, 0, 1)

        # Continue with a successfully allocated job        
        # Update job info
        _job.start_time = start_time
        _job.assigned_nodes = _nodes

        # Initialize the output writers
        if self.first_time_dispatch == None:
            self.first_time_dispatch = start_time
            for writer in self._writers:
                writer.start()
        ans = (1, 0, 0)
        real_end_time = _job.start_time + _job.duration
        if _job.duration == 0:
            self._logger.trace('{}: {} Dispatched and Finished at the same moment. Job Lenght 0'.format(self.current_time, _id))
            ans = (0, 1, 0)
        else:
            # Include real ending time int time points
            self.time_points.add(real_end_time)

        # Move to running jobs
        self.running.append(_id)

        # Relate ending time with job id
        if real_end_time not in self.real_ending:
            self.real_ending[real_end_time] = []
        self.real_ending[real_end_time].append(_id)

        return ans    

    def submit_event(self, _id):
        """

        Internal method for Job's queueing.

        """
        self.queued.append(_id)

    def next_events(self):
        """

        Return the jobs that belongs to the next time point.

        :return: Array of jobs recently submitted + queued available at current time.

        """
        if len(self.time_points) > 0:
            self.current_time = self.time_points.pop(0)
        else:
            self._logger.trace('No more time points... but there still jobs in the queue')
            self.current_time += 1
        submitted = self.loaded.pop(self.current_time, [])
        new_queue = self.queued + submitted
        self._logger.trace('{} Next events: \n-Recently submited: {}\n-Already queued: {}'.format(self.current_time, submitted, self.queued))
        self.queued.clear()

        return new_queue

    def has_events(self):
        """

        :return: True if are loaded, queued or running jobs. False otherwise.

        """
        return (self.loaded or self.queued or self.running)
    
    def call_dispatcher(self, event_dict, events):
        """
            Call the defined dispatcher. In addition, before the dispatcher call, the exec_before_dispatching method of 
            AdditionalData objects is called passing the job dictionary and current queue job ids. After the dispatcher call, 
            it calls the exec_after_dispatching and pass the job dictionary, the dispatching tuple and the rejected list.    
            
            :return: Return a tuple with a list of jobs to dispatch and to reject.  
        """
        for ad in self.additional_data:
            ad.exec_before_dispatching(event_dict, events)
                         
        to_dispatch, rejected = self.dispatcher.schedule(self.current_time, event_dict, events)
        
        for ad in self.additional_data:
            _tmp = ad.exec_after_dispatching(event_dict, to_dispatch, rejected)
            if _tmp:
                to_dispatch, rejected = _tmp
        return to_dispatch, rejected

    def dispatch_events(self, event_dict, to_dispatch, time_diff, omit_timediff=True):
        """

        Internal method for processing the job's dispatching. Jobs are started if start time is equals to current time.

        :param event_dict: Actual Loaded, queued and running jobs in a dictionary {id: job object}
        :param to_dispatch: A tuple which contains the (start time, job id, nodes)
        :param time_diff: Time which takes the dispatching processing time.
        :param omit_timediff: If True the time spent in generating a decision is considered as 0. False this time is considered, and all events in \
            in [current time, current time + time diff] are moved to the new current time (current time + time diff). The latter isn't implemented. 

        :return return a tuple of (#dispatched, #Dispatched + Finished (0 duration), #postponed)
        """       
        if omit_timediff:
            time_diff = 0
        
        n_disp = 0
        n_disp_finish = 0
        n_post = 0
        
        for (_time, _id, _nodes) in to_dispatch:
            assert(isinstance(_id, str)), 'Please check your return tuple in your Dispatching method. _id must be a str type. Received wrong type: {}'.format(_id.__class__)
            assert(_time is None or _time >= self.current_time), 'Receiving wrong schedules.'

            #===================================================================
            # Time must be equal or later than current time.
            #     Equals will be dispatched in the momement, instead later ones, which will be requeued with expected ending time of the job that release the resources.
            # If the expected ending is surpass, because the job takes more time to finish, the time tuple\'s element must be None. '
            #===================================================================
            if not _nodes:
                # For blocked jobs
                if _time is not None and _time != self.current_time:
                    self.time_points.add(_time)
                #==============================================================
                # Maintaining the event in the queue
                #==============================================================
                self.submit_event(_id)
                n_post += 1
                continue
            
            #=======================================================================
            # started, started_ended, postponed
            #=======================================================================
            dispatched, ended, postponed = self.dispatch_event(event_dict[_id], _time, time_diff, _nodes)
            n_disp += dispatched
            n_disp_finish += ended
            n_post += postponed   
        
        if n_disp_finish > 0:
            self.release_ended_events(event_dict)        
        return (n_disp, n_disp_finish, n_post)

    def release_ended_events(self, event_dict):
        """

        Internal method for completed jobs. Removes from the dictionary finished jobs.

        :param event_dict: Actual Loaded, queued and running jobs in a dictionary {id: job object}

        :return: return Array list of jobs objects.

        """
        for ad in self.additional_data:
            ad.exec_before_completion()
            
        _es = self.move_to_finished(event_dict)
        
        if _es:
            _removed_jobs = []
            for _e in _es:
                self.resource_manager.remove_event(_e)
                # Freeing mem (finished events)
                _removed_jobs.append(event_dict.pop(_e))
            
            for ad in self.additional_data:
                ad.exec_after_completion(_removed_jobs)
                        
        return _es

    def simulated_status(self):
        """

        Show the current state of the system in terms of loaded, queued, running and finished jobs.

        :return: String including the system info.

        """
        return ('Loaded {}, Queued {}, Running {}, and Finished {} Jobs'.format(len(self.loaded), len(self.queued), len(self.running), self.finished))

    def availability(self):
        """

        Current availability of the system.

        :return: Return the availability of the system.

        """
        return self.resource_manager.current_availability

    def usage(self):
        """

        Current usage of the system

        :return: Return the usage of the system

        """
        return self.resource_manager.current_usage

    def simulated_current_time(self):
        """

        Current time

        :return: Return the current simulated time

        """
        return self.current_time

    def stop_writers(self):
        """
        Stops the output writer threads and closes the file streams
        """
        for i in range(len(self._writers)):
            self._writers[i].stop()
            self._writers[i] = None
        
        for ad in self.additional_data:
            ad.stop()
            
    @staticmethod
    def _schd_write_preprocessor(event):
        """
        To be used as a pre-processor for AsyncWriter objects applied to event schedules.
        Pre-processes an event object and converts it to a String representation.
        It uses the format specified in the SCHEDULE_OUTPUT constant.

        :param event: The event to be written to output
        """
        constants = CONSTANT()
        _dict = constants.SCHEDULE_OUTPUT
        _attrs = {}
        for a, av in _dict['attributes'].items():
            try:
                _attrs[a] = locate(av[-1])(*event.subattr(event, av[:-1]))
            except ValueError:
                _attrs[a] = 'NA'
        output_format = _dict['format']
        return output_format.format(**_attrs) + '\n'

    @staticmethod
    def _schd_pprint_preprocessor(event):
        """
        To be used as a pre-processor for AsyncWriter objects applied to pretty-print event schedules.
        Pre-processes an event object and converts it to a String representation.
        It uses the format specified in the PPRINT_SCHEDULE_OUTPUT constant.

        :param event: The event to be written to output
        """
        constants = CONSTANT()
        _dict = constants.PPRINT_SCHEDULE_OUTPUT
        _order = _dict['order']
        _attrs = {}
        for a, av in _dict['attributes'].items():
            try:
                _attrs[a] = locate(av[-1])(*event.subattr(event, av[:-1]))
            except ValueError:
                _attrs[a] = 'NA'
        output_format = _dict['format']
        values = [_attrs[k] for k in _order]
        if event.end_order == 1:
            return (output_format.format(*_order) + '\n', output_format.format(*values) + '\n')
        else:
            return output_format.format(*values) + '\n'

    def __str__(self):
        """

        Str representation of the event job_mapper
        
        :return: Return the current system info.

        """
        return 'Loaded: {}\nQueued: {}\nRunning: {}\nReal job finish on: {},\nFinished: {}\nNext time events: {}'.format(self.loaded, self.queued, self.running, self.real_ending, self.finished, self.time_points)


class EventException(Exception):
    pass
