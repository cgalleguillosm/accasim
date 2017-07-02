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
from time import clock as _clock
from datetime import datetime
from abc import abstractmethod, ABC
from accasim.utils.reader_class import reader
from accasim.utils.misc import CONSTANT, default_swf_mapper, watcher_demon
from accasim.base.event_class import event, event_mapper
from accasim.base.resource_manager_class import resource_manager 
from accasim.base.scheduler_class import scheduler_base
from accasim.base.event_class import job_factory
from threading import Thread, Event as THEvent
from os import getpid as _getpid
from psutil import Process as _Process

class simulator_base(ABC):

    def __init__(self, _resource_manager, _reader, _job_factory, _scheduler):
        self.constants = CONSTANT()
        self.real_init_time = datetime.now()
        assert(isinstance(_reader, reader))
        self.reader = _reader
        assert(isinstance(_resource_manager, resource_manager))
        self.resource_manager = _resource_manager
        assert(isinstance(_job_factory, job_factory))
        assert(self.check_request(_job_factory.attrs_names)), 'System resources must be included in Job Factory descrpition.'
        self.job_factory = _job_factory
        assert(isinstance(_scheduler, scheduler_base))
        self.scheduler = _scheduler

        self.mapper = event_mapper(self.resource_manager)

    @abstractmethod
    def start_simulation(self):
        raise NotImplementedError('Must be implemented!')

    @abstractmethod
    def load_events(self):
        raise NotImplementedError('Must be implemented!')

    def check_request(self, attrs_names):
        _system_resources = self.resource_manager.resources.system_resource_types
        for _res in _system_resources:
            if not(_res in attrs_names):
                return False
        return True


class hpc_simulator(simulator_base):
    
    def __init__(self, _resource_manager, _reader, _scheduler, _job_factory=None, **kwargs):
        if _job_factory is None:
            _job_factory = job_factory(_resource_manager, mapper=default_swf_mapper)
        simulator_base.__init__(self, _resource_manager, _reader, _job_factory, _scheduler)
        self.start_time = None
        self.end_time = None
        self.max_sample = 2
        self.daemons = {}
        self.loaded_jobs = 0
        self.benchFile = None
        
    def monitor_datasource(self, _stop):
        '''
        runs continuously and updates the global data
        Useful for daemons
        '''
        while (not _stop.is_set()):
            self.constants.running_at['current_time'] = self.mapper.current_time
            self.constants.running_at['running_jobs'] = {x: self.mapper.events[x] for x in self.mapper.running}
            time.sleep(self.constants.running_at['interval'])
    
    #===========================================================================
    # def daemon_init(self):         
    #     _iter_func = lambda act, next: act.get(next) if isinstance(act, dict) else (getattr(act, next)() if callable(getattr(act, next)) else getattr(act, next))
    #     for _name, d in self.daemons.items():
    #         _class = d['class']
    #         if not _class:
    #             continue
    #         _args = []
    #         for _arg in d['args']:
    #             if isinstance(_arg, tuple):
    #                 res = reduce(_iter_func, _arg[1].split('.'), self if not _arg[0] else _arg[0])
    #                 _args.append(res)
    #             else:
    #                 _args.append(_arg)
    #         self.daemons[_name]['object'] = _class(*_args)
    #         self.daemons[_name]['object'].start()
    #===========================================================================
            
    def start_simulation(self, watcher=False, *args, **kwargs):
        # TODO Load dynamically as daemon_init.
        # The initial values could be set in the simulation call, but also the datasource for these variables could be setted in the call.
        # Obviously the monitor must load also dynamically.
        #=======================================================================
        # running_at = {
        #     'interval': 1,
        #     'current_time': self.mapper.current_time,
        #     'running_jobs': {}
        # }
        # self.constants.load_constant('running_at', running_at)
        # _stop = THEvent()
        # monitor = Thread(target=self.monitor_datasource, args=[_stop])
        # simulation = Thread(target=self.start_hpc_simulation, args=args, kwargs=kwargs)
        # monitor.daemon = True
        # # simulation.daemon = True
        # monitor.start()
        # simulation.start()
        # # Starting the daemons
        # self.daemon_init()
        # simulation.join()
        # # Stopping the daemons
        # [d['object'].stop() for d in self.daemons.values() if d['object']]
        # _stop.set()
        #=======================================================================
        if watcher:
            functions = {
                'usage_function': self.mapper.usage,
                'availability_function': self.mapper.availability,
                'simulated_status_function': self.mapper.simulated_status,
                'current_time_function': self.mapper.simulated_current_time
            }
            self.daemons['watcher'] = {
                'class': watcher_demon,
                'args': [self.constants.WATCH_PORT, functions],
                'object': None
            }
        self.reader.open_file()
        #=======================================================================
        # if 'tweak_function' in kwargs:
        # 	_func = kwargs['tweak_function']
        # 	assert(callable(_func))
        # 	self.tweak_function = _func
        #=======================================================================
        # self.start_hpc_simulation(**kwargs)
        simulation = Thread(target=self.start_hpc_simulation, args=args, kwargs=kwargs)
        simulation.start()
        # Starting the daemons
        self.daemon_init()
        simulation.join()
        # Stopping the daemons    
        [d['object'].stop() for d in self.daemons.values() if d['object']]
        
    def start_hpc_simulation(self, _debug=False, tweak_function=None):
        #=======================================================================
        # The following list can be useful for improving the incremental loading
        # it includes queued (submission) points of all jobs.
        # When the file is sorted all queued times are returned
        #=======================================================================
        # kwargs['queued_times']        
        
        #=======================================================================
        # Load events corresponding at the "current time" and the next one
        #=======================================================================
        if hasattr(self.constants, 'benchmark_output_filepath') and self.constants.benchmark_output_filepath is not None:
            self.benchFile = open(self.constants.benchmark_output_filepath, 'w')
        else:
            self.benchFile = None
        event_dict = self.mapper.events
        self.start_time = _clock()
        self.constants.load_constant('start_time', self.start_time)
        
        self.load_events(event_dict, self.mapper, _debug, self.max_sample, tweak_function)
        events = self.mapper.next_events()

        #=======================================================================
        # Loop until there are not loaded, queued and running jobs
        #=======================================================================
        while events or self.mapper.has_events():
            _actual_time = self.mapper.current_time
            benchStartTime = _clock() * 1000
            if _debug:
                print('{} INI: Loaded {}, Queued {}, Running {}, Finished {}'.format(_actual_time, len(self.mapper.loaded), len(self.mapper.queued), len(self.mapper.running), len(self.mapper.finished)))
            self.mapper.release_ended_events(event_dict)

            queuelen = len(events)
            schedStartTime = _clock() * 1000
            schedEndTime = schedStartTime
            if events:
                if _debug:
                    print('{} DUR: To Schedule {}'.format(_actual_time, len(events)))
                to_dispatch = self.scheduler.schedule(self.mapper.current_time, event_dict, events, _debug)
                # to_dispatch = self.scheduler.schedule(self.mapper.current_time, event_dict, events, len(self.mapper.finished) > 15000)
                if _debug:
                    print('{} DUR: To Dispatch {}. {}'.format(_actual_time, len(to_dispatch), self.resource_manager.resources.usage()))
                time_diff = 0
                schedEndTime = _clock() * 1000
                try:
                    self.mapper.dispatch_events(event_dict, to_dispatch, time_diff, _debug)
                except AssertionError as e:
                    print('{} DUR: {}'.format(_actual_time, e))
                    print('{} DUR: Loaded {}, Queued {}, Running {}, Finished {}'.format(_actual_time, len(self.mapper.loaded), len(self.mapper.queued), len(self.mapper.running), len(self.mapper.finished)))
                    _exit()
                                   
            if _debug:
                print('{} END: Loaded {}, Queued {}, Running {}, Finished {}'.format(_actual_time, len(self.mapper.loaded), len(self.mapper.queued), len(self.mapper.running), len(self.mapper.finished)))

            #===================================================================
            # Loading next jobs
            #===================================================================
            if len(self.mapper.loaded) < 10:
                sample = self.max_sample if(len(self.mapper.loaded) < self.max_sample) else 2
                self.load_events(event_dict, self.mapper, _debug, sample, tweak_function)
            #===================================================================
            # Continue with next events            
            #===================================================================
            events = self.mapper.next_events()

            if self.benchFile is not None:
                benchEndTime = _clock() * 1000
                benchMemUsage = self.memory_usage_psutil()
                scheduleTime = schedEndTime - schedStartTime
                dispatchTime = benchEndTime - benchStartTime - scheduleTime
                self.write_to_benchmark(_actual_time, queuelen, benchEndTime - benchStartTime, scheduleTime, dispatchTime, benchMemUsage)

        if self.benchFile is not None:
            self.benchFile.close()
        self.end_time = _clock()
        assert((len(self.mapper.finished) == len(set(self.mapper.finished))))
        assert(self.loaded_jobs == len(self.mapper.finished)), 'Loaded {} and Finished {}'.format(self.loaded_jobs, len(self.mapper.finished))
        # self.statics_write_out()
        self.mapper.current_time = None

    def statics_write_out(self):
        wtimes = self.mapper.wtimes
        slds = self.mapper.slowdowns
        with open(self.constants.statistics_output_filepath, 'a') as f:
            f.write('Total jobs: %i\n' % (self.loaded_jobs))
            f.write('Makespan: %s\n' % (self.mapper.last_run_time - self.mapper.first_time_dispatch))
            f.write('Avg. waiting times: %s\n' % (reduce(lambda x, y: x + y, wtimes) / float(len(wtimes))))
            f.write('Avg. slowdown: %s\n' % (reduce(lambda x, y: x + y, slds) / float(len(slds))))

    def write_to_benchmark(self, time, queueSize, stepTime, schedTime, simTime, memUsage):
        """
            Writes to an output file the resource usage string related to the current simulation step.

            The output string contains 6 fields, corresponding to the input arguments, separated by ":", and can
            be easily parsed by any Python program.

        :param time: the timestamp relative to the simulation step
        :param queueSize: the size of the queue at the simulation step (before scheduling)
        :param stepTime: the total time required to perform the simulation step
        :param schedTime: the time related to the scheduling procedure
        :param simTime: the remaining time used in the step, related to the simulation process
        :param memUsage: memory usage (expressed in MB) at the simulation step
        """
        benchString = '%s:%s:%s:%s:%s:%s\n' % (time, queueSize, stepTime, schedTime, simTime, memUsage)
        self.benchFile.write(benchString)

    def load_events(self, jobs_dict, mapper, _debug=False, time_samples=2, dict_tweak=None):
        _time = None
        while not self.reader.EOF and time_samples > 0:
            _dicts = self.reader.next_dicts()
            if not _dicts:
                break
            tmp_dict = {}
            job_list = []
            for _dict in _dicts:
                if callable(dict_tweak):
                    dict_tweak(_dict)
                je = self.job_factory.factory(**_dict)
                if self.checkJobValidity(je):
                    self.loaded_jobs += 1
                    tmp_dict[je.id] = je
                    job_list.append(je)
                elif _debug:
                    print("Job %s violates the system's resource constraints and will be discarded" % je.id)
                if _time != je.queued_time:
                    _time = je.queued_time
                    time_samples -= 1
            mapper.load_events(job_list)
            jobs_dict.update(tmp_dict)

    def checkJobValidity(self, job):
        """
            Simple method that checks if the loaded job violates the system's resource constraints.

        :param job: Job object 
        :return: True if the job is valid, false otherwise
        """
        resGroups = self.resource_manager.groups_available_resource()
        validGroups = 0
        for group in resGroups.values():
            valid = True
            for k, res in job.requested_resources.items():
                if group[k] < res:
                    valid = False
                    break
            if valid:
                validGroups += 1
        return validGroups > 0

    def memory_usage_psutil(self):
        # return the memory usage in MB
        process = _Process(_getpid())
        memr = process.memory_info().rss / float(2 ** 20)
        return memr
    
    def daemon_init(self):         
        _iter_func = lambda act, next: act.get(next) if isinstance(act, dict) else (getattr(act, next)() if callable(getattr(act, next)) else getattr(act, next))
        for _name, d in self.daemons.items():
            _class = d['class']
            if not _class:
                continue
            _args = []
            for _arg in d['args']:
                if isinstance(_arg, tuple):
                    res = reduce(_iter_func, _arg[1].split('.'), self if not _arg[0] else _arg[0])
                    _args.append(res)
                else:
                    _args.append(_arg)
            self.daemons[_name]['object'] = _class(*_args)
            self.daemons[_name]['object'].start()
