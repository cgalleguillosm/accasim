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
import logging

from inspect import stack
from time import perf_counter as clock, sleep
from datetime import datetime
from abc import abstractmethod, ABC
from os import getpid, path
from psutil import Process
from _functools import reduce
from time import time
from queue import Queue
from logging import handlers
from sys import version_info

from accasim.utils.reader_class import DefaultReader, Reader
from accasim.utils.misc import CONSTANT, DEFAULT_SIMULATION, DEFAULT_SWF_MAPPER, load_config, clean_results
from accasim.utils.misc import SystemStatus
from accasim.utils.file import path_leaf, save_jsonfile, dir_exists
from accasim.base.event_class import EventManager, AttributeType
from accasim.base.resource_manager_class import Resources, ResourceManager
from accasim.base.scheduler_class import SchedulerBase
from accasim.base.event_class import JobFactory
from accasim.base.additional_data import AdditionalData, AdditionalDataError
from accasim.utils.async_writer import AsyncWriter
import traceback


class SimulatorBase(ABC):
    
    LOG_LEVEL_INFO = 'INFO'
    LOG_LEVEL_DEBUG = 'DEBUG'
    LOG_LEVEL_TRACE = 'TRACE'    
    
    def __init__(self, config_file=None, **kwargs):
        """

        Simulator base constructor

        :param resource_manager: Resource manager class instantiation
        :param reader: Reader class instantiation
        :param job_factory: Job Factory instantiation
        :param _dispatcher: Dispatcher instantiation
        :param config_file: Path to the config file in json format.
        :param \*\*kwargs: Dictionary of key:value parameters to be used in the simulator. It overwrites the current parameters. All parameters will be available on the constant variable

        """        
        self.constants = CONSTANT()
        self.timeout = kwargs.pop('timeout', None)
        self._id = kwargs.pop('id', None)
        self._log_level = kwargs.pop('LOG_LEVEL', self.LOG_LEVEL_INFO)
        self.define_default_constants(config_file, **kwargs)

        self._logger, self._logger_listener = self.define_logger()
        self.real_init_time = datetime.now()
        
        # self.dispatcher = None
        self.reader = None
        self.resource_manager = None
        self.job_factory = None
        self.mapper = None
        self.additional_data = None
               
        if self.constants.OVERWRITE_PREVIOUS:
            self.remove_previous()

    def define_logger(self):
        self._define_trace_logger()
        FORMAT = '%(asctime)-15s %(module)s-%(levelname)s: %(message)s'
        
        queue = Queue(-1)
        queue_handler = handlers.QueueHandler(queue)
        handler = logging.StreamHandler()
        handler.setLevel(self._log_level)
        listener = handlers.QueueListener(queue, handler)
        
        logger_name = 'accasim'
        logger = logging.getLogger(logger_name)    
        logger.addHandler(queue_handler)
        formatter = logging.Formatter(FORMAT)
        handler.setFormatter(formatter)
        logger.setLevel(getattr(logging, self._log_level))
        
        self.constants.load_constant('LOGGER_NAME', logger_name)
        return logger, listener
         
    def _define_trace_logger(self):
        level = logging.TRACE = logging.DEBUG - 5
    
        def log_logger(self, message, *args, **kwargs):
            if self.isEnabledFor(level):
                self._log(level, message, args, **kwargs)

        logging.getLoggerClass().trace = log_logger
    
        def log_root(msg, *args, **kwargs):
            logging.log(level, msg, *args, **kwargs)

        logging.addLevelName(level, "TRACE")
        logging.trace = log_root
        
    @abstractmethod
    def start_simulation(self):
        """

        Simulation initialization

        """
        raise NotImplementedError('Must be implemented!')

    @abstractmethod
    def load_events(self):
        """

        Method that loads the job from a datasource. Check the default implementation in the Simulator class.

        """
        raise NotImplementedError('Must be implemented!')

    def additional_data_init(self, _additional_data):
        """

        Initializes the additional_data classes or set the event manager in the objects

        :param _additional_data: A list of AdditionalData objects or classes

        :return: Return a list with all the AdditionalData objects ready to be executed

        """
        _ad = []
        for ad in _additional_data:
            if not ad:
                raise AdditionalDataError('AdditionalData class/object is None')
                
            if isinstance(ad, AdditionalData):
                ad.set_event_manager(self.mapper)
                _ad.append(ad)
            elif issubclass(ad, AdditionalData):
                _ad.append(ad(self.mapper))
            else:
                raise Exception('Additional data class must be a subclass of the AdditionalData class. Received {}'.format(AdditionalData.__class__))
        return _ad

    def check_request(self, attrs_names):
        """

        Verifies that the job factory attributes can be supported by the system resurces.

        :return: True if attributes are supported, False otherwise.

        """
        _system_resources = self.resource_manager.system_resource_types
        for _res in _system_resources:
            if not (_res in attrs_names):
                print('Resource \'{}\' is not included in the Job dict.'.format(_res))
                return False
        return True

    def generate_enviroment(self, config_path):
        """

        Generated the syntethic system from the config file

        :param config_path: Path the config file

        :return: resource manager object.

        """
        config = load_config(config_path)
        equiv = config.pop('equivalence', {})
        start_time = config.pop('start_time', 0)
        resources = Resources(**config)
        return ResourceManager(resources), equiv, start_time

    def define_filepaths(self, **kwargs):
        """

        Add to the kwargs useful filepaths.

        """
        kwargs['WORKLOAD_FILENAME'] = path_leaf(kwargs['WORKLOAD_FILEPATH'])[1]
        if 'RESULTS_FOLDER_PATH' not in kwargs:
            # New in 3.5 
            filename = stack()[-1].filename
            script_path, script_name = path_leaf(filename)
            rfolder = kwargs.pop('RESULTS_FOLDER_NAME')
            kwargs['RESULTS_FOLDER_PATH'] = path.join(script_path, rfolder)
        dir_exists(kwargs['RESULTS_FOLDER_PATH'], create=True)
        return kwargs

    def set_workload_input(self, workload_path, **kwargs):
        """

        Creates a default reader object

        :param workload_path: Path to the workload
        :param \*\*kwargs: extra arguments

        :return: A reader object

        """
        return DefaultReader(workload_path, **kwargs)

    def prepare_arguments(self, possible_arguments, arguments):
        """

        Verifies arguments for a specific instantation and create the dictionary.

        :Note:

            this method will be moved to misc

        :param possible_arguments: Required arguments.
        :param arguments: Available arguments.

        :return: Dictionary with the corresponding arguments.

        """
        return {k: v for k, v in arguments.items() if k in possible_arguments}

    def define_default_constants(self, config_filepath, **kwargs):
        """

        Defines the default constants of the simulator, and update if the user gives new values.

        :param config_filepath: Path to the config file in json format

        """
        config = DEFAULT_SIMULATION
        for k, v in config.items():
            if k not in kwargs:
                kwargs[k] = v
        if config_filepath:
            for k, v in load_config(config_filepath).items():
                kwargs[k] = v
        kwargs = self.define_filepaths(**kwargs)
        self.constants.load_constants(kwargs)

    def show_config(self):
        """

        Shows the current simulator config

        """
        self._logger.info('Initializing the simulator')
        self._logger.info('Settings: ')
        self._logger.info('\tSystem Configuration file: {}'.format(self.constants.SYS_CONFIG_FILEPATH))
        self._logger.info('\tWorkload file: {}'.format(self.constants.WORKLOAD_FILEPATH))
        self._logger.info('\tResults folder: {}{}.'.format(self.constants.RESULTS_FOLDER_PATH,
                                               ', Overwrite previous files' if self.constants.OVERWRITE_PREVIOUS else ''))
        self._logger.info('\t\t ({}) Dispatching Plan Output. Prefix: {}'.format(self.on_off(self.constants.SCHEDULING_OUTPUT),
                                                                     self.constants.SCHED_PREFIX))
        self._logger.info('\t\t ({}) Statistics Output. Prefix: {}'.format(self.on_off(self.constants.STATISTICS_OUTPUT),
                                                               self.constants.STATISTICS_PREFIX))
        self._logger.info('\t\t ({}) Dispatching Plan. Pretty Print Output. Prefix: {}'.format(
            self.on_off(self.constants.PPRINT_OUTPUT), self.constants.PPRINT_PREFIX))
        self._logger.info('\t\t ({}) Benchmark Output. Prefix: {}'.format(self.on_off(self.constants.BENCHMARK_OUTPUT),
                                                              self.constants.BENCHMARK_PREFIX))
        self._logger.info('Ready to Start')

    def on_off(self, state):
        """

        True: ON, False: OFF
        Just for visualization purposes.

        :param state: State of a constant. True or False

        """
        return 'ON' if state else 'OFF'

    def remove_previous(self):
        """

        To clean the previous results.

        """
        _wouts = [(self.constants.SCHEDULING_OUTPUT, self.constants.SCHED_PREFIX),
                  (self.constants.STATISTICS_OUTPUT, self.constants.STATISTICS_PREFIX),
                  (self.constants.PPRINT_OUTPUT, self.constants.PPRINT_PREFIX),
                  (self.constants.BENCHMARK_OUTPUT, self.constants.BENCHMARK_PREFIX)]

        _paths = [path.join(self.constants.RESULTS_FOLDER_PATH, _prefix + self.constants.WORKLOAD_FILENAME) for
                  state, _prefix in _wouts if state]
        clean_results(*_paths)

    def _clean_simulator_constants(self):
        self.constants.clean_constants()

    def _save_parameters(self, _parameters, filename='simulator_parameters.json'):
        filename_path = path.join(self.constants.RESULTS_FOLDER_PATH, filename)
        _dict = {
            _param: getattr(self.constants, _param) for _param in _parameters
        }
        save_jsonfile(filename_path, _dict)

    def _generated_filepaths(self):
        possible_filepaths = [
            (self.constants.STATISTICS_OUTPUT, self.constants.STATISTICS_PREFIX, path.join(self.constants.RESULTS_FOLDER_PATH, self.constants.STATISTICS_PREFIX + self.constants.WORKLOAD_FILENAME)),
            (self.constants.BENCHMARK_OUTPUT, self.constants.BENCHMARK_PREFIX, path.join(self.constants.RESULTS_FOLDER_PATH, self.constants.BENCHMARK_PREFIX + self.constants.WORKLOAD_FILENAME)),
            (self.constants.SCHEDULING_OUTPUT, self.constants.SCHED_PREFIX, path.join(self.constants.RESULTS_FOLDER_PATH, self.constants.SCHED_PREFIX + self.constants.WORKLOAD_FILENAME)),
            (self.constants.PPRINT_OUTPUT, self.constants.PPRINT_PREFIX, path.join(self.constants.RESULTS_FOLDER_PATH, self.constants.PPRINT_PREFIX + self.constants.WORKLOAD_FILENAME))
        ]
        return {f[1]:f[2] for f in possible_filepaths if f[0]}


class Simulator(SimulatorBase):
    """

    Default implementation of the SimulatorBase class.

    """

    def __init__(self, workload, sys_config, dispatcher, resource_manager=None, reader=None, job_factory=None,
                 additional_data=[], simulator_config=None, overwrite_previous=True,
                 scheduling_output=True, pprint_output=False, benchmark_output=False, statistics_output=True, save_parameters=None,
                 show_statistics=True, **kwargs):
        """

        Constructor of the HPC Simulator class.

        :param workload: Filepath to the workload, it is used by the reader. If a reader is not given, the default one is used.
        :param sys_config: Filepath to the synthetic system configuration. Used by the resource manager to create the system.
        :param dispatcher: Dispatching method
        :param resource_manager: Optional. Instantiation of the resource_manager class.
        :param reader: Optional. Instantiation of the reader class.
        :param job_factory: Optional. Instantiation of the job_factory class.
        :param additional_data: Optional. Array of Objects or Classes of AdditionalData class.
        :param simulator_config: Optional. Filepath to the simulator config. For replacing the misc.DEFAULT_SIMULATION parameters.
        :param overwrite_previous: Default True. Overwrite previous results.
        :param scheduling_output: Default True. Dispatching plan output. Format modificable in DEFAULT_SIMULATION
        :param pprint_output: Default False. Dispatching plan output in pretty print version. Format modificable in DEFAULT_SIMULATION
        :param benchmark_output: Default False. Measurement of the simulator and dispatcher performance.
        :param statistics_output: Default True. Statistic of the simulation.
        :param save_parameters: List of simulation name paremeters to be saved in the target results folder. None or empty for not saving the parameters.
        :param show_statistics: Default True. Show Statistic after finishing the simulation.
        :param \*\*kwargs: Optional parameters to be included in the Constants.

        """ 
        assert(version_info >= (3, 5,)), 'Unsupported python version. Try with 3.5 or newer.' 
        
        kwargs['OVERWRITE_PREVIOUS'] = overwrite_previous
        kwargs['SYS_CONFIG_FILEPATH'] = sys_config
        kwargs['WORKLOAD_FILEPATH'] = workload
        kwargs['SCHEDULING_OUTPUT'] = scheduling_output
        kwargs['PPRINT_OUTPUT'] = pprint_output
        kwargs['BENCHMARK_OUTPUT'] = benchmark_output
        kwargs['STATISTICS_OUTPUT'] = statistics_output
        kwargs['SHOW_STATISTICS'] = show_statistics
        
        _uargs = []
        
        if not resource_manager:
            resource_manager, equiv, start_time = self.generate_enviroment(sys_config)
            kwargs['equivalence'] = equiv
            kwargs['start_time'] = start_time
        if not job_factory:
            kwargs['job_mapper'] = DEFAULT_SWF_MAPPER
            if not kwargs.get('EXTENDED_JOB_DESCRIPTION', False):
                kwargs['job_attrs'] = self.default_job_description()
            else:
                kwargs['job_attrs'] = self.extended_job_description()
            _jf_arguments = ['job_class', 'job_attrs', 'job_mapper']
            args = self.prepare_arguments(_jf_arguments, kwargs)
            _uargs += _jf_arguments
            job_factory = JobFactory(resource_manager, **args)
        if workload and not reader:
            _reader_arguments = ['max_lines', 'tweak_function', 'equivalence', 'start_time']
            args = self.prepare_arguments(_reader_arguments, kwargs)
            reader = self.set_workload_input(workload, job_factory=job_factory, **args)
            _uargs += _reader_arguments
        
        for _u in _uargs:
            kwargs.pop(_u, None)
        SimulatorBase.__init__(self, config_file=simulator_config, **kwargs)
        
        if not isinstance(additional_data, list):
            assert (isinstance(additional_data, AdditionalData) or issubclass(additional_data,
                                                                               AdditionalData)), 'Only subclasses of AdditionalData class are acepted as additional_data argument '
            additional_data = [additional_data]
        
        assert (isinstance(resource_manager, ResourceManager))
        self.resource_manager = resource_manager
        
        assert (isinstance(dispatcher, SchedulerBase))
        dispatcher.set_resource_manager(resource_manager)
        # self.dispatcher = dispatcher
            
        assert (isinstance(reader, Reader))
        self.reader = reader
                
        assert (isinstance(job_factory, JobFactory))
        self.job_factory = job_factory
        
        additional_data = self.additional_data_init(additional_data)
        self.mapper = EventManager(self.resource_manager, dispatcher, additional_data)

        if save_parameters:
            self._save_parameters(save_parameters)

        if benchmark_output:
            self._usage_writer = AsyncWriter(path=path.join(self.constants.RESULTS_FOLDER_PATH,
                self.constants.BENCHMARK_PREFIX + self.constants.WORKLOAD_FILENAME),
                pre_process_fun=Simulator.usage_metrics_preprocessor)
            self._process_obj = Process(getpid())
        else:
            self._usage_writer = None
            self._process_obj = None

        self.start_simulation_time = None
        self.end_simulation_time = None
        self.max_sample = 2
        self.daemons = {}
        self.loaded_jobs = 0
        self.dispatched_jobs = 0
        self.rejected_jobs = 0

    def monitor_datasource(self, _stop):
        """

        runs continuously and updates the global data
        Useful for daemons

        :param _stop: Signal for stop

        """
        while (not _stop.is_set()):
            self.constants.running_at['current_time'] = self.mapper.current_time
            self.constants.running_at['running_jobs'] = {x: self.mapper.events[x] for x in self.mapper.running}
            sleep(self.constants.running_at['interval'])

    def start_simulation(self, system_status=False, **kwargs):
        """

        Initializes the simulation

        :param init_unix_time: Adjustement for job timings. If the first job corresponds to 0, the init_unix_time must corresponds to the real submit time of the workload. Otherwise, if the job contains the real submit time, init_unix_time is 0.
        :param system_status: Initializes the system status daemon.
        :param system_utilization: Initializes the running jobs visualization using matplotlib.
        :param \*\*kwargs: a 'tweak_function' to deal with the workloads.

        """
        #=======================================================================
        # System status is the main entry point to get access to the current 
        # simulation data. It is used also for the visualization component.
        #=======================================================================
        if system_status:
            functions = {
                'usage_function': self.mapper.usage,
                'availability_function': self.mapper.availability,
                'simulated_status_function': self.mapper.simulated_status,
                'current_time_function': self.mapper.simulated_current_time
            }
            self.daemons['system_status'] = {
                'class': SystemStatus,
                'args': [self.constants.WATCH_PORT, functions],
                'object': None
            }        
        
        # @TODO
        # Add the usage_writer to the daemons array to auto-on/off process
        self._logger_listener.start() 
        if self._usage_writer:
            self._usage_writer.start()
            
        # Starting the daemons
        self.daemon_init()
        
        self.show_config()
        sim_error = None
        try:
            self.start_hpc_simulation(**kwargs)
        except Exception as e:
            sleep(1)
            print('The simulation will be stopped. Reason: {}'.format(e))        
            sim_error = e
        
        [d['object'].stop() for d in self.daemons.values() if d['object']]
                     
        if self._usage_writer:
            self._usage_writer.stop()
            self._usage_writer = None
 
        # @TODO
        self.mapper.stop_writers()
        
        filepaths = self._generated_filepaths()
        self._clean_simulator_constants()
        self._logger_listener.stop()
        if sim_error:
            raise sim_error
        return filepaths

    def start_hpc_simulation(self, **kwargs):
        """

        Initializes the simulation in a new thread. It is called by the start_timulation using its arguments.

        """
        if self.timeout:
            init_sim_time = time()
            ontime = True
        # =======================================================================
        # Load events corresponding at the "current time" and the next one
        # =======================================================================
        event_dict = self.mapper.events
        self.start_simulation_time = clock()
        self.constants.load_constant('start_simulation_time', self.start_simulation_time)

        self._logger.info('Starting the simulation process.')
        self.load_events(self.mapper.current_time, event_dict, self.mapper, self.max_sample)
        events = self.mapper.next_events()

        # =======================================================================
        # Loop until there are not loaded, queued and running jobs
        # =======================================================================
        while events or self.mapper.has_events():
            current_time = self.mapper.current_time
            benchStartTime = clock() * 1000
            self.mapper.release_ended_events(event_dict)
            queued_len = len(events)

            # ===================================================================
            # External behavior
            # ===================================================================
            # self.execute_additional_data()

            schedEndTime = schedStartTime = clock() * 1000

            if events:
                # to_dispatch, rejected = self.dispatcher.schedule(self.mapper.current_time, event_dict, events)
                to_dispatch, rejected = self.mapper.call_dispatcher(event_dict, events)
                for r in rejected:
                    del event_dict[r]
                dispatched_len = len(to_dispatch)
                rejected_len = len(rejected)
                assert(queued_len == dispatched_len + rejected_len), 'Some queued jobs ({}/{}) were not included in the dispatching decision.'\
                    .format(dispatched_len + rejected_len, queued_len)
                self.rejected_jobs += rejected_len
                
                schedEndTime = clock() * 1000
                time_diff = int((schedEndTime - schedStartTime) / 1000)
                
                (n_disp, n_disp_finish, n_post) = self.mapper.dispatch_events(event_dict, to_dispatch, time_diff)
                self.dispatched_jobs += n_disp + n_disp_finish
                
            # ===================================================================
            # Loading next jobs based on Time points
            # ===================================================================
            if len(self.mapper.loaded) < 10:
                sample = self.max_sample if (len(self.mapper.loaded) < self.max_sample) else 2
                self.load_events(current_time, event_dict, self.mapper, sample)
            # ===================================================================
            # Continue with next events
            # ===================================================================
            events = self.mapper.next_events()

            if self.constants.BENCHMARK_OUTPUT:                
                benchEndTime = clock() * 1000
                benchMemUsage = self._process_obj.memory_info().rss / float(2 ** 20)
                scheduleTime = schedEndTime - schedStartTime
                dispatchTime = benchEndTime - benchStartTime - scheduleTime
                
                self._usage_writer.push((current_time, queued_len, benchEndTime - benchStartTime, scheduleTime,
                                        dispatchTime, benchMemUsage))

            if self.timeout and self.timeout <= int(time() - init_sim_time):
                ontime = False
                break
            
        self.end_simulation_time = clock()
        if not self.timeout or self.timeout and ontime:
            assert (self.loaded_jobs == self.dispatched_jobs + self.rejected_jobs), \
                'Loaded {} != dispatched + rejected {} jobs'.format(self.loaded_jobs, self.dispatched_jobs + self.rejected_jobs)

        self.statics_write_out(self.constants.SHOW_STATISTICS, self.constants.STATISTICS_OUTPUT)
        self._logger.info('Simulation process completed.')
        self.mapper.current_time = None

    @staticmethod
    def usage_metrics_preprocessor(entry):
        """
        To be used as a pre-processor for AsyncWriter objects applied to usage metrics.
        Pre-processes a tuple of usage metrics containing 6 fields. The fields are the following:

        - time: the timestamp relative to the simulation step
        - queueSize: the size of the queue at the simulation step (before scheduling)
        - stepTime: the total time required to perform the simulation step
        - schedTime: the time related to the scheduling procedure
        - simTime: the remaining time used in the step, related to the simulation process
        - memUsage: memory usage (expressed in MB) at the simulation step

        :param entry: Tuple of data to be written to output
        """
        sep_token = ';'
        bline = sep_token.join([str(v) for v in entry]) + '\n'
        return bline

    def statics_write_out(self, show, save):
        """

        Write the statistic output file

        :param show: True for showing the statistics, False otherwise.
        :param save: True for saving the statistics, False otherwise.

        """
        if not (show or save):
            return
        wtimes = self.mapper.wtimes
        slds = self.mapper.slowdowns
        sim_time_ = 'Simulation time: {0:.2f} secs\n'.format(self.end_simulation_time - self.start_simulation_time)
        disp_method_ = 'Dispathing method: {}\n'.format(self.mapper.dispatcher)
        total_jobs_ = 'Total jobs: {}\n'.format(self.loaded_jobs)
        makespan_ = 'Makespan: {}\n'.format(self.mapper.last_run_time - self.mapper.first_time_dispatch if self.mapper.last_run_time and self.mapper.first_time_dispatch else 'NA')
        if wtimes:
            avg_wtimes_ = 'Avg. waiting times: {:.2f}\n'.format(reduce(lambda x, y: x + y, wtimes) / float(len(wtimes)))
        else:
            avg_wtimes_ = 'Avg. waiting times: NA\n'
            
        if slds:
            avg_slowdown_ = 'Avg. slowdown: {:.2f}\n'.format(reduce(lambda x, y: x + y, slds) / float(len(slds)))
        else:
            avg_slowdown_ = 'Avg. slowdown: NA\n'
            
        if show:
            self._logger.info('\t ' + sim_time_[:-1])
            self._logger.info('\t ' + disp_method_[:-1])
            self._logger.info('\t ' + total_jobs_[:-1])
            self._logger.info('\t ' + makespan_[:-1])
            self._logger.info('\t ' + avg_wtimes_[:-1])
            self._logger.info('\t ' + avg_slowdown_[:-1])
            
        if save:
            _filepath = path.join(self.constants.RESULTS_FOLDER_PATH,
                                   self.constants.STATISTICS_PREFIX + self.constants.WORKLOAD_FILENAME)
            with open(_filepath, 'a') as f:
                f.write(sim_time_)
                f.write(disp_method_)
                f.write(total_jobs_)
                f.write(makespan_)
                f.write(avg_wtimes_)
                f.write(avg_slowdown_)

    def load_events(self, current_time, jobs_dict, mapper, time_samples=2):
        """

        Incremental loading. Load the new jobs into the

        :param current_time: Current simulation time.
        :param jobs_dict: Dictionary of the current load, queued and running jobs
        :param mapper: Job event mapper object
        :param time_samples: Default 2. It load the next two time steps.

        """
        next_tpoints, parsed_jobs = self.reader.next(current_time, time_points=time_samples)
        tmp_dict = {}
        job_list = []

        for next_tpoint in next_tpoints:
            for job in parsed_jobs[next_tpoint]:
                self.loaded_jobs += 1
                tmp_dict[job.id] = job
                job_list.append(job)
        mapper.load_events(job_list)
        jobs_dict.update(tmp_dict)

    def _loaded_jobs(self):
        return sum([len(jobs) for _, jobs in self.mapper.loaded.items()])

    def daemon_init(self):
        """

        Initialization of the simulation daemons. I.e. system_utilization or system_status

        """
        _iter_func = lambda act, next: act.get(next) if isinstance(act, dict) else (
        getattr(act, next)() if callable(getattr(act, next)) else getattr(act, next))
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

    def default_job_description(self):
        """

        Method that returns the minimal attributes of a job. Default values: ID, Expected Duration, CORE and MEM.
        
        :return: Array of Attributes

        """
        # Attribute to identify the user
        user_id = AttributeType('user_id', int)

        # New attributes required by the Dispatching methods.
        expected_duration = AttributeType('expected_duration', int)
        queue = AttributeType('queue', int)

        # Default system resources: core and mem.
        total_cores = AttributeType('core', int)
        # total_mem = AttributeType('mem', int)

        # return [total_cores, total_mem, expected_duration, queue, user_id]
        return [total_cores, expected_duration, queue, user_id]

    def extended_job_description(self):
        """

        Method that returns extra attributes of a job. By default it includes the default attributes: ID, Expected Duration, CORE and MEM.
        
        :return: Array of Attributes

        """
        description = self.default_job_description()
        
        executable = AttributeType('executable', str, optional=True)
        group_id = AttributeType('group_id', int, optional=True)
        status = AttributeType('status', int, optional=True)
        partition = AttributeType('partition', int, optional=True)
        
        return description + [executable, group_id, status]
