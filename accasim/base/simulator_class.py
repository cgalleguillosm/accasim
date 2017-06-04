from time import clock as _clock
from datetime import datetime
from abc import abstractmethod, ABC
from accasim.utils.reader_class import reader
from accasim.utils.misc import CONSTANT
from accasim.base.event_class import event, event_mapper
from accasim.base.resource_manager_class import resource_manager 
from accasim.base.scheduler_class import scheduler_base
from accasim.base.event_class import job_factory
import sys

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
    
	def __init__(self, _resource_manager, _reader, _job_factory, _scheduler, **kwargs):
		simulator_base.__init__(self, _resource_manager, _reader, _job_factory, _scheduler)
		self.start_time = None
		self.end_time = None
		self.max_sample = 2
		if 'daemon' in kwargs:
			self.daemons = kwargs['daemon']
		self.loaded_jobs = 0
        
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
            
	def start_simulation(self, *args, **kwargs):  
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
		self.reader.open_file()
		self.start_hpc_simulation(True)
        
	def start_hpc_simulation(self, _debug=False, **kwargs):        
        #=======================================================================
        # The following list can be useful for improving the incremental loading
        # it includes queued (submission) points of all jobs.
        # When the file is sorted all queued times are returned
        #=======================================================================
        # kwargs['queued_times']        
        
        #=======================================================================
        # Load events corresponding at the "current time" and the next one
        #=======================================================================
		event_dict = self.mapper.events
		self.start_time = _clock()
        
		self.load_events(event_dict, self.mapper, self.max_sample)
		events = self.mapper.next_events()

        #=======================================================================
        # Loop until there are not loaded, queued and running jobs
        #=======================================================================
		while events or self.mapper.has_events():
			_actual_time = self.mapper.current_time        
			if _debug:
				print('{} INI: Loaded {}, Queued {}, Running {}, Finished {}'.format(_actual_time, len(self.mapper.loaded), len(self.mapper.queued), len(self.mapper.running), len(self.mapper.finished)))
			self.mapper.release_ended_events(event_dict)

			if events:                
				if _debug:
					print('{} DUR: To Schedule {}'.format(_actual_time, len(events)))              
				to_dispatch = self.scheduler.schedule(self.mapper.current_time, event_dict, events, _debug)
				# to_dispatch = self.scheduler.schedule(self.mapper.current_time, event_dict, events, len(self.mapper.finished) > 15000)
				if _debug:
					print('{} DUR: To Dispatch {}. {}'.format(_actual_time, len(to_dispatch), self.resource_manager.resources.usage()))
				time_diff = 0
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
				self.load_events(event_dict, self.mapper, sample)
            #===================================================================
            # Continue with next events            
            #===================================================================
			events = self.mapper.next_events()

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
                            
	def load_events(self, jobs_dict, mapper, job_tweak=None, time_samples=2):
		_time = None
		while not self.reader.EOF and time_samples > 0:
			_dicts = self.reader.next_dicts()
			tmp_dict = {}
			job_list = []
			for _dict in _dicts:
				je = self.job_factory.factory(**_dict)
				if callable(job_tweak):
					je = job_tweak(je)
				self.loaded_jobs += 1
				tmp_dict[je.id] = je
				job_list.append(je)
				if _time != je.queued_time:
					_time = je.queued_time
					time_samples -= 1
			mapper.load_events(job_list)
			jobs_dict.update(tmp_dict)
