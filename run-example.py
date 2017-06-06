"""

"""
from os import path as _path
from accasim.utils import misc, reader_class
from accasim.base.resource_manager_class import resources_class 
from accasim.base.event_class import job_factory, attribute_type
from accasim.base.allocator_class import allocator_simple
from accasim.base.scheduler_class import simple_heuristic
from accasim.base.simulator_class import hpc_simulator
import sys
import os
from builtins import property

def custom_job_factory(_rm):
    # New attribute to identify the job
    user_id = attribute_type('user_id', int)
    # New attributes required by the Dispatching methods.
    expected_time = attribute_type('expected_duration', int)

    # Request of resources 
    total_cores = attribute_type('core', int)
    total_mem = attribute_type('mem', int)
    requested_nodes = attribute_type('requested_nodes', int)
    requested_resources = attribute_type('requested_resources', dict) 
    total_mem = attribute_type('mem', int)
    new_attrs = [user_id, expected_time, total_cores, total_mem, requested_nodes, requested_resources]        
    # return job_factory(_rm, attrs=new_attrs, mapper=misc.default_swf_mapper)
    return job_factory(_rm, attrs=new_attrs, mapper=misc.default_swf_mapper)

def tweak_dict(_dict):
    # At this point, where this function is called, the dict for the job have the assignation from the log data.
    # As in the SWF workload logs the numbers of cores are not expressed, just the number of requested processors, we have to tweak this information
    # i.e we replace the processor for core entry. 
    _processors = _dict.pop('requested_number_processors')
    _memory = _dict.pop('requested_memory')
    # For this system configuration. Each processor has 2 cores, then required_processor x 2 cores =  required cores of the job 
    _dict['core'] = _processors * 2 if _processors != -1 else _dict.pop('allocated_processors') * 2
    # The requested memory is given for each processor then memory x processors = required memory of the job
    _dict['mem'] = _memory * _processors if _memory != -1 else _dict.pop('used_memory') * _processors
    # If the following keys are not given, these are calculated by the job factory with the previous data
    # In this dataset there is no way to know how many cores of each node where requested, by default both cores are requested by processor
    # Each node has two processor, then it is possible to alocate upto 2 processor by node for the same job.
    _dict['requested_nodes'] = _processors  
    _dict['requested_resources'] = {'core': 2, 'mem': _memory}
    # A final and important tweak is modifying the time corresponding to 0 time. For this workload as defined in the file
    # the 0 time corresponds to Sun Jul 28 09:04:05 CEST 2002 (1027839845 unix time)
    _dict['submit_time'] = _dict.pop('submit_time') + 1027839845
    print(_dict)
    return _dict

CONFIG_FOLDER = 'config/'
ESSENTIALS_FILENAME = 'essentials.config'
      
# Singleton dictionary that holds all the global variables for the system
constant = misc.CONSTANT()

# The values are loaded using load_constant method that receive a dictionary.

constant.load_constants(misc.load_config(_path.join(CONFIG_FOLDER, ESSENTIALS_FILENAME)))
input_filename = 'HPC2N-2002-2.2.1-cln.swf'
input_filepath = os.path.join('workloads/', input_filename)

# Just requiered if the file isn't sorted by submit time.
# sort_file(input_filepath)

# Load the workload parser
# The default_swf_parse_config contains the regular expressions reading SWF files  
wlp = reader_class.workload_parser(*misc.default_swf_parse_config)
reader = reader_class.reader(input_filepath, wlp)
#===============================================================================
# time_samples = 1
# reader.open_file()
# while not reader.EOF and time_samples > 0:
#     # The reader object returns a dictionary for each matched line
#     _dicts = reader.next_dicts(5)
#     if _dicts:
#         for _d in _dicts:
#             print(_d)
#             _d['expected_time'] = _d['requested_time']
#             _job = jf.factory(**_d)
#             print(_job.id, _job.expected_time)
#         break
#===============================================================================
# Load system configuration and create the resource manager instance
config = misc.load_config(_path.join(constant.CONFIG_FOLDER, constant.CONFIG_FILE))
resources = resources_class(**config)
rm = resources.resource_manager()
 
# Factory for generating custom Job Events
jf = custom_job_factory(rm)
 
# alloc = getattr(allocators, allocator_now)('test', rm)
_seed = 'test'
alloc = allocator_simple(_seed, rm)
schldr = simple_heuristic(_seed, rm, alloc, 'fifo')
_id_schd = schldr.get_id()
# constant.load_constant('resource_manager_instance', rm)
# constant.load_constant('start_time', time.clock())
# constant.load_constant('input_filepath', input_filepath)
constant.load_constant('sched_output_filepath', os.path.join('results', 'sched-' + input_filename))
constant.load_constant('pprint_output_filepath', os.path.join('results', 'pprint-' + input_filename))
constant.load_constant('stats_output_filepath', os.path.join('results', 'stats-' + input_filename))
constant.load_constant('resource_order', ['core', 'mem'])
# constant.load_constant('pprint_output_filepath', os.path.join(target_folder_paths['pprint'], constant.PPRINT_RESULTS_FILE_PREFIX + _id_schd + filename))
# constant.load_constant('statistics_output_filepath', os.path.join(target_folder_paths['statistics'], constant.STATISTICS_FILE_PREFIX + _id_schd + filename))
# clean_results(constant.output_filepath, constant.pprint_output_filepath, constant.statistics_output_filepath)
# 
# Instancing the simulator object
# _resource_manager, _reader, _job_factory, _scheduler
simulator = hpc_simulator(
    rm, reader, jf, schldr
    #     daemon=daemons
)
 
print('- The simulator will start... %s%s' % (_id_schd, input_filepath))
# simulator.start_simulation(_debug=args.debug)
simulator.start_simulation(_debug=True, tweak_function=tweak_dict)
# simulator.start_simulation(_debug=True)
# with open(constant.statistics_output_filepath) as f:
#     print(f.read())
msg = '- Simulation time: %0.2f secs' % (simulator.end_time - simulator.start_time)
print(msg)
# with open(constant.statistics_output_filepath, 'a') as f:
#     f.write(msg)
#===============================================================================
