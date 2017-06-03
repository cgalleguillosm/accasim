"""

"""
from os import path as _path
from accasim.utils import misc, reader_class
from accasim.base.resource_manager_class import resources_class 
from accasim.base.event_class import job_factory, attribute_type
import sys
from builtins import property

def custom_job_factory():
    # New attributes
    expected_time = attribute_type('expected_time', int)    
    new_attrs = [expected_time]        
    return job_factory(attrs=new_attrs, mapper=misc.default_swf_mapper)

CONFIG_FOLDER = 'config/'
ESSENTIALS_FILENAME = 'essentials.config'
      
# Singleton dictionary that holds all the global variables for the system
constant = misc.CONSTANT()

# The values are loaded using load_constant method that receive a dictionary.

constant.load_constants(misc.load_config(_path.join(CONFIG_FOLDER, ESSENTIALS_FILENAME)))
input_filepath = 'workloads/HPC2N-2002-2.2.1-cln.swf'

# Just requiered if the file isn't sorted by submit time.
# sort_file(input_filepath)

# Factory for generating custom Job Events
jf = custom_job_factory()

# Load the workload parser
# The default_swf_parse_config contains the regular expressions reading SWF files  
wlp = reader_class.workload_parser(*misc.default_swf_parse_config)
reader = reader_class.reader(input_filepath, wlp)
time_samples = 1
reader.open_file()
while not reader.EOF and time_samples > 0:
    # The reader object returns a dictionary for each matched line
    _dicts = reader.next_dicts(5)
    if _dicts:
        for _d in _dicts:
            print(_d)
            _d['expected_time'] = _d['requested_time']
            _job = jf.factory(**_d)
            print(_job.id, _job.expected_time)
        break
# Load system configuration and create the resource manager instance
config = misc.load_config(_path.join(constant.CONFIG_FOLDER, constant.CONFIG_FILE))
resources = resources_class(**config)
rm = resources.resource_manager()
 
# alloc = getattr(allocators, allocator_now)('test', rm)
# schldr = getattr(schedulers, scheduler_now)('test', rm, alloc, policy)
# _id_schd = schldr.get_id() + '-'
# target_folders = (constant.RESULTS_PATH, ['schedules', 'pprint', 'statistics'])
# target_folder_paths = {}
# for t_folder in target_folders[1]:
#     target_path = os.path.join(target_folders[0], t_folder, filename[:13])
#     if not os.path.exists(target_path):
#         os.makedirs(target_path)
#     target_folder_paths[t_folder] = target_path
# constant.load_constant('resource_manager_instance', rm)
# constant.load_constant('start_time', time.clock())
# constant.load_constant('input_filepath', input_filepath)
# constant.load_constant('output_filepath', os.path.join(target_folder_paths['schedules'], constant.RESULTS_FILE_PREFIX + _id_schd + filename))
# constant.load_constant('pprint_output_filepath', os.path.join(target_folder_paths['pprint'], constant.PPRINT_RESULTS_FILE_PREFIX + _id_schd + filename))
# constant.load_constant('statistics_output_filepath', os.path.join(target_folder_paths['statistics'], constant.STATISTICS_FILE_PREFIX + _id_schd + filename))
# clean_results(constant.output_filepath, constant.pprint_output_filepath, constant.statistics_output_filepath)
# 
# # Instancing the simulator object
# simulator = hpc_simulator(
#     reader_impl(input_filepath),
#     resource_manager=rm,
#     scheduler=schldr,
#     daemon=daemons
# )
# 
# print('- The simulator will start... %s%s' % (_id_schd, filename))
# simulator.start_simulation(_debug=args.debug)
# with open(constant.statistics_output_filepath) as f:
#     print(f.read())
# msg = '- Simulation time: %0.2f secs' % (simulator.end_time - simulator.start_time)
# print(msg)
# with open(constant.statistics_output_filepath, 'a') as f:
#     f.write(msg)
#===============================================================================
