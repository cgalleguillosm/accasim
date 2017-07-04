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

    # Request of resources, this attributes must cover all the resources declarated in the HPC2N.config
    # I.e. core and mem.
    total_cores = attribute_type('core', int)
    total_mem = attribute_type('mem', int)

    # This attributes are required to be set, if not by default are calculated. As in this example (explained in tweak_dict function)
    requested_nodes = attribute_type('requested_nodes', int)
    requested_resources = attribute_type('requested_resources', dict)

    new_attrs = [user_id, expected_time, total_cores, total_mem, requested_nodes, requested_resources]

    # Instanciate and return the Job Factory
    return job_factory(_rm, attrs=new_attrs, mapper=misc.default_swf_mapper)


def tweak_dict(_dict):
    # At this point, where this function is called, the _dict object have the assignation from the log data.
    # As in the SWF workload logs the numbers of cores are not expressed, just the number of requested processors, we have to tweak this information
    # i.e we replace the number of processors by the number of requested cores.
    _processors = _dict.pop('requested_number_processors') if _dict['requested_number_processors'] != -1 else _dict.pop(
        'allocated_processors')
    _memory = _dict.pop('requested_memory') if _dict['requested_memory'] != -1 else _dict.pop('used_memory')
    # For this system configuration. Each processor has 2 cores, then total required cores are calculated as
    # required_processor x 2 cores =  required cores of the job
    _dict['core'] = _processors * 2
    # The requested memory is given for each processor, therefore the total of requested memory is calculated as memory x processors = required memory of the job
    _dict['mem'] = _memory * _processors
    # If the following keys are not given, these are calculated by the job factory with the previous data
    # In this dataset there is no way to know how many cores of each node where requested, by default both cores are requested by processor
    # Each node has two processor, then it is possible to alocate upto 2 processor by node for the same job.
    _dict['requested_nodes'] = _processors
    _dict['requested_resources'] = {'core': 2, 'mem': _memory}
    # A final and important tweak is modifying the time corresponding to 0 time. For this workload as defined in the file
    # the 0 time corresponds to Sun Jul 28 09:04:05 CEST 2002 (1027839845 unix time)
    _dict['submit_time'] = _dict.pop('submit_time') + 1027839845
    assert (
    _dict['core'] >= 0), 'Please consider to clean your data cannot exists requests with any info about core request.'
    assert (
    _dict['mem'] >= 0), 'Please consider to clean your data cannot exists requests with any info about mem request.'
    return _dict


"""
Example of implementation of the HPC Simulator
"""

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
# The default parser is loaded if any parser is given. It contains the regular expressions reading SWF files
# By default lines starting with '$' are avoided
# It is possible to test only the first N readable lines
#reader = reader_class.reader(input_filepath, max_lines=100)
reader = reader_class.reader(input_filepath)
# Or ommit this value and read the entire file.
# reader = reader_class.reader(input_filepath, wlp)

# Load system configuration and create the resource manager instance
config = misc.load_config(_path.join(constant.CONFIG_FOLDER, constant.CONFIG_FILE))
resources = resources_class(**config)
rm = resources.resource_manager()

# Factory for generating custom Job Events
jf = custom_job_factory(rm)

# Random seed
_seed = 'test'
# Instantiation of the allocating algorithm, it is used by the scheduling algorithm
alloc = allocator_simple(_seed, rm)
# Instantiation of the scheduling algorithm
schldr = simple_heuristic(_seed, rm, alloc, 'fifo')

# constant.load_constant('start_time', time.clock())
# Output files
# Schedule must loaded by default
constant.load_constant('sched_output_filepath', os.path.join('results', 'sched-' + input_filename))
# Pretty print schedule
constant.load_constant('pprint_output_filepath', os.path.join('results', 'pprint-' + input_filename))
# Some statistics
constant.load_constant('statistics_output_filepath', os.path.join('results', 'stats-' + input_filename))
# Output file for the performance benchmark data
constant.load_constant('benchmark_output_filepath', os.path.join('results', 'bench-' + input_filename))
# Required for sorting the resources when are written out
constant.load_constant('resource_order', ['core', 'mem'])

# Clean the previous outputs
misc.clean_results(constant.sched_output_filepath, constant.pprint_output_filepath, constant.statistics_output_filepath)

# Instancing the simulator object
simulator = hpc_simulator(
    rm, reader, schldr
    #     daemon=daemons
)

print('- The simulator will start... %s' % (input_filepath))

# Starting the simulation
simulator.start_simulation(_debug=False, tweak_function=tweak_dict)
# with open(constant.statistics_output_filepath) as f:
#     print(f.read())
msg = '- Simulation time: %0.2f secs' % (simulator.end_time - simulator.start_time)
print(msg)
# with open(constant.statistics_output_filepath, 'a') as f:
#     f.write(msg)
# ===============================================================================
