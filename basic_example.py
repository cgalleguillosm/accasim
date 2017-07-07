from accasim.base.simulator_class import hpc_simulator
from accasim.base.scheduler_class import fifo_sched
from accasim.base.allocator_class import ffp_alloc

# workload = 'workloads/HPC2N-2002-2.2.1-cln.swf'
workload = 'workloads/workload.swf'
sys_cfg = 'config/HPC2N.config'

_seed = 'example'
allocator = ffp_alloc(_seed)
dispatcher = fifo_sched(_seed, allocator)
simulator = hpc_simulator(sys_cfg, workload, dispatcher, RESULTS_FOLDER_NAME='results', pprint_output=True, benchmark_output=True) 
simulator.start_simulation(1027839845)
