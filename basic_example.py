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
from accasim.base.simulator_class import hpc_simulator
from accasim.base.scheduler_class import fifo_sched
from accasim.base.allocator_class import ffp_alloc

# workload = 'workloads/HPC2N-2002-2.2.1-cln.swf'
workload = 'workloads/workload.swf'
sys_cfg = 'config/HPC2N.config'

_seed = 'example'
allocator = ffp_alloc()
dispatcher = fifo_sched(allocator)
simulator = hpc_simulator(sys_cfg, workload, dispatcher, RESULTS_FOLDER_NAME='results') 
simulator.start_simulation(1027839845)
