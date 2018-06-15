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
from argparse import ArgumentParser

from accasim.base.simulator_class import Simulator
from accasim.base.scheduler_class import JobVerification, FirstInFirstOut as fifo_sched
from accasim.base.allocator_class import FirstFit as ff_alloc


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('system', metavar='s', type=str, help='System name')
    parser.add_argument('iteration', metavar='i', type=str, help='Iteration number')
    args = parser.parse_args()
    
    i = args.iteration
    system = args.system
    
    workload = 'workloads/{}.swf'.format(system)
    sys_config = 'config/{}.config'.format(system)
    
    allocator = ff_alloc()
    dispatcher = fifo_sched(allocator, job_check=JobVerification.REJECT)
    
    simulator = Simulator(workload, sys_config, dispatcher, \
                    RESULTS_FOLDER_NAME='results/{}/{}'.format(system, i), LOG_LEVEL='INFO', \
                    pprint_output=False, scheduling_output=False, benchmark_output=False, \
                    system_status=False)
    simulator.start_simulation(generate_plot=False)
