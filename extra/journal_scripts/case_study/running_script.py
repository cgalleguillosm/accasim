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
from accasim.base.allocator_class import FirstFit, BestFit
from accasim.base.scheduler_class import FirstInFirstOut, ShortestJobFirst, LongestJobFirst, EASYBackfilling
from accasim.experimentation.experiment import Experiment

if __name__ == '__main__':
    workload = 'workloads/HPC2N-2002-2.2.1-cln.swf'
    sys_cfg = 'HPC2N.config'
    numRuns = 10

    for i in range(numRuns):
        experiment_name = 'Run_{}'.format(i)
        experiment = Experiment(experiment_name, workload, sys_cfg)
        sched_list = [FirstInFirstOut, ShortestJobFirst, LongestJobFirst, EASYBackfilling]    
        alloc_list = [FirstFit, BestFit]
        experiment.generate_dispatchers(sched_list, alloc_list)
        experiment.run_simulation()
        
    exit(0)
