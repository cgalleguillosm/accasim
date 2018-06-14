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
from accasim.base.scheduler_class import FirstInFirstOut, JobVerification
from accasim.base.scheduler_class import ShortestJobFirst, LongestJobFirst, EASYBackfilling
from accasim.experimentation.experiment import Experiment
import os

if __name__ == '__main__':
    workload = 'workloads/HPC2N-2002-2.2.1-cln.swf'
    sys_cfg = 'config/HPC2N.config'
    numRuns = 10

    for i in range(numRuns):
        childPid = os.fork()
        if childPid == 0:
            experiment = Experiment('Run_' + str(i), workload, sys_cfg)
            dispatcher1 = FirstInFirstOut(FirstFit(), job_check=JobVerification.CHECK_REQUEST)
            experiment.add_dispatcher('FIFO-FF', dispatcher1)
            dispatcher2 = FirstInFirstOut(BestFit(), job_check=JobVerification.CHECK_REQUEST)
            experiment.add_dispatcher('FIFO-BF', dispatcher2)
            dispatcher3 = LongestJobFirst(FirstFit(), job_check=JobVerification.CHECK_REQUEST)
            experiment.add_dispatcher('LJF-FF', dispatcher3)
            dispatcher4 = LongestJobFirst(BestFit(), job_check=JobVerification.CHECK_REQUEST)
            experiment.add_dispatcher('LJF-BF', dispatcher4)
            dispatcher5 = ShortestJobFirst(FirstFit(), job_check=JobVerification.CHECK_REQUEST)
            experiment.add_dispatcher('SJF-FF', dispatcher5)
            dispatcher6 = ShortestJobFirst(BestFit(), job_check=JobVerification.CHECK_REQUEST)
            experiment.add_dispatcher('SJF-BF', dispatcher6)
            dispatcher7 = EASYBackfilling(FirstFit(), job_check=JobVerification.CHECK_REQUEST)
            experiment.add_dispatcher('EBF-FF', dispatcher7)
            dispatcher8 = EASYBackfilling(BestFit(), job_check=JobVerification.CHECK_REQUEST)
            experiment.add_dispatcher('EBF-BF', dispatcher8)
            experiment.run_simulation(generate_plot=False)
            exit(0)
        else:
            os.waitpid(childPid, 0)
    exit(0)
