from psutil import Process as _Process, NoSuchProcess
import subprocess, os
from time import sleep, perf_counter
from datetime import time, datetime
from argparse import ArgumentParser
import sys
from os.path import isdir
from os import makedirs

def memory_usage_psutil(pid):
    try:
        process = _Process(pid)
        memr = process.memory_info().rss / float(2 ** 20)
        pids = process.children(recursive=False)
        for s_pid in pids:
            s_process = _Process(s_pid)
            memr += (s_process.memory_info().rss / float(2 ** 20))
        return memr
    except NoSuchProcess as e:
        return 0
    
def dir_exists(dir_path, create=False):
    exists = isdir(dir_path)
    if create and not exists:
        makedirs(dir_path)
        return True
    return exists
    
if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-s', '--systems', type=str, nargs='+', help='System names', required=False)
    args = parser.parse_args()

    if args.systems:
        _systems = args.systems
    else:
        _systems = ('seth', 'ricc', 'mc')
    py_exec = sys.executable
    cmd = "{},sys_reject.py,{},{}"

    for system in _systems:
        exec_path = os.path.abspath('.')
        for i in range(10):
            print('instance {} iter {}'.format(system, i))
            FNULL = open(os.devnull, 'w')
            
            _cmd = cmd.format(py_exec, system, i)
            
            start = perf_counter()
            p_sim = subprocess.Popen(_cmd.split(','), cwd=exec_path, stdout=FNULL, stderr=subprocess.STDOUT)
            pid_sim = p_sim.pid
            
            dir_exists('results/{}'.format(system), True)
            
            with open('results/{}/memory_{}'.format(system, i), 'w') as f:                
                f.write('total\n')
            
            while p_sim.poll() is None:
                sim = memory_usage_psutil(pid_sim)
                with open('results/{}/memory_{}'.format(system, i), 'a') as f:
                    f.write("{}\n".format(sim))
                sleep(0.01)
            total_time = perf_counter() - start
            with open('results/times', 'a+') as f:
                f.write('{} {} {} \n'.format(system, i, total_time))
            sleep(10)
