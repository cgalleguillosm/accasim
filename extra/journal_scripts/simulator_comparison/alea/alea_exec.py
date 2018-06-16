from psutil import Process as _Process, NoSuchProcess
import subprocess, os
from time import sleep, perf_counter
from string import Template
from os.path import isdir
from os import makedirs

ALEA_CONFIG_FP = 'alea/configuration.properties'

def memory_usage_psutil(pid):
    try:
        process = _Process(pid)
        memr = process.memory_info().rss / float(2 ** 20)
        pids = process.children(recursive=True)
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
    
def delete_file(fp):
    if os.path.exists(fp):
        os.remove(fp)
    
def define_config(system, start_time, total_jobs):
    t = None
    with open('base_configuration.properties') as f:
        t = Template(f.read())
    config_data = t.substitute({'system': system, 'start_time': start_time, 'total_jobs':total_jobs})
    
    with open(ALEA_CONFIG_FP , 'w') as f:
        f.write(config_data)
    
    
if __name__ == '__main__':

    # System name, start time, wl size
    instances = (('seth', 1027839845, 200500), ('ricc', 1272639895, 445000), ('mc', 1356994806, 5731100),)
    exec_path = os.path.join(os.path.abspath('.'), 'alea')
    
    for (instance, start_time, total_jobs) in instances:
        
        define_config(instance, start_time, total_jobs)
        
        for i in range(10):
            print('instance {} iter {}'.format(instance, i))
            FNULL = open(os.devnull, 'w')
            cmd = "java,-jar,Alea.jar"
            
            start = perf_counter()
            p_sim = subprocess.Popen(cmd.split(','), cwd=exec_path, stdout=FNULL, stderr=subprocess.STDOUT)
            pid_sched = p_sim.pid
            
            dir_exists('results/{}'.format(instance), True)
            
            with open('results/{}/memory_{}'.format(instance, i), 'w') as f:
                f.write('simulator\n')
            
            while p_sim.poll() is None:
                sched = memory_usage_psutil(pid_sched)
                with open('results/{}/memory_{}'.format(instance, i), 'a') as f:
                    f.write("{}\n".format(sched))
                sleep(0.01)
                
            total_time = perf_counter() - start
            with open('results/times', 'a+') as f:
                f.write('{} {} {}\n'.format(instance, i, total_time))
            sleep(10)
        delete_file(ALEA_CONFIG_FP)
