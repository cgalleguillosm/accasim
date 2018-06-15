from psutil import Process as _Process, NoSuchProcess
import subprocess, os
from time import sleep, perf_counter
from os.path import isdir
from os import makedirs

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
    
if __name__ == '__main__':

    instances = ('seth', 'ricc', 'mc',)
    exec_path = os.path.abspath('.')
    for instance in instances:
        for i in range(10):
            FNULL = open(os.devnull, 'w')
			# Simulator cmd line. All the args where given by Millian Poquet, those arguments disable any type of simulator logging
			# The following files must exist:
			# 	./platform/{instance}.xml
			# 	./workloads/{instance}.json
            cmd_1 = "batsim,--quiet,--disable-schedule-tracing,--disable-machine-state-tracing,-p,platforms/{}.xml,-w,workloads/{}.json,-e,results/{}_{}_out".format(instance, instance, instance, i)
            # Then the scheduler
            cmd_2 = "batsched,-v,rejecter"
            
            dir_exists('results/{}'.format(instance), True)
            
            start = perf_counter()
			# The output of the simulator and scheduler are sent to /dev/null
            p_batsim = subprocess.Popen(cmd_1.split(','), cwd=exec_path, stdout=FNULL, stderr=subprocess.STDOUT)
            p_sched = subprocess.Popen(cmd_2.split(','), cwd=exec_path, stdout=FNULL, stderr=subprocess.STDOUT)
			
            pid_sim = p_batsim.pid
            pid_sched = p_sched.pid
            
            with open('results/{}/memory_{}'.format(instance, i), 'w') as f:
                f.write('simulator;scheduler;total\n')
            
            while p_batsim.poll() is None or p_sched.poll() is None:
                sim = memory_usage_psutil(pid_sim)
                sched = memory_usage_psutil(pid_sched)
                with open('results/{}/memory_{}'.format(instance, i), 'a') as f:
                    f.write("{};{};{}\n".format(sim, sched, sim + sched))
				# Calculate the memory usage every 10 ms
                sleep(0.01)
            total_time = perf_counter() - start
			
			# Save the total time
            with open('results/times', 'a+') as f:
                f.write('{} {} {}\n'.format(instance, i, total_time))
				
			# Wait 10 secs for the next iteration
            sleep(10)
