import os
import sys
import json
import logging
from datetime import datetime
from random import seed, random, randint, sample
from collections import namedtuple
from sortedcontainers.sortedlist import SortedList
from json import dump, load

accasim = os.path.abspath(os.path.join('../../accasim'))
sys.path.insert(0, accasim)

import unittest

from accasim.base.resource_manager_class import Resources
from accasim.base.allocator_class import FirstFit

JobClass = namedtuple('Job', ['id', 'requested_nodes', 'requested_resources'])

class ReaderTests(unittest.TestCase):
           
    SYS_DEFINITION = None
           
    def init_rm(self):        
        fp = 'data/RICC.config'
        
        data = None
        with open(fp) as f:
            data = json.load(f)
            
        start_time = data.pop('start_time', 0)
        equiv = data.pop('equivalence', {})
        resources = Resources(**data)
        resource_manager = resources.resource_manager()
        self.SYS_DEFINITION = resources._definition
               
        return resource_manager
    
    def define_trace_level(self):
        def log_logger(self, message, *args, **kwargs):
            if self.isEnabledFor(level):
                self._log(level, message, args, **kwargs)
    
        def log_root(msg, *args, **kwargs):
            logging.log(level, msg, *args, **kwargs)
            
        level = logging.TRACE = logging.DEBUG - 5
        logging.addLevelName(level, "TRACE")
        logging.trace = log_root
        logging.getLoggerClass().trace = log_logger          
        
    def create_allocator(self):
        self.define_trace_level()
        resource_manager = self.init_rm()        
        return FirstFit(0, resource_manager=resource_manager)

    def _count_allocations(self, allocation):
        return sum([1 for a in allocation if a[2]])        
        
    def test_empty_allocation(self):
        allocator = self.create_allocator()
        allocation = allocator.allocating_method([], 1)        
         
        self.assertEqual(self._count_allocations(allocation), 0)
        
    def _create_jobs(self, n, n_jobs, requests):
        jobs = []
        for i in range(n_jobs):
            job = self._create_job(n + i)
            jobs.append(job)
            requests[job.id] = job 
            
        return jobs
        
    def _create_job(self, id):     
        
        rn = randint(1, self.SYS_DEFINITION[0]['nodes'])
        rr = {
            k: randint(1, v)            
            for k, v in self.SYS_DEFINITION[0]['resources'].items()
        }
        
        return JobClass(**{'id': id, 'requested_nodes':rn, 'requested_resources': rr })
    
    def _end_running_jobs(self, running_jobs, allocator, requests):
        if not running_jobs:
            return running_jobs
        
        if random() >= 0.5:
            return running_jobs
        
        sample_size = randint(1, len(running_jobs))
        idxs = SortedList(sample(range(len(running_jobs)), sample_size))
        
        cur_resources = allocator.get_resources()
        
        for idx in reversed(idxs):
            
            job = running_jobs.pop(idx)
            job_request = requests.pop(job[1])
                    
            for node in job[2]: 
                for k, v in job_request.requested_resources.items():
                    cur_resources[node][k] += v 
        
        return running_jobs
                            
        
    def test_dynamic_allocation(self):
        seed('test_dynamic_allocation')
        
        allocator = self.create_allocator()
        
        cum_n = 0
        
        queued_jobs = []
        running_jobs = []
        requests = {}
        allocated_data = {}
        with open('data/dynamic_allocation.json', 'r') as fp:
            allocated_data = load(fp)
        
        for i in range(500):
            # Stop some running jobs
            running_jobs = self._end_running_jobs(running_jobs, allocator, requests)
            
            n_jobs = randint(1, 20)
            # Create n_jobs, and put in the queue with the previous ones
            queued_jobs.extend(self._create_jobs(cum_n, n_jobs, requests))
                     
            # Try to allocate all the queued jobs 
            allocation = allocator.allocate(queued_jobs, 1)
            
            # Store idx of allocated jobs 
            to_remove = []
            for idx, a in enumerate(allocation):
                if a[2]:
                    to_remove.append(idx)
            
            for idx in reversed(to_remove):
                job_allocation = allocation.pop(idx)
                queued_jobs.pop(idx)
                running_jobs.append(job_allocation)
                self.assertCountEqual(job_allocation[2], allocated_data[str(job_allocation[1])])
                self.assertListEqual(job_allocation[2], allocated_data[str(job_allocation[1])])
            
            cum_n += n_jobs       
        
if __name__ == '__main__':
    unittest.main()
        
