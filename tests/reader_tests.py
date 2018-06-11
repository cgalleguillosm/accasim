import os
import sys
import json
from sortedcontainers.sortedlist import SortedList

accasim = os.path.abspath(os.path.join('../../accasim'))
sys.path.insert(0, accasim)

import unittest

from accasim.utils.reader_class import DefaultReader
from accasim.base.resource_manager_class import Resources
from accasim.utils.misc import DEFAULT_SWF_MAPPER
from accasim.base.event_class import AttributeType
from accasim.base.event_class import JobFactory

class ReaderTests(unittest.TestCase):
           
    def init_reader(self):        
        workload = 'data/workload.swf'
        job_factory, equiv, start_time = self.setup_job_factory()
        reader = DefaultReader(workload, job_factory=job_factory, start_time=start_time, equivalence=equiv)
        return reader, start_time
        
    def setup_job_factory(self):
        fp = 'data/RICC.config'
        
        data = None
        with open(fp) as f:
            data = json.load(f)
            
        start_time = data.pop('start_time', 0)
        equiv = data.pop('equivalence', {})
        resources = Resources(**data)
        resource_manager = resources.resource_manager()
        
        job_factory_config = {
            'job_mapper': DEFAULT_SWF_MAPPER,
            'job_attrs': self.default_job_description(),
        }
        
        job_factory = JobFactory(resource_manager, **job_factory_config)
        
        return job_factory, equiv, start_time
        
    def default_job_description(self):
        # Attribute to identify the user
        user_id = AttributeType('user_id', int)

        # New attributes required by the Dispatching methods.
        expected_duration = AttributeType('expected_duration', int)
        queue = AttributeType('queue', int)

        # Default system resources: core and mem.
        total_cores = AttributeType('core', int)
        total_mem = AttributeType('mem', int)

        return [total_cores, total_mem, expected_duration, queue, user_id]        
        
    def test_nlines(self):
        reader, start_time = self.init_reader()
        tpoints = SortedList()
        time_points, jobs = reader.next(start_time)
        tpoints.update(time_points)
        total_jobs = len(jobs)
        while True:
            if not tpoints:
                break
            _time = tpoints.pop(0)
            time_points, jobs = reader.next(_time)
            total_jobs += sum([ len(js) for js in jobs.values()])
            tpoints.update(time_points)
            
        self.assertEqual(total_jobs, 25)        
                
if __name__ == '__main__':
    unittest.main()
        
