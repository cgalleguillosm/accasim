import os
import sys
import json

accasim = os.path.abspath(os.path.join('../../accasim'))
sys.path.insert(0, accasim)

import unittest

from accasim.base.resource_manager_class import Resources

class ResourcesTests(unittest.TestCase):
    
    def load_sys_config(self):
        fp = 'data/system_def.config'
        
        data = None
        with open(fp) as f:
            data = json.load(f)
        
        return data['groups'], data['resources']
        
    def test_init_resources(self):
        
        groups, resources = self.load_sys_config()      
        resources = Resources(groups, resources)


class ResourcesTotalCheckTests(unittest.TestCase):
    
    def load_sys_config(self):
        fp = 'data/RICC.config'
        
        data = None
        with open(fp) as f:
            data = json.load(f)
        
        return data['groups'], data['resources']
        
    def test_init_resources(self):
        
        groups, resources = self.load_sys_config()      
        resources = Resources(groups, resources)
        total_resources = resources.system_capacity('total')
        
        self.assertTrue(total_resources['core'] == 8384, 'Incorrect core def.')
        self.assertTrue(total_resources['mem'] == 12576000000, 'Incorrect core def.')
        
if __name__ == '__main__':
    unittest.main()
        
