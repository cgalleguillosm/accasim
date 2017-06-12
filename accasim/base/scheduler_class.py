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
from accasim.base.resource_manager_class import resource_manager
from accasim.base.allocator_class import allocator_base
from accasim.utils.misc import CONSTANT
import random
import logging
from abc import abstractmethod, ABC


class scheduler_base(ABC):
    def __init__(self, _seed, _resource_manager, _allocator, **kwargs):
        """
        Construct a scheduler
            
        :param _seed: Seed for the random state
        :param _resource_manager: A Resource Manager object for dealing with system resources.
        :param _allocator: Allocator object to be used by the scheduler to allocater after schedule
                 
        """
        random.seed(_seed)
        self.constants = CONSTANT()
        assert isinstance(_resource_manager, resource_manager), 'Resource Manager not valid for scheduler'
        self.resource_manager = _resource_manager
        assert isinstance(_allocator, allocator_base), 'Allocator not valid for scheduler'
        self.allocator = _allocator
    
    @property
    def POLICIES(self):
        """
        A dictionary of the supported sorting policies for the events
        """
        raise NotImplementedError 
    
    @abstractmethod
    def get_id(self):
        """
        Must return the full ID of the scheduler, including policy and allocator.
        
        :return: the scheduler's id.
        """
        raise NotImplementedError
    
    @abstractmethod
    def schedule(self, cur_time, es_dict, es):
        """
        This function must map the queued events to available nodes at the current time.
            
        :param cur_time: current time
        :param es_dict: dictionary with full data of the job events
        :param events to be scheduled
            
        :return a list of tuples including (time to schedule, event id, list of assigned nodes)  
        """
        raise Exception('This function must be implemented!!')
    
class simple_heuristic(scheduler_base):
    """
    Simple scheduler, sorts the event depending on the chosen policy.
    
    If a single job allocation fails, all subsequent jobs fail too.
    Sorting as name, sort funct parameters
    """

    POLICIES = {
        #=======================================================================
        # 'fifo': {
        #     'key': lambda x: eval('x.queued_time'),
        #     'reverse': False
        # },
        #=======================================================================
        'fifo':{
            'key': lambda x: getattr(x, 'queued_time'),
            'reverse': False
        },
        'longest_duration': {
            'key': lambda x: eval('x.expected_duration'),
            'reverse': True
        },
        'shortest_duration': {
            'key': lambda x: eval('x.expected_duration'),
            'reverse': False
        },
    }

    def __init__(self, _seed, _resource_manager, _allocator, _name, **kwargs):
        scheduler_base.__init__(self, _seed, _resource_manager, _allocator)
        assert (_name in self.POLICIES)
        self.name = _name

    def get_id(self):
        """
        Returns the full ID of the scheduler, including policy and allocator.

        :return: the scheduler's id.
        """
        return '-'.join([self.__class__.__name__, self.name, self.allocator.get_id()])

    def schedule(self, cur_time, es_dict, es, _debug=False):
        """
        This function must map the queued events to available nodes at the current time.
        
        :param cur_time: current time
        :param es_dict: dictionary with full data of the events
        :param es: events to be scheduled
        :param _debug: Flag to debug
        
        :return a tuple of (time to schedule, event id, list of assigned nodes)  
        """
# ALLOCATOR LOGIC
        avl_resources = self.resource_manager.availability()
        self.allocator.set_resources(avl_resources)
# ---------------
        running_events = self.resource_manager.actual_events
        logging.debug('---- %s ----' % cur_time)
        logging.debug('Available res: \n%s' % '\n'.join(
            ['%s: %s' % (k_1, ', '.join(['%s: %s' % (k_2, v_2) for k_2, v_2 in v_1.items()])) for k_1, v_1 in
             avl_resources.items() if v_1['core'] > 0 and v_1['mem'] > 0]))
        logging.debug('Running es: %s' % ''.join(['%s,' % r for r in running_events]))

        # Building the jobs list from the ids, and sorting it
        to_schedule_e = [es_dict[e] for e in es]
        to_schedule_e.sort(**self._sorting_function())
        _time = cur_time
# ALLOCATOR LOGIC
        allocated_events = self.allocator.search_allocation(to_schedule_e, _time, skip=False, debug=_debug)
# ---------------
        return allocated_events

    def _sorting_function(self):
        """
        Method which returns the lambda corresponding to the chosen policy for sorting.
        """
        return {k: v for k, v in self.POLICIES[self.name].items()}
