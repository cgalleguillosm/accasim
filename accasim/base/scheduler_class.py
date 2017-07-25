"""
MIT License

Copyright (c) 2017 cgalleguillosm, AlessioNetti

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
from accasim.base.resource_manager_class import resource_manager as resource_manager_class 
from accasim.base.allocator_class import allocator_base
from accasim.utils.misc import CONSTANT
from copy import deepcopy, copy
from abc import abstractmethod, ABC
import sys
import random
import logging


class scheduler_base(ABC):
    
    """
    
        This class allows to implement dispatching methods by integrating with an implementation of this class an allocator (:class:`accasim.base.allocator_class.allocator_base`). 
        An implementation of this class could also serve as a entire dispatching method if the allocation class is not used as default (:class:`.allocator` = None), but the resource manager must
        be set on the allocator using :func:`accasim.base.allocator_class.allocator_base.set_resource_manager`.
        
    """
    
    def __init__(self, seed, resource_manager, allocator=None):
        """
        
        Construct a scheduler
            
        :param seed: Seed for the random state
        :param resource_manager: A Resource Manager object for dealing with system resources.
        :param allocator: Allocator object to be used by the scheduler to allocater after schedule generation. If an allocator isn't defined, the scheduler class must generate the entire dispatching plan.
                 
        """
        random.seed(seed)
        self.constants = CONSTANT()
        self.allocator = None
        if allocator:
            assert isinstance(allocator, allocator_base), 'Allocator not valid for scheduler'
            self.allocator = allocator
        self.set_resource_manager(resource_manager)
        self.internal_ref = {}
        
    @property
    def name(self):
        """
        
        Name of the schedulign method
        
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
    def scheduling_method(self, cur_time, es_dict, es, debug=False):
        """
        
        This function must map the queued events to available nodes at the current time.
            
        :param cur_time: current time
        :param es_dict: dictionary with full data of the job events
        :param es: events to be scheduled
        :param debug: Debugging flag
            
        :return a list of tuples including (time to schedule, event id, list of assigned nodes)  
        
        """
        raise Exception('This function must be implemented!!')
    
    def set_resource_manager(self, resource_manager):
        """
        
        Set a resource manager. 

        :param resource_manager: An instantiation of a resource_manager class or None 
        
        """        
        if resource_manager:
            if self.allocator:
                self.allocator.set_resource_manager(resource_manager)
            assert isinstance(resource_manager, resource_manager_class), 'Resource Manager not valid for scheduler'
            self.resource_manager = resource_manager
        else:
            self.resource_manager = None
            
    def schedule(self, cur_time, es_dict, es, _debug=False):
        """
        
        Method for schedule. It calls the specific scheduling method.
        
        :param cur_time: current time
        :param es_dict: dictionary with full data of the events
        :param es: events to be scheduled
        :param _debug: Flag to debug
        
        :return: a tuple of (time to schedule, event id, list of assigned nodes)
        
        """
        assert(self.resource_manager is not None), 'The resource manager is not defined. It must defined prior to run the simulation.'
        if _debug:
            print('{}: {} queued jobs to be considered in the dispatching plan'.format(cur_time, len(es)))
        dispatching_plan = self.scheduling_method(cur_time, es_dict, es, _debug)
        if self.allocator:
            dispatching_plan = self.allocator.allocate(dispatching_plan, cur_time, _debug)
        return dispatching_plan
    
    def __str__(self):
        return self.get_id()
    
class simple_heuristic(scheduler_base):
    """
    
    Simple scheduler, sorts the event depending on the chosen policy.
    
    If a single job allocation fails, all subsequent jobs fail too.
    Sorting as name, sort funct parameters
    
    """

    def __init__(self, seed, resource_manager, allocator, name, sorting_parameters, **kwargs):
        scheduler_base.__init__(self, seed, resource_manager, allocator)
        self.name = name
        self.sorting_parameters = sorting_parameters

    def get_id(self):
        """
        
        Returns the full ID of the scheduler, including policy and allocator.

        :return: the scheduler's id.
        
        """
        return '-'.join([self.__class__.__name__, self.name, self.allocator.get_id()])

    def scheduling_method(self, cur_time, es_dict, es, _debug=False):
        """
        
        This function must map the queued events to available nodes at the current time.
        
        :param cur_time: current time
        :param es_dict: dictionary with full data of the events
        :param es: events to be scheduled
        :param _debug: Flag to debug
        
        :return: a tuple of (time to schedule, event id, list of assigned nodes)  
        
        """

        avl_resources = self.resource_manager.availability()
        self.allocator.set_resources(avl_resources)

        running_events = self.resource_manager.actual_events

        # Building the jobs list from the ids, and sorting it
        to_schedule_e = [es_dict[e] for e in es]
        to_schedule_e.sort(**self.sorting_parameters)
        _time = cur_time

        return to_schedule_e
        # allocated_events = self.allocator.allocate(to_schedule_e, _time, skip=False, debug=_debug)

        # return allocated_events

    
class fifo_sched(simple_heuristic):
    """

    **FIFO scheduling policy.** 
    
    The first come, first served (commonly called FIFO ‒ first in, first out) 
    process scheduling algorithm is the simplest process scheduling algorithm. 
        
    """
    name = 'FIFO'
    """ Name of the Scheduler policy. """
    
    sorting_arguments = {
            'key': lambda x: x.queued_time,
            'reverse': False
        }
    """ This sorting function allows to sort the jobs in relation of the scheduling policy. """

    def __init__(self, _allocator, _resource_manager=None, _seed=0, **kwargs):
        """
        
        FIFO Constructor
        
        """
        simple_heuristic.__init__(self, _seed, _resource_manager, _allocator, self.name, self.sorting_arguments, **kwargs)
        
class ljf_sched(simple_heuristic):
    """
    
    **LJF scheduling policy.**
    
    Longest Job First (LJF) sorts the jobs, where the longest jobs are preferred over the shortest ones.  
        
    """
    name = 'LJF'
    """ Name of the Scheduler policy. """
    
    sorting_arguments = {
            'key': lambda x: x.expected_duration,
            'reverse': True
        }
    """ This sorting function allows to sort the jobs in relation of the scheduling policy. """

    def __init__(self, _allocator, _resource_manager=None, _seed=0, **kwargs):
        """
        
        LJF Constructor
        
        """
        simple_heuristic.__init__(self, _seed, _resource_manager, _allocator, self.name, self.sorting_arguments, **kwargs)
        
class sjf_sched(simple_heuristic):
    """
    
    **SJF scheduling policy.**
    
    Shortest Job First (SJF) sorts the jobs, where the shortest jobs are preferred over the longest ones.
    
    """
    name = 'SJF'
    """ Name of the Scheduler policy. """
    
    sorting_arguments = {
            'key': lambda x: x.expected_duration,
            'reverse': False
        }
    """ This sorting function allows to sort the jobs in relation of the scheduling policy. """

    def __init__(self, _allocator, _resource_manager=None, _seed=0, **kwargs):
        """
    
        SJF Constructor
    
        """
        simple_heuristic.__init__(self, _seed, _resource_manager, _allocator, self.name, self.sorting_arguments, **kwargs)

class easybf_sched(scheduler_base):
    """
    
    EASY Backfilling scheduler. 
    
    Whenever a job cannot be allocated, a reservation is made for it. After this, the following jobs are used to
    backfill the schedule, not allowing them to use the reserved nodes.
       
    This dispatching methods includes its own calls to the allocator over the dispatching process. 
    Then it isn't use the auto allocator call, after the schedule generation.    
     
    """
    
    name = 'EASY_Backfilling'
    """ Name of the Scheduler policy. """
    
    def __init__(self, allocator, resource_manager=None, seed=0, **kwargs):
        """
    
        Easy BackFilling Constructor
    
        """
        scheduler_base.__init__(self, seed, resource_manager, allocator=None)
        self.blocked_job_id = None
        self.reserved_slot = (None, [])
        self.blocked_resources = False
        self.nonauto_allocator = allocator
        self.allocator_rm_set = False
        self.nonauto_allocator.set_resource_manager(resource_manager)
        if self.allocator_rm_set:
            if not self.nonauto_allocator.resource_manager:
                self.allocator_rm_set = True
        
    def get_id(self):
        """
    
        Returns the full ID of the scheduler, including policy and allocator.

        :return: the scheduler's id.
    
        """
        return '-'.join([self.name, self.nonauto_allocator.name])

    def scheduling_method(self, cur_time, es_dict, es, _debug=False):
        """

        This function must map the queued events to available nodes at the current time.
        
        :param cur_time: current time
        :param es_dict: dictionary with full data of the events
        :param es: events to be scheduled
        :param _debug: Flag to debug
        
        :return: a tuple of (time to schedule, event id, list of assigned nodes)  

        """
        
        if not self.allocator_rm_set:
            self.nonauto_allocator.set_resource_manager(self.resource_manager)
                    
        avl_resources = self.resource_manager.availability()
        self.nonauto_allocator.set_resources(avl_resources)

        if _debug:
            print('---------%s---------' % (cur_time))
        reserved_time = self.reserved_slot[0]
        reserved_nodes = self.reserved_slot[1]
        _jobs_allocated = []

        # Trying to allocate the blocked job on the reserved resources
        # if reserved_time is not None and (reserved_time == cur_time or self.blocked_resources):  # Continue waiting until it finishes, for a more stricted it must kill the jobs when ==
        if (reserved_time is not None and (reserved_time == cur_time or reserved_time < cur_time and self.blocked_resources)):  # Continue waiting until it finishes, for a more stricted it must kill the jobs when ==
            if _debug:
                print(cur_time, ': Allocating the blocked node')
            _e = es_dict[self.blocked_job_id]
            blocked_job_allocation = self.nonauto_allocator.allocate(_e, cur_time, skip=False)
            if blocked_job_allocation[0] is not None:
                _jobs_allocated.append(blocked_job_allocation)
                assert(es[0] == self.blocked_job_id), '%s' % es
                self.blocked_job_id = None
                self.reserved_slot = (None, [])
                self.blocked_resources = False
                _removed_id = es.pop(0)
                if _debug:
                    print(cur_time, 'Removing the blocked job (%s) from the queue list ' % _removed_id)
                    print('The resources were unblocked')
            else:
                if _debug:
                    print(cur_time, ': Nodes ', reserved_nodes, ', are not already free for the blocked node.')
                    for _node in reserved_nodes:
                        print(cur_time, ': Resources ', avl_resources[_node])
        
        # This var stores the info for allocated jobs
        _ready_dispatch = []
        # If there is no blocked job, we execute the first part of the schedule, same as in the simple scheduler
        if not self.blocked_resources:            
            _tmp_jobs_allocated, _id_jobs_nallocated = self._job_allocation(cur_time, es, es_dict, _debug)
            _jobs_allocated += [_j[0] for _j in _tmp_jobs_allocated]
            _ready_dispatch += [_j[1] for _j in _tmp_jobs_allocated]
            if _debug:
                print('nallocated ', _id_jobs_nallocated)
            if not _id_jobs_nallocated:
                if _debug:
                    print(cur_time, ': 1st return ', _jobs_allocated)
                return  _jobs_allocated
        else:
            _id_jobs_nallocated = es
        #=======================================================================
        # Finding next available slot
        # to_dispatch corresponds to the recently jobs to be dispatched and revent corresponds to actual running events
        # The next avl slot is searched into a sorted list of expected ending times based on the walltime
        #=======================================================================
        if _debug:
            print(cur_time, ': Blocked: ', self.blocked_job_id)
            print('To dispatch: ', _jobs_allocated)

        # We refresh the info on the blocked job and prepare the remaining ones for backfilling
        self.blocked_job_id = _id_jobs_nallocated[0]
        self.blocked_resources = True         
        for_backfilling = _id_jobs_nallocated[1:]
        running_events = self.resource_manager.actual_events

        # If no reservation was already made for the blocked job, we make one
        if reserved_time is None:
            # All running events are computed, and the earliest slot in which the blocked job fits is computed.
            revents = [(job_id, es_dict[job_id].start_time + es_dict[job_id].expected_duration, assigns) for job_id, assigns in running_events.items()]
            future_endings = revents + _ready_dispatch
            if _debug:
                print('FE: ', future_endings)
            # Sorting by soonest finishing
            future_endings.sort(key=lambda x: x[1], reverse=False)
            self.reserved_slot = self._search_slot(avl_resources, future_endings, es_dict[self.blocked_job_id], _debug)
            reserved_time = self.reserved_slot[0]
            reserved_nodes = self.reserved_slot[1]
            
        # Backfilling the schedule
        # running_events is a dict with the job id as key. Its value corresponds to the assignation {node: {resource: value}}
        if _debug:
            print('Blocked node ', self.blocked_job_id, ', until ', self.reserved_slot)
        
        _jobs_allocated = [(None, self.blocked_job_id, []) ] + _jobs_allocated
        
        if not for_backfilling:
            # If there are not jobs for backfilling return the allocated job plus the blocked one
            if _debug:
                print(cur_time, ': 2nd return ', _jobs_allocated)
            return _jobs_allocated  
        
        if _debug:
            print(cur_time, ': For BF', for_backfilling)

        # Trying to backfill the remaining jobs in the queue
        to_schedule_e_backfill = [es_dict[_id] for _id in for_backfilling]
        backfill_allocation = self.nonauto_allocator.allocate(to_schedule_e_backfill,
                                                               cur_time,
                                                               reserved_time=reserved_time,
                                                               reserved_nodes=reserved_nodes,
                                                               skip=True,
                                                               debug=_debug)
        _jobs_allocated += backfill_allocation

        if _debug:
            print(cur_time, ': 3rd return ', _jobs_allocated)
        return _jobs_allocated

    def _search_slot(self, avl_resources, future_endings, e, _debug):
        """

        Computes a reservation for the blocked job e, by simulating the release of the resources for the running
        events, once they finish. The earliest slot in which e fits is chosen.
        
        :param avl_resources: Actual available resources
        :param future_endings: List of tuples of runninng events + events to be dispatched. (id, expected_ending time, assigns{node: {used attr: used value}} )
        :param e: Event to be fitted in the time slot
            
        :return: a tuple of time of the slot and nodes

        """
        virtual_resources = deepcopy(avl_resources)
        virtual_allocator = copy(self.nonauto_allocator)

        for fe in future_endings:
            # print('FE: ', fe[0], fe[1], fe[2])
            _time = fe[1]
            for node, used_resources in fe[2].items():
                for attr, attr_value in used_resources.items(): 
                    virtual_resources[node][attr] += attr_value
            # The algorithm first checks if the job fits on the new released virtual resources; if it does
            # then it passes them to the allocator, which sorts them
            if self._event_fits_resources(virtual_resources, e.requested_nodes, e.requested_resources):
                virtual_allocator.set_resources(virtual_resources)
                reservation = virtual_allocator.allocate(e, _time, skip=False, debug=_debug)
                if reservation[0] is not None:
                    return _time, reservation[2]
        raise Exception('Can\'t find the slot.... no end? :(')

    def _event_fits_resources(self, avl_resources, n_nodes, requested_resources):
        nodes = 0
        for node, resources in avl_resources.items():
            nodes += self._event_fits_node(resources, requested_resources)
        return nodes >= n_nodes

    def _event_fits_node(self, resources, requested_resources):
        # min_availability is the number of job units fitting in the node. It is initialized at +infty,
        # since we must compute a minimum
        min_availability = sys.maxsize
        # if a job requests 0 resources, the request is deemed as not valid
        valid_request = False
        for k, v in requested_resources.items():
            # for each resource type, we compute the number of job units fitting for it, and refresh the minimum
            if v > 0 and min_availability > (resources[k] // v):
                valid_request = True
                min_availability = resources[k] // v
                # if the minimum reaches 0 (no fit) we break from the cycle
                if min_availability <= 0:
                    min_availability = 0
                    break
        if valid_request:
            return min_availability
        else:
            return 0

    def _job_allocation(self, cur_time, es, es_dict, _debug):
        """

        This method tries to allocate as many jobs as possible in the first part of the scheduling scheme.
        
        As soon as one allocation fails, all subsequent jobs fail too. Then, the return tuple contains info about
        the allocated jobs (assigned nodes and such) and also the ids about the non-allocated jobs, that can be used
        for backfilling.
        
        :param cur_time: the current time
        :param es: the list of events to be scheduled
        :param es_dict: the dictionary of events
        :param _debug: debug flag
        
        :return: a tuple (ready_dispatch,idx_notdispatched), where ready_dispatch contains the assignment info on the allocated jobs, and idx_notdispatched contains the ids of the jobs that could not be scheduled.

        """
        # Variables that keep the jobs to be dispatched 
        ready_distpach = []
        # Trying to allocate the jobs
        to_schedule_e = [es_dict[e] for e in es]
        allocation = self.nonauto_allocator.allocate(to_schedule_e, cur_time, skip=False, debug=_debug)
        # Computing the index of the first non-allocated job
        _idx_notdispatched = next((idx for idx, al in enumerate(allocation) if al[0] is None), len(es))
        # Building the ready_dispatch list for successful allocations, containing the assigned nodes info
        for al in allocation[:_idx_notdispatched]:
            _e = es_dict[al[1]]
            requested_resources = _e.requested_resources
            assigned_nodes = al[2]
            ready_distpach.append(
                (
                    al,
                    (_e.id, cur_time + _e.expected_duration, {node: requested_resources for node in assigned_nodes})
                )
            )
        # ids of jobs that could be used to backfilling the schedule
        return ready_distpach, es[_idx_notdispatched:] if _idx_notdispatched != len(es) else []
