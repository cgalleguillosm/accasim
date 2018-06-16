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
from copy import deepcopy, copy
from abc import abstractmethod, ABC
from sys import maxsize
from random import seed
from enum import Enum

from accasim.base.resource_manager_class import ResourceManager 
from accasim.base.allocator_class import AllocatorBase


class DispatcherError(Exception):
    pass
    
class JobVerification(Enum):
    
    REJECT = -1  # All jobs are rejected
    NO_CHECK = 0  # No verification
    CHECK_TOTAL = 1  # Total requested resources are verified  
    CHECK_REQUEST = 2  # Each node x resources are verified


class SchedulerBase(ABC):
    
    """
    
        This class allows to implement dispatching methods by integrating with an implementation of this class an allocator (:class:`accasim.base.allocator_class.AllocatorBase`). 
        An implementation of this class could also serve as a entire dispatching method if the allocation class is not used as default (:class:`.allocator` = None), but the resource manager must
        be set on the allocator using :func:`accasim.base.allocator_class.AllocatorBase.set_resource_manager`.
        
    """
    
    ALLOW_MAPPING_SAME_NODE = True
    
    def __init__(self, _seed, resource_manager, allocator=None, job_check=JobVerification.CHECK_REQUEST, **kwargs):
        """
        
        Construct a scheduler
            
        :param _seed: Seed for the random state
        :param resource_manager: A Resource Manager object for dealing with system resources.
        :param allocator: Allocator object to be used by the scheduler to allocater after schedule generation. If an allocator isn't defined, the scheduler class must generate the entire dispatching plan.
                 
        """
        seed(_seed)
        self._counter = 0
        self.allocator = None
        self._system_capacity = None
        self._nodes_capacity = None
        
        if allocator:
            assert isinstance(allocator, AllocatorBase), 'Allocator not valid for scheduler'
            self.allocator = allocator
        self.set_resource_manager(resource_manager)
        self.internal_ref = {}
        assert(isinstance(job_check, JobVerification)), 'job_check invalid type. {}'.format(job_check.__class__)
        if job_check == JobVerification.REJECT:
            print('All jobs will be rejected, and for performance purposes the rejection messages will be omitted.')
        self._job_check = job_check
        
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
            assert isinstance(resource_manager, ResourceManager), 'Resource Manager not valid for scheduler'
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
        self._counter += 1
        
        rejected = []
        accepted = []
        # Verify jobs with the defined Job Policy
        for e in es:
            job = es_dict[e]
            if not job.get_checked() and not self._check_job_request(job):
                if self._job_check != JobVerification.REJECT:
                    self._logger.warning('{} has been rejected by the dispatcher. ({})'.format(e, self._job_check))
                rejected.append(e)
                continue
            accepted.append(e)
        
        # At least each job need 1 core and 1 kb/mb/gb of mem to run
        min_required_avl = ['core', 'mem']
        if any([self.resource_manager.resources.full[res] for res in min_required_avl]):
            if _debug:
                print("There is no availability of one of the min required resource to run a job. The dispatching process will be delayed until there is enough resources.")
            return [ (None, e, []) for e in accepted], rejected      
          
        if _debug:
            print('{}: {} queued jobs to be considered in the dispatching plan'.format(cur_time, len(es)))
        
        to_allocate = []
        # On accepted jobs by policy, try to schedule with the scheduling policy
        if accepted:
            to_allocate, to_reject = self.scheduling_method(cur_time, es_dict, es, _debug)
            rejected += to_reject
            if _debug:
                for e in to_reject:
                    self._logger.warning('{} has been rejected by the dispatcher. (Scheduling policy)'.format(e)) 
            
        if to_allocate and self.allocator:
            dispatching_plan = self.allocator.allocate(to_allocate, cur_time, _debug)
        else:
            dispatching_plan = to_allocate
        
        if _debug:
            print("################## Dispatching decision n: {} #######################".format(self._counter))
        
        return dispatching_plan, rejected

    def _check_job_request(self, _job):
        """
        Simple method that checks if the loaded _job violates the system's resource constraints.
        :param _job: Job object
        :return: True if the _job is valid, false otherwise
        """
        _job.set_checked(True)
        if self._job_check == JobVerification.REJECT:
            return False
        
        elif self._job_check == JobVerification.NO_CHECK:
            return True
        
        elif self._job_check == JobVerification.CHECK_TOTAL:
            # We verify that the _job does not violate the system's resource constraints by comparing the total
            if not self._system_capacity:
                self._system_capacity = self.resource_manager.system_capacity('total')
            return not any([_job.requested_resources[res] * _job.requested_nodes > self._system_capacity[res] for res in _job.requested_resources.keys()])
                
        elif self._job_check == JobVerification.CHECK_REQUEST:
            if not self._nodes_capacity:
                self._nodes_capacity = self.resource_manager.system_capacity('nodes')
            # We verify the _job request can be fitted in the system        
            _requested_resources = _job.requested_resources
            _requested_nodes = _job.requested_nodes

            _fits = 0
            _diff_node = 0 
            for _node, _attrs in self._nodes_capacity.items():
                # How many time a request fits on the node
                _nfits = min([_attrs[_attr] // req for _attr, req in _requested_resources.items() if req > 0 ])
                # Update current number of times the current job fits in the nodes
                if _nfits > 0:
                    _fits += _nfits
                    _diff_node += 1
                    
                if self.ALLOW_MAPPING_SAME_NODE:
                    # Since _fits >> _diff_node this logical comparison is omitted.
                    if _fits >= _requested_nodes: 
                        return True
                else:
                    if _diff_node >= _requested_nodes:
                        return True
            
            return False
        raise DispatcherError('Invalid option.') 
    
    def __str__(self):
        return self.get_id()
    
class SimpleHeuristic(SchedulerBase):
    """
    
    Simple scheduler, sorts the event depending on the chosen policy.
    
    If a single job allocation fails, all subsequent jobs fail too.
    Sorting as name, sort funct parameters
    
    """

    def __init__(self, seed, resource_manager, allocator, name, sorting_parameters, **kwargs):
        SchedulerBase.__init__(self, seed, resource_manager, allocator, **kwargs)
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

        return to_schedule_e, []
        # allocated_events = self.allocator.allocate(to_schedule_e, _time, skip=False, debug=_debug)

        # return allocated_events

    
class FirstInFirstOut(SimpleHeuristic):
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
        SimpleHeuristic.__init__(self, _seed, _resource_manager, _allocator, self.name, self.sorting_arguments, **kwargs)
        
class LongestJobFirst(SimpleHeuristic):
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
        SimpleHeuristic.__init__(self, _seed, _resource_manager, _allocator, self.name, self.sorting_arguments, **kwargs)
        
class ShortestJobFirst(SimpleHeuristic):
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
        SimpleHeuristic.__init__(self, _seed, _resource_manager, _allocator, self.name, self.sorting_arguments, **kwargs)

class EASYBackfilling(SchedulerBase):
    """
    
    EASY Backfilling scheduler. 
    
    Whenever a job cannot be allocated, a reservation is made for it. After this, the following jobs are used to
    backfill the schedule, not allowing them to use the reserved nodes.
       
    This dispatching methods includes its own calls to the allocator over the dispatching process. 
    Then it isn't use the auto allocator call, after the schedule generation.    
     
    """
    
    name = 'EBF'
    """ Name of the Scheduler policy. """
    
    def __init__(self, allocator, resource_manager=None, seed=0, **kwargs):
        """
    
        Easy BackFilling Constructor
    
        """
        SchedulerBase.__init__(self, seed, resource_manager, allocator=None, **kwargs)
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

    def scheduling_method(self, cur_time, jobs_dict, queued_jobs, _debug=False):
        """

        This function must map the queued events to available nodes at the current time.
        
        :param cur_time: current time
        :param jobs_dict: dictionary with full data of the events
        :param queued_jobs: events to be scheduled
        :param _debug: Flag to debug
        
        :return: a tuple of (time to schedule, event id, list of assigned nodes)  

        """
        if not self.allocator_rm_set:
            self.nonauto_allocator.set_resource_manager(self.resource_manager)
                    
        avl_resources = self.resource_manager.availability()
        self.nonauto_allocator.set_resources(avl_resources)

        reserved_time = self.reserved_slot[0]
        reserved_nodes = self.reserved_slot[1]
        _jobs_allocated = []
        _ready_dispatch = []

        # Trying to allocate the blocked job on the reserved resources
        # if reserved_time is not None and (reserved_time == cur_time or self.blocked_resources):  # Continue waiting until it finishes, for a more stricted it must kill the jobs when ==
        if (reserved_time is not None and (reserved_time == cur_time or reserved_time < cur_time and self.blocked_resources)):  # Continue waiting until it finishes, for a more stricted it must kill the jobs when ==
            if _debug:
                print('{}: Allocating the blocked node'.format(cur_time))
            _e = jobs_dict[self.blocked_job_id]
            blocked_job_allocation = self.nonauto_allocator.allocate(_e, cur_time, skip=False)
            if blocked_job_allocation[0] is not None:
                _jobs_allocated.append(blocked_job_allocation)
                assigned_nodes = blocked_job_allocation[2]
                requested_resources = _e.requested_resources
                _ready_dispatch.append((_e.id, cur_time + _e.expected_duration, {node: requested_resources for node in assigned_nodes}))
                assert(queued_jobs[0] == self.blocked_job_id), '{}'.format(queued_jobs)
                self.blocked_job_id = None
                reserved_time, reserved_nodes = self.reserved_slot = (None, []) 
                self.blocked_resources = False
                _removed_id = queued_jobs[0]  # queued_jobs.pop(0)
                queued_jobs = queued_jobs[1:]
                if _debug:
                    print('{}: Removing the blocked job ({}) from the queue list '.format(cur_time, _removed_id))
                    print('The resources were unblocked')
            else:
                if _debug:
                    print('{}: Nodes {}, are not already free for the blocked node.'.format(cur_time, reserved_nodes))
                    for _node in reserved_nodes:
                        print('{}: Resources {}'.format(cur_time, avl_resources[_node]))
        # This var stores the info for allocated jobs
        # If there is no blocked job, we execute the first part of the schedule, same as in the simple scheduler
        if not self.blocked_resources:     
            if _debug:
                print('{}: there is not a blocked job. The algorithms performs FIFO Scheduling'.format(cur_time))       
            _tmp_jobs_allocated, _id_jobs_nallocated = self._job_allocation(cur_time, queued_jobs, jobs_dict, _debug)
            _jobs_allocated += [_j[0] for _j in _tmp_jobs_allocated]
            _ready_dispatch += [_j[1] for _j in _tmp_jobs_allocated]
            if not _id_jobs_nallocated:
                return  _jobs_allocated, []
        else:
            _id_jobs_nallocated = queued_jobs

        #=======================================================================
        # Finding next available slot
        # to_dispatch corresponds to the recently jobs to be dispatched and revent corresponds to actual running events
        # The next avl slot is searched into a sorted list of expected ending times based on the walltime
        #=======================================================================

        # We refresh the info on the blocked job and prepare the remaining ones for backfilling
        self.blocked_job_id = _id_jobs_nallocated[0]
        self.blocked_resources = True         
        for_backfilling = _id_jobs_nallocated[1:]
        running_events = self.resource_manager.actual_events

        if _debug:
            print('{}: Blocked Job: {} Request: {} x {}'.format(cur_time, self.blocked_job_id, jobs_dict[self.blocked_job_id].requested_nodes, jobs_dict[self.blocked_job_id].requested_resources))
            print('{}: Jobs Allocated: {} Available to Fill the GAP {}'.format(cur_time, _jobs_allocated, for_backfilling))


        # If no reservation was already made for the blocked job, we make one
        if not reserved_time:
            # All running events are computed, and the earliest slot in which the blocked job fits is computed.
            if _debug:
                print("{}: Reserved time {}".format(cur_time, reserved_time))
                print("{}: Running events {}".format(cur_time, running_events))
            revents = [(job_id, jobs_dict[job_id].start_time + jobs_dict[job_id].expected_duration, assigns) for job_id, assigns in running_events.items()]
            future_endings = revents + _ready_dispatch
            # Sorting by soonest finishing
            future_endings.sort(key=lambda x: x[1], reverse=False)
            self.reserved_slot = self._search_slot(avl_resources, future_endings, jobs_dict[self.blocked_job_id], _debug)
            reserved_time = self.reserved_slot[0]
            reserved_nodes = self.reserved_slot[1]
        assert(self.reserved_slot[0]), 'There is no reserved time!'
        # Backfilling the schedule
        # running_events is a dict with the job id as key. Its value corresponds to the assignation {node: {resource: value}}
        if _debug:
            print('{}: Blocked node {} until {}'.format(cur_time, self.blocked_job_id, self.reserved_slot))
        
        _jobs_allocated = [(None, self.blocked_job_id, []) ] + _jobs_allocated
        
        if not for_backfilling:
            # If there are not jobs for backfilling return the allocated job plus the blocked one
            return _jobs_allocated, []  
        
        if _debug:
            print('{}: To fill: {}'.format(cur_time, for_backfilling))

        # Trying to backfill the remaining jobs in the queue
        to_schedule_e_backfill = [jobs_dict[_id] for _id in for_backfilling]
        backfill_allocation = self.nonauto_allocator.allocate(to_schedule_e_backfill,
                                                               cur_time,
                                                               reserved_time=reserved_time,
                                                               reserved_nodes=reserved_nodes,
                                                               skip=True,
                                                               debug=_debug)
        _jobs_allocated += backfill_allocation

        return _jobs_allocated, []

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
        
        if _debug:
            print(e.requested_nodes, e.requested_resources)
            print('Running: ', len(future_endings))
        for i, fe in enumerate(future_endings):
            if _debug:
                print('{} future ending: {}'.format(i, fe))
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
        min_availability = maxsize
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
