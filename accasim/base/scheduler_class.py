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
import logging

from sys import maxsize
from random import seed
from abc import abstractmethod, ABC
from sortedcontainers.sortedlist import SortedListWithKey
from enum import Enum

from accasim.base.resource_manager_class import ResourceManager 
from accasim.base.allocator_class import AllocatorBase
from accasim.utils.misc import CONSTANT

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
    MAXSIZE = maxsize
    ALLOW_MAPPING_SAME_NODE = True
    
    def __init__(self, _seed, resource_manager, allocator=None, job_check=JobVerification.NO_CHECK, **kwargs):
        """
        
        Construct a scheduler
            
        :param seed: Seed for the random state
        :param resource_manager: A Resource Manager object for dealing with system resources.
        :param allocator: Allocator object to be used by the scheduler to allocater after schedule generation. If an allocator isn't defined, the scheduler class must generate the entire dispatching plan.
        :param job_check: A job may be rejected if it doesnt comply with:
                    - JobVerification.REJECT: Any job is rejected
                    - JobVerification.NO_CHECK: All jobs are accepted
                    - JobVerification.CHECK_TOTAL: If the job requires more resources than the available in the system.
                    - JobVerification.CHECK_REQUEST: if an individual request by node requests more resources than the available one.        
        """
        seed(_seed)
        self._counter = 0
        self.constants = CONSTANT()
        self.allocator = None
        self.logger = None
        self._system_capacity = None
        self._nodes_capacity = None
        
        if allocator:
            assert isinstance(allocator, AllocatorBase), 'Allocator not valid for scheduler'
            self.allocator = allocator
        self.set_resource_manager(resource_manager)
        
        assert(isinstance(job_check, JobVerification)), 'job_check invalid type. {}'.format(job_check.__class__)
        self._job_check = job_check
        
        # Check resources
        self._min_required_availability = kwargs.pop('min_resources', None)  # ['core', 'mem']s        
        
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
    def scheduling_method(self, cur_time, es_dict, es):
        """
        
        This function must map the queued events to available nodes at the current time.
            
        :param cur_time: current time
        :param es_dict: dictionary with full data of the job events
        :param es: events to be scheduled
            
        :return a tuple of (time to schedule, event id, list of assigned nodes), an array jobs id of rejected jobs  
        
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
            
    def schedule(self, cur_time, es_dict, es):
        """
        
        Method for schedule. It calls the specific scheduling method.
        
        :param cur_time: current time
        :param es_dict: dictionary with full data of the events
        :param es: events to be scheduled
        
        :return: a tuple of (time to schedule, event id, list of assigned nodes), array of rejected job ids.
        
        """
        assert(self.resource_manager is not None), 'The resource manager is not defined. It must defined prior to run the simulation.'
        if not self.logger:
            self.logger = logging.getLogger(self.constants.LOGGER_NAME)

        self._counter += 1
        self.logger.debug("{} Dispatching: #{} decision".format(cur_time, self._counter))
        self.logger.debug('{} Dispatching: {} queued jobs'.format(cur_time, len(es)))
        self.logger.debug('{} Dispatching: {}'.format(cur_time, self.resource_manager.current_usage()))

        rejected = []
        
        # At least a job need 1 core and 1 kb/mb/gb of mem to run
        if self._min_required_availability and any([self.resource_manager.resources.full[res] for res in self._min_required_availability]):
            self.logger.debug("There is no availability of one of the min required resource to run a job. The dispatching process will be delayed until there is enough resources.")
            return [(None, e, []) for e in es], rejected

        accepted = []
        
        for e in es:
            job = es_dict[e]
            if not job.get_checked() and not self._check_job_request(job):
                logging.critical('{} has been rejected by the dispatcher. ({})'.format(e, self._job_check))
                rejected.append(e)
                continue
            accepted.append(job)
        to_schedule, to_reject = self.scheduling_method(cur_time, accepted, es_dict)
        rejected += to_reject
        for e in to_reject:
            logging.critical('{} has been rejected by the dispatcher. (Scheduling policy)'.format(e))         
        
        if self.allocator:
            dispatching_plan = self.allocator.allocate(to_schedule, cur_time)
        else:
            dispatching_plan = to_schedule
            
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

    def scheduling_method(self, cur_time, jobs, es_dict):
        """
        
        This function must map the queued events to available nodes at the current time.
        
        :param cur_time: current time
        :param es_dict: dictionary with full data of the events
        :param es: events to be scheduled
        
        :return: a tuple of (time to schedule, event id, list of assigned nodes), an array jobs id of rejected jobs  
        
        """
        to_reject = []
               
        to_schedule = SortedListWithKey(jobs, **self.sorting_parameters)
        return to_schedule, to_reject

class FirstInFirstOut(SimpleHeuristic):
    """

    **FirstInFirstOut scheduling policy.** 
    
    The first come, first served (commonly called FirstInFirstOut ‒ first in, first out) 
    process scheduling algorithm is the simplest process scheduling algorithm. 
        
    """
    name = 'FirstInFirstOut'
    """ Name of the Scheduler policy. """
    
    sorting_arguments = {
            'key': lambda x: x.queued_time
        }
    """ This sorting function allows to sort the jobs in relation of the scheduling policy. """

    def __init__(self, _allocator, _resource_manager=None, _seed=0, **kwargs):
        """
        
        FirstInFirstOut Constructor
        
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
            'key': lambda x:-x.expected_duration
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
            'key': lambda x: x.expected_duration
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
    
    name = 'EASY_Backfilling'
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

    def scheduling_method(self, cur_time, queued_jobs, es_dict):
        """

        This function must map the queued events to available nodes at the current time.
        
        :param cur_time: current time
        :param queued_jobs: events to be scheduled
        :param es_dict: dictionary with full data of the events
        
        :return: a tuple of (time to schedule, event id, list of assigned nodes)  

        """
        to_reject = []
        if not self.allocator_rm_set:
            self.nonauto_allocator.set_resource_manager(self.resource_manager)
                    
        avl_resources = self.resource_manager.current_availability()
        self.nonauto_allocator.set_resources(avl_resources)

        reserved_time = self.reserved_slot[0]
        reserved_nodes = self.reserved_slot[1]
        _jobs_allocated = []
        _ready_dispatch = []

        # Trying to allocate the blocked job on the reserved resources
        # if reserved_time is not None and (reserved_time == cur_time or self.blocked_resources):  
        # Continue waiting until it finishes, for a more stricted it must kill the jobs when ==
        # Continue waiting until it finishes, for a more stricted it must kill the jobs when ==
        if (reserved_time is not None and (reserved_time == cur_time or reserved_time < cur_time and self.blocked_resources)):  
            self.logger.trace('{}: Allocating the blocked node'.format(cur_time))
            _e = es_dict[self.blocked_job_id]
            blocked_job_allocation = self.nonauto_allocator.allocating_method(_e, cur_time, skip=False)
            if blocked_job_allocation[0] is not None:
                _jobs_allocated.append(blocked_job_allocation)
                assigned_nodes = blocked_job_allocation[2]
                requested_resources = _e.requested_resources
                _ready_dispatch.append((_e.id, cur_time + _e.expected_duration, {node: requested_resources for node in assigned_nodes}))
                
                assert(queued_jobs[0].id == self.blocked_job_id), '{}'.format(queued_jobs)
                self.blocked_job_id = None
                reserved_time, reserved_nodes = self.reserved_slot = (None, []) 
                self.blocked_resources = False
                _removed_id = queued_jobs[0]
                queued_jobs = queued_jobs[1:]
                self.logger.trace('{}: Removing the blocked job ({}) from the queue list '.format(cur_time, _removed_id))
                self.logger.trace('The resources were unblocked')
            else:
                self.logger.trace('{}: Nodes {}, are not already free for the blocked node.'.format(cur_time, reserved_nodes))
                for _node in reserved_nodes:
                    self.logger.trace('{}: Resources {}'.format(cur_time, avl_resources[_node]))
        # This var stores the info for allocated jobs
        # If there is no blocked job, we execute the first part of the schedule, same as in the simple scheduler
        if not self.blocked_resources:     
            self.logger.trace('{}: there is not a blocked job. The algorithms performs FirstInFirstOut Scheduling'.format(cur_time))       
            _tmp_jobs_allocated, _id_jobs_nallocated = self._job_allocation(cur_time, queued_jobs, es_dict)
            _jobs_allocated += [_j[0] for _j in _tmp_jobs_allocated]
            _ready_dispatch += [_j[1] for _j in _tmp_jobs_allocated]
            if not _id_jobs_nallocated:
                return  _jobs_allocated, to_reject
        else:
            _id_jobs_nallocated = queued_jobs

        #=======================================================================
        # Finding next available slot
        # to_dispatch corresponds to the recently jobs to be dispatched and revent corresponds to actual running events
        # The next avl slot is searched into a sorted list of expected ending times based on the walltime
        #=======================================================================

        # We refresh the info on the blocked job and prepare the remaining ones for backfilling
        self.blocked_job_id = _id_jobs_nallocated[0].id
        self.blocked_resources = True         
        for_backfilling = _id_jobs_nallocated[1:]
        running_events = self.resource_manager.running_jobs
        
        self.logger.trace('{}: Blocked Job: {} Request: {} x {}'.format(cur_time, self.blocked_job_id, es_dict[self.blocked_job_id].requested_nodes, es_dict[self.blocked_job_id].requested_resources))
        self.logger.trace('{}: Jobs Allocated: {} Available to Fill the GAP {}'.format(cur_time, _jobs_allocated, for_backfilling))


        # If no reservation was already made for the blocked job, we make one
        if not reserved_time:
            # All running events are computed, and the earliest slot in which the blocked job fits is computed.
            self.logger.trace("{}: Reserved time {}".format(cur_time, reserved_time))
            self.logger.trace("{}: Running events {}".format(cur_time, running_events))
            revents = [(job_id, es_dict[job_id].start_time + es_dict[job_id].expected_duration, assigns) for job_id, assigns in running_events.items()]
            future_endings = revents + _ready_dispatch
            # Sorting by soonest finishing
            future_endings.sort(key=lambda x: x[1], reverse=False)
            self.reserved_slot = self._search_slot(future_endings, es_dict[self.blocked_job_id])
            reserved_time = self.reserved_slot[0]
            reserved_nodes = self.reserved_slot[1]
            # Restore resource availability
            self.nonauto_allocator.set_resources(avl_resources)
        assert(self.reserved_slot[0]), 'There is no reserved time!'
        # Backfilling the schedule
        # running_events is a dict with the job id as key. Its value corresponds to the assignation {node: {resource: value}}
        self.logger.trace('{}: Blocked node {} until {}'.format(cur_time, self.blocked_job_id, self.reserved_slot))
        
        _jobs_allocated = [(None, self.blocked_job_id, []) ] + _jobs_allocated
        
        if not for_backfilling:
            # If there are not jobs for backfilling return the allocated job plus the blocked one
            return _jobs_allocated, to_reject  
        
        self.logger.trace('{}: To fill: {}'.format(cur_time, for_backfilling))

        # Trying to backfill the remaining jobs in the queue
        to_schedule_e_backfill = for_backfilling
        backfill_allocation = self.nonauto_allocator.allocating_method(to_schedule_e_backfill,
                                                               cur_time,
                                                               reserved_time=reserved_time,
                                                               reserved_nodes=reserved_nodes,
                                                               skip=True)
        _jobs_allocated += backfill_allocation

        return _jobs_allocated, to_reject

    def _search_slot(self, future_endings, e):
        """

        Computes a reservation for the blocked job e, by simulating the release of the resources for the running
        events, once they finish. The earliest slot in which e fits is chosen.
        
        :param avl_resources: Actual available resources
        :param future_endings: List of tuples of runninng events + events to be dispatched. (id, expected_ending time, assigns{node: {used attr: used value}} )
        :param e: Event to be fitted in the time slot
            
        :return: a tuple of time of the slot and nodes

        """
        virtual_resources = self.resource_manager.current_availability()
        
        self.logger.trace('Job {}: requested nodes {} x resources {}'.format(e.id, e.requested_nodes, e.requested_resources))
        self.logger.trace('Running: {}'.format(len(future_endings)))
        for i, fe in enumerate(future_endings):
            self.logger.trace('{} future ending: {}'.format(i, fe))
            _time = fe[1]
            for node, used_resources in fe[2].items():
                for attr, attr_value in used_resources.items(): 
                    virtual_resources[node][attr] += attr_value
            # The algorithm first checks if the job fits on the new released virtual resources; if it does
            # then it passes them to the allocator, which sorts them
            if self._event_fits_resources(virtual_resources, e.requested_nodes, e.requested_resources):
                # virtual_allocator.set_resources(virtual_resources)
                # reservation = virtual_allocator.allocate(e, _time, skip=False)
                self.nonauto_allocator.set_resources(virtual_resources)
                reservation = self.nonauto_allocator.allocating_method(e, _time, skip=False)
                if reservation[0] is not None:
                    return _time, reservation[2]
        raise Exception('Can\'t find a slot.... no end? :(')

    def _event_fits_resources(self, avl_resources, n_nodes, requested_resources):
        nodes = 0
        for node, resources in avl_resources.items():
            nodes += self._event_fits_node(resources, requested_resources)
        return nodes >= n_nodes

    def _event_fits_node(self, resources, requested_resources):
        # min_availability is the number of job units fitting in the node. It is initialized at +infty,
        # since we must compute a minimum
        min_availability = self.MAXSIZE
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

    def _job_allocation(self, cur_time, to_schedule, es_dict):
        """

        This method tries to allocate as many jobs as possible in the first part of the scheduling scheme.
        
        As soon as one allocation fails, all subsequent jobs fail too. Then, the return tuple contains info about
        the allocated jobs (assigned nodes and such) and also the ids about the non-allocated jobs, that can be used
        for backfilling.
        
        :param cur_time: the current time
        :param es: the list of events to be scheduled
        :param es_dict: the dictionary of events
        
        :return: a tuple (ready_dispatch,idx_notdispatched), where ready_dispatch contains the assignment info on the allocated jobs, and idx_notdispatched contains the ids of the jobs that could not be scheduled.

        """
        # Variables that keep the jobs to be dispatched 
        ready_distpach = []
        n_to_schedule = len(to_schedule)
        # Trying to allocate the jobs
        allocation = self.nonauto_allocator.allocating_method(to_schedule, cur_time, skip=False)
        # Computing the index of the first non-allocated job
        _idx_notdispatched = next((idx for idx, al in enumerate(allocation) if al[0] is None), n_to_schedule)
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
        return ready_distpach, to_schedule[_idx_notdispatched:] if _idx_notdispatched != n_to_schedule else []
