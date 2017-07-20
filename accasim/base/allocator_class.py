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
import random
import sys
from abc import abstractmethod, ABC
from accasim.utils.misc import CONSTANT
from accasim.base.resource_manager_class import resource_manager



class allocator_base(ABC):
    """
    
    The base abstract interface all allocators must comply to.
    
    """

    def __init__(self, seed, resource_manager=None, **kwargs):
        """
    
        Allocator constructor (based on scheduler)

        :param seed: Seed if there is any random event
        :param res_man: resource manager for the system.
        :param kwargs: Nothing for the moment
                 
        """
        random.seed(seed)
        self._constants = CONSTANT()
        self._avl_resources = []
        self._sorted_keys = []
        self.set_resource_manager(resource_manager)

    @abstractmethod
    def get_id(self):
        """
    
        Abstract method. Must be implemented by the subclass. 
        Must return the identification of the allocator. 
        
        :return: Allocator identification (for instance its name).    
    
        """
        raise NotImplementedError

    @abstractmethod
    def set_resources(self, res):
        """
    
        Abstract method. Must be implemented by the subclass.
        This method sets the internal reference to the dictionary of available resources in the system.
        If the reference points to a list used also outside of this class, the object should be deepcopied.
        
        If necessary, the resources are also sorted.
            
        :param res: the list of currently available resources in the system.       
    
        """
        raise NotImplementedError

    @abstractmethod
    def set_attr(self, **kwargs):
        """
    
        Abstract method. Must be implemented by the subclass.
        Method used to set internal parameters and meta-data for the allocator.
        
        Its behavior depends on the specific allocator that is being used, and some arguments may be discarded.
        
        :param kwargs: the internal parameters to be set, depending on the allocator
    
        """
        raise NotImplementedError

    @abstractmethod
    def allocating_method(self, es, cur_time, skip=False, reserved_time=None, reserved_nodes=None, debug=False):
        """
    
        Abstract method. Must be implemented by the subclass.
        This method must try to allocate the scheduled events contained in es. It will stop as soon as an event cannot
        be allocated, to avoid violations of the scheduler's priority rules, or proceed with other events depending
        on the skip parameter.
        
        The method must support both list of events for es, in which case it will return a list, or single events.
        If there is at least one successful allocation, avl_resources is updated and sorted again efficiently.

        :param es: the event(s) to be allocated
        :param cur_time: current time, needed to build the schedule list
        :param skip: determines if the allocator can skip jobs
        :param reserved_time: beginning of the next reservation slot (used for backfilling)
        :param reserved_nodes: nodes already reserved (used for backfilling)
        :param debug: Debugging flag

        :return: a list of assigned nodes of length e.requested_nodes, for all events that could be allocated. The list is in the format (time,event,nodes) where time can be either cur_time or None.
        
        """
        raise NotImplementedError
    
    def allocate(self, es, cur_time, skip=False, reserved_time=None, reserved_nodes=None, debug=False):
        """
    
        This is the method that is called by the Scheduler to allocate the scheduled jobs. First, It verifies the data consistency and availability, 
        and then call to the implemented allocation policy.   
        
        
        :param es: the event(s) to be allocated
        :param cur_time: current time, needed to build the schedule list
        :param skip: determines if the allocator can skip jobs
        :param reserved_time: beginning of the next reservation slot (used for backfilling)
        :param reserved_nodes: nodes already reserved (used for backfilling)
        :param debug: Debugging flag
        
        :return: the return of the implemented allocation policy.

        """
        assert(self.resource_manager is not None), 'The resource manager is not defined. It must defined prior to run the simulation.'
        if debug:
            print('{}: {} queued jobs to be considered in the dispatching plan'.format(cur_time, len(es)))
        return self.allocating_method(es, cur_time, debug)
    
    def set_resource_manager(self, _resource_manager):
        """
        Internally set the resource manager to deal with resource availability.
        
        :param _resource_manager: A resource manager instance or None. If a resource manager is already instantiated,
             it's used for set internally set it and obtain the system capacity for dealing with the request verifications.
             The dispathing process can't start without a resource manager. 
             
        """
        if _resource_manager:
            assert isinstance(_resource_manager, resource_manager), 'Resource Manager not valid for scheduler'
            self.resource_manager = _resource_manager
            self._base_availability = self.resource_manager.get_total_resources()
        else:
            self.resource_manager = None

    def __str__(self):
        """
        
            Retrieves the identification of the allocator.
        
        """
        return self.get_id()

class ffp_alloc(allocator_base):
    """
    
    A simple allocator. Does not sort the resources.
        
    This allocator supports both single events and lists of events. It also
     supports backfilling. No sorting of the resources is done, so they are
     considered as they are given in input.
    
    """

    name = 'First_Fit'

    def __init__(self, seed=0, resource_manager=None, **kwargs):
        """
    
        Constructor for the class.
        
        :param seed: seed for random events (not used)
        :param resource_manager: reference to the system resource manager
        :param kwargs: None at the moment
    
        """
        allocator_base.__init__(self, seed, resource_manager)
        if self.resource_manager:
            self._base_availability = self.resource_manager.get_total_resources()

    def get_id(self):
        return self.__class__.__name__

    def set_resources(self, res):
        """
    
        Sets in the internal variable avl_resources the current available resources for the system. It also sorts
        them, if the sort_resources method is implemented.
        
        :param res: the list of currently available resources for the system
    
        """
        self._avl_resources = res
        self._sorted_keys = self._sort_resources()

    def set_attr(self, **kwargs):
        """
    
        Method used to set internal parameters and meta-data for the allocator.

        Its behavior depends on the specific allocator that is being used, and some arguments may be discarded.
        It is not actively used in this simple allocator (for the moment).

        :param kwargs: None for the moment
    
        """
        pass

    def allocating_method(self, es, cur_time, skip=False, reserved_time=None, reserved_nodes=None, debug=False):
        """
    
        Given a job list es, this method searches for a suitable allocation for as many jobs as possible.
        
        In normal allocation, the method stops as soon as an event in the list cannot be allocated. In this case,
        ths list of already allocated jobs is returned. This is done to be coherent with the scheduler's rules.
        As an alternative, the skip parameter can be supplied to allow the scheduler to skip unallocated jobs.
        This method also support backfilling schedule. In this case, the backfilling parameters are supplied,
        and the allocator tries to fit jobs without delaying the reserved job. In this second case,
        the method does not stop when a job cannot be allocated, but simply skips it.
        
        es can be a list or a single event object. The return type (list or single tuple) changes accordingly.
        
        :param es: the event(s) to be allocated
        :param cur_time: current time, needed to build the schedule list
        :param skip: determines if the allocator can skip jobs
        :param reserved_time: beginning of the next reservation slot (used for backfilling)
        :param reserved_nodes: nodes already reserved (used for backfilling)

        :return: a list of assigned nodes of length e.requested_nodes, for all events that could be allocated. The list is in the format (time,event,nodes) where time can be either cur_time or None.
    
        """
        if not isinstance(es, (list, tuple)):
            listAsInput = False
            es = [es]
        else:
            listAsInput = True

        allocation = []
        success_counter = 0
        for e in es:
            requested_nodes = e.requested_nodes
            requested_resources = e.requested_resources
            # We verify that the job does not violate the system's resource constraints
            for t in requested_resources.keys():
                assert requested_resources[t] * requested_nodes <= self._base_availability[t], 'There are %i %s total resources in the system, requested %i by job %s' % (self._base_availability[t], t, requested_resources[t] * requested_nodes, e.id)

            # If the input arguments relative to backfilling are not supplied, the method operates in regular mode.
            # Otherwise, backfilling mode is enabled, allowing the allocator to skip jobs and consider the reservation.
            nodes_to_discard = self._compute_reservation_overlaps(e, cur_time, reserved_time, reserved_nodes, debug)
            backfilling_overlap = False if len(nodes_to_discard) == 0 else True

            assigned_nodes = []
            nodes_left = requested_nodes
            for node in self._sorted_keys:
                # The algorithm check whether the given node belongs to the list of reserved nodes, in backfilling.
                # If it does, the node is discarded.
                resources = self._avl_resources[node]
                if backfilling_overlap and node in nodes_to_discard:
                    continue
                # We compute the number of job units fitting in the current node, and update the assignment
                fits = int(self._event_fits_node(resources, requested_resources))
                if nodes_left <= fits:
                    assigned_nodes += [node] * nodes_left
                    nodes_left = 0
                else:
                    assigned_nodes += [node] * fits
                    nodes_left -= fits
                if nodes_left <= 0:
                    break

            # If, after analyzing all nodes, the allocation is still not complete, the partial allocation
            # is discarded.
            if nodes_left > 0:
                assigned_nodes = []
            assert not assigned_nodes or requested_nodes == len(assigned_nodes), 'Requested' + str(requested_nodes) + ' got ' + str(len(assigned_nodes))

            # If a correct allocation was found, we update the resources of the system, sort them again, and
            # add the allocation to the output list.
            if assigned_nodes:
                allocation.append((cur_time, e.id, assigned_nodes))
                self._update_resources(assigned_nodes, requested_resources)
                self._adjust_resources(self._sorted_keys)
                success_counter += 1
                if debug:
                    print('Allocation successful for event %s' % (e.id))
            # If no correct allocation could be found, two scenarios are possible: 1) normally, the allocator stops
            # here and returns the jobs allocated so far 2) if the skip parameter is enabled, the job is just
            # skipped, and we proceed with the remaining ones.
            else:
                if debug:
                    print('Allocation failed for event %s with %s nodes left' % (e.id, nodes_left))
                allocation.append((None, e.id, []))
                if not skip:
                    # if jobs cannot be skipped, at the first allocation fail all subsequent jobs fail too
                    for ev in es[(success_counter + 1):]:
                        allocation.append((None, ev.id, []))
                    if debug:
                        print('Cannot skip jobs, %s additional pending allocations failed' % (len(es) - success_counter - 1))
                    break
        if debug:
            print('There were %s successful allocations out of %s events' % (success_counter, len(es)))
        return allocation if listAsInput else allocation[0]

    def _compute_reservation_overlaps(self, e, cur_time, reserved_time, reserved_nodes, debug=False):
        """
    
        This method considers an event e, the current time, and a list of reservation start times with relative
        reserved nodes, and returns the list of reserved nodes that cannot be accessed by event e because of overlap.
        
        :param e: the event to be allocated
        :param cur_time: the current time
        :param reserved_time: the list (or single element) of reservation times
        :param reserved_nodes: the list of lists (or single list) of reserved nodes for each reservation
        :param debug: the debug flag
        
        :return: the list of nodes that cannot be used by event e
    
        """
        if reserved_time is None or reserved_nodes is None:
            return []
        else:
            if not isinstance(reserved_time, (list, tuple)):
                if cur_time + e.expected_duration > reserved_time:
                    if debug:
                        print('Backfill: Event %s is overlapping with reservation at time %s in backfilling mode' % (e.id, reserved_time))
                    return reserved_nodes
                else:
                    return []
            else:
                overlap_list = []
                for ind, evtime in enumerate(reserved_time):
                    if cur_time + e.expected_duration > evtime:
                        if debug:
                            print('Backfill: Event %s is overlapping with reservation at time %s in backfilling mode' % (e.id, evtime))
                        overlap_list += reserved_nodes[ind]
                return list(set(overlap_list))

    def _update_resources(self, reserved_nodes, requested_resources):
        """
    
        Updates the internal avl_resources list after a successful allocation.
        
        :param reserved_nodes: the list of nodes assigned to the allocated job
        :param requested_resources: the list of resources requested by the job per each node
    
        """
        for node in reserved_nodes:
            resource = self._avl_resources[node]
            for attr, v in requested_resources.items():
                assert resource[attr] - v >= 0, 'In node {}, the resource {} is going below to 0'.format(node, attr)
                resource[attr] -= v

    def _sort_resources(self):
        """
    
        Method which sorts the available resources dict. Not used in this class, but can be overridden by extended
        classes.

        :return: the sorted list of node keys (in this case, identical to the original)
    
        """
        return self._avl_resources.keys()

    def _adjust_resources(self, sorted_keys):
        """
    
        Method which restores the resources' sorting after a successful allocation. Not used in this class, has
        to be overridden.

        :param sorted_keys: the list of keys that needs to be adjusted
        
        :return: none
    
        """
        pass

    def _event_fits_node(self, resources, requested_resources):
        """
    
        Checks if the job with requested_resources fits the node with resources available. Returns the number
        of job units that fit the node
        
        :param resources: the node's available resources
        :param requested_resources: the job's requested resources
        
        :return: the number of job units fitting in the node
    
        """
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
        
class consolidate_alloc(ffp_alloc):
    """
    
    Consolidate Allocator
    It is an allocator which sorts the nodes basing on the amount of free resources, trying to consolidate.
        
    The less the available resources, the higher the priority.
    The allocator is based on allocator_simple, changing only the sort and adjust methods.   
    
    """
    
    name = 'Consolidate'

    def __init__(self, seed=0, resource_manager=None, **kwargs):
        """
        
        Constructor for the class.

        :param seed: seed for random events (not used)
        :param resource_manager: reference to the system resource manager
        :param kwargs: None at the moment
        
        """
        ffp_alloc.__init__(self, seed, resource_manager)

        self.ranking = lambda x: sum(self._avl_resources[x].values())
        """
        
            Defines the ranking operator for sorting. Must use the self._avl_resources argument
            (the available resource dictionary). x represents a key.
        
        """

    def _sort_resources(self):
        """
        
        This method sorts the keys of the available resources dictionary, basing on the ranking operator.
        
        It is called after the resources are set in the allocator.
        
        :return: the list of sorted keys (node ids) for the resources
        
        """
        assert self._avl_resources is not None, 'The dictionary of available resources must be non-empty.'
        return sorted(self._avl_resources.keys(), key=self.ranking, reverse=False)

    def _adjust_resources(self, sorted_keys):
        """
        
        Adjusts the sorting of the resources after a successful allocation. 
        
        This method still uses python's sort method, because the Timsort implementation has O(n) complexity
        on mostly sorted data. Even with a custom implementation, the average case would cost O(n) at best.
        
        :param sorted_keys: the list of keys, almost sorted, that needs to be adjusted
        
        """
        assert self._avl_resources is not None, 'The dictionary of available resources must be non-empty.'
        assert sorted_keys is not None, 'The list of keys must be non-empty'
        sorted_keys.sort(key=self.ranking, reverse=False)
