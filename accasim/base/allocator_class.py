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

from random import seed
from sys import maxsize 
from sortedcontainers import SortedList
from abc import abstractmethod, ABC
from accasim.base.resource_manager_class import ResourceManager


class AllocatorBase(ABC):
    """
    
    The base abstract interface all allocators must comply to.
    
    """

    MAXSIZE = maxsize

    def __init__(self, _seed, resource_types=['core'], **kwargs):
        """
    
        Allocator constructor (based on scheduler)

        :param seed: Seed if there is any random event
        :param res_man: resource manager for the system.
        :param kwargs: Nothing for the moment
                 
        """
        seed(_seed)
        self.avl_resources = None
        self.node_names = None
        self.resource_manager = None
        
        self._logger = logging.getLogger('accasim')
        # The list of resource types that are necessary for job execution. Used to determine wether a node can be used
        # for allocation or not
        self.nec_res_types = resource_types

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
    
    def get_resources(self):
        """
            Returns the internal reference to the dictionary of available resources in the system. 
            It includes the last virtual allocations.
        """
        return self.avl_resources

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
    def allocating_method(self, es, cur_time, skip=False, reserved_time=None, reserved_nodes=None):
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

        :return: a list of assigned nodes of length e.requested_nodes, for all events that could be allocated. The list is in the format (time,event,nodes) where time can be either cur_time or None.
        
        """
        raise NotImplementedError
    
    def allocate(self, es, cur_time, skip=False, reserved_time=None, reserved_nodes=None):
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
        self._logger.debug('{}: {} queued jobs to be considered in the dispatching plan'.format(cur_time, len(es) if isinstance(es, (list, tuple, SortedList)) else 1))

        # Update current available resources
        self.set_resources(self.resource_manager.current_availability)
        dispatching_decision = self.allocating_method(es, cur_time, skip=skip, reserved_time=reserved_time, reserved_nodes=reserved_nodes)
        
        return dispatching_decision 
    
    def set_resource_manager(self, _resource_manager):
        """
        Internally set the resource manager to deal with resource availability.
        
        :param _resource_manager: A resource manager instance or None. If a resource manager is already instantiated,
             it's used for set internally set it and obtain the system capacity for dealing with the request verifications.
             The dispathing process can't start without a resource manager. 
             
        """
        assert isinstance(_resource_manager, ResourceManager), 'Resource Manager not valid for scheduler'
        self.resource_manager = _resource_manager
        self._define_mappers()
            
    def _define_mappers(self):
        if not self.node_names:
            self.node_names = self.resource_manager.node_names

    def __str__(self):
        """
        
            Retrieves the identification of the allocator.
        
        """
        return self.get_id()


class FirstFit(AllocatorBase):
    """
    
    A simple First-Fit allocator. Does not sort the resources.
        
    This allocator supports both single events and lists of events. It also
     supports backfilling. No sorting of the resources is done, so they are
     considered as they are given in input.
    
    """

    name = 'FF'

    def __init__(self, seed=0, **kwargs):
        """
    
        Constructor for the class.
        
        :param seed: seed for random events (not used)
        :param resource_manager: reference to the system resource manager
        :param kwargs: None at the moment
    
        """
        AllocatorBase.__init__(self, seed, **kwargs)    
        self.sorted_keys = None    

    def get_id(self):
        return self.__class__.__name__

    def set_resources(self, res):
        """
    
        Sets in the internal variable avl_resources the current available resources for the system. It also sorts
        them, if the sort_resources method is implemented.
        
        :param res: the list of currently available resources for the system
    
        """
        self.avl_resources = res
        self._adjust_resources()

    def set_attr(self, **kwargs):
        """
    
        Method used to set internal parameters and meta-data for the allocator.

        Its behavior depends on the specific allocator that is being used, and some arguments may be discarded.
        It is not actively used in this simple allocator (for the moment).

        :param kwargs: None for the moment
    
        """
        pass

    def allocating_method(self, es, cur_time, skip=False, reserved_time=None, reserved_nodes=None):
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
        if not isinstance(es, (list, tuple, SortedList)):
            listAsInput = False
            es = [es]
        else:
            listAsInput = True
        
        allocation = []
        success_counter = 0

        for e in es:
            requested_nodes = e.requested_nodes
            requested_resources = e.requested_resources

            # If the input arguments relative to backfilling are not supplied, the method operates in regular mode.
            # Otherwise, backfilling mode is enabled, allowing the allocator to skip jobs and consider the reservation.
            nodes_to_discard = self._compute_reservation_overlaps(e, cur_time, reserved_time, reserved_nodes)
            backfilling_overlap = False if len(nodes_to_discard) == 0 else True

            assigned_nodes = []
            nodes_left = requested_nodes
                
            for node in self.sorted_keys:
                # Check whether the given node belongs to the list of reserved nodes, in backfilling.
                # If it does, the node is discarded.
                if backfilling_overlap:  # and node in nodes_to_discard:
                    continue
                # First, compute the number of job units fitting in the current node, and then update the job assignment
                resources = self.avl_resources[node]
                fits = self._event_fits_node(resources, requested_resources)                   
                
                if fits == 0:
                    continue
                
                if nodes_left <= fits:
                    assigned_nodes += [node] * nodes_left
                    nodes_left = 0
                else:
                    assigned_nodes += [node] * fits
                    nodes_left -= fits
                
                # Break the loop when the request is satisfied
                if nodes_left == 0:
                    break

            # If, after analyzing all nodes, the allocation is still not complete, the partial allocation
            # is discarded.            
            if nodes_left > 0:
                assigned_nodes = []
            assert len(assigned_nodes) in (0, requested_nodes,), 'Requested' + str(requested_nodes) + ' got ' + str(len(assigned_nodes))

            # If a correct allocation was found, we update the resources of the system, sort them again, and
            # add the allocation to the output list.
            if assigned_nodes:
                allocation.append((cur_time, e.id, assigned_nodes))
                # print(e.id, e.requested_nodes, e.requested_resources, len(assigned_nodes), assigned_nodes)
                self._update_resources(assigned_nodes, requested_resources)
                # Sort keys if it is necessary (BF)
                self._adjust_resources(assigned_nodes)
                success_counter += 1
                self._logger.trace('Allocation successful for event {}'.format(e.id))
            #===================================================================
            # If no correct allocation could be found, two scenarios are possible: 
            #     1) normally, the allocator stops here and returns the jobs allocated so far 
            #     2) if the skip parameter is enabled, the job is just skipped, and we proceed with the remaining ones.
            #===================================================================
            else:
                self._logger.trace('Allocation failed for event {} with {} nodes left'.format(e.id, nodes_left))
                allocation.append((None, e.id, []))
                if not skip:
                    # if jobs cannot be skipped, at the first allocation fail all subsequent jobs fail too
                    for e in es[(success_counter + 1):]:
                        allocation.append((None, e.id, []))
                    self._logger.trace('Cannot skip jobs, {} additional pending allocations failed {}'.format(len(es) - success_counter - 1, es[success_counter:]))
                    self._logger.trace('')
                    break
        self._logger.trace('{}/{} successful allocations of events'.format(success_counter, len(es)))
        return allocation if listAsInput else allocation[0]

    def _compute_reservation_overlaps(self, e, cur_time, reserved_time, reserved_nodes):
        """
    
        This method considers an event e, the current time, and a list of reservation start times with relative
        reserved nodes, and returns the list of reserved nodes that cannot be accessed by event e because of overlap.
        
        :param e: the event to be allocated
        :param cur_time: the current time
        :param reserved_time: the list (or single element) of reservation times
        :param reserved_nodes: the list of lists (or single list) of reserved nodes for each reservation
        
        :return: the list of nodes that cannot be used by event e
    
        """
        if reserved_time is None or reserved_nodes is None:
            return []
        else:
            if not isinstance(reserved_time, (list, tuple)):
                if cur_time + e.expected_duration > reserved_time:
                    return reserved_nodes
                else:
                    return []
            else:
                overlap_list = []
                for ind, evtime in enumerate(reserved_time):
                    if cur_time + e.expected_duration > evtime:
                        overlap_list += reserved_nodes[ind]
                return list(set(overlap_list))

    def _update_resources(self, reserved_nodes, requested_resources):
        """
    
        Updates the internal avl_resources list after a successful allocation.
        
        :param reserved_nodes: the list of nodes assigned to the allocated job
        :param requested_resources: the list of resources requested by the job per each node
    
        """
        for node in reserved_nodes:
            resource = self.avl_resources[node]
            for attr, v in requested_resources.items():
                if v == 0:
                    continue
                cur_q = resource[attr]
                assert cur_q - v >= 0, 'In node {}, the resource {} is going below to 0'.format(node, attr)
                resource[attr] -= v

    def _adjust_resources(self, nodes=None):
        """

        Method which must sort the node list at the beginning and after a successful allocation. 
        It must sort the self.sorted_keys attribute.

        """
        if not nodes:
            self.sorted_keys = []
            for node in self.node_names:
                add = True
                for res, avl in self.avl_resources[node].items():
                    if res in self.nec_res_types and avl == 0:
                        add = False
                        break
                if add:
                    self.sorted_keys.append(node)
        else:
            to_remove = set()
            for node in nodes:
               for res, avl in self.avl_resources[node].items():
                   if res in self.nec_res_types and avl == 0:
                       to_remove.add(node)
                       break
            for node in to_remove:
                self.sorted_keys.remove(node)

    def _event_fits_node(self, resources, requested_resources):
        _fits = self.MAXSIZE

        for res_type, req in requested_resources.items():
            # The current res_type isnt requested 
            if req == 0:
                continue
            fit = resources[res_type] // req
            # The current res_type cannot be fitted in this node, returning without checking the remaining resource types.
            if fit == 0:
                return 0
            # We maintain the minimum value 
            if _fits > fit:
                _fits = fit
        return _fits


class BestFit(FirstFit):
    """
    
    Best-Fit Allocator
    It is an allocator which sorts the nodes basing on the amount of free resources, trying to consolidate.
        
    The less the available resources, the higher the priority.
    The allocator is based on ffp_alloc, changing only the sort and adjust methods.   
    
    """
    
    name = 'BF'

    def __init__(self, seed=0, **kwargs):
        """
        
        Constructor for the class.

        :param seed: seed for random events (not used)
        :param resource_manager: reference to the system resource manager
        :param kwargs: None at the moment
        
        """
        FirstFit.__init__(self, seed)

        self.ranking = lambda x: sum(self.avl_resources[x].values())
        """
        
            Defines the ranking operator for sorting. Must use the self.avl_resources argument
            (the available resource dictionary). x represents a key.
        
        """
        
    def _adjust_resources(self, nodes=None):
        """

        Method which must sort the node list at the beginning and after a successful allocation. 
        It must sort the self.sorted_keys attribute.

        """
        if not nodes:
            self.sorted_keys = []
            for node in self.node_names:
                add = True
                for res, avl in self.avl_resources[node].items():
                    if res in self.nec_res_types and avl == 0:
                        add = False
                        break
                if add:
                    self.sorted_keys.append(node)
        else:
            to_remove = set()
            for node in nodes:
               for res, avl in self.avl_resources[node].items():
                   if res in self.nec_res_types and avl == 0:
                       to_remove.add(node)
                       break
            for node in to_remove:
                self.sorted_keys.remove(node)
        self.sorted_keys.sort(key=self.ranking, reverse=False)
