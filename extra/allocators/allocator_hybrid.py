"""
MIT License

Copyright (c) 2017 anetti

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
from extra.allocators.allocator_weighted import allocator_weighted
from accasim.base.allocator_class import ffp_alloc


class allocator_hybrid(allocator_weighted):
    """
    This allocator is a combination of the Weighted and Balanced allocators.
    
    Intuitively, it will separate and interleave the nodes according to the critical resources they possess like in
    the Balanced allocator; however, each of those lists is sorted individually like in the Weighted allocator.
    """

    name = 'Hybrid'

    def __init__(self, seed, res_man, **kwargs):
        """
        Constructor for the class.

        :param seed: seed for random events (not used)
        :param resource_manager: reference to the system resource manager
        :param kwargs: critical_res = defines the set of resource types to be preserved (default mic,gpu); 
                       window_size = defines the window size for job resource analysis(default 100);            
        """
        ffp_alloc.__init__(self, seed, res_man)

        win_key = 'window_size'
        res_key = 'critical_res'

        # If the user doesn't supply the set of resources to balance, mic and gpu are used by default
        if res_key in kwargs.keys():
            self._critical_resources = kwargs[res_key]
            assert all(res in self.resource_manager.resource_types() for res in self._critical_resources), 'Selected resource types for interleaving are not correct'
        else:
            self._critical_resources = [r for r in ('mic', 'gpu') if r in self.resource_manager.resource_types()]
        # The default window size to be used for job analysis is 100
        self._windowsize = (kwargs[win_key] if win_key in kwargs.keys() and kwargs[win_key] >= 0 else 100)

        # Dummy values for the parameters of the failed allocation heuristic, not used here
        self._modifierbounds = [1, 1]
        self._numsteps = 1
        self._modifierstep = 0
        self._critical_modifier = {}
        for k in self._critical_resources:
            self._critical_modifier[k] = self._modifierbounds[0]

        # The resource types in the system; stored for efficiency
        self._types = self.resource_manager.resource_types()
        # The scheduling plan computed by the scheduler, if present
        self._schedule = None
        # The event dictionary used to retrieve job information from the schedule
        self._event_dictionary = None
        # The number of jobs currently considered for analysis (max is window_size)
        self._jobstoallocate = 0
        # The counters for resources required by jobs in the analysis window
        self._rescounters = {}
        for k in self._types:
            self._rescounters[k] = 1
        # Same as rescounters, but used for the overlapping events in the schedule, and not in the queue
        self._jobstoschedule = 0
        self._schedulecounters = {}
        for k in self._types:
            self._schedulecounters[k] = 1
        # The weights associated to the resource types
        self._weights = {}
        for k in self._types:
            self._weights[k] = 0
        # The ID to associate to nodes/jobs that possess no critical resources
        self._noneID = 'None'

    def _update_weights(self):
        """
        Computes the weights associated to each resource type, basing on the current resource counters.

        The weights consider the average number of resources required by jobs in the window, normalized by the
        base availability of each resource. Then the weights are multiplied by the current load factor for each
        resource, to preserve resources that are becoming scarce in the system. Finally, critical resources that are
        to be preserved at all times in the system (supplied by the user) see their weights multiplied by a constant
        modifier.
        """
        # The amount of current used resources in the system. Used to compute the load rate
        used_resources = {}
        for t in self._types:
            base = self.resource_manager.get_total_resources(t)
            avl = self.resource_manager.availability()
            qt = base[t] - sum([avl[node][t] for node in avl.keys()])
            used_resources[t] = qt
        #used_resources = self.resource_manager.get_used_resources()
        for k in self._types:
            self._weights[k] = (self._rescounters[k] + self._schedulecounters[k]) / (
            self._jobstoallocate + self._jobstoschedule + 1)
            # Might be useful to smooth out and compress average values
            # self._weights[k] = sqrt(self._weights[k])
            self._weights[k] *= (used_resources[k] + 1) / (self._base_availability[k] * self._base_availability[k])
            # Alternative weighting strategy, considers only the load factor: simpler, but with worse results
            # self._weights[k] *= (used_resources[k] + 1) / (self._base_availability[k])

    def _get_sorted_nodes(self, e):
        """
        Given an event e to be allocated, the method returns the sorted list of nodes which best fit the job,
        adjusted like in the balanced allocator.

        :param e: The event to be allocated
        :return: The sorted list of nodes that best fit the job
        """
        assert self._avl_resources is not None, 'The dictionary of available resources must be non-empty.'

        # res_lists is a dictionary containing, for each resource type, the list of nodes that have them. The lists
        # do not overlap, and each node falls in the list whose resource it has in the greatest quantity.
        # If a node has none of the critical resource, it will fall in a special 'none' list
        res_lists = {self._noneID: []}
        for k in self._critical_resources:
            res_lists[k] = []

        # All the nodes in the avl_resources dictionary are classified, according to the critical resources
        # they possess
        for node, res in self._avl_resources.items():
            res_lists[self._critical_list_select(res)].append(node)

        res_lists[self._noneID] = self._get_sorted_node_sublist(e,res_lists[self._noneID])
        for key in self._critical_resources:
            res_lists[key] = self._get_sorted_node_sublist(e,res_lists[key])

        # The lists are then combined, by placing in front the 'none' list, which is a buffer for the critical res
        final_list = res_lists[self._noneID]
        remaining_nodes = sum([len(l) for l in res_lists.values()]) - len(final_list)

        # The algorithm would 'pop' elements from the lists' heads in succession. To avoid this, as it is expensive,
        # we use a dictionary of starting indexes for each list
        start_indexes = dict.fromkeys(self._critical_resources, 0)
        # After the 'none' list, the remaining lists are interleaved, and at each step an element is picked from
        # the longest list in res_lists, in order to balance their usage.
        for i in range(remaining_nodes):
            rr_res = self._get_longest_list(res_lists, start_indexes)
            final_list.append(res_lists[rr_res][start_indexes[rr_res]])
            start_indexes[rr_res] += 1

        return final_list

    def _get_sorted_node_sublist(self, e, nodes):
        """
        Given a sublist of nodes and an event e, the method returns the sorted version of the sublist, like in the
        weighted allocator. It is used to sort the single node lists, relative to the various resource types.
        
        :param e: the event to be allocated
        :param nodes: the nodes' sublist to be sorted
        :return: the sorted nodes' sublist
        """
        nodelist = []
        # For each node in the system, the job "fit", which is the number of job units fitting the node, is computed
        for node in nodes:
            fits = self._event_fits_node(self._avl_resources[node], e.requested_resources)
            # If the node has not enough resources to fit the job, it is simply discarded
            if fits == 0:
                continue
            elif fits > e.requested_nodes:
                fits = e.requested_nodes
            # The nodes are ranked by the amount of weighted resources left after allocating the job
            rank = sum(
                (self._avl_resources.get(node).get(k) - e.requested_resources[k] * fits) * self._weights[k] for k in
                self._types)
            # Alternative ranking, similar to a weighted consolidate; usually performs worse than the above
            # rank = sum((self._avl_resources.get(node).get(k)) * self._weights[k] for k in self._types)
            # We use a temporary list to store the node ID and its ranking
            nodelist.append((node, rank))
        # Lastly, sorting is performed. Note that sorting is performed only on nodes that actually fit the job, thus
        # resulting in smaller instances and lower times compared to, for example, the consolidate allocator
        nodelist.sort(key=lambda x: x[1])
        # The list of sorted node IDs is returned
        return [x[0] for x in nodelist]

    def _critical_list_select(self, noderes):
        """
        A simple method which, given the resources of a node, returns the key of the critical resource for which
        such node has greater availability. If there are no critical resources available, a 'none' key is returned.

        :param noderes: the resources dictionary of a node
        :return: the key of the critical resource to which assign the node
        """
        maxval = 0
        maxkey = self._noneID
        for cres in self._critical_resources:
            if noderes[cres] > maxval:
                maxval = noderes[cres]
                maxkey = cres
        return maxkey

    def _get_longest_list(self, res_lists, start_indexes):
        """
        Given the dictionary of critical resources lists, the method returns the key of the next list from which
        an element has to be picked in order to build the final nodes list.

        :param res_lists: the critical resources' lists dictionary
        :param start_indexes: the dictionary of starting indexes for each list (to simulate a pop operation)
        :return: the key of the next list to be picked
        """
        maxkey = self._noneID
        maxval = 0
        for k in self._critical_resources:
            modifier = len(res_lists[k]) - start_indexes[k]
            if modifier > maxval:
                maxval = modifier
                maxkey = k
        return maxkey
