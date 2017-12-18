"""
MIT License

Copyright (c) 2017 anetti, cgalleguillosm

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
from accasim.base.allocator_class import ffp_alloc
from copy import copy


class allocator_balanced(ffp_alloc):
    """
    An allocator which considers a set of critical and scarce resource types (like GPUs or MICs), and tries to balance 
    the allocation to the respective nodes in order to avoid fragmentation and waste.
    
    The algorithm will collect the nodes having different critical resource types in distinct sets. It will then build 
    a list of nodes to be used in the allocation process: the nodes having no critical resource types are placed in 
    front, followed by the nodes having critical resources; these nodes are interleaved, in order to balance their usage
    and avoid favoring a specific resource type.
    The resource types to be balanced can be given as input. If a node has more than one type of critical resource
    available, it will be considered for the type for which it has the greatest availability.
    """

    name = 'Balanced'

    def __init__(self, seed, res_man, **kwargs):
        """
        Constructor for the class.
        :param seed: seed for random events (not used)
        :param resource_manager: reference to the system resource manager
        :param kwargs: critical_res = defines the set of resource types to be balanced (default mic,gpu); 
        """
        ffp_alloc.__init__(self, seed, res_man)

        res_key = 'critical_res'
        # If the user doesn't supply the set of resources to balance, mic and gpu are used by default
        if res_key in kwargs.keys():
            self._critical_resources = kwargs[res_key]
            assert all(res in self.resource_manager.resource_types() for res in
                       self._critical_resources), 'Selected resource types for interleaving are not correct'
        else:
            self._critical_resources = [r for r in ('mic', 'gpu') if r in self.resource_manager.resource_types()]
        # The ID to associate to nodes/jobs that possess no critical resources
        self._noneID = 'None'
        # The lists containing the single node IDs, per resource type
        self._res_lists = None

    def _sort_resources(self):
        """
        This method sorts the keys of the available resources dictionary, basing on the ranking policy.
        It is called after the resources are set in the allocator.
        :return: the list of sorted keys (node ids) for the resources
        """
        assert self._avl_resources is not None, 'The dictionary of available resources must be non-empty.'

        # res_lists is a dictionary containing, for each resource type, the list of nodes that have them. The lists
        # do not overlap, and each node falls in the list whose resource it has in the greatest quantity.
        # If a node has none of the critical resource, it will fall in a special 'none' list
        self._res_lists = {self._noneID: []}
        for k in self._critical_resources:
            self._res_lists[k] = []

        # All the nodes in the avl_resources dictionary are classified, according to the critical resources
        # they possess
        for node, res in self._avl_resources.items():
            self._res_lists[self._critical_list_select(res)].append(node)

        return self._convert_to_final_list(self._res_lists)

    def _adjust_resources(self, sorted_keys):
        """
        Adjusts the sorting of the resources after a successful allocation. 
        In order to improve efficiency, this method uses the self._res_lists dictionary stored after a set_resources
        call. The lists are not built from scratch, and a minor adjustment is performed to restore sorting.
        :param sorted_keys: the list of keys, almost sorted, that needs to be adjusted
        """
        assert self._avl_resources is not None, 'The dictionary of available resources must be non-empty.'
        assert sorted_keys is not None, 'The list of keys must be non-empty'
        assert self._res_lists is not None, 'Cannot adjust resources if they have not been initialized'
        sorted_keys.clear()
        sorted_keys += self._convert_to_final_list(self._res_lists)

    def _update_resources(self, reserved_nodes, requested_resources):
        """
        Updates the internal avl_resources list after a successful allocation.

        Also, this method updates the internal self._res_lists dictionary, moving nodes whose resources have changed
        to the appropriate list in the dictionary. Such lists will then be sorted again at the next adjust_resources
        call.
        :param reserved_nodes: the list of nodes assigned to the allocated job
        :param requested_resources: the list of resources requested by the job per each node
        """
        # Nodes involved in the update are removed from the respective lists
        temp_node_list = []
        for node in set(reserved_nodes):
            # Small trick: if a node belongs to the None list before the update, we do not consider it for the
            # resource adjustment, as it will be placed again in the None list, which is not sorted
            rr_res = self._critical_list_select(self._avl_resources[node])
            if rr_res is not self._noneID:
                self._res_lists[self._critical_list_select(self._avl_resources[node])].remove(node)
                temp_node_list.append(node)
        ffp_alloc._update_resources(self, reserved_nodes, requested_resources)
        # Again, nodes that are involved in the update are re-added to the lists, according to their new resources
        for node in temp_node_list:
            self._res_lists[self._critical_list_select(self._avl_resources[node])].append(node)

    def _convert_to_final_list(self, res_lists):
        """
        A method which considers a dictionary of node lists for each critical resource, and builds a final sorted
        list of nodes from it.

        :param res_lists: the dictionary of node lists, for each critical resource
        :return: the final sorted list to be used by the allocator
        """
        # Each separate list is sorted, so that nodes with the most available critical resources will be placed
        # at the end
        res_lists[self._noneID].sort(key=lambda x: x)
        for key in self._critical_resources:
            res_lists[key].sort(key=lambda x: (self._avl_resources.get(x).get(key), x))

        # The lists are then combined, by placing in front the 'none' list, which is a buffer for the critical res
        final_list = copy(res_lists[self._noneID])
        remaining_nodes = len(self._avl_resources) - len(final_list)

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
