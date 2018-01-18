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
from accasim.base.allocator_class import ff_alloc


class allocator_weighted(ff_alloc):
    """
    An allocator which tries to perform optimization on the allocation of all events in the queue.

    When the user supplies the queue of events to be allocated, the average number of resources required per-type
    is computed, together with the current load rate. The average is weighted according to the expected length of
    each job. These values are used as weights to indicate the value of a resource. Also, the allocator computes 
    the resource weights considering also the base availability in the system, assigning higher values to scarcer 
    resources, to assess which are actually more precious and need to be preserved in a global manner. Each job is 
    then allocated on the nodes on which the minimum number of weighted resources is left (after the allocation), 
    in a similar fashion to the best-fit allocator. 
    
    The user can also supply a "critical resources" list: for such resources, an additional heuristic is used, and in
    this case the allocator goes under the name of Priority-Weighted; this consists in a priority value, per resource 
    type, which will increase as allocations fail for jobs that require critical resources, and will decrease for each 
    successful allocation. If a jobs requires multiple critical resource types, all of their priority values will be
    affected by the allocation. This tends to assign greater weight to critical resources actually requested by jobs, 
    and will make the allocator statistically preserve such resources.
    """

    name = 'Weighted'

    def __init__(self, seed, res_man, **kwargs):
        """
        Constructor for the class.

        :param seed: seed for random events (not used)
        :param resource_manager: reference to the system resource manager
        :param kwargs: critical_res = defines the set of resource types to be preserved (default none); 
                       critical_bounds = the lower and upper bounds for the allocation fail heuristic (default [1,10]);
                       critical_steps = the number of steps in the scale between the critical_bounds (default 9);
                       window_size = defines the window size for job resource analysis(default 100);            
        """
        ff_alloc.__init__(self, seed, res_man)

        win_key = 'window_size'
        res_key = 'critical_res'
        steps_key = 'critical_steps'
        bound_key = 'critical_bounds'

        # If the user doesn't supply the set of resources to balance, mic and gpu are used by default
        if res_key in kwargs.keys():
            self._critical_resources = kwargs[res_key]
            assert all(res in self.resource_manager.resource_types() for res in self._critical_resources), 'Selected resource types for interleaving are not correct'
        else:
            self._critical_resources = [] # Priority-Weighted heuristic is disabled by default
        # The default window size to be used for job analysis is 100
        self._windowsize = (kwargs[win_key] if win_key in kwargs.keys() and kwargs[win_key] >= 0 else 100)
        # The lower and upper bounds for the weight modifier of critical resources
        if bound_key in kwargs.keys() and len(kwargs[bound_key]) == 2 and 1 <= kwargs[bound_key][0] <= kwargs[bound_key][1]:
            self._modifierbounds = kwargs[bound_key]
        else:
            self._modifierbounds = [1, 10]
        # The number of steps in the scale between the lower and upper bound for the allocation fail heuristic
        self._numsteps = (kwargs[steps_key] if steps_key in kwargs.keys() and kwargs[steps_key] > 0 else 9)

        # Parameters relative to the allocation fail metric for jobs requiring critical resources
        # The step for increasing and decreasing the modifiers
        self._modifierstep = (self._modifierbounds[1] - self._modifierbounds[0]) / self._numsteps
        # The dictionary containing the current modifier values
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

        # Two variables used to compute the weighted average of job resources' requests; they store the total
        # length of jobs in the analysis window
        self._cumulative_length = 0
        # Same as above, but for jobs overlapping the current one in the schedule (for CP)
        self._cumulative_schedule = 0

    def set_resources(self, res):
        """
        Sets in the internal variable avl_resources the current available resources for the system.

        :param res: the list of currently available resources for the system
        """
        self._avl_resources = res
        self._sorted_keys = self._trim_nodes(list(self._avl_resources.keys()))

    def set_attr(self, **kwargs):
        """
        Method used to set internal parameters and meta-data for the allocator.

        Its behavior depends on the specific allocator that is being used, and some arguments may be discarded.
        In this optimization-based allocator, the method is used to set the internal variable related to the 
        schedule plan, if present. It is useful for the CP scheduler, which is the only one that computes an actual
        scheduling plan.

        :param kwargs: schedule = the scheduling plan, in a tuple of the type (es_dict,dict(job_id: start_time))
        """
        schedule_key = 'schedule'
        if schedule_key in kwargs.keys():
            self._event_dictionary = kwargs[schedule_key][0]
            self._schedule = kwargs[schedule_key][1]
        elif 'dict' in kwargs.keys():
            self._event_dictionary = kwargs['dict']

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

        :return: a list of assigned nodes of length e.requested_nodes, for all events that could be allocated. The
        list is in the format (time,event,nodes) where time can be either cur_time or None.
        """
        if not isinstance(es, (list, tuple)):
            listAsInput = False
            es = [es]
        else:
            listAsInput = True

        allocation = []
        success_counter = 0
        event_counter = 0
        # Create aux resources for this allocation
        self._set_aux_resources()

        # The required resources' counters are initialized according to the event queue given as input
        self._initialize_counters(es)
        if debug:
            print('Queue length %i - Window size %i' % (len(es),self._jobstoallocate))
            print('Distribution: ')
            print(self._rescounters.values())

        for e in es:
            requested_nodes = e.requested_nodes
            requested_resources = e.requested_resources

            # We verify that the job does not violate the system's resource constraints
            for t in requested_resources.keys():
                assert requested_resources[t] * requested_nodes <= self._base_availability[t], 'There are %i %s total resources in the system, requested %i ' % (self._base_availability[t], t, requested_resources[t])

            # If the input arguments relative to backfilling are not supplied, the method operates in regular mode.
            # Otherwise, backfilling mode is enabled, allowing the allocator to skip jobs and consider the reservation.
            nodes_to_discard = self._compute_reservation_overlaps(e, cur_time, reserved_time, reserved_nodes, debug)
            backfilling_overlap = False if len(nodes_to_discard) == 0 else True

            self._find_overlapping_jobs(e, cur_time)
            # After each allocation, the weights are updated according to the new analysis window
            self._update_weights()
            # After computing the weights, the sorted list of available nodes for allocation is computed
            nodekeys = self._get_sorted_nodes(e)

            assigned_nodes = []
            nodes_left = requested_nodes
            for node in nodekeys:
                # The algorithm check whether the given node belongs to the list of reserved nodes, in backfilling.
                # If it does, the node is discarded.
                resources = self._avl_resources[node]
                if backfilling_overlap and node in nodes_to_discard:
                    continue
                # We compute the number of job units fitting in the current node, and update the assignment
                fits = self._event_fits_node(resources, requested_resources)
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
            assert not assigned_nodes or requested_nodes == len(assigned_nodes), 'Requested' + str(
                requested_nodes) + ' got ' + str(len(assigned_nodes))

            # If a correct allocation was found, we update the resources of the system, sort them again, and
            # add the allocation to the output list.
            self._update_modifiers(e, assigned_nodes)
            if assigned_nodes:
                allocation.append((cur_time, e.id, assigned_nodes))
                self._update_resources(assigned_nodes, requested_resources)
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
            # After trying to allocate the current event, the analysis window is shifted by one: the previous first
            # event is removed from the counters, and the next one falling in the window is added to them
            event_counter += 1
            self._update_counters(es, event_counter)
        if debug:
            print('There were %s successful allocations out of %s events' % (success_counter, len(es)))
        self._reset_schedule()
        return allocation if listAsInput else allocation[0]

    def _initialize_counters(self, es):
        """
        A method which initializes the required resources counters for the job queue. 
        
        It is executed only once for each queue allocation request, and then the counters are dynamically 
        and efficiently updated through the update_counters method.
        
        :param es: the list of events to be allocated
        """
        self._cumulative_length = 0
        self._cumulative_schedule = 0
        # The counters are initialized to 1, to avoid having weights equal to 0
        for k in self._types:
            self._rescounters[k] = 1
        # The size of the analysis window is computed
        self._jobstoallocate = len(es) - 1 if len(es) - 1 <= self._windowsize else self._windowsize
        # For each job in the window, we then add to the counters its total number of resources required per-type
        for i in range(self._jobstoallocate):
            req_res = es[i + 1].requested_resources
            req_nodes = es[i + 1].requested_nodes
            self._cumulative_length += es[i + 1].expected_duration + 1
            for k in self._types:
                self._rescounters[k] += req_res[k] * req_nodes * (es[i + 1].expected_duration + 1)

    def _update_counters(self, es, startindex):
        """
        A method which updates the counters of resources requested by jobs.
        
        It operates in O(1) time, since we just need to remove the old job from the counters and add the new one,
        if present.
        
        :param es: The list of jobs to be allocated
        :param startindex: The starting index of the new window (including the job that is to be allocated now)
        """
        # Adding new event to counters
        if startindex + self._windowsize < len(es):
            e = es[startindex + self._windowsize]
            req_res = e.requested_resources
            req_nodes = e.requested_nodes
            self._cumulative_length += e.expected_duration + 1
            for k in self._types:
                self._rescounters[k] += req_res[k] * req_nodes * (e.expected_duration + 1)
        else:
            self._jobstoallocate -= 1
        # Removing old event from counters
        if 0 <= startindex < len(es):
            e = es[startindex]
            req_res = e.requested_resources
            req_nodes = e.requested_nodes
            self._cumulative_length -= e.expected_duration + 1
            for k in self._types:
                self._rescounters[k] -= req_res[k] * req_nodes * (e.expected_duration + 1)
                assert self._rescounters[k] >= 1, 'A resource is going below zero during counter update'

    def _update_weights(self):
        """
        Computes the weights associated to each resource type, basing on the current resource counters.
        
        The weights consider the average number of resources required by jobs in the window, weighted according to
        the job's expected duration. This average per-resource request is then normalized by the
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
            #self._weights[k] = (self._rescounters[k] + self._schedulecounters[k]) / (self._jobstoallocate + self._jobstoschedule + 1)
            self._weights[k] = (self._rescounters[k] + self._schedulecounters[k]) / (self._cumulative_length + self._cumulative_schedule + 1)
            # Might be useful to smooth out and compress average values
            # self._weights[k] = sqrt(self._weights[k])
            self._weights[k] *= (used_resources[k] + 1) / (self._base_availability[k] * self._base_availability[k])
            # Alternative weighting strategy, considers only the load factor: simpler, but with worse results
            # self._weights[k] *= (used_resources[k] + 1) / (self._base_availability[k])
            # The weights related to critical resources are multiplied by the allocation fail heuristic
            if k in self._critical_resources:
                self._weights[k] *= self._critical_modifier[k]

    def _find_overlapping_jobs(self, e, cur_time):
        """
        A method which searches for overlapping jobs in the schedule with the one to be allocated now.
        
        The schedule is to be set by the user via the set_attr method before allocation. The metric used to
        determine overlap is the expected duration of the job. All the data computed from the resources needed
        by the overlapping jobs is stored in the schedulecounters and jobstoschedule, identical in function to
        rescounters and jobstoallocate.
        
        :param e: the event to be allocated now
        :param cur_time: the current time, in which allocation will be performed
        """
        # In any case, the max number of jobs we want to consider is windowsize
        maxjobs = self._windowsize - self._jobstoallocate
        if maxjobs == 0 or self._schedule is None or self._event_dictionary is None:
            return
        # The resource counters are reset
        for k in self._types:
            self._schedulecounters[k] = 0
        self._jobstoschedule = 0
        self._cumulative_schedule = 0
        # The expected end time of the current job is used to estimate overlap with future jobs
        end_time = cur_time + e.expected_duration
        for jobid, jobtime in self._schedule.items():
            if cur_time < jobtime < end_time:
                # For overlapping jobs we update the resource counters like in regular allocation
                req_res = self._event_dictionary[jobid].requested_resources
                req_nodes = self._event_dictionary[jobid].requested_nodes
                self._cumulative_schedule += self._event_dictionary[jobid].expected_duration
                for k in self._types:
                    self._schedulecounters[k] += req_res[k] * req_nodes * self._event_dictionary[jobid].expected_duration
                self._jobstoschedule += 1
                # If the maximum number of jobs in the window has been reached, we break from the cycle
                if self._jobstoschedule >= maxjobs:
                    break

    def _update_modifiers(self, e, success):
        """
        This method updates the modifiers used for critical resources (if active) upon a failed 
        or successful allocation.
        
        :param e: the event subject to allocation
        :param success: boolean value; True if the allocation succeeded, False otherwise
        """
        noderes = e.requested_resources
        for cres in self._critical_resources:
            if noderes[cres] > 0:
                if not success:
                    self._critical_modifier[cres] += self._modifierstep
                    if self._critical_modifier[cres] > self._modifierbounds[1]:
                        self._critical_modifier[cres] = self._modifierbounds[1]
                else:
                    self._critical_modifier[cres] -= self._modifierstep
                    if self._critical_modifier[cres] < self._modifierbounds[0]:
                        self._critical_modifier[cres] = self._modifierbounds[0]

    def _reset_schedule(self):
        """
        A method which resets all internal variables related to scheduling, after allocation of all jobs is performed.
        
        The use of this method implies that the schedule must be set every time after calling the search_allocation
        method. 
        """
        self._jobstoschedule = 0
        self._schedule = None
        self._event_dictionary = None
        for k in self._types:
            self._schedulecounters[k] = 0

    def _get_sorted_nodes(self, e):
        """
        Given an event e to be allocated, the method returns the sorted list of nodes which best fit the job.
        
        :param e: The event to be allocated
        :return: The sorted list of nodes that best fit the job
        """
        assert self._avl_resources is not None, 'The dictionary of available resources must be non-empty.'
        nodelist = []
        s_nodes = self._find_sat_nodes(e.requested_resources)
        # For each node in the system, the job "fit", which is the number of job units fitting the node, is computed
        for node in s_nodes: #self._avl_resources.keys():
            fits = self._event_fits_node(self._avl_resources[node], e.requested_resources)
            # If the node has not enough resources to fit the job, it is simply discarded
            if fits == 0:
                continue
            elif fits > e.requested_nodes:
                fits = e.requested_nodes
            # The nodes are ranked by the amount of weighted resources left after allocating the job
            rank = sum((self._avl_resources.get(node).get(k) - e.requested_resources[k] * fits) * self._weights[k] for k in self._types)
            # Alternative ranking, similar to a weighted consolidate; usually performs worse than the above
            # rank = sum((self._avl_resources.get(node).get(k)) * self._weights[k] for k in self._types)
            # We use a temporary list to store the node ID and its ranking
            nodelist.append((node,rank))
        # Lastly, sorting is performed. Note that sorting is performed only on nodes that actually fit the job, thus
        # resulting in smaller instances and lower times compared to, for example, the consolidate allocator
        nodelist.sort(key=lambda x: x[1])
        # The list of sorted node IDs is returned
        return [x[0] for x in nodelist]
