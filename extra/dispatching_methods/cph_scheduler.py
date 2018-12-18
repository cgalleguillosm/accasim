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
from collections import namedtuple
from ortools.constraint_solver import pywrapcp
from accasim.base.scheduler_class import SchedulerBase 
from _functools import reduce
from _bisect import bisect_left
from bisect import bisect_right

class sorted_object_list():
    """
    
    Sorted Object list, with two elements for comparison, the main and the tie breaker. Each object must have an id for identification
    
    """

    def __init__(self, sorting_priority, _list=[]):
        """
    
        Sorted object list constructor. 
        
        :param sorting_priority: Dictionary with the 'main' and 'break_tie' keys for selecting the attributes for sorting. The value of the key corresponds to the object attribute.
        :param _list: Optional. Initial list  
    
        """
        assert (isinstance(sorting_priority, dict) and set(['main', 'break_tie']) <= set(sorting_priority.keys()))

        self.main_sort = sorting_priority['main']
        self.break_tie_sort = sorting_priority['break_tie']
        self.list = []
        self.main = []
        self.secondary = []
        self.map = {
            'pos': {},
            'id': {}
        }
        self.objects = {}
        # dict values, function or inner attributes of wrappred objs
        self._iter_func = lambda act, next: act.get(next) if isinstance(act, dict) else (
            getattr(act, next)() if callable(getattr(act, next)) else getattr(act, next))

        if _list:
            self.add(*_list)

    def add(self, *args):
        """
    
        Add new elements to the list
        
        :param \*args: List of new elements 
    
        """
        for arg in args:
            _id = getattr(arg, 'id')
            if _id in self.map['id']:
                continue
            self.objects[_id] = arg
            _main = reduce(self._iter_func, self.main_sort.split('.'), arg)
            _sec = reduce(self._iter_func, self.break_tie_sort.split('.'), arg)
            _pos = bisect_left(self.main, _main)
            main_pos_r = bisect_right(self.main, _main)
            if _pos == main_pos_r:
                self.list.insert(_pos, _id)
                self.main.insert(_pos, _main)
                self.secondary.insert(_pos, _sec)
            else:
                _pos = bisect_left(self.secondary[_pos:main_pos_r], _sec) + _pos
                self.list.insert(_pos, _id)
                self.main.insert(_pos, _main)
                self.secondary.insert(_pos, _sec)
            self.map_insert(self.map['id'], self.map['pos'], _pos, _id)

    def map_insert(self, ids_, poss_, new_pos, new_id):
        """
    
        Maps the new element to maintain the sorted list.
    
        :param ids_: Current id of the object
        :param poss_: Current position of the object
        :param new_pos: New position
        :param new_id: New id
    
        """
        n_items = len(ids_)
        if n_items > 0:
            if not (new_pos in poss_):
                poss_[new_pos] = new_id
                ids_[new_id] = new_pos
            else:
                self.make_map(ids_, poss_, new_pos)
        else:
            ids_[new_id] = new_pos
            poss_[new_pos] = new_id

    def make_map(self, ids_, poss_, new_pos=0, debug=False):
        """
    
        After a removal of a element the map must be reconstructed.
    
        """
        for _idx, _id in enumerate(self.list[new_pos:]):
            ids_[_id] = _idx + new_pos
            poss_[_idx + new_pos] = _id
        if len(ids_) == len(poss_):
            return
        for p in list(poss_.keys()):
            if p > _idx:
                del poss_[p]

    def remove(self, *args, **kwargs):
        """
    
        Removal of an element
    
        :param \*args: List of elements
    
        """
        for id in args:
            assert (id in self.objects)
            del self.objects[id]
            self._remove(self.map['id'][id], **kwargs)

    def _remove(self, _pos, **kwargs):
        """
        
        Removal of an element
        
        :param \*args: List of elements
        
        """
        del self.list[_pos]
        del self.secondary[_pos]
        del self.main[_pos]

        _id = self.map['pos'].pop(_pos)
        del self.map['id'][_id]
        self.make_map(self.map['id'], self.map['pos'], **kwargs)

    def get(self, pos):
        """
        
        Return an element in a specific position
        
        :param pos: Position of the object 
        
        :return: Object in the specified position
        
        """
        return self.list[pos]

    def get_object(self, id):
        """
        
        Return an element with a specific id.
        
        :param id: Id of the object 
        
        :return: Obect with the specific id
        
        """
        return self.objects[id]

    def get_list(self):
        """
        
        :return: The sorted list of ids of elements
        
        """
        return self.list

    def get_object_list(self):
        """
        
        :return: The sorted list of objects
        
        """
        return [self.objects[_id] for _id in self.list]

    def __len__(self):
        return len(self.list)

    # Return None if there is no coincidence
    def pop(self, id=None, pos=None):
        """
        
        Pop an element of the sorted list. 
        
        :param id: id to be poped
        :param pos: pos to be poped
        
        :return: Object
        
        """
        assert (not all([id, pos])), 'Pop only accepts one or zero arguments'
        if not self.list:
            return None
        elif id:
            return self._specific_pop_id(id)
        elif pos:
            return self._specific_pop_pos(pos)
        else:
            _id = self.list[0]
            self._remove(0)
            return self.objects.pop(_id)

    def _specific_pop_id(self, id):
        _obj = self.objects.pop(id, None)
        if _obj:
            self._remove(self.map['id'][id])
        return _obj

    def _specific_pop_pos(self, pos):
        _id = self.map['pos'].pop(pos, None)
        if _id:
            self.map['pos'][pos] = _id
            self._remove(pos)
        return self.objects.pop(_id, None)

    def __iter__(self):
        self.actual_index = 0
        return self

    def __next__(self):
        try:
            self.actual_index += 1
            return self.list[self.actual_index - 1]
        except IndexError:
            raise StopIteration

    def get_reversed_list(self):
        """
        
        :return:  Reversed list of ids
        
        """
        return list(reversed(self.list))

    def get_reversed_object_list(self):
        """
        
        :return: Reversed list of objects
        
        """
        return [self.objects[_id] for _id in reversed(self.list)]

    def __str__(self):
        return str(self.list)

class cph_scheduler(SchedulerBase):
    """
    A scheduler which uses constraint programming to plan a sub-optimal schedule.
    This scheduler don't use the automatic feature of the automatic allocation, then it isn't
    
    """
    name = 'CPH'
    
    def __init__(self, allocator, resource_manager=None, _seed=0, _ewt={'default': 1800}, **kwargs):
        SchedulerBase.__init__(self, _seed, None)
        self.ewt = _ewt
        self.manual_allocator = allocator
        
        self.max_ewt = 0
        self.QueuedJobClass = namedtuple('Job', ['id', 'first', 'second'])
        self.queued_jobs = sorted_object_list({'main': 'first', 'break_tie':'second'})
        self.numberOfIterations = 1
        self.max_timelimit = 150000  # 2.5min
        
           
    def scheduling_method(self, cur_time, es, es_dict):
        """
            This function must map the queued events to available nodes at the current time.
            
            :param cur_time: current time
            :param es_dict: dictionary with full data of the events
            :param es: events to be scheduled
            :param _debug: Flag to debug
            
            :return a tuple of (time to schedule, event id, list of assigned nodes)  
        """
        _debug = False
        if not self.manual_allocator.resource_manager:
            self.manual_allocator.set_resource_manager(self.resource_manager)
        allocation = []
        avl_resources = self.resource_manager.current_availability
        self.manual_allocator.set_resources(avl_resources)
        for i in range(self.numberOfIterations):
            timelimit = 1000
            solve_tries = 1
            sol_found = False
            temp_sched = {}
            while not sol_found and timelimit < self.max_timelimit:                        
                sol_found = self.cp_model(es, es_dict, temp_sched, timelimit, cur_time,
                                          self.queued_jobs, avl_resources, _debug)
                if _debug:
                    print('#{} Try. Results {}'.format(solve_tries, temp_sched))
                solve_tries += 1
                timelimit *= 2
            assert(len(es) == len(temp_sched)), 'Some Jobs were not scheduled.'
            allocation = self.allocate(cur_time, temp_sched, es_dict, _debug)        
        return allocation   

    def allocate(self, cur_time, schedule_plan, es_dict, _debug):
        """
        Prepare jobs to be allocated. Just jobs that must be started at the current time are considered.
        @param cur_time: Current simulation time
        @param schedule_plan: schedule plan  
        @param es_dict: Dictionary of the current jobs.
        @param _debug: Debug flag
        
        @return: dispatching plan        
        """
        allocated_events = []
        to_schedule_now = []
        to_schedule_later = []
        # Building the list of jobs that can be allocated now, in the computed solution
        for _id, _time in schedule_plan.items():
            if _time == cur_time:
                to_schedule_now.append(es_dict[_id])
                if _debug:
                    print('Trying to Allocate {} job.'.format(_id))
            else:
                to_schedule_later.append(es_dict[_id])
                if _debug:
                    print('{} incorrect allocation. {}'.format(_id, 'Postponed job (Estimated time: {})'.format(_time)))
        # Trying to allocate all jobs scheduled now
        self.manual_allocator.set_attr(schedule=(es_dict, schedule_plan))
        allocation = self.manual_allocator.allocate(to_schedule_now, cur_time, skip=True)

        for (time, idx, nodes) in allocation:
            if time is not None:
                if _debug:
                    print('{} correct allocation. Removing from priority job list'.format(idx))
                self.queued_jobs.remove(idx)
            else:
                if _debug:
                    print('{} incorrect allocation. {}'.format(idx, 'Insufficient resources'))

        allocated_events += allocation
        # All jobs to be scheduled later are considered as discarded by the allocator
        allocated_events += [(None, ev.id, []) for ev in to_schedule_later]
        return allocated_events, []

    def cp_model(self, es, es_dict, temp_sched, timelimit, cur_time, queued_jobs, avl_resources, _debug):
        """
        Implementation of the CP Model using OR-Tools to generate the schedule plan.
        
        @param es: Queued jobs to be scheduled.
        @param es_dict: Dictionary of the current jobs.
        @param temp_sched: Storages the scheduled jobs.
        @param timelimit: Limit of the search process in ms.
        @param cur_time: Current simulated time
        @param queued_jobs: Already queued jobs and sorted.
        @param avl_resources: Availability of the resources
        @param _debug: Debug flag.
        
        @return True is solved, False otherwise. 
        
        """
        search_needed = False
        solver = False
        
        solver = pywrapcp.Solver('CPSolver_relaxedResources')
        q_length = 100
        job_real_vars_count = 0 
        
        running_events = self.resource_manager.current_allocations
        max_mks = 0 
        job_map = {}
        for _e in running_events:
            e = es_dict[_e]
            job_map[_e] = e
            mks = e.expected_duration - (cur_time - e.start_time)
            if mks >= 0:
                max_mks += mks
        
        for i, job_obj in enumerate(es):
            if i < q_length:
                # To be scheduled
                # e = es_dict[_e]
                max_mks += job_obj.expected_duration  # es_dict[_e].expected_duration
                job_map[job_obj.id] = job_obj
            else:
                # Postponed
                temp_sched[job_obj.id] = None
        
        self.queued_jobs.add(*self.prepare_job(job_map.values(), cur_time))
        prb_vars = []
        for _id in self.queued_jobs:
            name = _id
            duration = job_map[_id].expected_duration
            if _id in running_events:
                if _debug:
                    print('{} has an assigned start, and will not be modified.'.format(_id))
                start_min = 0
                start_max = start_min

                elapsed_time = (cur_time - job_map[_id].start_time)
                duration = (duration - elapsed_time)
            else:
                """
                Arguments: int64 start_min, int64 start_max, int64 duration, bool optional, const std::string& name
                Creates an interval var with a fixed duration. The duration must be greater than 0. 
                If optional is true, then the interval can be performed or unperformed. 
                If optional is false, then the interval is always performed.                        
                """
                if _debug:               
                    print('{} is a new Interval variable, without any assignation'.format(_id))
                start_min = 0
                start_max = max_mks
                search_needed = True
                job_real_vars_count += 1
            var = solver.FixedDurationIntervalVar(start_min, start_max, duration, False, name)
            prb_vars.append(var)
        
        if search_needed:
            solved = False
            _keys = self.resource_manager.resource_types  # ()
            total_capacity = {}
            total_demand = {}
            for _key in _keys:
                total_capacity[_key] = sum([ resources[_key] for node, resources in avl_resources.items()])
                # The resources used by running jobs are loaded into the capacity
                total_capacity[_key] += sum(es_dict[_sjob].requested_nodes * es_dict[_sjob].requested_resources[_key] for _sjob in running_events)
                total_demand[_key] = [ es_dict[_sjob].requested_nodes * es_dict[_sjob].requested_resources[_key] for _sjob in self.queued_jobs]
                if _debug:
                    print('Loading constraints related to capacity and demand of {}: Capacity {} - Demand {}'.format(_key, total_capacity[_key], total_demand[_key]))
                _name = 'cum_{}'.format(_key)
                _cum = solver.Cumulative(prb_vars, total_demand[_key], total_capacity[_key], _name)
                solver.AddConstraint(_cum)
            
            wt = []
            for i, _sjob in enumerate(self.queued_jobs):
                _job = es_dict[_sjob]
                _id = _job.id
                ewt = self.get_ewt(_job.queue) 
                wgt = int((self.max_ewt * 100000) / ewt)  #  5 digits
                jstart = prb_vars[i].SafeStartExpr(max_mks - _job.expected_duration)
                qtime = jstart + (_job.queued_time - cur_time)
                prod = (qtime * wgt)
                wt.append(prod.Var())
                
            # Minimize the weighted queue time
            objective_var = solver.Sum(wt).Var()
            objective_monitor = solver.Minimize(objective_var, 1)           
            db = solver.HeuristicSearch(prb_vars)

            limit = solver.TimeLimit(timelimit)

            log = solver.SearchLog(10000)
            solver.NewSearch(db, objective_monitor, limit)

            # Start Search
            while solver.NextSolution():
                solved = True
                if _debug:
                    print('Solved during search')
                    for i, _j in enumerate(prb_vars):
                        print('{}: {} EST {}, LST {}, EET, LET'.format(i, _j, _j.StartMin(), _j.StartMax(), _j.EndMin(), _j.EndMax()))
                for _j in prb_vars:
                    if _j.Name() in running_events:
                        if _debug:
                            print('{} is already running, it not will be considered to update the actual schedule.'.format(_j.Name()))
                        # self.queued_jobs.remove(_j.Name())
                        continue
                    if _debug:
                        print('Loading {} into the schedule. '.format(_j.Name()))
                    temp_sched[_j.Name()] = _j.StartMin() + cur_time
            if not solved:
                solver.EndSearch()
                for _e in es:
                    temp_sched[_e] = None                    
        else:
            solved = True
        for _j in prb_vars:
            if _j.Name() in running_events:
                if _debug:
                    print('{} is already running, it not will be considered to update the actual schedule.'.format(_j.Name()))
                self.queued_jobs.remove(_j.Name())
        return solved
            
    def get_id(self):
        """
        Returns the full ID of the scheduler, including policy and allocator.

        :return: the scheduler's id.
        """
        return '-'.join([self.__class__.__name__, self.name, self.manual_allocator.get_id()])
        
    def prepare_job(self, jobs, cur_time):
        """
        Establishes the priority of each job.
        
        @param jobs: List of jobs
        @param cur_time: Current simulation time.
        
        @return A list with the calculation of the priorities for each job.
        """
        sort_helper = {
            e.id: 
            {
                    'qtime':cur_time - e.queued_time + 1,
                    'ewt': self.get_ewt(e.queue),
                    'dur': e.expected_duration,
                    'req': sum([e.requested_nodes * val for attr, val in e.requested_resources.items()])
                }  
            for e in jobs
        }
        ended = set(self.queued_jobs) - set(sort_helper.keys())
        self.queued_jobs.remove(*ended) 
        self.max_ewt = max([v['ewt'] for v in sort_helper.values()])
        return [ 
            self.QueuedJobClass(**{
                'id': e.id,
                'first':-(self.max_ewt * sort_helper[e.id]['qtime']) / sort_helper[e.id]['ewt'],
                'second': sort_helper[e.id]['dur'] * sort_helper[e.id]['req']
            }) for e in jobs]    

    def get_ewt(self, queue_type):
        """
        @param queue_type: Name of the queue.
        
        @return: EWT for a specific queue type
        """
        if queue_type in self.ewt:
            return self.ewt[queue_type]
        return self.ewt['default']
