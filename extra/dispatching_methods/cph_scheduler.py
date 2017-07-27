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
from accasim.base.scheduler_class import scheduler_base
from accasim.utils.misc import sorted_object_list 

class cph_scheduler(scheduler_base):
    """
    A scheduler which uses constraint programming to plan a sub-optimal schedule.
    This scheduler don't use the automatic feature of the automatic allocation, then it isn't
    
    """
    name = 'CPH'
    
    def __init__(self, allocator, resource_manager=None, _seed=0, _ewt={'default': 1800}, **kwargs):
        scheduler_base.__init__(self, _seed, resource_manager, None)
        self.ewt = _ewt
        self.manual_allocator = allocator
        
        self.max_ewt = 0
        self.QueuedJobClass = namedtuple('Job', ['id', 'first', 'second'])
        self.queued_jobs = sorted_object_list({'main': 'first', 'break_tie':'second'})
        self.numberOfIterations = 1
        self.max_timelimite = 150000  # 2.5min
        self.prev_solved = None
        
           
    def scheduling_method(self, cur_time, es_dict, es, _debug):
        """
            This function must map the queued events to available nodes at the current time.
            
            :param cur_time: current time
            :param es_dict: dictionary with full data of the events
            :param es: events to be scheduled
            :param _debug: Flag to debug
            
            :return a tuple of (time to schedule, event id, list of assigned nodes)  
        """
        if not self.manual_allocator.resource_manager:
            self.manual_allocator.set_resource_manager(self.resource_manager)
        allocation = []
        avl_resources = self.resource_manager.availability()
        self.manual_allocator.set_resources(avl_resources)
        for i in range(self.numberOfIterations):
            timelimit = 1000
            solve_tries = 1
            sol_found = False
            temp_sched = {}
            while not sol_found and timelimit < self.max_timelimite:                        
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
        allocation = self.manual_allocator.allocate(to_schedule_now, cur_time, skip=True, debug=_debug)

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
        return allocated_events

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
        
        running_events = self.resource_manager.actual_events
        max_mks = 0 
        job_map = {}
        for _e in running_events:
            e = es_dict[_e]
            job_map[_e] = e
            max_mks += e.expected_duration - (cur_time - e.start_time)
        
        for _e in es:
            e = es_dict[_e]
            max_mks += es_dict[_e].expected_duration
            job_map[_e] = e
        
        jobs_outOfQ_fakeStart = max_mks
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
                estimated_remaining_time = (duration - elapsed_time)

                # FIX the (-remaining time) where sometimes the job length is overestimated. Just use 1 to speed up the search process
                duration = 1 if estimated_remaining_time <= 0 else estimated_remaining_time
            elif job_real_vars_count > q_length:
                if _debug:
                    print('{} is posponed because there are more than {} jobs ready with a higher priority.'.format(_id, q_length))
                start_min = jobs_outOfQ_fakeStart
                start_max = jobs_outOfQ_fakeStart
                jobs_outOfQ_fakeStart += duration  
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
            _keys = self.resource_manager.resource_types()
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
                wgt = int(self.max_ewt / ewt)
                jstart = prb_vars[i].StartExpr()
                qtime = jstart - (_job.queued_time - cur_time)
                prod = (qtime * wgt)
                wt.append(prod.Var())
                
            # Minimize the weighted queue time
            objective_var = solver.Sum(wt).Var()
            objective_monitor = solver.Minimize(objective_var, 1)           
            db = solver.HeuristicSearch(prb_vars)

            restart = solver.ConstantRestart(1000)
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
