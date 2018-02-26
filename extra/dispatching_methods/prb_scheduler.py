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
from accasim.base.scheduler_class import scheduler_base

class prb_scheduler(scheduler_base):
    """
    PRB type scheduler. Sorts the events depending on their expected and accumulated waiting time in the queue.
    
    In this scheduler, jobs can be skipped. If one fails, allocation is still tried on the following jobs.
    Sorting as name, sort funct parameters
    """
    name = 'PRB'
    def __init__(self, _allocator, _resource_manager=None, _seed=0, _ewt={'default': 1800}, **kwargs):
        scheduler_base.__init__(self, _seed, _resource_manager, _allocator)
        self.ewt = _ewt

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

        :return a tuple of (time to schedule, event id, list of assigned nodes)  
        """
        avl_resources = self.resource_manager.availability()
        self.allocator.set_resources(avl_resources)

        # Sorted by more time in queue time, break ties with more simpliest requests
        sorted_es = self._sort_events(cur_time, es_dict, es)
        
        event_list = [es_dict[e] for e in sorted_es]
        allocated_events = self.allocator.allocate(event_list, cur_time, skip=True, debug=_debug)

        return allocated_events

    def _get_ewt(self, queue_type):
        """
        Returns the expected waiting time for the selected queue.
        
        :param queue_type: the queue type
        :return: the expected waiting time
        """
        if queue_type in self.ewt:
            return self.ewt[queue_type]
        return self.ewt['default']

    def _sort_events(self, cur_time, events_dict, events):
        """
        Method which sorts the events depending on their waiting times in the queue.
        
        :param cur_time: the current time
        :param events_dict: the events dictionary
        :param events: the list of events to be scheduled
        :return: the sorted list of events
        """
        if len(events) <= 1:
            return events
        
        sort_helper = {
            e:
                {
                    'qtime': cur_time - events_dict[e].queued_time + 1,
                    'ewt': self._get_ewt(events_dict[e].queue),
                    'dur': events_dict[e].expected_duration,
                    'req': sum([events_dict[e].requested_nodes * val for attr, val in
                                events_dict[e].requested_resources.items()])
                }
            for e in events
        }
        sort_helper['max_ewt'] = max([v['ewt'] for v in sort_helper.values()])
        
        return sorted(events, key=lambda e: (
        -(sort_helper['max_ewt'] * sort_helper[e]['qtime']) / sort_helper[e]['ewt'],
        sort_helper[e]['dur'] * sort_helper[e]['req']))