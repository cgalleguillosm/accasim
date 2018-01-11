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
from accasim.base.resource_manager_class import resources_class
from accasim.utils.misc import obj_assertion, str_datetime
from accasim.utils.data_fitting import distribution_fit
from accasim.experimentation.schedule_writer import workload_writer

from abc import ABC
from scipy import stats as _statistical_distributions
from scipy.stats import percentileofscore as _percentileofscore
from statistics import pstdev as _pstdev
from math import exp as _exp, log as _log
from random import random as _random
import warnings
from collections import Counter


class generator(ABC):
    def __init__(self, distributions):
        """

        :param distributions:
        """
        assert (len(distributions) > 0)
        self.distribution_fit = distribution_fit(distributions)

    def dist_cdf(self, x, dist_name, dist_param, optional):
        """

        :param x:
        :param dist_name:
        :param dist_param:
        :param optional:
        :return:
        """
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            dist = getattr(_statistical_distributions, dist_name)
            return dist.cdf(x, *dist_param, **optional)

    def dist_rand(self, dist_name, dist_param, optional):
        """

        :param dist_name:
        :param dist_param:
        :param optional:
        :return:
        """
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            dist = getattr(_statistical_distributions, dist_name)
            return dist.rvs(*dist_param, **optional)

    def _generate_dist_params(self, data):
        """

        :param data:
        :return:
        """
        (dist_name, mle, param) = self.distribution_fit.auto_best_fit(data)
        return {
            'dist_name': dist_name,
            'dist_param': param[:-2],
            'optional': {
                'loc': param[-2],
                'scale': param[-1]
            }
        }


class job_generator(generator):
    SERIAL = 1
    PARALLEL = 2

    def __init__(self, total_nodes, resources_types, serial_prob, parallel_prob, parallel_node_prob, performance,
                 min_request, max_request):
        """

        :param total_nodes:
        :param resources_types:
        :param serial_prob:
        :param parallel_prob:
        :param parallel_node_prob:
        :param performance:
        :param min_request:
        :param max_request:
        """
        distributions = ['gamma', 'expon', 'erlang', 'beta', 'arcsine']
        self.total_nodes = total_nodes
        self.resources = list(min_request.keys())  # resources_types
        self.performance = performance
        self.params = None
        self.minimal_request = min_request
        self.maximal_request = max_request
        generator.__init__(self, distributions)
        self._init_probabilities(serial_prob, parallel_prob, parallel_node_prob)

    def add_sample(self, log_runtimes):
        """

        :param log_runtimes:
        :return:
        """
        self.params = self._generate_dist_params(log_runtimes)

    def next_job(self):
        """

        :return:
        """
        assert (self.params), 'Sample data must be added first!'
        gflops = self.dist_rand(**self.params)
        type, nodes, request = self._generate_request()
        runtime = int(self._calc_runtime(gflops, nodes, request))
        return type, runtime, nodes, request

    def _central_node_prob(self, par_node_prob):
        """

        :param par_node_prob:
        :return:
        """
        central = 0
        accum_prob = 0
        for i, prob in par_node_prob.items():
            central = i
            accum_prob += prob
            if round(accum_prob) >= 0.5:
                break
        return central, accum_prob

    def _init_probabilities(self, serial_prob, parallel_prob, par_node_prob):
        """

        :param serial_prob:
        :param parallel_prob:
        :param par_node_prob:
        :return:
        """
        assert (serial_prob + parallel_prob == 1), 'Jobs types (serial, parallel) probabilities dont sum 1'
        assert (round(sum([p for p in par_node_prob.values()]), 10) in (0, 1)), 'Parallel node probabilities dont sum 1'

        _calc_node_size = lambda x: _log(x) / _log(2)

        self.p_serial = serial_prob
        self.p_parallel = parallel_prob
        self._nodes = [n for n in par_node_prob]
        self._node_probabilities = [p for n, p in par_node_prob.items()]
        self._node_base_min = _calc_node_size(1)
        self._node_base_max = _calc_node_size(len(self._nodes))
        central, self._nodes_prob = self._central_node_prob(par_node_prob)
        self._node_base_med = _calc_node_size(central)

    def _generate_nodes(self):
        """

        :return:
        """
        u = _random()
        if u <= self._nodes_prob:
            a = self._node_base_min
            b = self._node_base_med
        else:
            a = self._node_base_med
            b = self._node_base_max
        nodes = round(2 ** self._uniform_dist(a, b))
        return nodes

    def _uniform_dist(self, a, b):
        """

        :param a:
        :param b:
        :return:
        """
        return (_random() * (b - a)) + a

    def _calc_runtime(self, gflops, nodes, request):
        """

        :param gflops:
        :param nodes:
        :param request:
        :return:
        """
        part_power = sum([nodes * request[r] for r, p in self.performance.items() if r in request])
        if not part_power:
            return 0
        return gflops / part_power

    def _generate_request(self, job_profiles=None):
        """

        :param job_profiles:
        :return:
        """
        request = {k: 0 for k in self.resources}
        r = _random()
        if r <= self.p_serial:
            nodes = 1
            type = self.SERIAL
        else:
            nodes = self._generate_nodes()
            type = self.PARALLEL
        if job_profiles:
            self._job_request_profile(type, nodes, request)
        else:
            self._job_request(type, nodes, request)

        return type, nodes, request

    def _job_request(self, type, nodes, request):
        """

        :param type:
        :param nodes:
        :param request:
        :return:
        """
        for res in request:
            _min = self.minimal_request[res]
            _max = self.maximal_request[res]
            if type == self.PARALLEL and nodes == 1 and res == 'core':
                _min = 2
            if type == self.SERIAL:
                if not (res in ('core', 'mem')):
                    _max = 0
                if res == 'core':
                    _max = 1
            v = round(self._uniform_dist(_min, _max))
            request[res] = v

    def _job_request_profile(self, nodes, request):
        """

        :param nodes:
        :param request:
        :return:
        """
        raise NotImplementedError()


class arrive_generator(generator):
    HOURS_PER_DAY = 24
    MINUTES_PER_DAY = 60 * HOURS_PER_DAY
    SECONDS_PER_DAY = 60 * MINUTES_PER_DAY

    BUCKETS = 48  # Every 30min
    SECONDS_IN_BUCKET = SECONDS_PER_DAY / BUCKETS

    def __init__(self, initial_time, day_prob, month_prob, cyclic_day_start=0, max_arrive_time=5 * 24 * 3600):
        """

        :param initial_time:
        :param cyclic_day_start:
        :param max_arrive_time: in secs
        """
        assert (0 <= cyclic_day_start < 24), 'The cicle must start between [0, 23].'

        self.initial_time = initial_time
        self.day_prob = day_prob
        self.month_prob = month_prob
        self.cyclic_day_start = cyclic_day_start
        self.initial_bucket = int(
            round((str_datetime(self.initial_time).get_hours() * self.BUCKETS) / self.HOURS_PER_DAY))
        self.distribution_plot = False

        self.time_from_begin = {}
        self.params = {}
        self.current = {}
        self.weights = {}
        self.means = {}
        self.points = {}
        self.reminders = {}

        distributions = ['gamma', 'dweibull', 'lognorm', 'genexpon', 'expon', 'exponnorm', 'exponweib', 'exponpow', 'truncexpon']
        generator.__init__(self, distributions)

        self.initialized = False
        self.TOO_MUCH_ARRIVE_TIME = _log(max_arrive_time)

    def add_sample(self, submission_times, name='general', rush_hours=(8, 17)):
        """

        :param submission_times:
        :param name:
        :param rush_hours:
        :return:
        """
        total_jobs = len(submission_times)
        assert(total_jobs >= 1000), 'Data might no be representative. There are just {} jobs.'.format(total_jobs)
        
        _bucket_number = lambda _dtime: _dtime.get_hours() * (self.BUCKETS // self.HOURS_PER_DAY) + (
        _dtime.get_minutes() // 30)
        assert (name not in self.params), 'Sample {} already exists.'.format(name)
        submission_times = sorted(submission_times)
        sample_size = len(submission_times)
        data = {type: [] for type in ['total', 'rush']}

        init, end = rush_hours

        max_arrive_time_diff = 0
        ia_times = []

        for i, (cur_time, next_time) in enumerate(zip(submission_times[:-1], submission_times[1:])):
            ia_time = abs(next_time - cur_time)  # Must be always positive... using abs just in case
            _datetime = str_datetime(cur_time)
            _hour = _datetime.get_hours()
            _pos = _bucket_number(_datetime)
            
            data['total'].append(_pos)
            ia_times.append(ia_time)
            # Store jobs that were submitted at rush hours
            if init < _hour < end:
                data['rush'].append(_pos)
                
            if ia_time > max_arrive_time_diff:
                max_arrive_time_diff = ia_time
        avg_iatimes = sum(ia_times) / len(ia_times)
        avg_percentile = _percentileofscore(ia_times, avg_iatimes)
        iatimes_stdev = _pstdev(ia_times)
        # print('Avg. ', avg_iatimes, ', Max diff', max_arrive_time_diff, ', Percentile', avg_percentile, 'Std', iatimes_stdev)
        _log_arrive_time = _log(max_arrive_time_diff)
        
        if self.TOO_MUCH_ARRIVE_TIME > _log_arrive_time:
            self.TOO_MUCH_ARRIVE_TIME = _log_arrive_time

        for type in data:
            if not data[type]:
                continue
            if name not in self.params:
                self.params[name] = {}
            self.params[name][type] = self._generate_dist_params(data[type])

            if self.distribution_plot:
                self._save_distribution_plot(name, data, type)

    def next_time(self, generation_stats, sample_name='general'):
        """

        :param sample_name:
        :return:
        """
        assert (len(self.params) > 0), 'Data samples must be added before try to generate a next time arrive.'

        if not self.initialized:
            self._initialize()
        dist_params = self.params[sample_name]['total']
        
        current_day, current_month = self._date_info(self.time_from_begin[sample_name]) 
        
        cur_day_jobs = generation_stats['current_d'][current_day - 1]
        cur_month_jobs = generation_stats['current_m'][current_month - 1]
        dprob = self.day_prob[current_day - 1]
        mprob = self.month_prob[current_month - 1]
        try:
            if cur_day_jobs / generation_stats['current_jobs'] < dprob: 
                current_rate = (cur_day_jobs / generation_stats['total_jobs']) 
            elif cur_month_jobs / generation_stats['current_jobs'] < mprob:
                current_rate = (cur_month_jobs / generation_stats['total_jobs']) 
            else:
                current_rate = 1
        except ZeroDivisionError:
            current_rate = 1
        factor = (1 - current_rate)
        
        cur_max_ia_time = self.TOO_MUCH_ARRIVE_TIME - (self.TOO_MUCH_ARRIVE_TIME - _log(self.SECONDS_IN_BUCKET)) * factor
        
        while True:
            rnd_value = self.dist_rand(**dist_params)
            if rnd_value <= cur_max_ia_time: 
                break
            
        current_bucket = self.current[sample_name]
        weights = self.weights[sample_name]
        
        self.points[sample_name] += _exp(rnd_value) / self.SECONDS_IN_BUCKET
        next_arrive = 0

        while self.points[sample_name] > weights[current_bucket]:
            self.points[sample_name] -= weights[current_bucket]
            current_bucket = (current_bucket + 1) % self.BUCKETS
            next_arrive += self.SECONDS_IN_BUCKET

        new_reminder = self.points[sample_name] / weights[current_bucket]
        more_time = self.SECONDS_IN_BUCKET * (new_reminder - self.reminders[sample_name])
        next_arrive += more_time
        self.reminders[sample_name] = new_reminder
        self.time_from_begin[sample_name] += int(next_arrive)  # int(round(next_arrive))

        # Update all attributes
        self.current[sample_name] = current_bucket

        return self.time_from_begin[sample_name]

    def _date_info(self, timestamp):
        dtime = str_datetime(timestamp)
        return dtime.get_weekday(), dtime.get_month()


    def _save_distribution_plot(self, name, data, data_type):
        """

        :param name:
        :param data:
        :param data_type:
        :return:
        """
        import matplotlib.pyplot as plt
        x = range(self.BUCKETS)
        _data = data[data_type]
        plt.hist(_data, self.BUCKETS, normed=True)
        _param = self.params[name][data_type]
        dist_name = _param['dist_name']
        dist_param = _param['dist_param']
        optional = _param['optional']
        dist = getattr(_statistical_distributions, dist_name)
        pdf_fitted = dist.pdf(x, *dist_param, **optional)  # * len(_data)
        plt.plot(pdf_fitted, label=dist_name)
        plt.show()

    def _initialize(self):
        """

        :return:
        """
        for _name, params in self.params.items():
            dist_params = params['total']
            self.weights[_name] = [self.dist_cdf(i + 0.5, **dist_params) - self.dist_cdf(i - 0.5, **dist_params) for i
                                   in range(self.BUCKETS)]
            self.means[_name] = sum(self.weights[_name]) / self.BUCKETS
            self.weights[_name] = [w / self.means[_name] for w in self.weights[_name]]
            self.current[_name] = self.initial_bucket
            self.points[_name] = 0
            self.reminders[_name] = 0
            self.time_from_begin[_name] = self.initial_time
        self.initialized = True


class workload_generator:
    def __init__(self, init_time, base_reader, performance, min_request, max_request, resources_obj=None,
                 sys_config=None, non_processing_resources=['mem'], **kwargs):
        """

        :param init_time:
        :param base_reader:
        :param performance:
        :param min_request:
        :param max_request:
        :param resources_obj:
        :param sys_config:
        :param non_processing_resources:
        :param kwargs:
        """
        show_msg = False
        if 'show_msg' in kwargs:
            show_msg = kwargs['show_msg']
        resources = self._set_resources(resources_obj, sys_config)
        _submissiont_times, _job_total_opers, serial_prob, nodes_parallel_prob, day_prob, month_prob = self._initialize(base_reader,
                                                                                                  performance,
                                                                                                  resources,
                                                                                                  non_processing_resources)
        parallel_prob = 1 - serial_prob

        self.arrive_generator = arrive_generator(init_time, day_prob, month_prob)
        if show_msg:
            print('Arrive generator samples...')
        self.arrive_generator.add_sample(_submissiont_times)
        if show_msg:
            print('Arrive generator samples... Loaded')

        total_nodes = sum([d['nodes'] for d in resources.definition])
        total_resources = resources.total_resources()
        resource_types = list(resources.total_resources().keys())

        self.job_generator = job_generator(total_nodes, resource_types, serial_prob, parallel_prob, nodes_parallel_prob,
                                           performance, min_request, max_request)

        if show_msg:
            print('Job generator samples...')
        self.job_generator.add_sample(_job_total_opers)
        if show_msg:
            print('Job generator samples... Loaded')

    def _initialize(self, base_reader, performance, resources, non_processing_resources):
        """

        :param base_reader:
        :param performance:
        :param resources:
        :param non_processing_resources:
        :return:
        """
        total_nodes = sum([d['nodes'] for d in resources.definition])
        resource_types = resources.total_resources().keys()

        proc_units = [res_type for res_type in resource_types if not (res_type in non_processing_resources)]

        _job_runtimes = []
        _job_submission_times = []
        _job_types = {'serial': 0, 'parallel': 0}
        _job_total_opers = []
        _job_days = [0 for i in range(7)]
        _job_months = [0 for i in range(12)]
        _nodes_requested = {n + 1: 0 for n in range(total_nodes)}
        _n_parallel_requests = 0
        total_jobs = 0
        while True:
            _dict = base_reader.next()
            if not _dict:
                break
            submisison_time = _dict['queued_time']
            weekday, month = self._date_info(submisison_time)
            _job_days[weekday - 1] += 1
            _job_months[month - 1] += 1
            duration = int(_dict['duration'])
            requested_nodes = int(_dict['requested_nodes'])
            requested_resources = _dict['requested_resources']
            requested_proc_units = {k: v for k, v in requested_resources.items() if k in proc_units}

            _job_runtimes.append(duration)
            _job_submission_times.append(submisison_time)
            _job_type = self._define_job_type(requested_nodes, requested_proc_units)
            _job_total_opers.append(self._job_operations(duration, requested_nodes, requested_proc_units, performance))

            _job_types[_job_type] += 1
            if _job_type == 'parallel':
                _nodes_requested[requested_nodes] += 1
                _n_parallel_requests += 1

            assert (requested_nodes < total_nodes), 'More nodes than available have been requested. ({} > {})'.format(
                requested_nodes, total_nodes)
            total_jobs += 1

        _job_types.update((k, v / total_jobs) for k, v in _job_types.items())
        _job_days = [d / total_jobs for d in _job_days]
        _job_months = [m / total_jobs for m in _job_months]

        if _n_parallel_requests > 1:
            nodes_parallel_prob = {n: _nodes_requested[n] / _n_parallel_requests for n in _nodes_requested}

        return _job_submission_times, _job_total_opers, _job_types['serial'], nodes_parallel_prob, _job_days, _job_months

    def generate_jobs(self, n, writer=None):
        """

        :param n:
        :param writer:
        :return:
        """
        if not writer:
            obj_assertion(writer, workload_writer,
                          'Received {} type as system resource. resources_class type expected.',
                          [workload_writer.__class__.__name__])
        generation_stats = {
            'current_jobs': 0,
            'total_jobs': n,
            'current_d': [0 for i in range(7)],
            'current_m': [0 for i in range(12)]
        }
        jobs = {}
        for i in range(n):
            submit_time = self.arrive_generator.next_time(generation_stats)
            self._update_stats(generation_stats, submit_time)
            job_type, duration, nodes, request = self.job_generator.next_job()
            jobs[i + 1] = {
                'job_number': i + 1,
                'submit_time': submit_time,
                'duration': duration,
                'nodes': nodes,
                'resources': request
            }
            writer.add_newline(jobs[i + 1])
            print(i + 1, jobs[i + 1])
        print(generation_stats)
        return jobs

    def _update_stats(self, stats, stime):
        day, month = self._date_info(stime)
        stats['current_jobs'] += 1
        stats['current_d'][day - 1] += 1
        stats['current_m'][month - 1] += 1

    def _date_info(self, submission_date):
        dtime = str_datetime(submission_date)
        return dtime.get_weekday(), dtime.get_month()

    def _set_resources(self, resources_obj, sys_config):
        """

        :param resources_obj:
        :param sys_config:
        :return:
        """
        if resources_obj:
            obj_assertion(resources_obj, resources_class,
                          'Received {} type as system resource. resources_class type expected.',
                          [resources_obj.__class__.__name__])
            return resources_obj
        elif sys_config:
            config = load_config(sys_config)
            resources_obj = resources_class(node_prefix='', **config)
            return resources_obj
        else:
            raise Exception('A resources object or the path to the system config must be given.')

    def _define_job_type(self, nodes, proc_units):
        """

        :param nodes:
        :param proc_units:
        :return:
        """
        total_q = sum([v for k, v in proc_units.items()])
        if total_q == 1 and nodes == 1:
            return 'serial'
        return 'parallel'

    def _job_operations(self, duration, nodes, proc_units, performance):
        """

        :param duration:
        :param nodes:
        :param proc_units:
        :param performance:
        :return:
        """
        used_gflops = duration * nodes * sum([v * performance[k] for k, v in proc_units.items()])
        return used_gflops
