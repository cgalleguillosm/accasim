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
import warnings
import json
import time

from abc import ABC
from scipy import stats as _statistical_distributions
from scipy.stats import percentileofscore
from statistics import pstdev
from numpy import searchsorted, ndarray
from math import exp, log
from random import random
from sortedcontainers import SortedList
from _functools import reduce

from accasim.base.resource_manager_class import Resources
from accasim.utils.misc import obj_assertion, str_datetime, load_config
from accasim.utils.data_fitting import DistributionFitting
from accasim.experimentation.schedule_writer import WorkloadWriter, DefaultWriter
from accasim.experimentation.schedule_parser import WorkloadFileReader
from accasim.utils.reader_class import DefaultTweaker



class Generator(ABC):
    
    def __init__(self, distributions):
        """
        
        :param distributions:
        """
        if not distributions:
            distributions = [dist for dist in _statistical_distributions.__all__ if isinstance(getattr(_statistical_distributions, dist), _statistical_distributions.rv_continuous)]
        self.distribution_fit = DistributionFitting(distributions)

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

    def dist_rand(self, dist_name, dist_param, optional, size=1):
        """

        :param dist_name:
        :param dist_param:
        :param optional:
        :return:
        """
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            dist = getattr(_statistical_distributions, dist_name)
            return dist.rvs(*dist_param, size=size, **optional)
        
    def hist_rand(self, dist_name, dist_param, optional):
        """

        :param dist_name:
        :param dist_param:
        :param optional:
        :return:
        """
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            dist = getattr(_statistical_distributions, dist_name)
            return dist.ppf(random(), *dist_param, **optional)


    def _generate_dist_params(self, data, save=False, filepath=None):
        """

        :param data:
        :param save:
        :param filepath:
        :return:
        """
        ((dist_name, sse, param), bins, hist) = self.distribution_fit.auto_best_fit(data)
        if sse < 0:
            raise Exception('SSE negative.')
        params = {
            'dist_name': dist_name,
            'dist_param': param[:-2],
            'optional': {
                'loc': param[-2],
                'scale': param[-1]
            }
        }
        return (params, bins, hist,) 
        
    def _save_parameters(self, filepath, dist_params, **kwargs):
        """

        :param filepath:
        :param dist_params:
        :param kwargs:
        :return:
        """
        if not filepath:
            filepath = 'dist-params_{}.json'.format(int(time.time()))
        
        gen_parameters = {'params': dist_params}
        for k, v in kwargs.items():
            if isinstance(v, ndarray):
                v = v.tolist()
            gen_parameters[k] = v
        with open(filepath, 'w') as fp:
            json.dump(gen_parameters, fp)        
        

class JobGenerator(Generator):
    SERIAL = 1
    PARALLEL = 2

    def __init__(self, total_nodes, resources_types, serial_prob, parallel_prob, parallel_node_prob, performance,
                 min_request, max_request, params, max_opers_serial, max_parallel_duration, distributions=['gamma', 'expon', 'erlang', 'beta', 'arcsine']):
        """

        :param total_nodes:
        :param resources_types:
        :param serial_prob:
        :param parallel_prob:
        :param parallel_node_prob:
        :param performance:
        :param min_request:
        :param max_request:
        :param params:
        :param max_opers_serial:
        :param max_parallel_duration:
        :param distributions:
        """

        self.total_nodes = total_nodes
        self.resources = list(min_request.keys())  # resources_types
        self.performance = performance
        self.params = params['params'] if 'params' in params else {}
        self.minimal_request = min_request
        self.maximal_request = max_request
        Generator.__init__(self, distributions)
        self._init_probabilities(serial_prob, parallel_prob, parallel_node_prob)
        self.max_opers = params['max_opers'] if 'max_opers' in params else 0
        self.max_opers_serial = max_opers_serial 
        self.max_parallel_duration = max_parallel_duration

    def add_sample(self, jobs_flops, save=False):
        """

        :param jobs_flops:
        :param save:
        :return:
        """
        if not self.params:
            max_opers = max(jobs_flops)
            self.min_opers = min(jobs_flops)
            if not hasattr(self, 'max_opers') or self.max_opers < max_opers:
                self.max_opers = max_opers * 1.2  # at max 20% as in the sample
            self.params, self.density, self.bins = self._generate_dist_params(jobs_flops)
            if save:
                filename = 'job_params-{}'.format(int(time.time()))
                self._save_parameters(filename, self.params, min_opers=self.min_opers, max_opers=self.max_opers, density=self.density.tolist(), bins=self.bins.tolist())

    def next_job(self, size=1):
        """

        :param size:
        :return:
        """
        assert (self.params), 'Sample data must be added first!'
        values = []
        while len(values) < size:
            rnds = self.dist_rand(size=size - len(values), **self.params)
        
            i_rnds = searchsorted(self.bins, rnds)
            for i, rnd in zip(i_rnds, rnds): 
                if self.min_opers <= rnd <= self.max_opers:
                    if self.density[i - 1] > 0:
                        values.append(int(rnd))
                    elif random() < 0.05:
                        values.append(int(rnd))
        job_features = []
        for flops in values:
            type, nodes, request = self._generate_request(flops)
            runtime = int(self._calc_runtime(flops, nodes, request))
            job_features.append((type, runtime, nodes, request))
        return job_features

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

        _calc_node_size = lambda x: log(x) / log(2)

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
        u = random()
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
        return (random() * (b - a)) + a

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

    def _generate_request(self, gflops, job_profiles=None):
        """

        :param gflops:
        :param job_profiles:
        :return:
        """
        request = {k: 0 for k in self.resources}
        r = random()
        if r <= self.p_serial and gflops <= self.max_opers_serial:
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


class ArriveGenerator(Generator):
    HOURS_PER_DAY = 24
    MINUTES_PER_DAY = 60 * HOURS_PER_DAY
    SECONDS_PER_DAY = 60 * MINUTES_PER_DAY

    BUCKETS = 48  # Every 30min
    SECONDS_IN_BUCKET = SECONDS_PER_DAY / BUCKETS
    DISTRIBUTIONS = ['gamma', 'dweibull', 'lognorm', 'genexpon', 'expon', 'exponnorm', 'exponweib', 'exponpow', 'truncexpon']

    def __init__(self, initial_time, hour_prob, day_prob, month_prob, param, total_jobs, cyclic_day_start=0, max_arrive_time=5 * 24 * 3600, distributions=[]):
        """

        :param initial_time:
        :param hour_prob:
        :param day_prob:
        :param month_prob:
        :param param:
        :param total_jobs:
        :param cyclic_day_start:
        :param max_arrive_time:
        :param distributions:
        """
        assert (0 <= cyclic_day_start < 24), 'The cicle must start between [0, 23].'

        self.initial_time = initial_time
        self.hour_prob = hour_prob
        self.day_prob = day_prob
        self.month_prob = month_prob
        self.cyclic_day_start = cyclic_day_start
        self.total_jobs = total_jobs
        self.initial_bucket = int(
            round((str_datetime(self.initial_time).get_hours() * self.BUCKETS) / self.HOURS_PER_DAY))
        self.distribution_plot = False

        self.time_from_begin = {}
        self.params = param
        self.current = {}
        self.weights = {}
        self.means = {}
        self.points = {}
        self.reminders = {}

        if not distributions:
            distributions = self.DISTRIBUTIONS
        Generator.__init__(self, distributions)

        self.initialized = False
        self.TOO_MUCH_ARRIVE_TIME = log(max_arrive_time)

    def add_sample(self, submission_times, rush_hours=(8, 17), save=False):
        """

        :param submission_times:
        :param rush_hours:
        :param save:
        :return:
        """
        
        total_jobs = len(submission_times)
        assert(total_jobs >= 1000), 'Data might no be representative. There are just {} jobs.'.format(total_jobs)
        _bucket_number = lambda _dtime: _dtime.get_hours() * (self.BUCKETS // self.HOURS_PER_DAY) + (
        _dtime.get_minutes() // 30)
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
        avg_percentile = percentileofscore(ia_times, avg_iatimes)
        iatimes_stdev = pstdev(ia_times)
        
        _log_arrive_time = log(max_arrive_time_diff)
        
        if self.TOO_MUCH_ARRIVE_TIME > _log_arrive_time:
            self.TOO_MUCH_ARRIVE_TIME = _log_arrive_time

        if not self.params:
            for type in data:
                if not data[type]:
                    continue
                self.params[type], _, _ = self._generate_dist_params(data[type])
    
                if self.distribution_plot:
                    self._save_distribution_plot('arrive distribution', data, type)
            if save:
                filename = 'arrive_params-{}'.format(int(time.time()))
                self._save_parameters(filename, self.params)

    def next_time(self, generation_stats):
        """

        :param generation_stats:
        :return:
        """
        assert (len(self.params) > 0), 'Data samples must be added before try to generate a next time arrive.'

        if not self.initialized:
            self._initialize()
        sample_name = 'total'
        dist_params = self.params[sample_name]
        dist = {'dist_name': dist_params['dist_name'], 'dist_param': dist_params['dist_param'], 'optional':dist_params['optional']}
        
        current_hour, current_day, current_month = self._date_info(self.time_from_begin[sample_name])
                
        current_jobs = generation_stats['current_jobs']
        togen_jobs = generation_stats['total_jobs']
        if current_jobs == 0:
            current_rate = 1
        else:
            cur = [generation_stats['current_h'][current_hour - 1] / current_jobs, generation_stats['current_d'][current_day - 1] / current_jobs, generation_stats['current_m'][current_month - 1] / current_jobs]
            prob = [self.hour_prob[current_hour - 1], self.day_prob[current_day - 1], self.month_prob[current_month - 1]]
            progress = [ ((c * current_jobs) / (togen_jobs * p)) for c, p in zip(cur, prob)]
            progress = [p for p in progress if 0 < p <= 1]
            if not progress:
                current_rate = 1
            else:
                current_rate = reduce(lambda x, y: x * y, progress)
                
        factor = (1 - current_rate)
        cur_max_ia_time = self.TOO_MUCH_ARRIVE_TIME - (self.TOO_MUCH_ARRIVE_TIME - log(self.SECONDS_IN_BUCKET)) * factor
        
        assert(cur_max_ia_time <= self.TOO_MUCH_ARRIVE_TIME), 'wrong cur max ia time {} > {} ({} days)'.format(cur_max_ia_time, self.TOO_MUCH_ARRIVE_TIME, exp(cur_max_ia_time) / (3600 * 24))
        while True:
            rnd_value = self.dist_rand(**dist)
            if rnd_value <= cur_max_ia_time: 
                break
        current_bucket = self.current[sample_name]
        weights = self.weights[sample_name]
        
        self.points[sample_name] += exp(rnd_value) / self.SECONDS_IN_BUCKET
        next_arrive = 0

        while self.points[sample_name] > weights[current_bucket]:
            self.points[sample_name] -= weights[current_bucket]
            current_bucket = (current_bucket + 1) % self.BUCKETS
            next_arrive += self.SECONDS_IN_BUCKET
        
        new_reminder = self.points[sample_name] / weights[current_bucket]
        more_time = self.SECONDS_IN_BUCKET * (new_reminder - self.reminders[sample_name])
        next_arrive += more_time
        self.reminders[sample_name] = new_reminder
        self.time_from_begin[sample_name] += int(next_arrive)

        # Update all attributes
        self.current[sample_name] = current_bucket

        return self.time_from_begin[sample_name]

    def _date_info(self, timestamp):
        dtime = str_datetime(timestamp)
        return dtime.get_hours(), dtime.get_weekday(), dtime.get_month()


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
        _param = self.params[data_type]
        dist_name = _param['dist_name']
        dist_param = _param['dist_param']
        optional = _param['optional']
        dist = getattr(_statistical_distributions, dist_name)
        pdf_fitted = dist.pdf(x, *dist_param, **optional)
        plt.plot(pdf_fitted, label=dist_name)
        plt.show()

    def _initialize(self):
        """

        :return:
        """
        for _name, dist_params in self.params.items():
            dist = {'dist_name': dist_params['dist_name'], 'dist_param': dist_params['dist_param'], 'optional':dist_params['optional']}
            self.weights[_name] = [self.dist_cdf(i + 0.5, **dist) - self.dist_cdf(i - 0.5, **dist) for i
                                   in range(self.BUCKETS)]
            self.means[_name] = sum(self.weights[_name]) / self.BUCKETS
            self.weights[_name] = [w / self.means[_name] for w in self.weights[_name]]
            self.current[_name] = self.initial_bucket
            self.points[_name] = 0
            self.reminders[_name] = 0
            self.time_from_begin[_name] = self.initial_time
        self.initialized = True


class WorkloadGenerator:
    
    def __init__(self, workload, sys_config, performance, request_limits, reader_class=None, resources_target=None, user_behavior=None, walltime_calculation=None, non_processing_resources=['mem'], **kwargs):
        """

        :param workload:
        :param sys_config:
        :param performance:
        :param request_limits:
        :param reader_class:
        :param resources_target:
        :param user_behavior:
        :param walltime_calculation:
        :param non_processing_resources:
        :param kwargs:
        """
        self.walltime_calculation = walltime_calculation
        show_msg = False
        save_parameters = False
        job_parameters = {}
        arrive_parameters = {}
        job_gen_optional = {}
        arrive_gen_optional = {}
        
        config = load_config(sys_config)
        
        resources = self._set_resources(None, sys_config)
        equivalence = config.pop('equivalence', {})
        start_time = config.pop('start_time', int(time.time()))
        
        if not reader_class:
            reader_class = self._default_simple_swf_reader(workload, start_time, equivalence, resources)
               
        if not hasattr(reader_class, "next"):
            raise Exception('The reader_class must implement the next method.')
        
        if not resources_target:
            resources_target = self._set_resources(None, sys_config)
        
        total_jobs, _submissiont_times, _job_total_opers, serial_prob, nodes_parallel_prob, hour_prob, day_prob, month_prob, max_opers_serial, max_parallel_duration = self._initialize(reader_class,
                                                                                                  performance,
                                                                                                  resources,
                                                                                                  non_processing_resources)                
        if 'show_msg' in kwargs:
            show_msg = kwargs['show_msg']
        
        if 'job_parameters' in kwargs:
            job_parameters = kwargs['job_parameters']
            
        if 'job_distributions' in kwargs:
            job_gen_optional['distributions'] = kwargs['job_distributions']
            
        if 'arrive_parameters' in kwargs:
            arrive_parameters = kwargs['arrive_parameters']
            
        if 'save_parameters' in kwargs:
            save_parameters = kwargs['save_parameters']
            
        parallel_prob = 1 - serial_prob

        self.arrive_generator = ArriveGenerator(start_time, hour_prob, day_prob, month_prob, arrive_parameters, total_jobs)
        
        if show_msg:
            print('Arrive Generator samples...')
        self.arrive_generator.add_sample(_submissiont_times, save=save_parameters)
        if show_msg:
            print('Arrive Generator samples... Loaded')
            
        total_nodes = sum([d['nodes'] for d in resources.definition])
        total_resources = resources.total_resources()
        resource_types = list(resources.total_resources().keys())
        min_request, max_request = request_limits['min'], request_limits['max'] 

        self.job_generator = JobGenerator(total_nodes, resource_types, serial_prob, parallel_prob, nodes_parallel_prob,
                                           performance, min_request, max_request, job_parameters, max_opers_serial, max_parallel_duration, **job_gen_optional)

        if show_msg:
            print('Job Generator samples...')
        self.job_generator.add_sample(_job_total_opers, save=save_parameters)
        if show_msg:
            print('Job Generator samples... Loaded')

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
        _job_total_opers = SortedList()
        _job_hours = [0 for i in range(24)]
        _job_days = [0 for i in range(7)]
        _job_months = [0 for i in range(12)]
        _nodes_requested = {n + 1: 0 for n in range(total_nodes)}
        _n_parallel_requests = 0
        total_jobs = 0
        _max_opers_serial = 0
        _max_parallel_duration = 0
        while True:
            _dict = base_reader.next()
            if not _dict:
                break
            submisison_time = _dict['queued_time']
            hour, weekday, month = self._date_info(submisison_time)
            _job_hours[hour - 1] += 1
            _job_days[weekday - 1] += 1
            _job_months[month - 1] += 1
            duration = int(_dict['duration'])
            requested_nodes = int(_dict['requested_nodes'])
            requested_resources = _dict['requested_resources']

            requested_proc_units = {k: v for k, v in requested_resources.items() if k in proc_units}

            _job_runtimes.append(duration)
            _job_submission_times.append(submisison_time)
            _job_type = self._define_job_type(requested_nodes, requested_proc_units)
            _job_total_oper = self._job_operations(duration, requested_nodes, requested_proc_units, performance)
            _job_total_opers.add(_job_total_oper)

            _job_types[_job_type] += 1
            if _job_type == 'serial' and _max_opers_serial < _job_total_oper:
                _max_opers_serial = _job_total_oper
            if _job_type == 'parallel':
                if _max_parallel_duration < duration:
                    _max_parallel_duration = duration
                _nodes_requested[requested_nodes] += 1
                _n_parallel_requests += 1

            assert (requested_nodes < total_nodes), 'More nodes than available have been requested. ({} > {})'.format(
                requested_nodes, total_nodes)
            total_jobs += 1

        _job_types.update((k, v / total_jobs) for k, v in _job_types.items())
        _job_hours = [h / total_jobs for h in _job_hours]
        _job_days = [d / total_jobs for d in _job_days]
        _job_months = [m / total_jobs for m in _job_months]

        if _n_parallel_requests > 1:
            nodes_parallel_prob = {n: _nodes_requested[n] / _n_parallel_requests for n in _nodes_requested}
        
        return total_jobs, _job_submission_times, _job_total_opers, _job_types['serial'], nodes_parallel_prob, _job_hours, _job_days, _job_months, _max_opers_serial, _max_parallel_duration

    def generate_jobs(self, n, filename, overwrite=True, writer=None):
        """

        :param n:
        :param filename:
        :param overwrite:
        :param writer:
        :return:
        """
        if not writer:
            writer = DefaultWriter(filename, overwrite=overwrite)
        else:
            obj_assertion(writer, WorkloadWriter,
                          'Received {} type as system resource. resources_class type expected.',
                          [WorkloadWriter.__class__.__name__])
                
        generation_stats = {
            'current_jobs': 0,
            'total_jobs': n,
            'current_h': [0 for i in range(24)],
            'current_d': [0 for i in range(7)],
            'current_m': [0 for i in range(12)]
        }
        
        jobs = {}
        sub_times = []
        for i in range(n):
            if (i + 1) % 1000 == 0 :
                print('Generated {} jobs'.format(i + 1))
            submit_time = self.arrive_generator.next_time(generation_stats)
            sub_times.append(submit_time)
            self._update_stats(generation_stats, submit_time)
            job_type, duration, nodes, request = self.job_generator.next_job()[0]
            jobs[i + 1] = {
                'job_number': i + 1,
                'submit_time': submit_time,
                'duration': duration,
                'nodes': nodes,
                'resources': request,
                # Calculate walltime if method exists...
                'requested_time': self.walltime_calculation(i + 1, job_type, duration, nodes, request) if self.walltime_calculation else None 
            }                       
            writer.add_newline(jobs[i + 1])
        writer.close_file()
        print('Done. {} generated jobs.'.format(n))
        return jobs

    def _update_stats(self, stats, stime):
        """

        :param stats:
        :param stime:
        :return:
        """
        hour, day, month = self._date_info(stime)
        stats['current_jobs'] += 1
        stats['current_h'][hour - 1] += 1
        stats['current_d'][day - 1] += 1
        stats['current_m'][month - 1] += 1

    def _date_info(self, submission_date):
        """

        :param submission_date:
        :return:
        """
        dtime = str_datetime(submission_date)
        return dtime.get_hours(), dtime.get_weekday(), dtime.get_month()

    def _set_resources(self, resources_obj, sys_config):
        """

        :param resources_obj:
        :param sys_config:
        :return:
        """
        if resources_obj:
            obj_assertion(resources_obj, Resources,
                          'Received {} type as system resource. resources_class type expected.',
                          [resources_obj.__class__.__name__])
            return resources_obj
        elif sys_config:
            config = load_config(sys_config)
            resources_obj = Resources(node_prefix='', **config)
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
        used_gflops = int(duration * nodes * sum([v * performance[k] for k, v in proc_units.items()]))
        return used_gflops
    
    def _update_distinct_woption(self, name, main_value, value, default_value, converter):
        """

        :param name:
        :param main_value:
        :param value:
        :param default_value:
        :param converter:
        :return:
        """
        return lambda _dict: _dict.update({name: converter(_dict.pop(main_value) if _dict[main_value] != value else _dict.pop(default_value))}) or _dict

    def _update_name(self, current, new, converter):
        """

        :param current:
        :param new:
        :param converter:
        :return:
        """
        return lambda _dict: _dict.update({new: converter(_dict.pop(current))}) or _dict

    def _default_simple_swf_reader(self, workload, start_time, equivalence, resources):
        """

        :param workload:
        :param start_time:
        :param equivalence:
        :param resources:
        :return:
        """
        reg_exp = '\s*(?P<job_id>[-+]?\d+)\s*(?P<queue_time>[-+]?\d+)\s*([-+]?\d+)\s*(?P<duration>[-+]?\d+)\s*(?P<allocated_processors>[-+]?\d+)\s*([-+]?\d+\.\d+|[-+]?\d+)\s*(?P<used_memory>[-+]?\d+)\s*(?P<requested_number_processors>[-+]?\d+)\s*(?P<expected_duration>[-+]?\d+)\s*(?P<requested_memory>[-+]?\d+)\s*([-+]?\d+)\s*(?P<user>[-+]?\d+)\s*([-+]?\d+)\s*([-+]?\d+)\s*(?P<queue_number>[-+]?\d+)\s*([-+]?\d+)\s*([-+]?\d+)\s*([-+]?\d+)'
        
        _func1 = self._update_distinct_woption('total_processors', 'requested_number_processors', '-1', 'allocated_processors', int)
        _func2 = self._update_distinct_woption('mem', 'requested_memory', '-1', 'used_memory', int)
        _func3 = self._update_name('queue_time', 'queued_time', int)
   
        tweaker = DefaultTweaker(start_time, resources, equivalence)
        
        return WorkloadFileReader(workload, reg_exp, tweaker, [_func1, _func2, _func3])
