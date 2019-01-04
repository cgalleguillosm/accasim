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
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.backends.backend_pdf import PdfPages
from math import floor
from accasim.utils.reader_class import DefaultReader
from accasim.utils.misc import load_config, from_isodatetime_2_timestamp as timestamp_func, str_resources
from accasim.utils.file import path_leaf, load_jsonfile
from accasim.base.resource_manager_class import Resources
from accasim.experimentation.schedule_parser import define_result_parser
from accasim.utils.misc import DEFAULT_SIMULATION
from copy import deepcopy
from os.path import splitext, join
from scipy.signal import savgol_filter
from os.path import isfile
import numpy as np
from matplotlib.pyplot import boxplot


class PlotFactory:
    """
    A class for plot production and schedule files pre-processing.
    
    In this class, some basic algorithms are implemented for pre-processing the schedule files produced through 
    simulation, and for producing some common evaluation plots.
    """

    SCHEDULE_CLASS = 'schedule'
    BENCHMARK_CLASS = 'benchmark'
    SLOWDOWN_PLOT = 'slowdown'
    QUEUE_SIZE_PLOT = 'queue_size'
    LOAD_RATIO_PLOT = 'load_ratio'
    EFFICIENCY_PLOT = 'efficiency'
    SCALABILITY_PLOT = 'scalability'
    SIMULATION_TIME_PLOT = 'sim_time'
    SIMULAION_MEMORY_PLOT = 'sim_memory'

    PLOT_TYPES = {
        SCHEDULE_CLASS: [SLOWDOWN_PLOT, QUEUE_SIZE_PLOT, LOAD_RATIO_PLOT, EFFICIENCY_PLOT],
        BENCHMARK_CLASS: [SCALABILITY_PLOT, SIMULATION_TIME_PLOT, SIMULAION_MEMORY_PLOT]
    }

    def __init__(self, plot_class, sim_params_fname=None, config=None, resource=None, workload_parser=None, debug=False):
        """
        The constructor for the class.
        
        :param plot_class: the plot_class of files to be analyzed. Can be either 'schedule', if schedule files are going to be 
        analyzed, or 'benchmark' if resource usage log files will be analyzed;
        :params sim_params_fname: 
        :param config: The path to a system configuration file. Needed for the schedule meta-simulation;
        :param resource: a resource type in the system to be considered. If specified, all resource-related statistics
            will be computed in regards to this resource alone;
        :param workload_parser: 
        :param debug: Debug flag.
        """
        self._debug = debug
        if not (plot_class in self.PLOT_TYPES.keys()):
            if self._debug:
                print('Wrong Plot plot_class chosen. Selecting schedule plot_class by default...')
            plot_class = self.SCHEDULE_CLASS
        self._plot_class = plot_class

        self._sim_params_fname = sim_params_fname  # if sim_params_fname is not None and isfile(sim_params_fname) else None
        self._config = config
        self._resource = resource
        self._workload_parser = workload_parser

        self._preprocessed = False
        self._filepaths = []
        self._labels = []

        self._slowdowns = []
        self._queuesizes = []
        self._loadratiosX = []
        self._loadratiosY = []
        self._efficiencies = []

        self._simdata = []
        self._schedtimes = []
        self._mantimes = []
        self._simmemory = []
        self._scalabilitydataX = []
        self._scalabilitydataY = []

        self._resource_order = None
        if self._sim_params_fname is None:
            self._resource_order = DEFAULT_SIMULATION['RESOURCE_ORDER']

        # Base resource availability per-node (never changes)
        self._base_res = {}
        # Current resource availability per-node
        self._sys_res = {}
        # Aggregated used resources for all nodes
        self._used_res_sum = {}
        # Aggregate base resource availability for used nodes only
        self._avl_res_sum = {}
        # Aggregated base resource availability for all nodes
        self._base_res_sum = {}
        # Amount of currently used nodes
        self._used_nodes = 0
        # Number of total nodes in the system
        self._total_nodes = 0

    def set_files(self, paths, labels):
        """
        Set the paths and labels of the files to be analyzed.
        
        :param paths: A list of filepaths related to the files to be analyzed; 
        :param labels: the labels associated to each single file, used in the plots; must have the same length as paths;
        """
        self._preprocessed = False
        if not isinstance(paths, (list, tuple)):
            self._filepaths = [paths]
            self._labels = [labels]
        else:
            self._filepaths = paths
            self._labels = labels
            
        if len(self._filepaths) != len(self._labels):
            if self._debug:
                print("Filepaths and Labels lists must have the same lengths.")
            self._labels = []
            self._filepaths = []

    def pre_process(self, trimSlowdown=True, trimQueueSize=False):
        """
        Performs pre-processing on all specified files, according to their type.
        
        If the files are of the schedule type, a meta-simulation is run for each of them, computing data like slowdown,
        queue size, load ratios and such. If the data is of the benchmark type, the files are simply parsed and their
        information stored.
        
        :param: trimSlowdown: boolean flag. If True, slowdown values equal to 1 will be discarded. Default is True
        :param: trimQueueSize: boolean flag. If True, queue size values equal to 0 will be discarded. Default is False
        
        """
        if not self._preprocessed:
            # Perform pre-processing for schedule files
            if self._plot_class == self.SCHEDULE_CLASS:
                self._slowdowns = []
                self._queuesizes = []
                self._loadratiosX = []
                self._loadratiosY = []
                self._efficiencies = []
                self._preprocessed = True
                for f in self._filepaths:
                    # If an error is encountered on one of the files, the process is aborted
                    if not self._getScheduleData(f, self._config, self._resource, trimSlowdown, trimQueueSize):
                        self._preprocessed = False
                        break

            # Perform pre-processing for benchmark files
            elif self._plot_class == self.BENCHMARK_CLASS:
                self._simdata = []
                self._schedtimes = []
                self._mantimes = []
                self._simmemory = []
                self._scalabilitydataX = []
                self._scalabilitydataY = []
                self._preprocessed = True
                for f in self._filepaths:
                    if not self._getBenchmarkData(f):
                        self._preprocessed = False
                        break

        if not self._preprocessed:
            print("Could not process files, please ensure they are in the correct path and format.")

        return self._preprocessed

    def produce_plot(self, type, title='', scale='linear', xlim=(None, None), ylim=(None, None), legend=True, figsize=(7, 5), meansonly=False, alpha=0.005, smooth=30, output='Output.pdf', groups=1, **kwargs):
        """
        Produces a single plot on the pre-processed files.
        
        The user can produce plots among the available types. These are:
            - slowdown: a box-plot distribution plot for slowdown values across test instances
            - queue_size: a box-plot for queue size in the simulation across test instances
            - load_ratio: a distribution scatter plot for the load ratio in function of the number of used nodes, for
                test instances separately;
            - efficiency: a box-plot for resource allocation efficiency across test instances
            - scalability: a scalability plot for dispatching methods across test instances
            - sim_time: a bar plot for the simulation timings across test instances
            - sim_memory: a bar plot for memory usage across test instances
        
        :param type: the type of the plot, must be one of the above;
        :param title: the title of the plot;
        :param scale: the scale of the plot (see matplotlib documentation);
        :param xlim: the left-right bounds for axis scaling, is a tuple;
        :param ylim: the bottom-top bounds for axis scaling, is a tuple;
        :param legend: activates the legend, is a boolean;
        :param figsize: the size of the figure, is a tuple;
        :param meansonly: triggers the plot of mean values alone in box-plots, is a boolean;
        :param alpha: the alpha of certain features in plots, in particular for distribution scatter plots;
        :param smooth: smoothing factor used for the Savitzky-Golay filter in the scalabily plot. The lower the number,
            the higher the smoothing;
        :param output: path of the output PDF file;
        """
        if not self._preprocessed:
            self.pre_process()
            print("Plot_factory: Files were not pre-processed yet. Calling the pre_process method.")

        if type == self.SLOWDOWN_PLOT and self._plot_class == self.SCHEDULE_CLASS:
            self.box_plot(self._slowdowns, title=title, ylabel='Slowdown', scale=scale, xlim=xlim, ylim=ylim, figsize=figsize, meansonly=meansonly, output=output, groups=groups, **kwargs)
        elif type == self.QUEUE_SIZE_PLOT and self._plot_class == self.SCHEDULE_CLASS:
            self.box_plot(self._queuesizes, title=title, ylabel='Queue size', scale=scale, xlim=xlim, ylim=(0, None), figsize=figsize, meansonly=meansonly, output=output, groups=groups, **kwargs)
        elif type == self.LOAD_RATIO_PLOT and self._plot_class == self.SCHEDULE_CLASS:
            self.distribution_scatter_plot(self._loadratiosX, self._loadratiosY, title=title, scale=scale, xlim=(-0.01, 1.01), ylim=(-0.01, 1.01), figsize=figsize, alpha=alpha, output=output, **kwargs)
        elif type == self.EFFICIENCY_PLOT and self._plot_class == self.SCHEDULE_CLASS:
            self.box_plot(self._efficiencies, title=title, ylabel='Resource efficiency', scale=scale, xlim=xlim, ylim=ylim, figsize=figsize, meansonly=meansonly, output=output, groups=groups, **kwargs)
        elif type == self.SCALABILITY_PLOT and self._plot_class == self.BENCHMARK_CLASS:
            self.scalability_plot(self._scalabilitydataX, self._scalabilitydataY, title, scale=scale, xlim=xlim, ylim=ylim, figsize=figsize, legend=legend, smooth=smooth, output=output, **kwargs)
        elif type == self.SIMULATION_TIME_PLOT and self._plot_class == self.BENCHMARK_CLASS:
            self.box_plot_times(self._mantimes, self._schedtimes, title=title, scale=scale, xlim=xlim, ylim=ylim, figsize=figsize, legend=legend, output=output, **kwargs)
        elif type == self.SIMULAION_MEMORY_PLOT and self._plot_class == self.BENCHMARK_CLASS:
            self.box_plot_memory(self._simmemory, title=title, scale=scale, xlim=xlim, ylim=ylim, figsize=figsize, legend=legend, output=output, **kwargs)
        else:
            raise Exception("Plot type specified is not valid. Review the documentation for valid plot types.")


    def _getBenchmarkData(self, filepath):
        """
        Pre-processes a resource usage log file.
        
        :param filepath: the path to the log file;
        :return: True if successful, False otherwise;
        """
        if self._debug:
            print("- Pre-processing file " + filepath + "...")
        # Tries to read from the file, aborts if an error is encountered
        try:
            f = open(filepath)
            mantimes = []
            schedtimes = []
            mems = []
            simtime = 0
            disptime = 0
            maxqueuesize = 0

            for line in f:
                # Each line is parsed and values are extracted from it
                attrs = line.split(';')
                mantimes.append(float(attrs[4]))
                schedtimes.append((int(attrs[1]), float(attrs[3])))
                mems.append(float(attrs[5]))
                simtime += float(attrs[2])
                disptime += float(attrs[3])

                if int(attrs[1]) > maxqueuesize:
                    maxqueuesize = int(attrs[1])
            f.close()
        except Exception as e:
            raise Exception("Error encountered while pre-processing: " + str(e))

        # Certain statistics are computed from the data
        data = {}
        data['avgman'] = np.average(np.array(mantimes))
        data['avgsched'] = np.average(np.array([el[1] for el in schedtimes]))
        data['simtime'] = simtime / 1000.0
        data['schedtime'] = disptime / 1000.0
        data['mantime'] = data['simtime'] - data['schedtime']
        data['avgmem'] = np.average(np.array(mems))
        data['maxmem'] = np.max(np.array(mems))

        # The scalability data is computed through binning: we want to obtain an X, Y set, where in X are the distinct
        # queue sizes, and in Y are the average times in ms to perform dispatching on such queue sizes
        binningfactor = 1
        bins = int(floor(maxqueuesize / binningfactor))
        queuevalues = np.linspace(0, maxqueuesize, bins)
        mappinglist = []
        for i in range(bins):
            mappinglist.append([])
        step = (maxqueuesize) / (bins - 1)
        for qsize, stime in schedtimes:
            index = int(floor(qsize / step))
            mappinglist[index].append(stime)
        finallist = []
        finalqueuevalues = []
        for i in range(len(mappinglist)):
            l = mappinglist[i]
            if len(l) > 0:
                finallist.append(sum(l) / len(l))
                finalqueuevalues.append(queuevalues[i])

        self._mantimes.append(mantimes)
        self._schedtimes.append([el[1] for el in schedtimes])
        self._simmemory.append(mems)
        self._simdata.append(data)
        self._scalabilitydataX.append(finalqueuevalues)
        self._scalabilitydataY.append(finallist)
        return True

    def _getScheduleData(self, filepath, config, resource=None, trimSlowdown=True, trimQueueSize=False):
        """
        Performs pre-processing on a schedule file through a meta-simulation process.
        
        :param filepath: The path of the file to be analyzed;
        :param config: The path to the system configuration file;
        :param resource: A resource to be considered for resource-related metrics; if none is specified, all resource
            types are used;
        :param: trimSlowdown: boolean flag. If True, slowdown values equal to 1 will be discarded. Default is True
        :param: trimQueueSize: boolean flag. If True, queue size values equal to 0 will be discarded. Default is False
        :return: True if successful, False otherwise;
        """
        if self._debug:
            print("- Pre-processing file " + filepath + "...")
        # Generates the dictionary of system resources from the config file
        resobject, equiv = self._generateSystemConfig(config)
        self._base_res = resobject.availability()
        res_types = resobject._system_resource_types

        # Makes sure the resource type exists in the system
        if resource is not None and resource not in resobject._system_resource_types:
            if self._debug:
                print("Resource type " + resource + "is not valid. Using all available resources...")
            resource = None

        # Tries to read from the log file, aborts if an error is encountered
        try:
            _sim_params_path = None
            # If the simulator config path points to a file, it is considered as is
            if self._sim_params_fname is not None and isfile(self._sim_params_fname):
                _sim_params_path = self._sim_params_fname
            # If it is a plain string, it is used as a token for config files in the experimentation
            elif self._sim_params_fname is not None:
                _path, _filename = path_leaf(filepath)
                _sim_params_path = join(_path, self._sim_params_fname)
            # If it is none, the default_result_parser will use the DEFAULT_SIMULATION config

            if _sim_params_path is not None:
                _resource_order = load_jsonfile(_sim_params_path)['RESOURCE_ORDER']
            else:
                _resource_order = self._resource_order

            if self._workload_parser is not None:
                reader = DefaultReader(filepath, parser=self._workload_parser, equivalence=equiv)
            else:
                reader = DefaultReader(filepath, parser=define_result_parser(_sim_params_path), equivalence=equiv)

            slowdowns = []
            timePoints = set()
            jobs = {}
            rev_timePoints = {}
            if self._debug:
                print("Loading jobs...")
            while True:
                # Jobs are read and their slowdown values are stored
                job = reader._read()
                if job is not None:
                    job['start_time'] = timestamp_func(job['start_time'])
                    job['end_time'] = timestamp_func(job['end_time'])
                    job['queue_time'] = timestamp_func(job['queue_time'])
                    _start_time = job['start_time']
                    _end_time = job['end_time']
                    _queued_time = job['queue_time']
                    duration = _end_time - _start_time
                    wait = _start_time - _queued_time
                    slowdown = (wait + duration) / duration if duration != 0 else wait if wait != 0 else 1.0
                    if slowdown > 1.0 or not trimSlowdown:
                        slowdowns.append(slowdown)

                    job_id = job['job_id']
                    jobs[job_id] = job
                    # Timepoints for use in the simulation are stored
                    timePoints.add(_queued_time)
                    self._addToDictAsList(rev_timePoints, _queued_time, job_id, 'queue')
                    timePoints.add(_start_time)
                    self._addToDictAsList(rev_timePoints, _start_time, job_id, 'start')
                    if duration > 0:
                        timePoints.add(_end_time)
                        self._addToDictAsList(rev_timePoints, _end_time, job_id, 'end')
                else:
                    break
        except Exception as e:
            raise Exception("Error encountered while pre-processing: " + str(e))

        # It may happen that the slowdown list is empty if all jobs have a value equal to 1. In this case we add
        # a fake value, equal to 1 as well
        if trimSlowdown and len(slowdowns) == 0:
            slowdowns.append(1)

        if self._debug:
            print("Jobs loaded. Sorting...")

        # We compute the final set of distinct, ordered timepoints
        timePoints = sorted(timePoints)
        timePointsIDX = 0

        self._sys_res = deepcopy(self._base_res)
        self._base_res_sum = {k: sum(self._base_res[n][k] for n in self._base_res) for k in res_types}
        self._used_res_sum = {k: 0 for k in res_types}
        self._avl_res_sum = {k: 0 for k in res_types}
        self._used_nodes = 0
        self._total_nodes = len(self._base_res.values())

        queue = set()
        running = set()

        # Pre-allocating the lists to store performance metrics, for efficiency
        queued = [0] * len(timePoints)  # []
        resources = [0] * len(timePoints)  # []
        run = [0] * len(timePoints)  # []
        efficiency = [0] * len(timePoints)  # []
        efficiencyperjob = [0] * len(jobs)  # []
        efficiencyIDX = 0

        if self._debug:
            print("Sorting done. Starting simulation...")

        # Meta-simulation: goes on until there are no more timepoints to consider
        while timePointsIDX < len(timePoints):
            point = timePoints[timePointsIDX]
            timePointsIDX += 1

            # Adds to the queue jobs that were submitted in this timepoint
            jobstoqueue = rev_timePoints[point]['queue']
            # queue += len(jobstoqueue)
            queue.update(jobstoqueue)

            # Jobs that have terminated release their resources
            jobstoend = rev_timePoints[point]['end']
            if len(jobstoend) > 0:
                for j_id in jobstoend:
                    j = jobs[j_id]
                    req, assignations = self._getRequestedResources(_resource_order, j['assignations'])
                    self._deallocate_resources(req, assignations, resource)
                # running -= len(jobstoend)
                running = running - jobstoend

            # Jobs that have to start take their resources from the system
            jobstostart = rev_timePoints[point]['start']
            if len(jobstostart) > 0:
                for j_id in jobstostart:
                    j = jobs[j_id]
                    if j['end_time'] - j['start_time'] > 0:
                        req, assignations = self._getRequestedResources(_resource_order, j['assignations'])
                        self._allocate_resources(req, assignations, resource)
                        # running += 1
                        running.add(j_id)
                # queue -= len(jobstostart)
                queue = queue - jobstostart

                # Additionally, we store for every started job its resource allocation efficiency
                for j_id in jobstostart:
                    j = jobs[j_id]
                    if j['end_time'] - j['start_time'] > 0:
                        req, assignations = self._getRequestedResources(_resource_order, j['assignations'])
                        eff = self._getResourceEfficiency(req, assignations, self._sys_res, resource)
                        efficiencyperjob[efficiencyIDX] = eff
                        efficiencyIDX += 1

            # System metrics are computed AFTER dispatching
            queued[timePointsIDX - 1] = len(queue)  # queue
            run[timePointsIDX - 1] = len(running)  # running
            resources[timePointsIDX - 1] = self._getLoadRatio(resource)
            efficiency[timePointsIDX - 1] = self._getLoadRatioSelective(resource)

        if self._debug:
            print("Simulation done!")

        if trimQueueSize:
            queued = [q for q in queued if q != 0]
            run = [r for r in run if r != 0]

        # The metrics values for this instance are added to the internal variables
        self._slowdowns.append(slowdowns)
        self._queuesizes.append(queued)
               
        self._efficiencies.append(efficiencyperjob)
        self._loadratiosX.append([el[0] for el in efficiency])
        self._loadratiosY.append([el[1] for el in efficiency])
        return True

    def _addToDictAsList(self, dict, key, el, type):
        """
        Simple method that adds an element to a dictionary and creates sub-entries if needed.
        
        :param dict: The target dictionary 
        :param key: The key of the element to add
        :param el: The element to add
        :param type: The type of the element to add, used in the sub-dictionary for the key entry
        :return: None
        """
        if key not in dict:
            dict[key] = {'queue': set(), 'start': set(), 'end': set()}
        dict[key][type].add(el)

    def _allocate_resources(self, req, assignations, resource=None):
        """
        Method that allocates the resources for a certain starting job and updates all data structures related to
        resource usage
        
        :param req: The resource request of the job
        :param assignations: The list of nodes assigned to the job
        :param resource: A resource type to be considered for performance metrics (optional)
        :return: None
        """
        for node in assignations:
            # If the node goes from the unused to the used state, we update the number of used nodes and the amount
            # of available resources among the used nodes, for the efficiency plots
            if resource is None and all(self._sys_res[node][k] == self._base_res[node][k] for k in self._base_res[node].keys()):
                self._used_nodes += 1
                for k, v in self._base_res[node].items():
                    self._avl_res_sum[k] += v
            # If a specific resource type is considered, the same condition is triggered only if such resource is used
            elif resource is not None and self._sys_res[node][resource] == self._base_res[node][resource] and req[resource] > 0:
                self._used_nodes += 1
                self._avl_res_sum[resource] += self._base_res[node][resource]
            # Updating the per-node currently available resources
            for k, val in req.items():
                self._sys_res[node][k] -= val
                if self._sys_res[node][k] < 0:
                    self._sys_res[node][k] = 0
                    if self._debug:
                        print("Caution: resource " + k + " is going below zero.")

        # Updating the dictionary of per-type currently used resources
        for k, v in req.items():
            self._used_res_sum[k] += v * len(assignations)
            if self._used_res_sum[k] > self._avl_res_sum[k]:
                self._used_res_sum[k] = self._avl_res_sum[k]

    def _deallocate_resources(self, req, assignations, resource):
        """
        Method that de-allocates the resources for a certain starting job and updates all data structures related to
        resource usage
        
        :param req: The resource request of the job
        :param assignations: The list of nodes assigned to the job
        :param resource: A resource type to be considered for performance metrics (optional)
        :return: None
        """
        for node in assignations:
            for k, val in req.items():
                self._sys_res[node][k] += val
                if self._sys_res[node][k] > self._base_res[node][k]:
                    self._sys_res[node][k] = self._base_res[node][k]
                    if self._debug:
                        print("Caution: resource " + k + " is going beyond its base capacity.")
        # In this case the check for used-unused nodes must be performed after the resources are de-allocated
            if resource is None and all(self._sys_res[node][k] == self._base_res[node][k] for k in self._base_res[node].keys()):
                self._used_nodes -= 1
                for k, v in self._base_res[node].items():
                    self._avl_res_sum[k] -= v
            elif resource is not None and self._sys_res[node][resource] == self._base_res[node][resource] and req[resource] > 0:
                self._used_nodes -= 1
                self._avl_res_sum[resource] -= self._base_res[node][resource]

        # The method is specular to allocate_resources and works identically
        for k, v in req.items():
            self._used_res_sum[k] -= v * len(assignations)
            if self._used_res_sum[k] < 0:
                self._used_res_sum[k] = 0

    def _generateSystemConfig(self, config_path):
        """
        Generates a Resources object from a system configuration file.
        
        :param config_path: the path to the config file;
        :return: the Resources object and the resource equivalence;
        """
        try:
            config = load_config(config_path)
            equiv = config.pop('equivalence', {})
            # PEP 448 - Additional Unpacking Generalizations 
            # python 3.5 and newer 
            if not('node_prefix' in config):
                config['node_prefix'] = ''
            resources = Resources(**config)
            return resources, equiv
        except Exception as e:
            if config_path != '':
                print("Could not load system config: " + str(e))
            else:
                print("A system configuration file must be specified.")
            exit()

        return None, None

    def _getRequestedResources(self, _resource_order, assignations_str):
        """
        TO BE IMPLEMENTED:
        returns the requested resources for the input job.
        
        :param job: the dictionary related to the current job;
        :return: the dictionary of resources needed by each job unit, and the list of node assignations;
        """
        _assignations_list = assignations_str.split(str_resources.SEPARATOR)[0:-1]
        _nodes_list = [assign.split(';')[0] for assign in _assignations_list]
        _request = { k:int(v) for k, v in zip(_resource_order, _assignations_list[0].split(';')[1:])}
        return _request, _nodes_list

    def _getResourceEfficiency(self, reqres, nodes, sys_res, resource):
        """
        Computes the resource allocation efficiency metric for a certain input job.
        
        This method computed the resource allocation efficiency AFTER dispatching is performed, not before.
        
        :param reqres: the dictionary of resources requested by each job unit;
        :param nodes: the list of node assignations;
        :param sys_res: the dictionary of system resources;
        :param resource: the resource type to be considered (if present);
        :return: the resource allocation efficiency;
        """

        # Computing the amount of used resources by the job
        if resource is None:
            used = sum(r * len(nodes) for r in reqres.values())
        else:
            used = reqres[resource] * len(nodes)

        avl = 0
        # Computing the amount of available resources in nodes used by the job
        for node in set(nodes):
            if resource is None:
                avl += sum(r for r in sys_res[node].values())
            else:
                avl += sys_res[node][resource]
        return used / (avl + used)

    def _getLoadRatio(self, resource):
        """
        Returns the standard load ratio for the system.
        
        :param resource: the resource type to be considered (if present);
        :return: the load ratio;
        """
        loadratio = 0

        if resource is None:
            loadratio = sum(self._used_res_sum.values()) / sum(self._base_res_sum.values())
        elif resource in self._base_res_sum:
            loadratio = self._used_res_sum[resource] / self._base_res_sum[resource]

        return loadratio

    def _getLoadRatioSelective(self, resource):
        """
        Returns the per-step resource allocation efficiency.
        
        This is defined as a X,Y pair where X expresses the fraction of used nodes, and Y defines the fraction of used
        resources in such nodes.

        :param resource: the resource type to be considered (if present);
        :return: an X,Y pair expressing the per-step resource allocation efficiency;
        """
        loadratio = 0
        if self._used_nodes > 0:
            if resource is None:
                loadratio = sum(self._used_res_sum.values()) / sum(self._avl_res_sum.values())
            elif resource in self._avl_res_sum:
                loadratio = self._used_res_sum[resource] / self._avl_res_sum[resource]
            return self._used_nodes / self._total_nodes, loadratio
        else:
            return 0, 0

    def _getDistributionStats(self, data):
        """
        Returns some useful distribution statistics for the input data.
        
        The mean, minimum, maximum, median, and quartiles for the data are computed.
        
        :param data: The iterable for the input data;
        :return: a dictionary of statistics for the data distribution;
        """
        stats = {}
        stats['avg'] = np.average(data)
        stats['min'] = np.min(data)
        stats['max'] = np.max(data)
        stats['median'] = np.median(data)
        stats['quartiles'] = np.percentile(data, range(0, 100, 25))
        return stats

    def box_plot(self, data, title='', ylabel='', scale='linear', figsize=(7, 5), meansonly=False, output='Output.pdf', groups=1, **kwargs):
        """
        Produces a box-and-whiskers plot for the input data's distributions.
        
        :param data: the input data; must be a list, in which each element is again a list containing all of the data
            regarding a certain test instance; the ordering must be that of the labels;
        :param title: the title of the plot;
        :param ylabel: the Y-axis label;
        :param scale: the scale of the plot;
        :param figsize: the size of the figure, is a tuple;
        :param meansonly: if True only the mean values for each distribution are depicted;
        :param output: the path to the output file;
        :param **kwargs: 
            - fig_format: {
                'format': eps or pdf,
                'dpi': Int number
            }
            - xlim: the left-right axis boundaries, is a tuple;
            - ylim: the bottom-top axis boundaries, is a tuple;

        """
        color_cycler = ['b', 'r', 'y', 'g', 'c', 'm', 'k', 'w']
        hatch_cycler = ['/', '\\', '|', '-', '+', 'x', 'o', 'O', '.', '*']
        ncycle = 2
        fontsize = 12
        plt.rc('xtick', labelsize=fontsize)
        plt.rc('ytick', labelsize=fontsize)
        N = len(data)

        ylim = kwargs.pop('ylim', None)
        xlim = kwargs.pop('xlim', None)
        show_legend = kwargs.pop('show_legend', False)

        spacing = 0.2
        ind = [i * spacing for i in np.arange(N)]
        width = 0.1
        markersize = 250
        linecol = 'black'
        tricol = 'black'
        vertlinecol = 'gray'

        fig, ax = plt.subplots(figsize=figsize)

        c_group = 0
        c = groups
        r_hatch = len(hatch_cycler)
        color_list = []
        hatch_list = []
        for i, d in enumerate(data):
            color_list.append(color_cycler[c_group])
            hatch_list.append(hatch_cycler[len(hatch_cycler) - r_hatch] * ncycle)
            c -= 1
            if c == 0:
                c_group += 1
                c = groups
            r_hatch -= 1
            if r_hatch == 0:
                ncycle += 1
                r_hatch = len(hatch_cycler)
        bp = ax.boxplot(data, labels=self._labels, patch_artist=True, sym="", whis=[0, 100], showmeans=True, showfliers=False)
        
        for patch, color, hatch in zip(bp['boxes'], color_list, hatch_list):
            patch.set_facecolor(color)
            patch.set_alpha(0.75)
            patch.set_hatch(hatch)
        

        # add some text for labels, title and axes ticks
        ax.set_ylabel(ylabel, fontsize=fontsize)
        ax.set_xlabel('Dispatching method', fontsize=fontsize)
        ax.set_title(title)
        ax.set_yscale(scale)
        
        if show_legend:
            ax.legend(bp['boxes'], self._labels, bbox_to_anchor=(0., 1.02, 1., .102), loc=3, ncol=len(self._labels) // 2, mode="expand", borderaxespad=0.)
        
        if ylim:
            ax.set_ylim(top=ylim[1], bottom=ylim[0], emit=True, auto=False)
        if xlim:
            ax.set_xlim(left=xlim[0], right=xlim[1], emit=True, auto=False)

        plt.tight_layout()
        plt.grid(linestyle=':', color='gray', zorder=0)
        plt.show()

        fig_format = kwargs.pop('fig_format', {})
        fig.savefig(output, **fig_format)
        

    def box_plot_times(self, dataman, datasched, title='', scale='linear', xlim=(None, None), ylim=(None, None), figsize=(7, 5), legend=True, output='Output.pdf'):
        """
        Produces a bar plot for the timings in the simulations, across test instances.
        
        The bars will depict the average time required to perform dispatching in each simulation step, and the
        time required to perform simulation-related tasks in the simulation.
        
        :param dataman: the data for the time required in each step to perform simulation-related tasks. Is a list,
            where each element is again a list containing the data for a certain test instance;
        :param datasched: the data for the time required in each step to perform dispatching. Is a list, where
            each element is again a list containing the data for a certain test instance;
        :param title: the title of the plot;
        :param scale: the scale of the plot;
        :param xlim: the left-right boundaries for the plot, is a tuple;
        :param ylim: the bottom-top boundaries for the plot, is a tuple;
        :param figsize: the size of the figure, is a tuple;
        :param legend: enables or disables visualization of the legend;
        :param output: the path to the output file;
        """

        fontsize = 12
        plt.rc('xtick', labelsize=fontsize)
        plt.rc('ytick', labelsize=fontsize)
        N = len(dataman)

        spacing = 0.2
        ind = [i * spacing for i in np.arange(N)]
        width = 0.1
        markersize = 250

        fig, ax = plt.subplots(figsize=figsize)

        for i in range(N):
            avgman = np.average(np.array(dataman[i]))
            avgsched = np.average(np.array(datasched[i]))
            if i == 0:
                ax.add_patch(patches.Rectangle((ind[i], 0), width, avgman, facecolor='orange', edgecolor='black', hatch='//', alpha=0.75))  # , label='Simulation'))
                ax.add_patch(patches.Rectangle((ind[i], avgman), width, avgsched, facecolor='blue', edgecolor='black', hatch='\\', alpha=0.75, label='Dispatching decision'))
            else:
                ax.add_patch(patches.Rectangle((ind[i], 0), width, avgman, facecolor='orange', edgecolor='black', hatch='//', alpha=0.75))
                ax.add_patch(patches.Rectangle((ind[i], avgman), width, avgsched, facecolor='blue', edgecolor='black', hatch='\\', alpha=0.75))
            ax.scatter(ind[i] + width / 2, avgman + avgsched, marker='_', s=markersize / 4, zorder=0, color='black')

        # add some text for labels, title and axes ticks
        ax.set_ylabel('Time [ms]', fontsize=fontsize)
        ax.set_xlabel('Dispatching method', fontsize=fontsize)
        ax.set_title(title)
        ax.set_xticks([i + width / 2 for i in ind])
        if legend:
            ax.legend()
        ax.set_xticklabels(self._labels)
        ax.set_yscale(scale)
        ax.set_ylim(top=ylim[1], bottom=ylim[0], emit=True, auto=False)
        ax.set_xlim(left=xlim[0], right=xlim[1], emit=True, auto=False)

        plt.grid(linestyle=':', color='gray', zorder=0)
        plt.setp(plt.gca().get_legend().get_texts(), fontsize=fontsize)
        plt.show()

        ff = PdfPages(output)
        ff.savefig(fig)
        ff.close()

    def box_plot_memory(self, data, title='', scale='linear', xlim=(None, None), ylim=(None, None), figsize=(7, 5), legend=True, output='Output.pdf'):
        """
        Produces a bar plot for the memory usage in the simulations, across test instances.
        
        The bars depict average and maximum memory usage in the simulation.

        :param data: the data for memory usage in each simulation step. Is a list, where
            each element is again a list containing the data for a certain test instance;
        :param title: the title of the plot;
        :param scale: the scale of the plot;
        :param xlim: the left-right boundaries for the plot, is a tuple;
        :param ylim: the bottom-top boundaries for the plot, is a tuple;
        :param figsize: the size of the figure, is a tuple;
        :param legend: enables or disables visualization of the legend;
        :param output: the path to the output file;
        """
        fontsize = 12
        plt.rc('xtick', labelsize=fontsize)
        plt.rc('ytick', labelsize=fontsize)
        N = len(data)

        spacing = 0.2
        ind = [i * spacing for i in np.arange(N)]
        width = 0.1
        markersize = 250

        fig, ax = plt.subplots(figsize=figsize)

        for i in range(N):
            avgmem = np.average(np.array(data[i]))
            maxmem = np.max(np.array(data[i]))
            if i == 0:
                ax.add_patch(patches.Rectangle((ind[i], 0), width, avgmem, facecolor='orange', edgecolor='black', hatch='//', alpha=0.75, label='Avg. Mem'))
                ax.add_patch(patches.Rectangle((ind[i], avgmem), width, maxmem - avgmem, facecolor='blue', edgecolor='black', hatch='\\', alpha=0.75, label='Max. Mem'))
            else:
                ax.add_patch(patches.Rectangle((ind[i], 0), width, avgmem, facecolor='orange', edgecolor='black', hatch='//', alpha=0.75))
                ax.add_patch(patches.Rectangle((ind[i], avgmem), width, maxmem - avgmem, facecolor='blue', edgecolor='black', hatch='\\', alpha=0.75))
            ax.scatter(ind[i] + width / 2, maxmem, marker='_', s=markersize / 4, zorder=0, color='black')

        ax.set_ylabel('Average Memory Usage [MB]', fontsize=fontsize)
        ax.set_xlabel('Dispatching method', fontsize=fontsize)
        ax.set_title(title)
        ax.set_xticks([i + width / 2 for i in ind])
        if legend:
            ax.legend()
        ax.set_xticklabels(self._labels)
        ax.set_yscale(scale)
        ax.set_ylim(top=ylim[1], bottom=ylim[0], emit=True, auto=False)
        ax.set_xlim(left=xlim[0], right=xlim[1], emit=True, auto=False)

        plt.grid(linestyle=':', color='gray', zorder=0)
        plt.setp(plt.gca().get_legend().get_texts(), fontsize=fontsize)
        plt.show()

        ff = PdfPages(output)
        ff.savefig(fig)
        ff.close()

    def scalability_plot(self, xdata, ydata, title='', scale='linear', xlim=(None, None), ylim=(None, None), figsize=(7, 5), legend=True, smooth=30, linestyles=None, markers=None, output='Output.pdf'):
        """
        Creates a scalability plot for all test instances, where X represents the queue size, and Y the average
        time required by each dispatching method in the instances.
        
        :param xdata: the X data, containing the queue sizes for each test instance; is a list, where each element
            contains a list with the data for each test instance;
        :param ydata: the Y data, containing the average times required to perform dispatching in each test instance;
            is a list, where each element contains a list with the data for each test instance;
        :param title: the title of the plot;
        :param scale: the scale of the plot;
        :param xlim: the left-right boundaries for the plot, is a tuple;
        :param ylim: the bottom-top boundaries for the plot, is a tuple;
        :param figsize: the size of the figure, is a tuple;
        :param legend: enables or disables visualization of the legend; 
        :param smooth: smoothing factor for the Savitzky-Golay filter. The lower the number, the higher the smoothing;
        :param output: the path of the output file;
        """

        fontsize = 12
        plt.rc('xtick', labelsize=fontsize)
        plt.rc('ytick', labelsize=fontsize)
        if not linestyles:
            linestyles = ('-', '-', '--', '--', '-.', '-.', ':', ':')
        if not markers:
            markers = (None, 'o', None, '^', None, 's', None, 'p')
        
        numstyles = len(linestyles)

        fig, ax = plt.subplots(figsize=figsize)

        divideFactor = smooth

        for i in range(len(xdata)):
            markeroffset = floor(max(xdata[i]) / 20 + i * 2)
            if divideFactor > 1 and len(ydata[i]) >= divideFactor:
                win_len = floor(len(ydata[i]) / divideFactor)
                win_len += (win_len + 1) % 2
                if win_len < 5:
                    win_len = 5
                yfiltered = savgol_filter(ydata[i], win_len, 3)
            else:
                yfiltered = ydata[i]
            ax.plot(xdata[i], yfiltered, label=self._labels[i], linestyle=linestyles[i % numstyles], marker=markers[i % numstyles], markevery=markeroffset, zorder=2 if markers[i % numstyles] is None else 0)

        ax.set_ylabel('Time [ms]', fontsize=fontsize)
        ax.set_xlabel('Queue size', fontsize=fontsize)
        ax.set_title(title)
        if legend:
            ax.legend()

        ax.set_yscale(scale)
        ax.set_ylim(top=ylim[1], bottom=ylim[0], emit=True, auto=False)
        ax.set_xlim(left=xlim[0], right=xlim[1], emit=True, auto=False)

        plt.grid(linestyle=':', color='gray', zorder=0)
        plt.setp(plt.gca().get_legend().get_texts(), fontsize=fontsize)
        plt.show()

        ff = PdfPages(output)
        ff.savefig(fig)
        ff.close()

    def distribution_scatter_plot(self, xdata, ydata, title='', scale='linear', xlim=(0, 1.05), ylim=(0, 1.05), figsize=(7, 5), alpha=0.005, output='Output.pdf'):
        """
        Creates a distribution scatter plot for the system's resource efficiency. 
        
        The X values represent the amount of used nodes in a certain time step, while the Y values represent the
        fraction of used resources in such nodes. Darker areas of the plot represent values with higher frequency.
        The method creates one plot per test instance, automatically.
        
        :param xdata: 
        :param ydata: 
        :param alpha: the alpha to be used for each dot in the plot;
        :param title: the title of the plot;
        :param scale: the scale of the plot;
        :param xlim: the left-right boundaries for the plot, is a tuple;
        :param ylim: the bottom-top boundaries for the plot, is a tuple;
        :param figsize: the size of the figure, is a tuple;
        :param output: the path to the output files: the label for each test instance will be automatically added 
            for each file;
        """

        for i in range(len(xdata)):
            fig, ax = plt.subplots(figsize=figsize)

            ax.scatter(xdata[i], ydata[i], color='black', alpha=alpha, s=5)

            ax.set_title(title)
            ax.set_xlabel('Used Nodes')
            ax.set_ylabel('Used Resources')
            ax.set_yscale(scale)
            ax.set_ylim(top=ylim[1], bottom=ylim[0], emit=True, auto=False)
            ax.set_xlim(left=xlim[0], right=xlim[1], emit=True, auto=False)
            ax.grid(True)

            plt.show()
            splitoutput = splitext(output)
            ff = PdfPages(splitoutput[0] + '-' + self._labels[i] + '.pdf')
            ff.savefig(fig)
            ff.close()

    def get_preprocessed_benchmark_data(self):
        """
        Returns all of the pre-processed benchmark-related data.
        
        A tuple is returned; each element of the tuple is related to a specific kind of metric that was processed.
        Also, each element of the tuple is a list, with as many entries as the files that were processed, in the
        same order. Each element of these lists contains then the data related to a specific metric, for a specific
        test instance. All data is stored in standard Python lists.

        :return: a tuple in which every element is a list containing, in each element, a specific kind of data 
            regarding one of the test instances. The tuple contains, in this order:
            
            - the resource usage statistics' dictionaries;
            - the lists of dispatching times for each time step;
            - the lists of management times for each time step;
            - the lists of memory usage values for each time step;
            - the X scalability data containing the queue size for each test instance;
            - the Y scalability data containing the average dispatching times for each test instance;
        """
        if not self._preprocessed or self._plot_class != self.BENCHMARK_CLASS:
            return None, None, None, None, None, None
        else:
            return self._simdata, self._schedtimes, self._mantimes, self._simmemory, self._scalabilitydataX, self._scalabilitydataY

    def get_preprocessed_schedule_data(self):
        """
        Returns all of the pre-processed schedule-related data.

        A tuple is returned; each element of the tuple is related to a specific kind of metric that was processed.
        Also, each element of the tuple is a list, with as many entries as the files that were processed, in the
        same order. Each element of these lists contains then the data related to a specific metric, for a specific
        test instance. All data is stored in standard Python lists.

        :return: a tuple in which every element is a list containing, in each element, the data regarding one of the
            test instances. The tuple contains, in this order:

            - the slowdown values for jobs;
            - the queue sizes for all time steps;
            - the resource allocation efficiencies for all jobs;
            - the X data regarding the load ratios (fraction of used nodes) for all time steps;
            - the Y data regarding the load ratios (fraction of used resources) for all time steps;
        """
        if not self._preprocessed or self._plot_class != self.SCHEDULE_CLASS:
            return None, None, None, None, None
        else:
            return self._slowdowns, self._queuesizes, self._efficiencies, self._loadratiosX, self._loadratiosY

if __name__ == '__main__':
    # This is an example. It should not be executed here, but in a script in the project's root, where also
    # basic_example.py is, so that all imports can be resolved correctly.
    resultpath = ['Path/to/benchmark/file',
                  'Path/to/benchmark/file2']
    resultlabel = ['Label',
                   'Label2']
    plots = PlotFactory('benchmark')
    plots.set_files(resultpath, resultlabel)
    plots.pre_process()
    plots.produce_plot(type='scalability', title='My Scalability Plot')
