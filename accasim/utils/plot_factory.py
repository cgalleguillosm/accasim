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
from accasim.utils.reader_class import default_reader_class
from accasim.utils.misc import load_config, from_isodatetime_2_timestamp as _timestamp, str_resources
from accasim.utils.file import path_leaf, load_jsonfile
from accasim.base.resource_manager_class import resources_class
from copy import deepcopy
from os.path import splitext as _splitext, join as _join
import numpy as np


class plot_factory:
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
    SIMULAION_MEMORY_PLOT ='sim_memory'
    
    PLOT_TYPES = {
        SCHEDULE_CLASS: [SLOWDOWN_PLOT, QUEUE_SIZE_PLOT, LOAD_RATIO_PLOT, EFFICIENCY_PLOT],
        BENCHMARK_CLASS: [SCALABILITY_PLOT, SIMULATION_TIME_PLOT, SIMULAION_MEMORY_PLOT]
    }

    def __init__(self, plot_class, sim_params_fname, config=None, resource=None, workload_parser=None, debug=False):
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

        if not (plot_class in self.PLOT_TYPES.keys()):
            if self._debug:
                print('Wrong Plot plot_class chosen. Selecting schedule plot_class by default...') 
            plot_class = self.SCHEDULE_CLASS
        self._plot_class = plot_class

        self._debug = debug
        self._sim_params_fname = sim_params_fname
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


    def setFiles(self, paths, labels):
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

    def preProcess(self):
        """
        Performs pre-processing on all specified files, according to their type.
        
        If the files are of the schedule type, a meta-simulation is run for each of them, computing data like slowdown,
        queue size, load ratios and such. If the data is of the benchmark type, the files are simply parsed and their
        information stored.
        
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
                    if not self._getScheduleData(f, self._config, self._resource):
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

    def producePlot(self, type, title='', scale='linear', xlim=(None,None), ylim=(None,None), legend=True, figsize=(7,5), meansonly=False, alpha=0.005, output='Output.pdf'):
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
        :param output: path of the output PDF file;
        """
        if self._preprocessed:
            if type == self.SLOWDOWN_PLOT and self._plot_class == self.SCHEDULE_CLASS:
                self.boxPlot(self._slowdowns,title,'Slowdown', scale, xlim, (1, None), figsize, meansonly, output)
            elif type == self.QUEUE_SIZE_PLOT and self._plot_class == self.SCHEDULE_CLASS:
                self.boxPlot(self._queuesizes, title, 'Queue size', scale, xlim, (0, None), figsize, meansonly, output)
            elif type == self.LOAD_RATIO_PLOT and self._plot_class == self.SCHEDULE_CLASS:
                self.distributionScatterPlot(self._loadratiosX, self._loadratiosY, alpha, title, scale, xlim, ylim, figsize, output)
            elif type == self.EFFICIENCY_PLOT and self._plot_class == self.SCHEDULE_CLASS:
                self.boxPlot(self._efficiencies, title, 'Resource efficiency', scale, xlim, ylim, figsize, meansonly, output)
            elif type == self.SCALABILITY_PLOT and self._plot_class == self.BENCHMARK_CLASS:
                self.scalabilityPlot(self._scalabilitydataX, self._scalabilitydataY, title, scale, xlim, ylim, figsize, legend, output)
            elif type == self.SIMULATION_TIME_PLOT and self._plot_class == self.BENCHMARK_CLASS:
                self.boxPlotTimes(self._mantimes, self._schedtimes, title, scale, xlim, ylim, figsize, legend, output)
            elif type == self.SIMULAION_MEMORY_PLOT and self._plot_class == self.BENCHMARK_CLASS:
                self.boxPlotMemory(self._simmemory, title, scale, xlim, ylim, figsize, legend, output)
            else:
                raise Exception("Plot type specified is not valid. Review the documentation for valid plot types.")
        else:
            print("Files were not pre-processed yet. Please call the preProcess method first.")

    def _getBenchmarkData(self, filepath):
        """
        Pre-processes a resource usage log file.
        
        :param filepath: the path to the log file;
        :return: True if successful, False otherwise;
        """
        if self._debug:
            print("- Pre-processing file "+filepath+"...")
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
        for qsize,stime in schedtimes:
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

    def _getScheduleData(self, filepath, config, resource=None):
        """
        Performs pre-processing on a schedule file through a meta-simulation process.
        
        :param filepath: The path of the file to be analyzed;
        :param config: The path to the system configuration file;
        :param resource: A resource to be considered for resource-related metrics; if none is specified, all resource
            types are used;
        :return: True if successful, False otherwise;
        """
        if self._debug:
            print("- Pre-processing file " + filepath + "...")
        # Generates the dictionary of system resources from the config file
        resobject, equiv = self._generateSystemConfig(config)
        base_res = resobject.availability()

        # Makes sure the resource type exists in the system
        if resource is not None and resource not in resobject.system_resource_types:
            if self._debug:
                print("Resource type " + resource + "is not valid. Using all available resources...")
            resource = None

        # Tries to read from the log file, aborts if an error is encountered
        try:
            reader = default_reader_class(filepath, parser=self._workload_parser, equivalence=equiv) 
            _path, _filename = path_leaf(filepath)
            _sim_params_path = _join(_path, self._sim_params_fname)
            jobs = []
            slowdowns = []
            timePoints = []
            if self._debug:
                print("Loading jobs...")
            while True:
                # Jobs are read and their slowdown values are stored
                job = reader.read()
                if job is not None:
                    job['start_time'] = _timestamp(job['start_time'])
                    job['end_time'] = _timestamp(job['end_time'])
                    job['queue_time'] = _timestamp(job['queue_time'])
                    _start_time = job['start_time']
                    _end_time = job['end_time']
                    _queued_time = job['queue_time']
                    duration = _end_time - _start_time
                    wait = _start_time - _queued_time
                    slowdown = (wait + duration) / duration if duration != 0 else wait if wait != 0 else 1.0
                    #===========================================================
                    # What happen if there is only 1.0 slowdown values?? The chart must be skipped? or generated?
                    # if slowdown > 1.0:
                    #     slowdowns.append(slowdown)
                    #===========================================================
                    slowdowns.append(slowdown)
                    jobs.append(job)
                    # Timepoints for use in the simulation are stored
                    timePoints.append(_queued_time)
                    timePoints.append(_start_time)
                    timePoints.append(_end_time)
                else:
                    break
        except Exception as e:
            raise Exception("Error encountered while pre-processing: " + str(e))

        if self._debug:
            print("Jobs loaded. Sorting...")

        # Jobs are sorted by their submission time, like in the original simulation
        jobs.sort(key=lambda x: x['queue_time'])
        # We compute the final set of distinct, ordered timepoints
        timePoints = sorted(set(timePoints))

        queued = []
        resources = []

        timePointsIDX = 0
        jobIDX = 0
        sys_res = deepcopy(base_res)

        queue = []
        run = []
        efficiency = []
        efficiencyperjob = []
        running = []

        if self._debug:
            print("Sorting done. Starting simulation...")

        # Meta-simulations: goes on until there are no more timepoints to consider
        while timePointsIDX < len(timePoints):
            point = timePoints[timePointsIDX]
            timePointsIDX += 1

            # Adds to the queue jobs that were submitted in this timepoint; the index is persistent, for efficiency
            while jobIDX < len(jobs):
                j = jobs[jobIDX]
                assert j['queue_time'] >= point
                if j['queue_time'] == point:
                    queue.append(jobs[jobIDX])
                    jobIDX += 1
                else:
                    break

            # System metrics are computed BEFORE dispatching
            queued.append(len(queue) + 1)
            run.append(len(running) + 1)
            resources.append(self._getLoadRatio(base_res, sys_res, resource))
            efficiency.append(self._getLoadRatioSelective(base_res, sys_res, resource))
            
            # Jobs that have terminated release their resources
            jobstoend = [j for j in running if j['end_time'] == point]
            for j in jobstoend:
                req, assignations = self._getRequestedResources(_sim_params_path, j['assignations'])
                for node in assignations:
                    for k,val in req.items():
                        sys_res[node][k] += val
                running.remove(j)
            
            # Jobs that have to start take their resources from the system
            jobstostart = [j for j in queue if j['start_time'] == point]
            for j in jobstostart:
                if j['end_time'] - j['start_time'] > 0:
                    req, assignations = self._getRequestedResources(_sim_params_path, j['assignations'])
                    for node in assignations:
                        for k, val in req.items():
                            sys_res[node][k] -= val
                            if sys_res[node][k] < 0:
                                sys_res[node][k] = 0
                                if self._debug:
                                    print("Caution: resource " + k + " is going below zero.")
                    running.append(j)
                queue.remove(j)
            # Additionally, we store for every started job its resource allocation efficiency
            for j in jobstostart:
                if j['end_time'] - j['start_time'] > 0:
                    req, assignations = self._getRequestedResources(_sim_params_path,j['assignations'])
                    eff = self._getResourceEfficiency(req, assignations, sys_res, resource)
                    efficiencyperjob.append(eff)

        if self._debug:
            print("Simulation done!")

        # The metrics values for this instance are added to the internal variables
        self._slowdowns.append(slowdowns)
        self._queuesizes.append(queued)
        self._efficiencies.append(efficiencyperjob)
        self._loadratiosX.append([el[0] for el in efficiency])
        self._loadratiosY.append([el[1] for el in efficiency])
        return True

    def _generateSystemConfig(self, config_path):
        """
        Generates a resources_class object from a system configuration file.
        
        :param config_path: the path to the config file;
        :return: the resources_class job, and the resource equivalence;
        """
        try:
            config = load_config(config_path)
            equiv = config.pop('equivalence', {})
            resources = resources_class(**config, node_prefix='')
            return resources, equiv
        except Exception as e:
            if config_path != '':
                print("Could not load system config: "+str(e))
            else:
                print("A system configuration file must be specified.")
            exit()

        return None, None

    def _getRequestedResources(self, _params_path, assignations_str):
        """
        TO BE IMPLEMENTED:
        returns the requested resources for the input job.
        
        :param job: the dictionary related to the current job;
        :return: the dictionary of resources needed by each job unit, and the list of node assignations;
        """
        _resource_order = load_jsonfile(_params_path)['resource_order']
        _assignations_list = assignations_str.split(str_resources.SEPARATOR)[0:-1]
        _nodes_list = [assign.split(';')[0] for assign in _assignations_list]
        _request = { k:int(v) for k,v in zip(_resource_order, _assignations_list[0].split(';')[1:])}
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

    def _getLoadRatio(self, base_res, sys_res, resource):
        """
        Returns the standard load ratio for the system.
        
        :param base_res: the basic availability of resources in the system;
        :param sys_res: the system resources dictionary;
        :param resource: the resource type to be considered (if present);
        :return: the load ratio;
        """
        used = 0
        available = 0
        for k, node in sys_res.items():
            if resource is None:
                defaultavl = sum([r for r in base_res[k].values()])
                used += defaultavl - sum([r for r in node.values()])
                available += defaultavl
            elif base_res[k][resource] > 0:
                defaultavl = base_res[k][resource]
                used += defaultavl - node[resource]
                available += defaultavl

        return used / available

    def _getLoadRatioSelective(self, base_res, sys_res, resource):
        """
        Returns the per-step resource allocation efficiency.
        
        This is defined as a X,Y pair where X expresses the fraction of used nodes, and Y defines the fraction of used
        resources in such nodes.
        
        :param base_res: the basic resource availability in the system;
        :param sys_res: the system resources dictionary;
        :param resource: the resource type to be considered (if present);
        :return: an X,Y pair expressing the per-step resource allocation efficiency;
        """
        usednodes = 0
        usedres = 0
        avlres = 0
        for k, node in sys_res.items():
            if resource is None:
                defaultavl = sum([r for r in base_res[k].values()])
                used = defaultavl - sum([r for r in node.values()])
                # Adding the node to the used set if there are used resources in it
                if used > 0:
                    usednodes += 1
                    usedres += used
                    avlres += defaultavl
            elif base_res[k][resource] > 0:
                defaultavl = base_res[k][resource]
                used = defaultavl - node[resource]
                if used > 0:
                    usednodes += 1
                    usedres += used
                    avlres += defaultavl

        if usednodes > 0:
            return usednodes / len(base_res.values()), usedres / avlres
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

    def boxPlot(self, data, title='', ylabel='', scale='linear', xlim=(None,None), ylim=(None,None), figsize=(7,5), meansonly=False, output='Output.pdf'):
        """
        Produces a box-and-whiskers plot for the input data's distributions.
        
        :param data: the input data; must be a list, in which each element is again a list containing all of the data
            regarding a certain test instance; the ordering must be that of the labels;
        :param title: the title of the plot;
        :param ylabel: the Y-axis label;
        :param scale: the scale of the plot;
        :param xlim: the left-right axis boundaries, is a tuple;
        :param ylim: the bottom-top axis boundaries, is a tuple;
        :param figsize: the size of the figure, is a tuple;
        :param meansonly: if True only the mean values for each distribution are depicted;
        :param output: the path to the output PDF file;
        """
        fontsize = 12
        plt.rc('xtick', labelsize=fontsize)
        plt.rc('ytick', labelsize=fontsize)
        N = len(data)

        spacing = 0.2
        ind = [i * spacing for i in np.arange(N)]
        width = 0.1
        markersize = 250
        linecol = 'black'
        tricol = 'black'
        vertlinecol = 'gray'
        rectcol = 'blue'

        fig, ax = plt.subplots(figsize=figsize)

        for i,d in enumerate(data):
            mydata = self._getDistributionStats(d)
            if not meansonly:
                ax.add_patch(patches.Rectangle((ind[i], mydata.get('quartiles')[1]), width,mydata.get('quartiles')[3] - mydata.get('quartiles')[1], facecolor=rectcol, alpha=0.75))
                ax.plot([ind[i] + width/2, ind[i] + width/2], [mydata.get('min'), mydata.get('max')], color=vertlinecol, linestyle='-', linewidth=2, zorder=1)
                ax.scatter(ind[i] + width/2, mydata.get('max'), marker='_', s=markersize, zorder=2, color=linecol)
                ax.scatter(ind[i] + width/2, mydata.get('min'), marker='_', s=markersize, zorder=2, color=linecol)
                ax.scatter(ind[i] + width/2, mydata.get('median'), marker='_', s=markersize, zorder=2, color=linecol)
                ax.scatter(ind[i] + width/2, mydata.get('avg'), marker='^', s=markersize/4, zorder=2, color=tricol)
            else:
                # If meansonly is True, only a path is drawn for the mean values.
                ax.add_patch(patches.Rectangle((ind[i], 0), width,mydata.get('avg'), facecolor=rectcol, alpha=0.75))
                ax.scatter(ind[i] + width / 2, mydata.get('avg'), marker='_', s=markersize / 4, zorder=0, color=linecol)

        # add some text for labels, title and axes ticks
        ax.set_ylabel(ylabel, fontsize=fontsize)
        ax.set_xlabel('Dispatching method', fontsize=fontsize)
        ax.set_title(title)
        ax.set_xticks([i + width / 2 for i in ind])
        ax.set_xticklabels(self._labels)
        ax.set_yscale(scale)
        ax.set_ylim(top=ylim[1], bottom=ylim[0], emit=True, auto=False)
        ax.set_xlim(left=xlim[0], right=xlim[1], emit=True, auto=False)

        plt.grid(linestyle=':', color='gray', zorder=0)
        plt.show()

        ff = PdfPages(output)
        ff.savefig(fig)
        ff.close()

    def boxPlotTimes(self, dataman, datasched, title='', scale='linear', xlim=(None,None), ylim=(None,None), figsize=(7,5), legend=True, output='Output.pdf'):
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
                ax.add_patch(patches.Rectangle((ind[i], 0), width, avgman, facecolor='orange', edgecolor='black', hatch='//', alpha=0.75, label='Simulation'))
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

    def boxPlotMemory(self, data, title='', scale='linear', xlim=(None,None), ylim=(None,None), figsize=(7,5), legend=True, output='Output.pdf'):
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
            if i==0:
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

    def scalabilityPlot(self, xdata, ydata, title='', scale='linear', xlim=(None,None), ylim=(None,None), figsize=(7,5), legend=True, output='Output.pdf'):
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
        :param output: the path of the output file;
        """

        fontsize = 12
        plt.rc('xtick', labelsize=fontsize)
        plt.rc('ytick', labelsize=fontsize)

        linestyles = ('-', '-', '--', '--', '-.', '-.', ':', ':')
        markers = (None, 'o', None, '^', None, 's', None, 'p')
        numstyles = len(linestyles)

        fig, ax = plt.subplots(figsize=figsize)

        for i in range(len(xdata)):
            markeroffset = floor(max(xdata[i])/20 + i*2)
            ax.plot(xdata[i],ydata[i],label=self._labels[i], linestyle=linestyles[i % numstyles], marker=markers[i % numstyles], markevery=markeroffset, zorder=2 if markers[i % numstyles] is None else 0)

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

    def distributionScatterPlot(self, xdata, ydata, alpha=0.005, title='', scale='linear', xlim=(0,1.05), ylim=(0,1.05), figsize=(7,5), output='Output.pdf'):
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
            splitoutput = _splitext(output)
            ff = PdfPages(splitoutput[0] + '-' + self._labels[i] + '.pdf')
            ff.savefig(fig)
            ff.close()

    def getPreprocessedBenchmarkData(self):
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

    def getPreprocessedScheduleData(self):
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

if __name__=='__main__':
    # This is an example. It should not be executed here, but in a script in the project's root, where also
    # basic_example.py is, so that all imports can be resolved correctly.
    resultpath = ['Path/to/benchmark/file',
                  'Path/to/benchmark/file2']
    resultlabel = ['Label',
                   'Label2']
    plots = plot_factory(type='benchmark')
    plots.setFiles(resultpath,resultlabel)
    plots.preProcess()
    plots.producePlot(type='scalability', title='My Scalability Plot')
