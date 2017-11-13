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
from builtins import str
from os.path import join as _join
from accasim.base.allocator_class import allocator_base
from accasim.base.scheduler_class import scheduler_base
from accasim.base.simulator_class import hpc_simulator
from accasim.utils.file import file_exists, dir_exists, remove_dir, find_file_by
from accasim.utils.misc import obj_assertion, list_class_assertion, load_config, type_regexp
from accasim.utils.plot_factory import plot_factory
from accasim.experimentation import schedule_parser


class experiment_class:
    CUSTOM_ATTRIBUTES = {'SEPARATOR': '_SEPARATOR', 'RESULTS_FOLDER': '_RESULTS_FOLDER',
                         'SCHEDULE_PREFIX': '_SCHEDULE_PREFIX', 'BENCHMARK_PREFIX': '_BENCHMARK_PREFIX'}

    SIMULATOR_ATTRIBUTES = {'scheduling_output': True, 'pprint_output': False, 'benchmark_output': True,
                            'statistics_output': True, 'job_factory': None, 'reader': None,
                            'save_parameters': ['resource_order']}
    _SEPARATOR = '_'
    _RESULTS_FOLDER = 'results/{}/{}'
    _SCHEDULE_PREFIX = 'sched-'
    _BENCHMARK_PREFIX = 'bench-'
    _SIMULATOR_PARAMS_FILENAME = 'simulator_parameters.json'
    _PLOT_INPUTS = {
        plot_factory.SCHEDULE_CLASS: _SCHEDULE_PREFIX,
        plot_factory.BENCHMARK_CLASS: _BENCHMARK_PREFIX
    }

    def __init__(self, name, _sys_config, _workload, simulator_config, job_factory=None,
                 reader=None, **kwargs):
        """
        Experiment class constructor.

        This class allows to run automatically a set of simulations, and then generate automatically the plots. For the plot generation,
        scheduling_output and benchmark_output must be True (by default).

        :param name: Experiment name. The results of each simulator will be placed in the results/\'name\' folder.
        :param _sys_config: System configuration in json format.
        :param _workload: Workload file to be used in the simulation. Directly readed or used as workload generation input. This depends on the reader object.
        :param simulator_config: Configuration of the simulator parameters.
        :param job_factory: Optional. A job factory object.
        :param reader: Optional. A workload reader object for a custom reader. A SWF workload reader is defined as default.
        :param kwargs: \*\*kwargs: Optional parameters to be set in the experiment definition.
            - SEPARATOR: '_' by default. This separator is used to separate the name of the Dispatcher (schedule+allocation).
            - RESULTS_FOLDER: 'results/{}/{}' by default. Results folder where the experiments will placed. This name must contain the parent result folder, and
            two place holders, the first one belongs to the experiment name and the second one to the specific simulation.
            - SCHEDULE_PREFIX: 'sched-' by default. Corresponds the prefix name of the schedule files.
            - BENCHMAR_PREFIX: 'bench-' by default. Corresponds the prefix name of the benchmark files.
            - scheduling_output: True by default.
            - pprint_output: False by default.
            - benchmark_output: True by default.
            - statistics_output: True by default.
            - save_parameters: ['resource_order']. A list of simulator parameters to be saved as simulator_parameters.json in results/{name}/{dispatching_instance}.
        """
        self.name = name
        self.dispatchers = {}
        self.results = {}
        self.sys_config = file_exists(_sys_config, head_message='System configuration file: ')
        self.workload = file_exists(_workload, head_message='Workload file: ')
        self.simulator_config = file_exists(simulator_config, head_message='Simulator configuration file: ')

        self.parser = self._define_result_parser()
        self._customize(**kwargs)
        self._update_simulator_attrs(**kwargs)

    def add_dispatcher(self, name, dispatcher):
        """
        Add a dispatcher to the set of dispatchers.

        :param name: Dispatcher name.
        :param dispatcher: Dispatcher instantiation.

        """
        obj_assertion(name, str, 'Received {} type as dispatcher name. str type expected.',
                      [dispatcher.__class__.__name__])
        obj_assertion(dispatcher, scheduler_base, 'Received {} type as dispatcher. {} subclass expected.',
                      [dispatcher.__class__.__name__, scheduler_base.__name__])
        if name in self.dispatchers:
            print('Dispatcher {} already set. Skipping it'.format(name))
        self.dispatchers[name] = dispatcher

    def add_dispatchers(self, scheduler_list, allocator_list):
        """
        Generate a set of dispatchers from a combination of scheduler and allocation lists.

        :param scheduler_list: List of schedulers (:class:`accasim.base.scheduler_class.scheduler_base`).
        :param allocator_list: List of allocators (:class:`accasim.base.allocator_class.allocator_base`).

        """
        list_class_assertion(scheduler_list, scheduler_base,
                             error_msg='Scheduler objects must belong only to {} subclass',
                             msg_args=[scheduler_base.__name__])
        list_class_assertion(allocator_list, allocator_base,
                             error_msg='Allocator objects must belong only to {} subclass',
                             msg_args=[allocator_base.__name__])

        for _alloc_class in allocator_list:
            _alloc = _alloc_class()
            for _sched_class in scheduler_list:
                _dispatcher = _sched_class(_alloc)
                _name = self._generate_name(_dispatcher.name, _alloc.name)
                self.add_dispatcher(_name, _dispatcher)

    def run_simulation(self, generate_plot=True):
        """
        Starts the simulation process. It's uses each instance of dispatching method to create the experiment.
        After that all experiments are run, the comparing plots are generated the :attr:`.generate_plot` option is set as True.

        :param generate_plot: `True` if plots must be generated, otherwise `False`. 

        """
        _total = len(self.dispatchers)
        # Run n simulations depending in the number of the defined dispatchers
        for i, (_name, _dispatcher) in enumerate(self.dispatchers.items()):
            result_folder = self.create_folders(self.name, _name)
            self.results[_name] = result_folder
            self.SIMULATOR_ATTRIBUTES['RESULTS_FOLDER_NAME'] = result_folder
            print('{}/{}: Starting simulation of {}'.format(i + 1, _total, _name))
            simulator = hpc_simulator(self.sys_config, _dispatcher, self.workload,
                                      simulator_config=self.simulator_config,
                                      **self.SIMULATOR_ATTRIBUTES)
            simulator.start_simulation()
            print('\n')
        
        # Generate all plots available in the plot factory class
        if generate_plot:
            self.generate_plots(self._RESULTS_FOLDER.format(self.name, ''))
            print('Plot generation finished. ')

    def create_folders(self, name, instance_name, OVERWRITE=True):
        """
        Create a folder if does not exists. If :attr:`.OVERWRITE` is True, the existing folder will be deleted and then created.        
        
        :param name: Corresponds to the experiment name.
        :param instance_name: Name of the current simulation. By default is the name of the dispatcher.
        :param OVERWRITE: `True` to overwrite current results on the destiniy by deleting the entire folder.

        :return: Return the path of the new folder. 
        """
        results_folder = self._RESULTS_FOLDER.format(name, instance_name)
        if OVERWRITE:
            remove_dir(results_folder)
        dir_exists(results_folder, create=True)
        self.SIMULATOR_ATTRIBUTES['OVERWRITE'] = OVERWRITE
        return results_folder

    def retrieve_filepaths(self, dispatcher_results, **kwargs):
        """
        Retrieve all filepath related to the simulation results. Associates that files to the experiments.

        :param dispatcher_results: Simulation results path.
        :param kwargs: Corresponds to the type of prefix of the file. 
            Currently, there are two types of plots. Benchmark with 'bench-' and Schedule with 'sched-' file prefix.
        :return: list of experiments, list of file paths 
        """
        labels = []
        filepaths = []
        for _name, _path in dispatcher_results.items():
            labels.append(_name)
            filepaths.append(_join(_path, find_file_by(_path, **kwargs)))
        return labels, filepaths

    def generate_plots(self, experiment_folder):
        """
        Generate the all plots available in the :class:`accasim.utils.plot_factory` class. 
        
        :param experiment_folder: Path where the plots will be placed.
        """
        for plot_class, plot_types in plot_factory.PLOT_TYPES.items():
            _prefix = self._PLOT_INPUTS[plot_class]
            resultlabel, resultpath = self.retrieve_filepaths(self.results, prefix=_prefix)
            _plot_factory = plot_factory(plot_class, self._SIMULATOR_PARAMS_FILENAME, config=self.sys_config,
                                         workload_parser=self.parser)
            _plot_factory.setFiles(resultpath, resultlabel)
            _plot_factory.preProcess()
            for plot_type in plot_types:
                output_fpath_ = _join(experiment_folder, '{}.pdf'.format(plot_type))
                _plot_factory.producePlot(type=plot_type, output=output_fpath_)
                
    def _generate_name(self, _sched_name, _alloc_name):
        """
        Generate a name of the dispatcher using the scheduler and allocator names plus the :attr:`._SEPARATOR`

        :param _sched_name: Scheduler name identifier.
        :param _alloc_name: Allocation name identifier.
        :return: Dispatching name identifier.
        """
        return '{}{}{}'.format(_sched_name, self._SEPARATOR, _alloc_name)

    def _define_result_parser(self):
        """

        :return:
        """
        try:
            _schedule_output = load_config(self.simulator_config)['schedule_output']
            _separators = _schedule_output['separators']
            _format = _schedule_output['format']
            _attributes = _schedule_output['attributes']
        except KeyError as e:
            print(
                'Schedule output format not identified. Please check the simulator configuration file for the key \'schedule_output\'.')

        for _attr_name, _data_type in _attributes.items():
            _format = _format.replace('{' + _attr_name + '}', type_regexp(_data_type[-1]).format(_attr_name))
        return schedule_parser(_format)

    def _customize(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.CUSTOM_ATTRIBUTES:
                setattr(self, self.CUSTOM_ATTRIBUTES[k], v)

    def _update_simulator_attrs(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.SIMULATOR_ATTRIBUTES:
                self.SIMULATOR_ATTRIBUTES[k] = v
                
    