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
from os.path import join, devnull
from accasim.base.allocator_class import AllocatorBase
from accasim.base.scheduler_class import SchedulerBase 
from accasim.base.simulator_class import Simulator
from accasim.utils.file import file_exists, dir_exists, remove_dir, find_file_by
from accasim.utils.misc import obj_assertion, list_class_assertion
from accasim.utils.plot_factory import PlotFactory
from accasim.experimentation.schedule_parser import define_result_parser
import subprocess
from string import Template
from inspect import isclass
from enum import Enum
import sys
from time import sleep


_TEMPLATE = """
import sys
import os

from accasim.base.simulator_class import Simulator
$imports
        
def define_dispatcher():
    sched_class = $sched_name
    alloc_class = $alloc_name
    kwargs = $kwargs
    if alloc_class:
        return sched_class(alloc_class(), **kwargs)
    return sched_class(**kwargs)
        
if __name__ == '__main__':
    workload = '$workload'
    sys_config = '$sys_config'
    dispatcher = define_dispatcher()
    
    sim = Simulator(workload, sys_config, dispatcher, **$sim_kwargs)
    sim.start_simulation(**$run_kwargs)
"""           

class Experiment:
    
    CUSTOM_ATTRIBUTES = {'SEPARATOR': '_SEPARATOR', 'RESULTS_FOLDER': '_RESULTS_FOLDER',
                         'SCHEDULE_PREFIX': '_SCHEDULE_PREFIX', 'BENCHMARK_PREFIX': '_BENCHMARK_PREFIX'}

    SIMULATOR_ATTRIBUTES = {'scheduling_output': True, 'pprint_output': False, 'benchmark_output': True,
                            'statistics_output': True, 'job_factory': None, 'reader': None, 'additional_data': [],
                            'save_parameters': ['RESOURCE_ORDER'], 'timeout': None, 'id': None, 'skip':False, 'LOG_LEVEL': 'INFO',
                            'EXTENDED_JOB_DESCRIPTION': False}
    RUN_SIMULATOR_ATTRIBUTES = {'system_status': False}
    
    _SEPARATOR = '_'
    _RESULTS_FOLDER = 'results/{}/{}'
    _SCHEDULE_PREFIX = 'sched-'
    _BENCHMARK_PREFIX = 'bench-'
    _SIMULATOR_PARAMS_FILENAME = 'simulator_parameters.json'
    _PLOT_INPUTS = {
        PlotFactory.SCHEDULE_CLASS: _SCHEDULE_PREFIX,
        PlotFactory.BENCHMARK_CLASS: _BENCHMARK_PREFIX
    }

    def __init__(self, name, workload, sys_config, simulator_config=None, job_factory=None,
                 reader=None, **kwargs):
        """
        Experiment class constructor.

        This class allows to run automatically a set of simulations, and then generate automatically the plots. For the plot generation,
        scheduling_output and benchmark_output must be True (by default).

        :param name: Experiment name. The results of each simulator will be placed in the results/\'name\' folder.
        :param workload: Workload file to be used in the simulation. Directly readed or used as workload generation input. This depends on the reader object.
        :param sys_config: System configuration in json format.
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
            - timeout: Timeout in secs.
        """
        self.name = name
        self.dispatchers = {}
        self.results = {}
        self.sys_config = file_exists(sys_config, head_message='System configuration file: ')
        self.workload = file_exists(workload, head_message='Workload file: ')
        if simulator_config is not None:
            self.simulator_config = file_exists(simulator_config, head_message='Simulator configuration file: ')
        else:
            self.simulator_config = None

        self.parser = define_result_parser(self.simulator_config)
        self._customize(**kwargs)
        self._update_simulator_attrs(**kwargs)
        self._update_run_simulator_attrs(**kwargs)

    def add_dispatcher(self, name, dispatcher):
        """
        Add a dispatcher to the set of dispatchers.

        The dispatcher must be supplied in the form of a fully instanced SchedulerBase object, which includes an
        allocator as well.

        :param name: Dispatcher name.
        :param dispatcher: (scheduler class, allocator class, kwargs). Allocator class and kwargs can be both None

        """
        obj_assertion(name, str, 'Received {} type as dispatcher name. str type expected.',
                      [dispatcher.__class__.__name__])
        if name in self.dispatchers:
            print('Dispatcher {} already set. Skipping it'.format(name))
        self.dispatchers[name] = dispatcher

    def generate_dispatchers(self, scheduler_list, allocator_list, **kwargs):
        """
        Generate a set of dispatchers from a combination of scheduler and allocation lists.

        The input is given as a list of class names for schedulers and allocators: the method will then automatically
        generate all objects corresponding to all possible combinations of the supplied schedulers and allocators.

        :param scheduler_list: List of schedulers (:class:`accasim.base.scheduler_class.SchedulerBase`).
        :param allocator_list: List of allocators (:class:`accasim.base.allocator_class.AllocatorBase`).

        """
        list_class_assertion(scheduler_list, SchedulerBase,
                             error_msg='Scheduler objects must belong only to {} subclass',
                             msg_args=[SchedulerBase.__name__])
        list_class_assertion(allocator_list, AllocatorBase,
                             error_msg='Allocator objects must belong only to {} subclass',
                             msg_args=[AllocatorBase.__name__])
        for _alloc_class in allocator_list:
            _alloc = _alloc_class()
            for _sched_class in scheduler_list:
                _name = self._generate_name(_sched_class.name, _alloc_class.name)
                self.add_dispatcher(_name, (_sched_class, _alloc_class, kwargs,))


    def _run_simulation(self, name, dispatcher, create_script):
        """
        Runs a single simulation instance, with a specified dispatcher

        :param dispatcher: A dispatcher instantiation
        """
        if create_script:
            imports = ''
            import_vars = [(dispatcher[0].__module__, dispatcher[0].__name__,), (dispatcher[1].__module__, dispatcher[1].__name__,)] + \
                    [ (v.__module__, v.__name__,) for k, v  in dispatcher[2].items() if isclass(v)]
            for vars in import_vars:
                imports += 'from {} import {}\n'.format(*vars)
                               
            template = Template(_TEMPLATE)
            script = template.substitute(
                {
                    'workload': self.workload,
                    'sys_config': self.sys_config,
                    'imports': imports,
                    'sched_name': dispatcher[0].__name__,
                    'alloc_name': dispatcher[1].__name__,
                    'kwargs': {kw: v.value if isinstance(v, Enum) else v for kw, v in dispatcher[2].items()},
                    'sim_kwargs': self.SIMULATOR_ATTRIBUTES,
                    'run_kwargs': self.RUN_SIMULATOR_ATTRIBUTES
                }
            )
            
            fp = '{}_{}.py'.format(self.name, name)
            with open(fp, 'w+') as f:
                f.write(script)
    
            executable = sys.executable
            if not executable:
                executable = 'python3'
    
            _cmd = '{},-u,{}'.format(executable, fp)
            cwd = '.'
            FNULL = open(devnull, 'w')
            p_sim = subprocess.Popen(_cmd.split(','), cwd=cwd, stderr=subprocess.STDOUT)
            p_sim.wait()
        else:
            sched_class = dispatcher[0]
            alloc_class = dispatcher[1]
            kwargs = dispatcher[2]
            _dispatcher = sched_class(alloc_class(), **kwargs) if alloc_class else sched_class(**kwargs)
            simulator = Simulator(self.workload, self.sys_config, _dispatcher,
            simulator_config=self.simulator_config, **self.SIMULATOR_ATTRIBUTES)
            simulator.start_simulation(**self.RUN_SIMULATOR_ATTRIBUTES)
        
    def run_simulation(self, generate_plot=False, generate_scripts=False, wait=10):
        """
        Starts the simulation process. Its uses each instance of dispatching method to create the experiment.
        After that all experiments are run, the comparing plots are generated the :attr:`.generate_plot` option is set as True.

        :param generate_plot: `True` if plots must be generated, otherwise `False`. 

        """
        _total = len(self.dispatchers)
        # Run n simulations depending in the number of the defined dispatchers
        for i, (_name, _dispatcher) in enumerate(self.dispatchers.items()):
            result_folder = self.create_folders(self.name, _name)
            self.results[_name] = result_folder
            self.SIMULATOR_ATTRIBUTES['id'] = '{}_{}'.format(self.name, _name)
            self.SIMULATOR_ATTRIBUTES['RESULTS_FOLDER_NAME'] = result_folder
            print('{}/{}: Starting simulation of {}'.format(i + 1, _total, _name))
            self._run_simulation(_name, _dispatcher, generate_scripts)
            print('\n')
            sleep(wait)
        
        # Generate all plots available in the plot factory class
        if generate_plot:
            self.generate_plots(self._RESULTS_FOLDER.format(self.name, ''))
            print('Plot generation finished. ')

    def create_folders(self, name, instance_name, overwrite=True):
        """
        Create a folder if does not exists. If :attr:`.overwrite` is True, the existing folder will be deleted and then created.
        
        :param name: Corresponds to the experiment name.
        :param instance_name: Name of the current simulation. By default is the name of the dispatcher.
        :param overwrite: `True` to overwrite current results on the destiniy by deleting the entire folder.

        :return: Return the path of the new folder. 
        """
        results_folder = self._RESULTS_FOLDER.format(name, instance_name)
        if overwrite:
            remove_dir(results_folder)
        dir_exists(results_folder, create=True)
        self.SIMULATOR_ATTRIBUTES['OVERWRITE'] = overwrite
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
            filepaths.append(join(_path, find_file_by(_path, **kwargs)))
        return labels, filepaths

    def generate_plots(self, experiment_folder):
        """
        Generate the all plots available in the :class:`accasim.utils.PlotFactory` class.

        All plots are generated using the default parameters for each plot type. If users wish to produce plots with
        custom features and attributes, a PlotFactory object with its produce_plot method must be explicitly used,
        pointing at the result files produced by the simulations.
        
        :param experiment_folder: Path where the plots will be placed.
        """
        for plot_class, plot_types in PlotFactory.PLOT_TYPES.items():
            _prefix = self._PLOT_INPUTS[plot_class]
            resultlabel, resultpath = self.retrieve_filepaths(self.results, prefix=_prefix)
            _plot_factory = PlotFactory(plot_class, self._SIMULATOR_PARAMS_FILENAME, config=self.sys_config,
                                         workload_parser=self.parser)
            _plot_factory.set_files(resultpath, resultlabel)
            _plot_factory.pre_process()
            for plot_type in plot_types:
                output_fpath_ = join(experiment_folder, '{}.pdf'.format(plot_type))
                _plot_factory.produce_plot(type=plot_type, output=output_fpath_)
                
    def _generate_name(self, _sched_name, _alloc_name):
        """
        Generate a name of the dispatcher using the scheduler and allocator names plus the :attr:`._SEPARATOR`

        :param _sched_name: Scheduler name identifier.
        :param _alloc_name: Allocation name identifier.
        :return: Dispatching name identifier.
        """
        return '{}{}{}'.format(_sched_name, self._SEPARATOR, _alloc_name)

    def _customize(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.CUSTOM_ATTRIBUTES:
                setattr(self, self.CUSTOM_ATTRIBUTES[k], v)

    def _update_simulator_attrs(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.SIMULATOR_ATTRIBUTES:
                self.SIMULATOR_ATTRIBUTES[k] = v
                
    def _update_run_simulator_attrs(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.RUN_SIMULATOR_ATTRIBUTES:
                self.RUN_SIMULATOR_ATTRIBUTES[k] = v
