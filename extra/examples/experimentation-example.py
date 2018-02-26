from accasim.base.scheduler_class import fifo_sched, sjf_sched, ljf_sched, easybf_sched
from accasim.base.allocator_class import ff_alloc, bf_alloc
from accasim.experimentation.experiment import experiment_class as experiment


if __name__ == '__main__':
    #===========================================================================
    # The name of the experiment will be used by the experiment class 
    # to create a folder and later to put every output file of the simulator on it.
    # For each scheduling and allocation combination a sub-folder will be created. 
    #===========================================================================
    experiment_name = 'experiment-name'
    
    #===========================================================================
    # Workload under testing
    #===========================================================================
    workload = 'workloads/system-workload.swf'
    
    #===========================================================================
    # Configuration file of the sysmtem under study, the configuration must 
    # hold all the jobs requests. 
    #===========================================================================
    sys_config = 'config/system.config'
    
    #===========================================================================
    # Essentials files are specific settings of the simulator, such as 
    # dispatching plan's format, pprint format, visualization port, etc.
    # If an essential file isn't provided, the simulator will take the default values. 
    #===========================================================================
    essentials = 'config/essentials.config'

    #===========================================================================
    # For a minimal experiment class instantiation are required the experiment name,
    # the workload filepath and also the system configuration filpath. In this example
    # some optional arguments are also given: simulator_config, separator and timeout.
    #===========================================================================
    experimentation = experiment(experiment_name, workload, sys_config, essentials, SEPARATOR='#', timeout=3600)
    
    #===========================================================================
    # The lists of scheduler and allocator classes name are defined. 
    #===========================================================================
    sched_list = [fifo_sched, sjf_sched, ljf_sched, easybf_sched]
    alloc_list = [ff_alloc, bf_alloc]

    #===========================================================================
    # All possible combinations between scheduler and allocator are generated
    # and added to the experiment class using the following method.
    #===========================================================================
    experimentation.generate_dispatchers(sched_list, alloc_list)
    
    #===========================================================================
    # Finally, the simulation is started. By default the plots are automatically
    # generated, but it can be changed by passing a False flag to the run_simulation
    # method.
    #===========================================================================
    experimentation.run_simulation(generate_plot=False)
