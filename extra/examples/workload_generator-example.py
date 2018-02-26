from accasim.experimentation.workload_generator import workload_generator
    
if __name__ == '__main__':
    #===========================================================================
    # Workload filepath
    #===========================================================================
    workload = 'workload.swf'
    
    #==========================================================================
    # System config filepath
    #==========================================================================
    sys_config = 'config.config'
    
    #===========================================================================
    # Performance of the computing units
    #===========================================================================
    performance = { 'core': 3.334 / 2 }
    
    #===========================================================================
    # Request limits for each resource type
    #===========================================================================
    request_limits = {'min':{'core': 1, 'mem': 1000000 // 4}, 'max': {'core': 4, 'mem': 1000000}}
    
    #===========================================================================
    # Create the workload generator instance with the basic inputs
    #===========================================================================
    generator = workload_generator(workload, sys_config, performance, request_limits)
    #===========================================================================
    # Generate n jobs and save them to the nw filepath
    #===========================================================================
    n = 100
    nw_filepath = 'new_workload.swf'
    jobs = generator.generate_jobs(n, nw_filepath)
    
