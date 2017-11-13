========
Examples
========

Basic Implementation
====================

The starting point for launching a simulation is to instantiate the 
*hpc_simulator* class. It must receive as arguments at least a workload description 
-- such as a file path in SWF format, a system configuration file path in JSON format, 
and a dispatcher instance, with which the synthetic system is generated and loaded with all the default features. 

A minimal simulator instantiation is presented below. 

.. literalinclude:: ../../basic_example.py
	:language: python
	:linenos:
	:lines: 24-
	:caption: Minimal implementation
	
The workload descrition is a public workload trace collected from the `Seth cluster <https://www.hpc2n.umu.se/resources/hardware/seth>`_ 
belonging the High Performance Computing Center North (HPC2N) of the Swedish National Infrastructure for Computing. The workload trace file includes 200,735 jobs spanning 
through 4 years, from July 2002 to January 2006, and is available `on-line <http://www.cs.huji.ac.il/labs/parallel/workload/l\_hpc2n/index.html>`_ in the SWF format. 

Seth  was built in 2001 and is already retired by now. It ranked 59th in Top500 list\footnote{https://www.top500.org/}, the world's 500 fastest computers. It was composed of
120 nodes, each node with two AMD Athlon MP2000+ dual core processors with 1.667 GHz and 1 GB of RAM. Because multiple jobs can co-exist on the same node, we consider a better 
representation of the system,  made of cores instead of processors. Besides, it is required to define the start time of the workload, because the first job belongs to 0. 
To perform the change between processors and cores, the member equivalence is defined. It contains the actual name, the new one and the equivalence for a single unit. 
Therefore, we define the synthetic system in the configuration file with 120 nodes each with 4 cores and 1 GB of RAM, as depicted below:

.. literalinclude:: ../../config/HPC2N.config
	:language: json
	:linenos:
	:caption: Content of sys_config 


The dispatcher instance is composed by  implementations of the abstract *scheduler_base* and *allocator_base* classes. In this illustrative 
instantiation of the *hpc_simulator* class, *fifo_sched* implements *scheduler_base* using FIFO, whereas *ffp_alloc* implements *allocator_base* 
using  FFP, and both  *fifo_sched* and  *ffp_alloc* classes are available in the library for importing, as done in lines 2-3 of the example. 
AccaSim can be customized in its dispatching method by implementing the abstract *scheduler_base* and *allocator_base* classes as desired. 