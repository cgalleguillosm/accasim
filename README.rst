AccaSim: a Workload Management Simulator for Job Dispatching Research
=====================================================================

*current version:* |version|

AccaSim is a Workload management [sim]ulator for [H]PC systems, useful for developing dispatchers and conducting controlled experiments in HPC dispatching research. It is scalable and highly customizable, allowing to carry out large experiments across different workload sources, resource settings, and dispatching methods.

AccaSim enables users to design novel advanced dispatchers by exploiting information regarding the current system status, which can be extended for including custom behaviors such as power consumption and failures of the resources. The researchers can use AccaSim for instance to mimic any real system by setting up the synthetic resources suitably, develop advanced power-aware, fault-resilient dispatching methods, and test them over a wide range of workloads by generating them synthetically or using real workload traces from HPC users. 

For more information please visit the `webpage of AccaSim <http://accasim.readthedocs.io/en/latest/>`_

***************
What's new?
***************
- 23-06-2018 Major improvements. Additional data is executed during Submission, Dispatching and Completion events (before and after).
- 15-06-2018 Version 1.0 released.
- 01-06-2018 New version of the dispatchers. The job requests are verified during the scheduling process.
- 27-05-2018 New version of the resources and resource manager. All simulation methods were improved. Logger is used instead of printing messages.
- 21-05-2018 Asyncronous file writing to reduce the IO overhead of the simulations.
- 06-12-2017 A workload generator is available for generating new workloads from existing workloads.
- 13-11-2017 Simulating distinct dispatchers under the same system and simulator configuration can be managed under the Experimentation class. It also includes the automatic plot generation.
- 21-08-2017 Automatic plot generation for comparison of multiple workload schedules and benchmark files: Slowdown, Queue sizes, Load ratio, Efficiency, Scalability, Simulation time, Memory Usage.
- 19-07-2017 Documentation is moved to `http://accasim.readthedocs.io <http://accasim.readthedocs.io/en/latest/>`_ 
- 12-07-2017 First version of the package.
 