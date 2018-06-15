Batsim Experiments
==================
Before run the batsim_exec.py you will need to place the platforms and workload files into the respective folders
The following files must exist:
 	./platform/{instance}.xml
	./workloads/{instance}.json
Where instance coudl be seth, ricc or mc. 

This files can be generated based on the gist instructions (https://gist.github.com/mpoquet/56ec34c71fbe73bc8fb6ead959bdc2e9#file-prepare_instances-bash):

git clone https://gitlab.inria.fr/batsim/batsim.git batsim
cd batsim
git checkout v2.0.0

tools/energy_pform_generator_homo_nonet.py -n 240 -o platforms/seth.xml
tools/energy_pform_generator_homo_nonet.py -n 8192 -o platforms/ricc.xml
tools/energy_pform_generator_homo_nonet.py -n 8412 -o platforms/mc.xml

tools/swf_to_batsim_workload_delay.py ./workloads/HPC2N-2002-2.2-cln.swf ./workloads/seth.json -i1 -t -pf 240
tools/swf_to_batsim_workload_delay.py ./workloads/RICC-2010-2.swf ./workloads/ricc.json -i1 -t -pf 8192
tools/swf_to_batsim_workload_delay.py ./workloads/METACENTRUM-2013-2.swf ./workloads/mc.json -i1 -t -pf 8412


Install batsim (https://github.com/oar-team/batsim/blob/master/doc/run_batsim.md) 
Now, you can run the experiment using the batsim_exec.py. To calculate the performance metrics run the metrics_calculation.py script.


Alea Experiments
================

To run Alea experiments execute the alea_exec.py script with python 3.5 or newer. To calculate the performance metrics run the metrics_calculation.py script.

Accasim Experiments
===================

To run Accasim experiments execute the