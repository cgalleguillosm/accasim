Advanced allocators
====================

In this page, some advanced allocators implemented using AccaSim are shown. :ref:`Balanced`, :ref:`Weighted` and :ref:`Hybrid` has been used in [NettiISC18]_. 

.. _Balanced:

Balanced
--------

An allocator which considers a set of critical and scarce resource types (like GPUs or MICs), and tries to balance 
the allocation to the respective nodes in order to avoid fragmentation and waste.
    
The algorithm will collect the nodes having different critical resource types in distinct sets. It will then build 
a list of nodes to be used in the allocation process: the nodes having no critical resource types are placed in 
front, followed by the nodes having critical resources; these nodes are interleaved, in order to balance their usage
and avoid favoring a specific resource type.
The resource types to be balanced can be given as input. If a node has more than one type of critical resource
available, it will be considered for the type for which it has the greatest availability. 

.. container:: toggle

    .. container:: header

        **Show/Hide Code**

    .. literalinclude:: ../../extra/allocators/allocator_balanced.py
		:caption: Balanced
		:language: python
		:linenos:
		:lines: 24-

:download:`Download source code <../../extra/allocators/allocator_balanced.py>`

.. _Weighted:

Weighted
--------

An allocator which tries to perform optimization on the allocation of all events in the queue.

When the user supplies the queue of events to be allocated, the average number of resources required per-type
is computed, together with the current load rate. The average is weighted according to the expected length of
each job. These values are used as weights to indicate the value of a resource. Also, the allocator computes 
the resource weights considering also the base availability in the system, assigning higher values to scarcer 
resources, to assess which are actually more precious and need to be preserved in a global manner. Each job is 
then allocated on the nodes on which the minimum number of weighted resources is left (after the allocation), 
in a similar fashion to the best-fit allocator. 

The user can also supply a "critical resources" list: for such resources, an additional heuristic is used, and in
this case the allocator goes under the name of Priority-Weighted; this consists in a priority value, per resource 
type, which will increase as allocations fail for jobs that require critical resources, and will decrease for each 
successful allocation. If a jobs requires multiple critical resource types, all of their priority values will be
affected by the allocation. This tends to assign greater weight to critical resources actually requested by jobs, 
and will make the allocator statistically preserve such resources.

.. container:: toggle

    .. container:: header

        **Show/Hide Code**

    .. literalinclude:: ../../extra/allocators/allocator_weighted.py
		:caption: Balanced
		:language: python
		:linenos:
		:lines: 24-

:download:`Download source code <../../extra/allocators/allocator_weighted.py>`

.. _Hybrid:

Hybrid
------

This allocator is a combination of the Weighted and Balanced allocators.

Intuitively, it will separate and interleave the nodes according to the critical resources they possess like in
the Balanced allocator; however, each of those lists is sorted individually like in the Weighted allocator.

.. container:: toggle

    .. container:: header

        **Show/Hide Code**

    .. literalinclude:: ../../extra/allocators/allocator_hybrid.py
		:caption: Balanced
		:language: python
		:linenos:
		:lines: 24-

:download:`Download source code <../../extra/allocators/allocator_hybrid.py>`


Citations
---------

.. [NettiISC18] Netti et al. *Intelligent Resource Allocation for Heterogeneous HPC Systems* Submitted to ISC High Performance 2018.