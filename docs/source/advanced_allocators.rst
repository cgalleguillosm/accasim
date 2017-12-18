Advanced allocators
====================

In this page, some advanced allocators implemented using AccaSim are shown. :ref:`Balanced`, :ref:`Weighted` and :ref:`Hybrid`. 

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

An allocator which tries to perform optimization on the allocation of the queued jobs. 
Each job is allocated on the node on which after the allocation the minimum weighted resources will be obtained. 
The weights quantify the level of criticality of a certain resource type using three parameters: 
the average amount requested, load ratio and total capacity.

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
