Dispatchers
===========

In this page, the dispatchers implemented using AccaSim are shown. 

As our knowledge, the simulator has been used for experimentation in [GalleguillosMOD17]_. In this paper, several dispatching methods were used, most of them are available in AccaSim library:

	* FIFO (:class:`.fifo_sched`) + FFP (:class:`.ffp_alloc`).
	* SJF (:class:`.sjf_sched`) + FFP (:class:`.ffp_alloc`).
	* LJF (:class:`.ljf_sched`) + FFP (:class:`.ffp_alloc`).
	* EASY Backfilling (:class:`.easybf_sched`) + FFP (:class:`.ffp_alloc`).
	* :ref:`PRB` + FFP (:class:`.consolidate_alloc`).
	* :ref:`CPH` + FFP (:class:`.consolidate_alloc`).

.. _PRB:

Priority Rules Based (PRB)
--------------------------

PRB scheduling method makes use of the Estimated Waiting Time (EWT) for working, this method and the EWT were introduced in [BorghesiCLMB15]_. 

.. container:: toggle

    .. container:: header

        **Show/Hide Code**

    .. literalinclude:: ../../extra/dispatching_methods/prb_scheduler.py
		:caption: PRB
		:language: python
		:linenos:
		:lines: 24-

:download:`Download source code <../../extra/dispatching_methods/prb_scheduler.py>`

.. _CPH:
				
Constraint Programing  + Heuristic (CPH)
----------------------------------------

CPH scheduling method makes use of the Estimated Waiting Time (EWT) for working, this method and the EWT were introduced in [BorghesiCLMB15]_. 
This scheduler uses `OR-Tools library <https://developers.google.com/optimization/>`_. 

.. container:: toggle

    .. container:: header

        **Show/Hide Code**

    .. literalinclude:: ../../extra/dispatching_methods/cph_scheduler.py
		:caption: CPH
		:language: python
		:linenos:
		:lines: 24-

:download:`Download source code <../../extra/dispatching_methods/cph_scheduler.py>`.

Citations
---------
.. [BorghesiCLMB15] Andrea Borghesi, Francesca Collina, Michele Lombardi, Michela Milano, Luca Benini. *Power Capping in High Performance Computing Systems* in Proc. of CP 2015.
.. [GalleguillosMOD17] Cristian Galleguillos, Alina Sirbu, Zeynep Kiziltan, Ozalp Babaoglu, Andrea Borghesi, Thomas Bridi. *Data-Driven Job Dispatching* in Proc. of MOD 2017.