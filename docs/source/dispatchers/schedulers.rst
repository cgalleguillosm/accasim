Schedulers from literature
==========================

Priority Rules Based (PRB)
--------------------------

This implementation of PRB uses the Estimated Waiting Time (EWT) to stablish job priorities. EWT was introduced in [BorghesiCLMB15]_. 
This scheduler was used in [BorghesiCLMB15]_ and [GalleguillosMOD17]_. 

.. container:: toggle

    .. container:: header

        **Show/Hide Code**

    .. literalinclude:: ../../../extra/dispatching_methods/prb_scheduler.py
		:caption: PRB
		:language: python
		:linenos:
		:lines: 24-

:download:`Download source code <../../../extra/dispatching_methods/prb_scheduler.py>`

Hybrid Constraint Programming (HCP)
-----------------------------------

The HCP scheduler, used in [BorghesiCLMB15]_ and [GalleguillosMOD17]_, uses of the Estimated Waiting Time (EWT) to give priority to queued jobs. This scheduler uses 
`OR-Tools library <https://developers.google.com/optimization/>`_ to model and solve the scheduling problem.

.. container:: toggle

    .. container:: header

        **Show/Hide Code**

    .. literalinclude:: ../../../extra/dispatching_methods/cph_scheduler.py
		:caption: CPH
		:language: python
		:linenos:
		:lines: 24-

:download:`Download source code <../../../extra/dispatching_methods/cph_scheduler.py>`.


.. rubric:: Citations

.. [BorghesiCLMB15] Andrea Borghesi, Francesca Collina, Michele Lombardi, Michela Milano, Luca Benini. *Power Capping in High Performance Computing Systems* in Proc. of CP 2015.
.. [GalleguillosMOD17] Cristian Galleguillos, Alina Sirbu, Zeynep Kiziltan, Ozalp Babaoglu, Andrea Borghesi, Thomas Bridi. *Data-Driven Job Dispatching* in Proc. of MOD 2017.

:doc:`Return <index>`