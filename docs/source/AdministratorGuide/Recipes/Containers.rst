==========
Containers
==========

LHCb applications are ran in a containerised environment by passing the ``--allow-containers`` argument to ``lb-run``.
Before activating this feature the ``GaudiExecution/lbRunOptions`` configuration option under ``Operations`` must be set to include this argument, e.g. ``--siteroot=/cvmfs/lhcb.cern.ch/lib/ --allow-containers``.

Containerised LHCb applications can then be enabled or disabled at three levels of granularity.
In order of preference these are:
* For a single compute element by setting ``/Resources/Sites/${SITE_TYPE}/${SITE}/CEs/${COMPUTE_ELEMENT}/AllowContainers`` to ``yes``.
* For an entire site by setting ``/Resources/Sites/${SITE_TYPE}/${SITE}/AllowContainers`` to ``yes``.
* Globally by setting the ``Operations`` option ``GaudiExecution/AllowContainers`` to ``yes``.

Currently ``LbPlatformUtils`` only supports `Singularity <https://www.sylabs.io/guides/latest/user-guide/>`_ however future releases may allow the ``AllowContainers`` option to be used to set which container technologies can be used.
