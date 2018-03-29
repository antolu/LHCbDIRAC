===========
Productions
===========

These advices can be applied to all sort of productions.

********
Hospital
********

When?
=====

When all files have been processed except some that cannot make it due to either excessive CPU or memory (e.g. at IN2P3 jobs stalled)

How?
====

In the CS: /Operations/LHCb-Production/Hospital set 2 options:
HospitalSite: for example CLOUD.CERN.cern (or any other site without strict limitations)
Transformations: list of productions to be processed at the hospital queue

Then:
=====

reset the files Unused. They will be brokered to the designated hospital site, wherever the input data is.


Example:
========

.. code-block::

    [localhost] ~ $ dirac-transformation-debug 71500 --Status MaxReset --Info jobs
     Transformation 71500 (Active) of type DataStripping (plugin ByRunWithFlush, GroupSize: 1) in Real Data/Reco17/Stripping29r2
    BKQuery: {'StartRun': 199386L, 'ConfigName': 'LHCb', 'EndRun': 200350L, 'EventType': 90000000L, 'FileType': 'RDST', 'ProcessingPass': 'Real Data/Reco17', 'Visible': 'Yes', 'DataQualityFlag': ['OK', 'UNCHECKED'], 'ConfigVersion':
    'Collision17', 'DataTakingConditions': 'Beam6500GeV-VeloClosed-MagDown'}

    2 files found with status ['MaxReset']


     1 LFNs: ['/lhcb/LHCb/Collision17/RDST/00066581/0004/00066581_00043347_1.rdst'] : Status of corresponding 5 jobs (sorted):
    Jobs: 200059983, 200171070, 200415532, 201337455, 201397489
    Sites (CPU): LCG.IN2P3.fr (3609 s), LCG.IN2P3.fr (3635 s), LCG.IN2P3.fr (3626 s), LCG.IN2P3.fr (3617 s), LCG.IN2P3.fr (3649 s)
      5 jobs terminated with status: Failed; Job stalled: pilot not running; DaVinci step 1
    	  1 jobs stalled with last line: (LCG.IN2P3.fr) EventSelector     SUCCESS Reading Event record 3001. Record number within stream 1: 3001
    	  1 jobs stalled with last line: (LCG.IN2P3.fr) EventSelector     SUCCESS Reading Event record 4001. Record number within stream 1: 4001
    	  2 jobs stalled with last line: (LCG.IN2P3.fr) EventSelector     SUCCESS Reading Event record 5001. Record number within stream 1: 5001
    	  1 jobs stalled with last line: (LCG.IN2P3.fr) EventSelector     SUCCESS Reading Event record 3001. Record number within stream 1: 3001

     1 LFNs: ['/lhcb/LHCb/Collision17/RDST/00066581/0007/00066581_00074330_1.rdst'] : Status of corresponding 7 jobs (sorted):
    Jobs: 200339756, 200636702, 200762354, 200913317, 200945856, 201337457, 201397490
    Sites (CPU): LCG.IN2P3.fr (32 s), LCG.IN2P3.fr (44 s), LCG.IN2P3.fr (46 s), LCG.IN2P3.fr (45 s), LCG.IN2P3.fr (44 s), LCG.IN2P3.fr (3362 s), LCG.IN2P3.fr (38 s)
      7 jobs terminated with status: Failed; Job stalled: pilot not running; DaVinci step 1
    	  1 jobs stalled with last line: (LCG.IN2P3.fr) dirac-jobexec INFO: DIRAC JobID 200339756 is running at site LCG.IN2P3.fr
    	  1 jobs stalled with last line: (LCG.IN2P3.fr) dirac-jobexec/Subprocess VERBOSE: systemCall: ['lb-run', '--use-grid', '-c', 'best', '--use=AppConfig v3r [...]
    	  1 jobs stalled with last line: (LCG.IN2P3.fr) dirac-jobexec/Subprocess VERBOSE: systemCall: ['lb-run', '--use-grid', '-c', 'best', '--use=AppConfig v3r [...]
    	  1 jobs stalled with last line: (LCG.IN2P3.fr) dirac-jobexec/Subprocess VERBOSE: systemCall: ['lb-run', '--use-grid', '-c', 'best', '--use=AppConfig v3r [...]
    	  1 jobs stalled with last line: (LCG.IN2P3.fr) dirac-jobexec/Subprocess VERBOSE: systemCall: ['lb-run', '--use-grid', '-c', 'best', '--use=AppConfig v3r [...]
    	  1 jobs stalled with last line: (LCG.IN2P3.fr) EventSelector     SUCCESS Reading Event record 2001. Record number within stream 1: 2001
    	  1 jobs stalled with last line: (LCG.IN2P3.fr) dirac-jobexec/Subprocess VERBOSE: systemCall: ['lb-run', '--use-grid', '-c', 'best', '--use=AppConfig v3r [...]

So 2 files have to be hospitalised. Reset them Unused:

.. code-block::

    [localhost] ~ $ dirac-transformation-reset-files 71500 --Status MaxReset
    2 files were set Unused in transformation 71500
