==============
DataManagement
==============

A bit of cleaning
=================

From StorageUsagePlot
---------------------

From the StorageUsage plots, we can see that their are sometimes files left on BUFFER after a production has been finished.

To find them

.. code-block::

    [lxplus021] ~ $ dirac-dms-find-lfns --Path /lhcb/data/2015/RAW/SMOGPHY/LHCb/LEAD15/ --SE Tier1-Buffer | dirac-dms-replica-stats
    Got 3 LFNs
    [...]

Very often, it is because the run has been flagged BAD. This can be checked as follows:

.. code-block::

  dirac-bookkeeping-file-path  --GroupBy dataqualityflag --Summary <LFNS>



We make sure they were not processed before removing them

.. code-block::

    [lxplus021] ~ $ dirac-bookkeeping-get-file-descendants --Last
    Got 3 LFNs
    Getting descendants for 3 files (depth 1) : completed in 0.3 seconds
    NotProcessed :
        /lhcb/data/2015/RAW/SMOGPHY/LHCb/LEAD15/169028/169028_0000000546.raw
        /lhcb/data/2015/RAW/SMOGPHY/LHCb/LEAD15/169030/169030_0000000354.raw
        /lhcb/data/2015/RAW/SMOGPHY/LHCb/LEAD15/169034/169034_0000000323.raw
    [lxplus021] ~ $ dirac-dms-remove-replicas --Last --SE Tier1-Buffer
    Got 3 LFNs
    Removing replicas : completed in 8.6 seconds
    Successfully removed 3 replicas from IN2P3-BUFFER


Files unused in productions
---------------------------

If a run is flagged BAD during the processing, some files may have been added to a production, but then never get used. A very similar process can be done. In case of stripping, we can also go up to the parent files to remove the RA files from buffer:

.. code-block::

    [localhost] ~ $ dirac-transformation-debug 71500 --Status Unused --Info files | dirac-bookkeeping-get-file-ancestors | dirac-dms-replica-stats
    Getting ancestors for 41 files (depth 1) : completed in 12.0 seconds
    Got 108 LFNs
    Getting replicas for 108 LFNs : completed in 5.8 seconds
    108 files found with replicas

    Replica statistics:
      0 archive replicas: 108 files
    ---------------------
      0  other  replicas: 0 files
      1  other  replicas: 41 files
      2  other  replicas: 67 files
    ---------------------

    SE statistics:
            CERN-RAW: 67 files
            CNAF-RAW: 67 files
           CNAF-RDST: 41 files

    Sites statistics:
       LCG.CERN.cern: 67 files
         LCG.CNAF.it: 108 files
    [localhost] ~ $ dirac-bookkeeping-file-path --Last --GroupBy dataqualityflag --Summary
    Got 108 LFNs
    Successful :
        DataqualityFlag BAD : 108 files
    [localhost] ~ $ dirac-transformation-reset-files --New Removed --Last 71500
    Got 108 LFNs
    41 files were set Removed in transformation 71500

Files problematic in productions
--------------------------------

When a file goes problematic in a production, it can be removed from buffer. If it is for the stripping, also its raw ancestor can be removed. Example for a Stripping (here, all the prods for a given Stripping)

.. code-block::


    [localhost] ~ $ dirac-transformation-debug 69077,69073,68675,68486,69079,69075,68773,68771 --Status Problematic --Info files | dirac-dms-replica-stats
    Got 28 LFNs
    Getting replicas for 28 LFNs : completed in 4.7 seconds
    28 files found with replicas

    Replica statistics:
      0 archive replicas: 28 files
    ---------------------
      0  other  replicas: 0 files
      1  other  replicas: 0 files
      2  other  replicas: 28 files
    ---------------------
    [...]


    [localhost] ~ $ dirac-bookkeeping-get-file-ancestors --Last | dirac-dms-remove-replicas --SE Tier1-Buffer
    Getting ancestors for 28 files (depth 1) : completed in 6.5 seconds
    Got 56 LFNs
    Removing replicas : completed in 228.5 seconds
    Successfully removed 12 replicas from CERN-BUFFER
    Successfully removed 2 replicas from SARA-BUFFER
    Successfully removed 6 replicas from RRCKI-BUFFER
    Successfully removed 4 replicas from GRIDKA-BUFFER
    Successfully removed 8 replicas from IN2P3-BUFFER
    Successfully removed 24 replicas from RAL-BUFFER


We can then set these files as Removed in the removal transformation (setting them Done would not be very clean...)

.. code-block::

    [localhost] ~ $ dirac-transformation-reset-files --NewStatus Removed --Last 69128,69127,68831,68829
    Got 56 LFNs
    6 files were set Removed in transformation 69128
    7 files were set Removed in transformation 69127
    8 files were set Removed in transformation 68831
    7 files were set Removed in transformation 68829
