=======
Merging
=======

****************************
jobs failing during finalize
****************************

Problem:
--------


If a Merge job fails during finalisation, its input files may not be removed... In addition its output files may be incorrectly uploaded or registered
Therefore starting from the left non-merged files one may find anomalies and fix them. This requiers getting invisible files in the DataStripping productions and checking their descendants in the Merge production

Examples:
---------

Get the descendants of the DataStripping production (here 69528) that still have replicas, and check their descendants in the Merging production (here 69529)

.. code-block::

    [localhost] ~ $ dirac-bookkeeping-get-files --Production 69528 --Visi No | dirac-production-check-descendants 69529
    Got 59 LFNs
    Processing Merge production 69529
    Looking for descendants of type ['EW.DST', 'BHADRON.MDST', 'SEMILEPTONIC.DST', 'DIMUON.DST', 'CALIBRATION.DST', 'FTAG.DST', 'CHARMCOMPLETEEVENT.DST', 'BHADRONCOMPLETEEVENT.DST', 'CHARM.MDST', 'LEPTONIC.MDST']
    Getting files from the TransformationSystem...
    Found 59 processed files and 0 non processed files (1.2 seconds)
    Now getting daughters for 59 processed mothers in production 69529 (depth 1) : completed in 5.9 seconds
    Checking replicas for 2 files : found 2 files with replicas and 0 without in 0.4 seconds
    Checking FC for 2 file found in FC and not in BK |                                                  |Checking replicas for 2 files (not in Failover) : found 0 files with replicas and 0 without in 0.5 seconds
    : found 2 in Failover in 0.5 seconds

    Results:
    2 descendants were found in Failover and have no replica flag in BK
    All files:
    /lhcb/LHCb/Collision16/DIMUON.DST/00069529/0001/00069529_00012853_1.dimuon.dst
    /lhcb/LHCb/Collision16/BHADRONCOMPLETEEVENT.DST/00069529/0001/00069529_00012813_1.bhadroncompleteevent.dst
    You should check whether they are in a failover request by looking at their job status and in the RMS...
    To list them:     grep InFailover CheckDescendantsResults_69529-1.txt
    2 unique daughters found with real descendants
    No processed LFNs with multiple descendants found -> OK!
    No processed LFNs without descendants found -> OK!
    No non processed LFNs with multiple descendants found -> OK!
    No non processed LFNs with descendants found -> OK!
    Complete list of files is in CheckDescendantsResults_69529-1.txt
    Processed production 69529 in 9.4 seconds


After checking at the RMS whether they have matching Requests, and if so what happened to it, we can replicate them to final destination and then remove from Failover

.. code-block::

    [localhost] ~ $ grep InFailover CheckDescendantsResults_69529-1.txt | dirac-dms-replicate-to-run-destination --SE Tier1-DST
    Got 2 LFNs
    Replicating 2 files to CERN-DST-EOS
    Successful :
        CERN-DST-EOS :
            /lhcb/LHCb/Collision16/BHADRONCOMPLETEEVENT.DST/00069529/0001/00069529_00012813_1.bhadroncompleteevent.dst :
                 register : 0.757441997528
                replicate : 655.287761927
            /lhcb/LHCb/Collision16/DIMUON.DST/00069529/0001/00069529_00012853_1.dimuon.dst :
                 register : 0.632274866104
                replicate : 46.3780457973
    [localhost] ~ $ dirac-dms-remove-replicas --Last --SE Tier1-Failover
    Got 2 LFNs
    Removing replicas : completed in 7.4 seconds
    Successfully removed 2 replicas from SARA-FAILOVER


Finally, Check again and remove non-merged files

.. code-block::

    [localhost] ~ $ dirac-dms-remove-files --Last
    Got 59 LFNs
    Removing 59 files : completed in 103.1 seconds
    59 files in status Processed in transformation 69529: status unchanged
    Successfully removed 59 files


*************
Flushing runs
*************

When a file is problematic in the Stripping production, the run cannot be flushed automatically ( Number of ancestors != number of RAW in the run).
We list the runs in the Stripping productions (here 71498) that have problematic files, and we flush them in the Merging (here 71499)


.. code-block::

    [localhost] ~ $ dirac-transformation-debug 71498 --Status Problematic --Info files | dirac-bookkeeping-file-path --GroupBy RunNumber --Summary
    --List
    Got 29 LFNs
    Successful :
        RunNumber 201413 : 1 files
        RunNumber 201423 : 1 files
        RunNumber 201467 : 1 files
        RunNumber 201602 : 1 files
        RunNumber 201643 : 1 files
        RunNumber 201647 : 1 files
        RunNumber 201664 : 1 files
        RunNumber 201719 : 1 files
        RunNumber 201745 : 2 files
        RunNumber 201749 : 1 files
        RunNumber 201822 : 1 files
        RunNumber 201833 : 1 files
        RunNumber 201864 : 1 files
        RunNumber 201873 : 1 files
        RunNumber 201983 : 1 files
        RunNumber 202031 : 1 files
        RunNumber 202717 : 1 files
        RunNumber 202722 : 1 files
        RunNumber 202752 : 1 files
        RunNumber 202773 : 1 files
        RunNumber 202809 : 1 files
        RunNumber 202825 : 1 files
        RunNumber 202835 : 2 files
        RunNumber 202860 : 1 files
        RunNumber 202869 : 1 files
        RunNumber 202873 : 1 files
        RunNumber 202887 : 1 files

    List of RunNumber values
    201413,201423,201467,201602,201643,201647,201664,201719,201745,201749,201822,201833,201864,201873,201983,202031,202717,202722,2027
    52,202773,202809,202825,202835,202860,202869,202873,202887

Then flush the runs in the merging production

.. code-block::

    [localhost] ~ $ dirac-transformation-flush-runs 71499 --Runs
    201413,201423,201467,201602,201643,201647,201664,201719,201745,201749,201822,201833,201864,201873,201983,202031,202717,202722,2027
    52,202773,202809,202825,202835,202860,202869,202873,202887
    Runs being flushed in transformation 71499:
    201413,201423,201467,201602,201643,201647,201664,201719,201745,201749,201822,201833,201864,201873,201983,202031,202717,202722,2027
    52,202773,202809,202825,202835,202860,202869,202873,202887
    27 runs set to Flush in transformation 71499
