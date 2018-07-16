=======
Merging
=======


.. _mergingDMChecks:

*******************************
DM checks at the end of Merging
*******************************

When the Merging productions for a given Stripping are over, there are a couple of situations that might arise:

1. Output file is registered in the FC but is not registered in the BKK
2. Output file is registered in the FC but does not have the replica flag = No
3. Output file is registered in the BKK but not in the FC

First case
----------

Nothing can be done, we do not have all the info, we should just remove the file.
If it is a debug file, it will be cleaned anyway when cleaning the DEBUG storage.

Second case
-----------

Either the file is at its final destination, and in that case the replica flag can just be toggled (`--FixBK` in dirac-dms-check-fc2bkk), or the file is in a failover. In the later case, it is enough to replicate the file to its run destination (dirac-dms-replicate-to-run-destination) and remove the replica on the failover storage.

Third case
----------

If the file physically exists, the file can just be registered in the FC.

Examples
--------

The 3rd case is noticeable only in the output replication transformation, because it will mark these files as MissingInFC.
For the first two cases, the best is to use `dirac-dms-check-fc2bkk`.

For example

::

    [LHCbDirac prod] diracos $ dirac-dms-check-fc2bkk --Prod 69080,69076,68774,68772
    Processing production 69080
    Getting files from 18 directories  : found 5348 files with replicas and 0 without in 13.9 seconds
    Getting 5348 files metadata from BK : completed in 1.8 seconds
    >>>>
    1 files are in the FC but have replica = NO in BK
    ====== Now checking 1 files from FC to SE ======
    Checking replicas for 1 files : found 1 files with replicas and 0 without in 1.1 seconds
    Get FC metadata for 1 files to be checked:  : completed in 0.1 seconds
    Check existence and compare checksum file by file...
    Getting checksum of 1 replicas in 1 SEs
    0. At RAL-DST (1 files) : completed in 1.7 seconds
    Verifying checksum of 1 files
    No files in FC not in BK -> OK!
    No missing replicas at sites -> OK!
    No replicas have a bad checksum -> OK!
    All files exist and have a correct checksum -> OK!
    ====== Completed, 1 files are in the FC and SE but have replica = NO in BK ======
    1 files are visible, 0 files are invisible
    /lhcb/LHCb/Collision15/BHADRONCOMPLETEEVENT.DST/00069080/0000/00069080_00003151_1.bhadroncompleteevent.dst :
    Visi Y
    Full list of files:    grep InFCButBKNo CheckFC2BK-2.txt
    Use --FixBK to fix it (set the replica flag) or --FixFC (for removing from FC and storage)
    <<<<
    No files in FC not in BK -> OK!
    Processed production 69080
    Processing production 69076
    Getting files from 18 directories  : found 5789 files with replicas and 0 without in 10.3 seconds
    Getting 5789 files metadata from BK : completed in 2.9 seconds
    No files in FC with replica = NO in BK -> OK!
    No files in FC not in BK -> OK!
    Processed production 69076
    Processing production 68774
    Getting files from 18 directories  : found 7510 files with replicas and 0 without in 12.7 seconds
    Getting 7510 files metadata from BK : completed in 2.8 seconds
    No files in FC with replica = NO in BK -> OK!
    No files in FC not in BK -> OK!
    Processed production 68774
    Processing production 68772
    Getting files from 18 directories  : found 10702 files with replicas and 0 without in 14.8 seconds
    Getting 10702 files metadata from BK : completed in 4.2 seconds
    No files in FC with replica = NO in BK -> OK!
    >>>>
    1 files are in the FC but are NOT in BK:
    /lhcb/debug/Collision15/LEPTONIC.MDST/00068772/0000/00068772_00003806_1.leptonic.mdst
    Full list of files:    grep InFCNotInBK CheckFC2BK-3.txt
    Use --FixFC to fix it (remove from FC and storage)
    <<<<




    [LHCbDirac prod] diracos $ dirac-dms-check-fc2bkk --
    LFN=/lhcb/LHCb/Collision15/BHADRONCOMPLETEEVENT.DST/00069080/0000/00069080_00003151_1.bhadroncompleteevent.dst
    --FixBK
    Checking replicas for 1 files : found 1 files with replicas and 0 without in 0.3 seconds
    Getting 1 files metadata from BK : completed in 0.0 seconds
    >>>>
    1 files are in the FC but have replica = NO in BK
    ====== Now checking 1 files from FC to SE ======
    Checking replicas for 1 files : found 1 files with replicas and 0 without in 4.8 seconds
    Get FC metadata for 1 files to be checked:  : completed in 0.4 seconds
    Check existence and compare checksum file by file...
    Getting checksum of 1 replicas in 1 SEs
    0. At RAL-DST (1 files) : completed in 1.0 seconds
    Verifying checksum of 1 files
    No files in FC not in BK -> OK!
    No missing replicas at sites -> OK!
    No replicas have a bad checksum -> OK!
    All files exist and have a correct checksum -> OK!
    ====== Completed, 1 files are in the FC and SE but have replica = NO in BK ======
    1 files are visible, 0 files are invisible
    /lhcb/LHCb/Collision15/BHADRONCOMPLETEEVENT.DST/00069080/0000/00069080_00003151_1.bhadroncompleteevent.dst :
    Visi Y
    Full list of files:    grep InFCButBKNo CheckFC2BK-4.txt
    Going to fix them, setting the replica flag
           Successfully added replica flag to 1 files
    <<<<
    No files in FC not in BK -> OK!

    [LHCbDirac prod] diracos $ dirac-dms-remove-files
    /lhcb/debug/Collision15/LEPTONIC.MDST/00068772/0000/00068772_00003806_1.leptonic.mdst
    Removing 1 files : completed in 8.1 seconds
    Successfully removed 1 files




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

::

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

::

    [localhost] ~ $ grep InFailover CheckDescendantsResults_69529-1.txt | dirac-dms-replicate-to-run-destination --RemoveSource --SE Tier1-DST
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


Finally, Check again and remove non-merged files

::

    [localhost] ~ $ dirac-dms-remove-files --Last
    Got 59 LFNs
    Removing 59 files : completed in 103.1 seconds
    59 files in status Processed in transformation 69529: status unchanged
    Successfully removed 59 files




.. _mergingFlush:

*************
Flushing runs
*************

When a file is problematic in the Stripping production, or if a RAW file was not processed in the Reco, the run cannot be flushed automatically ( Number of ancestors != number of RAW in the run).
We list the runs in the Stripping productions (here 71498) that have problematic files, and we flush them in the Merging (here 71499)


::

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

::

    [localhost] ~ $ dirac-transformation-flush-runs 71499 --Runs
    201413,201423,201467,201602,201643,201647,201664,201719,201745,201749,201822,201833,201864,201873,201983,202031,202717,202722,2027
    52,202773,202809,202825,202835,202860,202869,202873,202887
    Runs being flushed in transformation 71499:
    201413,201423,201467,201602,201643,201647,201664,201719,201745,201749,201822,201833,201864,201873,201983,202031,202717,202722,2027
    52,202773,202809,202825,202835,202860,202869,202873,202887
    27 runs set to Flush in transformation 71499


Then, starting from the runs that are not flushed in the Merging, we can check if some RAW files do not have descendant

::

   dirac-bookkeeping-run-files <runNumber> | grep FULL | dirac-bookkeeping-get-file-descendants

The files that are marked as NotProcessed or NoDescendants are in runs that will need to be flushed by hand

Another way of understanding why a run is not flushed is by using dirac-transformation-debug. But this takes a looooong while

::

   dirac-transformation-debug --Status=Unused --Info=flush <mergingProd>
