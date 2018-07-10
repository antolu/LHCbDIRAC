===========
ReStripping
===========

***********
Pre-Staging
***********

We do the prestaging of the RDST and the RAW files before a re-stripping campaign. Both files are staged on the BUFFER storage of the run destination. A rough estimation of the last exercise is that we can stage about 100TB a day.

The staging of these two can be launched with a single command:

::

  dirac-dms-add-transformation --Start --BK=<BKPATH> --Runs=<RUNRANGES> --Plugin=ReplicateWithAncestors --DQFlags=OK,UNCHECKED --Dest=Tier1-Buffer


For example



::

  dirac-dms-add-transformation --Start --BK=/LHCb/Collision17//RealData/Reco17//RDST --Runs=191782:193500 --Plugin=ReplicateWithAncestors --DQFlags=OK,UNCHECKED --Dest=Tier1-Buffer


In practice, the transformation plugin only knows about the RDST, and then adds the ancestors to the tasks.

Important parameters
--------------------

A few operation parameters can be used to tune the staging. They are in `Operations/<setup>/TransformationPlugins/ReplicateWithAncestors`

TargetFilesAtDestination
^^^^^^^^^^^^^^^^^^^^^^^^

This number sets an upper limit to the number of files allowed in total per **directory**. We look at the LFNs in the transformation, and take the directory of level 4 (in that case `/lhcb/LHCb/Collision17/RDST`).

In order to know how many files are on an SE, we look into the StorageUsageDB for that specific directory.

Each SE has a share which is the TargetFilesAtDestination divided by the RAWShare of this site. The number of files in the SE should not be greater than this Share.

Since the StorageUsageDB is refreshed only once a day, we add the number of files Processed and Assigned in the last 12 hours to get a better estimate.

Example of logs in the TransformationAgent:

::

  (V) [NoThread] [-9999] Get storage usage for directories /lhcb/LHCb/Collision16/RDST
  (V) [NoThread] [-9999] Current storage usage per SE:
  (V) [NoThread] [-9999]     CERN-BUFFER: 2
  (V) [NoThread] [-9999]     CNAF-BUFFER: 41702
  (V) [NoThread] [-9999]      RAL-BUFFER: 29
  (V) [NoThread] [-9999]    RRCKI-BUFFER: 16
  (V) [NoThread] [-9999] Shares per SE for 400000 files:
  (V) [NoThread] [-9999]     CERN-BUFFER: 79888.0
  (V) [NoThread] [-9999]     CNAF-BUFFER: 57600.0
  (V) [NoThread] [-9999]   GRIDKA-BUFFER: 52920.0
  (V) [NoThread] [-9999]    IN2P3-BUFFER: 37080.0
  (V) [NoThread] [-9999]      PIC-BUFFER: 19840.0
  (V) [NoThread] [-9999]      RAL-BUFFER: 93152.0
  (V) [NoThread] [-9999]    RRCKI-BUFFER: 26400.0
  (V) [NoThread] [-9999]     SARA-BUFFER: 33120.0
  (V) [NoThread] [-9999] Number of files Assigned or recently Processed (12 hours):
  (V) [NoThread] [-9999]     CERN-BUFFER: 0
  (V) [NoThread] [-9999]     CNAF-BUFFER: 15897
  (V) [NoThread] [-9999]   GRIDKA-BUFFER: 0
  (V) [NoThread] [-9999]    IN2P3-BUFFER: 0
  (V) [NoThread] [-9999]      PIC-BUFFER: 0
  (V) [NoThread] [-9999]      RAL-BUFFER: 0
  (V) [NoThread] [-9999]    RRCKI-BUFFER: 0
  (V) [NoThread] [-9999]     SARA-BUFFER: 0
  [NoThread] [-9999] Maximum number of files per SE:
  [NoThread] [-9999]     CERN-BUFFER: 79886
  [NoThread] [-9999]     CNAF-BUFFER: 0
  [NoThread] [-9999]   GRIDKA-BUFFER: 52919
  [NoThread] [-9999]    IN2P3-BUFFER: 37080
  [NoThread] [-9999]      PIC-BUFFER: 19840
  [NoThread] [-9999]      RAL-BUFFER: 93122
  [NoThread] [-9999]    RRCKI-BUFFER: 26383
  [NoThread] [-9999]     SARA-BUFFER: 33120


MinFreeSpace
^^^^^^^^^^^^

This is a water mark, in TB, and per SE. The information is taken from <CacheFeederAgent>
Note that it is not very smart: if when we check, we are below the watermark, we do not create tasks. if we are above, we create them all, even if we will be well bellow after ! It is just a safeguard

*************
Input Removal
*************

Once a given RDST has been processed, it can be removed from BUFFER, as well as the associated raw file. This can be done with a single transformation:

::

  dirac-dms-add-transformation --Plugin=RemoveReplicasWithAncestors --FromSE=Tier1-Buffer --BK=<BKPATH> --ProcessingPass=PROCESSINGPASS> --DQFlags=OK,UNCHECKED --Runs=<RUNRANGES> --Start

For example

::

  dirac-dms-add-transformation --Plugin=RemoveReplicasWithAncestors --FromSE=Tier1-Buffer --BK=/LHCb/Collision17/Beam6500GeV-VeloClosed-MagDown/RealData/Reco17//RDST --ProcessingPass=Stripping29r2 --DQFlags=OK,UNCHECKED --Runs=199386:200000 --Start

A few important points:

- the BKPath must contain the condition because that his what is used to find the production in which we check the descendants
- the processing pass is the output of the production
- Although not strictly needed, it is good practice to have one removal transformation per production, with the same run ranges.
- the production manager should extend the run ranges of both the production and the removal transformation

******************
Output Replication
******************

In the current computing model, the output is replicated on an archive and at a second DST storage. This is done using the LHCbDSTBroadcast plugin

::

  dirac-dms-add-transformation --Plugin=LHCbDSTBroadcast --BK=<BKPATH> --Except <USELESSSTREAM> --Start

For example

::

  dirac-dms-add-transformation --Plugin=LHCbDSTBroadcast --BK=/LHCb/Collision15//RealData/Reco15a/Stripping24r1//ALL.DST,ALL.MDST --Except CALIBRATION.DST --Start


Typical useless streams are normally `CALIBRATION.DST` and `MDST.DST`


*****************
Productions check
*****************

The productions need to be checked for consistency and from the Datamanagement point of view.

For the DataManagement, please see :ref:`strippingDMChecks` and :ref:`mergingDMChecks`.

Also, some files might need to be cleaned manually because they were flagged bad during the production, see :ref:`dmCleanBadFiles`.
