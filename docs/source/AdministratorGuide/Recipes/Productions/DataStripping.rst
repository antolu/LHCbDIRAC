=========
Stripping
=========

.. _strippingDMChecks:

*********************************
DM checks at the end of Stripping
*********************************

The procedure is very similar to the Merging production: :ref:`mergingDMChecks`


***************
Troubleshooting
***************

files Assigned with no jobs.
============================

The first action to take is always to check the descendants. Then several cases may arise.

Jobs killed, data upload failing
--------------------------------

This can be seen from the log files.
In that case, we clean whatever the old job managed to produce, and we re-strip the input files

::

    [lxplus079] ~ $ grep InFCNotInBK CheckDescendantsResults_70268.txt | dirac-dms-check-fc2bkk --FixFC
    Got 16 LFNs
    Checking replicas for 16 files : found 16 files with replicas and 0 without in 0.2 seconds
    Getting 16 files metadata from BK : completed in 0.1 seconds
    >>>>
    16 files are in the FC but have replica = NO in BK
    ====== Now checking 16 files from FC to SE ======
    Checking replicas for 16 files : found 16 files with replicas and 0 without in 5.2 seconds
    Get FC metadata for 16 files to be checked:  : completed in 0.1 seconds
    Check existence and compare checksum file by file...
    Getting checksum of 16 replicas in 2 SEs
    0. At CERN-BUFFER (13 files) : completed in 2.1 seconds
    1. At PIC-BUFFER (3 files) : completed in 1.3 seconds
    Verifying checksum of 16 files
    No files in FC not in BK -> OK!
    No missing replicas at sites -> OK!
    No replicas have a bad checksum -> OK!
    All files exist and have a correct checksum -> OK!
    ====== Completed, 16 files are in the FC and SE but have replica = NO in BK ======
    0 files are visible, 16 files are invisible
    /lhcb/LHCb/Collision16/BHADRON.MDST/00070268/0006/00070268_00061489_1.Bhadron.mdst : Visi N
    /lhcb/LHCb/Collision16/BHADRON.MDST/00070268/0006/00070268_00063156_1.Bhadron.mdst : Visi N
    /lhcb/LHCb/Collision16/BHADRONCOMPLETEEVENT.DST/00070268/0006/00070268_00063156_1.BhadronCompleteEvent.dst : Visi N
    /lhcb/LHCb/Collision16/CALIBRATION.DST/00070268/0006/00070268_00060532_1.Calibration.dst : Visi N
    /lhcb/LHCb/Collision16/CALIBRATION.DST/00070268/0006/00070268_00063183_1.Calibration.dst : Visi N
    /lhcb/LHCb/Collision16/CHARM.MDST/00070268/0006/00070268_00060532_1.Charm.mdst : Visi N
    /lhcb/LHCb/Collision16/CHARM.MDST/00070268/0006/00070268_00063156_1.Charm.mdst : Visi N
    /lhcb/LHCb/Collision16/CHARM.MDST/00070268/0006/00070268_00063183_1.Charm.mdst : Visi N
    /lhcb/LHCb/Collision16/CHARMCOMPLETEEVENT.DST/00070268/0006/00070268_00063156_1.CharmCompleteEvent.dst : Visi N
    /lhcb/LHCb/Collision16/CHARMCOMPLETEEVENT.DST/00070268/0006/00070268_00063183_1.CharmCompleteEvent.dst : Visi N
    /lhcb/LHCb/Collision16/EW.DST/00070268/0001/00070268_00016205_1.EW.dst : Visi N
    /lhcb/LHCb/Collision16/EW.DST/00070268/0006/00070268_00061489_1.EW.dst : Visi N
    /lhcb/LHCb/Collision16/EW.DST/00070268/0006/00070268_00063156_1.EW.dst : Visi N
    /lhcb/LHCb/Collision16/FTAG.DST/00070268/0001/00070268_00016205_1.FTAG.dst : Visi N
    /lhcb/LHCb/Collision16/LEPTONIC.MDST/00070268/0001/00070268_00016205_1.Leptonic.mdst : Visi N
    /lhcb/LHCb/Collision16/LEPTONIC.MDST/00070268/0006/00070268_00063156_1.Leptonic.mdst : Visi N
    Full list of files:    grep InFCButBKNo CheckFC2BK-1.txt
    Going to fix them, by removing from the FC and storage
    Removing 16 files : completed in 22.1 seconds
    Successfully removed 16 files
    <<<<
    No files in FC not in BK -> OK!



RDST files should be reset Unused automatically by the DRA:
NotProcWithDesc /lhcb/LHCb/Collision16/RDST/00051872/0000/00051872_00004324_1.rdst
NotProcWithDesc /lhcb/LHCb/Collision16/RDST/00051872/0007/00051872_00073248_1.rdst
NotProcWithDesc /lhcb/LHCb/Collision16/RDST/00051872/0007/00051872_00075929_1.rdst
NotProcWithDesc /lhcb/LHCb/Collision16/RDST/00051872/0013/00051872_00134216_1.rdst
NotProcWithDesc /lhcb/LHCb/Collision16/RDST/00051872/0013/00051872_00138382_1.rdst
