Popularity scanning
===================

This file is produced by the PopularityAgent and stored on its work directory.

To produce the `popularity.csv` file, the scanning follows this algorithm:
 * list all the directories of the DFC
 * Convert each visible directory into a BK dictionary, getting the information from the StorageUsage (if it is cached), or from the BK itself
 * For each directory:

   - Get the number of PFNs and size per SE from the StorageUsageDB
   - Get the day by day usage of the directories and group them by week from the DataUsage (which is the StorageUsageDB..)

 * Sum the size and the number of files per:

   - directory
   - storage type (Archive, tape, disk)
   - StorageElement
   - Site
 * Assigns the dataset to a given storage type (see bellow)


.. _popularityCSV:

popularity.csv file
===================

This file is the output of all the processing chain. The fields are the following:

 * Name: full Bookkeeping path (e.g `/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/RealData/Reco14/Stripping20r1/90000000/SEMILEPTONIC.DST`)
 * Configuration: Configuration part, so DataType + Activity (`/LHCb/Collision11`)
 * ProcessingPass: guess... (`/RealData/Reco14/Stripping20r1`)
 * FileType: guess again (`SEMILEPTONIC.DST`)
 * Type: a number depending on the type of data:

   - 0: MC
   - 1: Real Data
   - 2: Dev simulation
   - 3: upgrade simulation
 * Creation-week: "week number" when this file was created (see bellow for details) (e.g. `104680`)
 * NbLFN: number of LFNs in the dataset
 * LFNSize: size of the dataset in TB
 * NbDisk: number of replicas on disk. Careful: if all LFNs have two replicas, you will have `NbDisk=2*NbLFN`.
 * DiskSize: effective size on disk of the dataset in TB (also related to the number of replicas)
 * NbTape: number of replicas on tape, which is not archive
 * TapeSize: effective size on tape of the dataset in TB.
 * NbArchived: number of replicas on Archive storage.
 * ArchivedSize: effective size on Archive storage in TB
 * CERN, CNAF,.. (all T1 sites): disk space used at the various sites.
 * NbReplicas: average number of replicas on disk (`NbDisk/LFN`)
 * NbArchReps: average nymber of replicas on Archive (`NbArchived/LFN`)
 * Storage: one of the following:

   - Active: If the production creating this file is either idle, completed or active
   - Archived: if there are on disk copies, only archive
   - Tape: if dataset is on RAW or RDST
   - Disk: otherwise
 * FirstUsage: first time the dataset was used (n "week number")
 * LastUsage: last time the dataset was used (in "week number")
 * Now: current week number
 * 1, 2, etc: number of access since `k` weeks ago. Note that these numbers are cumulative, that means that what was accessed 1 week ago is also counted in what was included 2 weeks ago.

***********
Week number
***********

This allows to have an easy way to compare the age of datasets. It is defined as `year * 52 + week number`