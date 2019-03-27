
Components
==========

The popularity analysis relies on a lot of components

**************
StorageUsageDB
**************

Despite its name, that is where both the StorageUsageAgent and the PopularityAgent stores their data. It is exposed via the StorageUsageHandler and the DataUsageHandler

*****************
StorageUsageAgent
*****************

This agent scans the DFC and stores the size and number of files per directory and per StorageElement in the StorageUsageDB.

*******************
StorageHistoryAgent
*******************

This agent crawls the StorageUsageDB, convert each directory into a bookkeeping path and fill in the following accounting:
 * Storage: space used/free per storage and/or directory
 * Data storage: spaced used per bookkeeping path
 * user storage: like Storage, but for user directories


****************
DataUsageHandler
****************

This service is called by the jobs to declare their use of a given directory. It is stored per directory and per day.


***************
PopularityAgent
***************

This agent goes through the StorageUsageDB and creates accounting entries for the popularity. It also caches the BK dictionary for each directory in the StorageUSageDB.


**************
DataPop server
**************

Yandex provided service that consumes our popularity CSV and make prediction on which dataset to remove. It is ran on our mesos cluster: https://lbmesosms02.cern.ch/marathon/ui/#/apps/%2Fdatapopserv

***********************
PopularityAnalysisAgent
***********************

This agents creates two files:
 * one CSV containing a summary of the popularity (see :ref:`popularityCSV` ).
 * one CSV, generated from the first one through the DataPop server
