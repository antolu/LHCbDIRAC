-----------------
Package LHCbDIRAC
-----------------

Version v8r2
------------

NEW
:::

 WorkloadManagementSystem
  - WMSSecureGW service (for BOINC)

CHANGE
::::::

 ResourceStatusSystem
  - adapt SLS for the new dashboard
 BookkeepingSystem
  - Improve the bookkeeping CLI
  - Resolve the database tags (condDB, DDDB) from the input production.
  - Stepid also added to the runs!
  - chanhge the log level to debug for the command line
 ProductionManagementSystem
  - Compare the dictionaries instead of strings
  - dirac-production-shifter script added support for hot productions and several other minor features
 TransformationSystem
  - GRIDDownlaoder
 Resources
  - Added setReplicaProblematic to BookkeepingDBClient

BUGFIX
::::::

 WorkloadManagementSystem
  - re-wrote dirac-wms-get-wn script
 Workflow
  - Added StepID in the Bookkeeping XML report files
 BookkeepingSystem
  - 'Visible' to 'Visibility' flag in script dirac-bookeeping-get-files
  - Corrected bookkeeping-get-stats script
  - calculate the number fo events correctly
  - Do not use cartesian joins
 ConfigurationSystem
  - treat nickname empty
 ProductionManagementSystem
  - Add the missing , to the state machine.
 DataManagementSystem
  - getValue instead of getOption for SEUsageAgent initialization
  - few minor bugs in scripts
  - attempt to fix LFN information on StorageHistory
 TransformationSystem
  - Adapted to DIRAC v6r14
 Interfaces
  - mostly pylint

