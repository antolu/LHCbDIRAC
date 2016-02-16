-----------------
Package LHCbDIRAC
-----------------

Version v8r2p17
---------------

NEW
:::

 BookkeepingSystem
  - bulk update of file metadata
  - trigger to update the luminosity
  - WNMJFHS06 added to the jobs table
  - The luminosity fix of the run can be triggered by a method.

CHANGE
::::::

 Core
  - Renamed dirac-architecture in dirac-platform
 BookkeepingSystem
  - use the stepid when count the number of events
  - use the actual production for the step metadata CHANGE (script): print non finished runs in get-stats
 Workflow
  - jobMaxCPUTime for MC moved to 48 hours

BUGFIX
::::::

 WorkloadManagementSystem
  - pilotWrapper should not change directory for running the pilot

