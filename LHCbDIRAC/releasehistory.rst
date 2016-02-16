-----------------
Package LHCbDIRAC
-----------------

Version v8r2p8
--------------

BUGFIX
::::::

 WorkloadManagementSystem
  - introduced temporary check for avoiding to match jobs on too slow CPUs (default limit is 3 HS06)

Version v8r2p7
--------------

BUGFIX
::::::

 TransformationSystem
  - correctly retrieve the bkk output in the RAWProcessing plugin FIX (script): in transformation-debug for checking flush

Version v8r2p6
--------------

BUGFIX
::::::

 TransformationSystem
  - correctly retrieve the bkk output in the RAWProcessing plugin FIX (script): in transformation-debug for checking flush

Version v8r2p4
--------------

CHANGE
::::::

 WorkloadManagementSystem
  - add option for linking in diraca-pilot-version

BUGFIX
::::::

 BookkeepingSystem
  - Do not execute the query parallel.

Version v8r2p3
--------------

BUGFIX
::::::

 BookkeepingSystem
  - By default the run is not finished
  - DQFlag changed to DataQualityFlag...

Version v8r2p2
--------------

BUGFIX
::::::

 Resources
  - LHCbOnline plugin fixed
  - Add hasAccess interface to the RAWIntegrityClient

Version v8r2p1
--------------

CHANGE
::::::

 ResourceStatusSystem
  - adapt SLS for the new dashboard

BUGFIX
::::::

 BookkeepingSystem
  - bad line in dirac-bookkeeping-gui script
 Resources
  - LHCbOnline plugin fixed

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

Version v8r1p17
---------------

Version v8r1p16
---------------

NEW
:::

 BookkeepingSystem
  - Add missing parameters

CHANGE
::::::

 ProductionManagementSystem
  - Active request can be moved to Accepted.
 BookkeepingSystem
  - Allow list of event types
 TransformationSystem
  - GRIDDownlaoder

BUGFIX
::::::

 BookkeepingSystem
  - IsFinished flag removed from the job parameter.
 DataManagementSystem
  - use fc.setReplicaProblematic in script

Version v8r1p15
---------------

BUGFIX
::::::

 TransformationSystem
  - correct setting start and end run even if not already present FIX (agent): small fix in plugins

Version v8r1p12
---------------

CHANGE
::::::

 BookkeepingSystem
  - return all the steps, if contains the given text. CHANGE (scripts): allow a list of file for setting visibility flag
 ProductionManagementSystem
  - Compare the dictionaries instead of strings
  - lhcb_admin also has the same role as diracAdmin...

BUGFIX
::::::

 ProductionManagementSystem
  - Add the missing , to the state machine.

Version v8r1p11
---------------

NEW
:::

 BookkeepingSystem
  - implement the run status.

CHANGE
::::::

 BookkeepingSystem
  - add the luminosity
 ProductionManagementSystem
  - The old portal URL replaced with the new portal URL...

BUGFIX
::::::

 ProductionManagementSystem
  - Added Cleaning->TrasformationCleaned and Idle->Stopped as allowed in the Production Status State Machine

Version v8r1p10
---------------

NEW
:::

 BookkeepingSystem
  - new function makeBKPath

CHANGE
::::::

 ProductionManagementSystem
  - The old portal URL replaced with the new portal URL...

BUGFIX
::::::

 ProductionManagementSystem
  - Added Cleaning->TrasformationCleaned and Idle->Stopped as allowed in the Production Status State Machine

Version v8r1p9
--------------

Version v8r1p8
--------------

BUGFIX
::::::

 ResourceStatusSystem
  - change permission in LHCbPrioxyAgent
 BookkeepingSystem
  - fix the getProductions

Version v8r1p5
--------------

CHANGE
::::::

 BookkeepingSystem
  - Return the productions of the deleted files.
  - remove obsolete methods in BKClient

BUGFIX
::::::

 ProductionManagementSystem
  - productions won't go to Idle status if there are files in Unused-inherited status
 WorkloadManagementSystem
  - Removed addition of LFN: in AncestorFilesAgent - an optimizer
 TransformationSystem
  - TS files state machine FIX (agent): add recovery for run 0 in RAWxxx plugins
  - RemoveReplicasWhenProcessed plugin using BK descendants rather than TS
 ConfigurationSystem
  - treat nickname empty

Version v8r1p4
--------------

BUGFIX
::::::

 DataManagementSystem
  - minor fix in script

Version v8r1p3
--------------

CHANGE
::::::

 TransformationSystem
  - add information in transformation-debug

BUGFIX
::::::

 ProductionManagementSystem
  - Conveying ancestorDepth parameter for production jobs

Version v8r1p2
--------------

BUGFIX
::::::

 ProductionManagementSystem
  - outputSE defined within the templates don't override HIST default output SE

Version v8r1p1
--------------

BUGFIX
::::::

 Workflow
  - Corrected typo RunNumber -> runNumber

Version v8r1
------------

NEW
:::

 Core
  - dirac-lhcb-mc-eventtype script
  - Added per run output mode
 DataManagementSystem
  - Added FCUtilities module
  - Added dirac-dms-chown-directory script
 ConfigurationSystem
  - Added recursive addition of users in DFC BUGFIX : fix typo for option in add-user-DFC

CHANGE
::::::

 Core
  - reverse order of CMT configs (for the case of steps with "ANY" CMTConfig)
 Interfaces
  - Forbidden to create jobs with prepend string in combination with output file names with underscores
 WorkloadManagementSystem
  - removed maxQueueSize from DBs
 ResourceStatusSystem
  - allow to register several email in an e-group
 BookkeepingSystem
  - Removed Summary option from dirac-bookkeeping-get-file-descendants
 Workflow
  - Moved createProdConfFile as generic function in ModuleBase
  - Added run info for calculating output in case per run output is selected
 ProductionManagementSystem
  - removed maxQueueSize from DBs
  - Added Templates directory (was in Workflow)
  - splitting MC by Brunel step (not Moore)
  - Added per run output mode to the template as default for real data productions
 DataManagementSystem
  - removed maxQueueSize from DBs
  - Simplified implementation of LogUpload Request Operation CHANGE (scripts): add new options in scripts
  - get VO name from CS and replace type() with isinstance() FIX (scripts): avoid loading CS in DMScript, use DMSHelpers for resolving SE groups
 TransformationSystem
  - remove maxqueuesize
  - added timeThis decorator for timing functions in the plug-ins, re-shuffled few things
  - removed maxQueueSize from DBs
  - added bulk querying for RunDestination table
  - Moved PluginScript in a separate moduke inside TransformationSystem.Utilities
  - getRunsDestination accepts and treats several types of inputs, returns a list of tuples NEW (agent): new plugin RAWReplication with Run2 policy (WARNING: don't use it without PR 2360 FIX(scripts): protect check-descendants from checking for merging productions CHANGE (agent): use DMSHelpers NEW (agent): new plugin RAWProcessing for Run2, move Client.Utilities to Utilities.PluginUtilities CHANGE (scripts): adapt to moved utility FIX (agent): crash when no counters existed, split counters by SE FIX (DB): fix the bad SQL statement when setting parameters FIX (agent): resolve SE groups in TS parameters
 ConfigurationSystem
  - remove reference to LFC

BUGFIX
::::::

 Core
  - Correct exit on error from dirac-architecture
  - Better logging
  - Better checks for types
  - AnalyseXMLSummary and AnalyseLogFiles now consider the GaudiFederation mechanism
 Interfaces
  - Removed UserOutputLFNPrepend from setOutput of LHCbJob
  - Removed unused getProdJobOutputData from DiracProduction
  - Better checks for types
  - removing underscore when prepending a file
 ResourceStatusSystem
  - fix typo in ShiftDBAgent
  - TopologyAgent now syncs a more precise list of resources
 BookkeepingSystem
  - Better checks for types
  - change the default values of the getFilesWithMetadata method.
 Workflow
  - Understanding used jobs with new output data structure
 ProductionManagementSystem
  - considering the case that stepOutputMask is empty
  - Set RAWProcessing as default plugin for reconstruction productions
 ConfigurationSystem
  - fix address reference
  - add_user_dfc using FCUtilities

Version v0r114
--------------

NEW
:::

 Core
  - dirac-lhcb-mc-eventtype script
 ConfigurationSystem
  - Added recursive addition of users in DFC BUGFIX : fix typo for option in add-user-DFC

CHANGE
::::::

 WorkloadManagementSystem
  - removed maxQueueSize from DBs
 BookkeepingSystem
  - Removed Summary option from dirac-bookkeeping-get-file-descendants
 Workflow
  - Moved createProdConfFile as generic function in ModuleBase
 ProductionManagementSystem
  - removed maxQueueSize from DBs
  - Added Templates directory (was in Workflow)
 DataManagementSystem
  - removed maxQueueSize from DBs
  - Simplified implementation of LogUpload Request Operation CHANGE (scripts): add new options in scripts
  - get VO name from CS and replace type() with isinstance() FIX (scripts): avoid loading CS in DMScript, use DMSHelpers for resolving SE groups
 TransformationSystem
  - remove maxqueuesize
  - added timeThis decorator for timing functions in the plug-ins, re-shuffled few things
  - removed maxQueueSize from DBs
  - added bulk querying for RunDestination table
  - Moved PluginScript in a separate moduke inside TransformationSystem.Utilities
  - getRunsDestination accepts and treats several types of inputs, returns a list of tuples NEW (agent): new plugin RAWReplication with Run2 policy (WARNING: don't use it without PR 2360 FIX(scripts): protect check-descendants from checking for merging productions CHANGE (agent): use DMSHelpers NEW (agent): new plugin RAWProcessing for Run2, move Client.Utilities to Utilities.PluginUtilities CHANGE (scripts): adapt to moved utility FIX (agent): crash when no counters existed, split counters by SE FIX (DB): fix the bad SQL statement when setting parameters
 ConfigurationSystem
  - remove reference to LFC

BUGFIX
::::::

 Core
  - Correct exit on error from dirac-architecture
  - Better logging
  - Better checks for types
  - AnalyseXMLSummary and AnalyseLogFiles now consider the GaudiFederation mechanism
 Interfaces
  - Removed UserOutputLFNPrepend from setOutput of LHCbJob
  - Removed unused getProdJobOutputData from DiracProduction
  - Better checks for types
 ResourceStatusSystem
  - fix typo in ShiftDBAgent
  - TopologyAgent now syncs a more precise list of resources
 BookkeepingSystem
  - Better checks for types
  - change the default values of the getFilesWithMetadata method.
 ProductionManagementSystem
  - considering the case that stepOutputMask is empty
 ConfigurationSystem
  - fix address reference

Version v8r0p24
---------------

Version v8r0p23
---------------

BUGFIX
::::::

 BookkeepingSystem
  - Handle correctly replicas when it is a list (the case when the Gaudi federation is enabled...)

Version v8r0p22
---------------

BUGFIX
::::::

 BookkeepingSystem
  - Fix the advanced save, because the API has changed.

Version v8r0p21
---------------

BUGFIX
::::::

 Core
  - remove hardcoded AllReplicas in InputDataByProtocol (that should be moved!!!!!!!)

Version v8r0p18
---------------

BUGFIX
::::::

 WorkloadManagementSystem
  - correct locations of DIRAC_VOMSES and VOMSDIR

Version v8r0p17
---------------

CHANGE
::::::

 TransformationSystem
  - give mor info on FTS jobs in transformation-debug

BUGFIX
::::::

 DataManagementSystem
  - DataIntegrity: empty directory is not necessarily an error FIX (agents): StorageUsage and StorageHistory for using DFC FIX (scripts): lfn-metadata for DFC

Version v8r0p15
---------------

CHANGE
::::::

 BookkeepingSystem
  - Add the replica and visibility flag to the getNbOfRawFiles method.
 DataManagementSystem
  - add storage at Tier1s in scan-popularity

BUGFIX
::::::

 ProductionManagementSystem
  - Added MCMerge production type
 BookkeepingSystem
  - Correctly handle the run numbers.
  - Return all failed and not processed files.
 TransformationSystem
  - Added MCMerge production type
 Workflow
  - Added MCMerge production type

Version v8r0p14
---------------

NEW
:::

 ResourceStatusSystem
  - LHCbPRProxyAgent

BUGFIX
::::::

 ProductionManagementSystem
  - slightly changed definition of idle (applies also to new empty productions)
 ResourceStatusSystem
  - Removed LFC from NagiosTopologyAgent

Version v8r0p13
---------------

NEW
:::

 ResourceStatusSystem
  - LHCbPRProxyAgent

BUGFIX
::::::

 ProductionManagementSystem
  - slightly changed definition of idle (applies also to new empty productions)
 ResourceStatusSystem
  - Removed LFC from NagiosTopologyAgent

Version v8r0p11
---------------

NEW
:::

 BookkeepingSystem
  - new option in script file-path

CHANGE
::::::

 TransformationSystem
  - CPUe is calculated as sum of all the steps CPUtime

BUGFIX
::::::

 BookkeepingSystem
  - The file types must used to determine the processing pass. NEW (scripts): new options in job-info

Version v8r0p10
---------------

BUGFIX
::::::

 ProductionManagementSystem
  - Avoid putting tuple in BKPath
  - Corrected setting of priority for MC testing jobs

Version v8r0p7
--------------

BUGFIX
::::::

 WorkloadManagementSystem
  - expanding environment variables
 Workflow
  - correctly interpreting the case of multiple data steps in the output step mask

Version v8r0p5
--------------

CHANGE
::::::

 ProductionManagementSystem
  - added 1 to the stepMask of MC simulation productions in testing phase
 TransformationSystem
  - MCExtensionAgent won't extend if CPUe is not defined
 Resources
  - BK catalog client returns OK for user files

BUGFIX
::::::

 Core
  - change definition of in lhcb-restart-agent-service
 ProductionManagementSystem
  - Increase the priority for testing MC jobs to 9
 BookkeepingSystem
  - get-stats script was not working if no --BK option
  - return in sendXMLReport
  - convert the production number to integer
 WorkloadManagementSystem
  - Better logging for the case of missing security variables
 TransformationSystem
  - MCSimualtionTestingAgent sends report only if necessary
  - MCSimualtionTestingAgent sends report to the correct mail address

Version v8r0p2
--------------

NEW
:::

 Core
  - checkStalledService script
  - lhcb-proxy-init first checks for security env variables to be set
 WorkloadManagementSystem
  - LHCb pilots: doing SetupProject LHCbDIRAC wherever possible, falling back to dirac-install when not available
  - introduced LHCbSiteDirector as extension of the DIRAC SiteDirector for sending lhcb pilots
  - LHCb pilot commands, specifically to use SetupProject instead of dirac-install as per LHCBDIRAC-191
  - LHCb Site director, to send LHCb-specific pilots
  - pilotVersion script, to update the pilot version in all the locations
  - Added setServerCertificates and ConfigureLHCbArchitecture command to the pilot
 BookkeepingSystem
  - VisibilityFlag added to the file metadata and the directory metadata.
  - the job metadata can be retrieved for a given dirac jobid or a given job name.
  - new options for getFiles
 Workflow
  - Added possibility to add an indexing production in the stripping
  - Special output SEs for certain output types can be set directly by the prods manager when launching a production
  - Moved SAM worfklow modules in
  - AnalyseFileAccess module
 ProductionManagementSystem
  - Created outputSEs dictionary to hold the relationship between output types and outputSEs, that can now be specified at workflow level for each of the output types by production manager
  - Introduced Completed status for production requests, to signal a production request that processed (or produced) all the requested events
  - Introduced MC testing phase as explain in Jira task LHCBDIRAC-301. New Testing state introduced.
  - new productionStatusAgent and RequestTrackingAgent
 TransformationSystem
  - GridCollectorAgent (agent for the indexer process)
  - Transformations "hot flag" (false by default, can be set via the web portal)
  - Introduced MC testing agent as per LHCBDIRAC-306
  - table in TransformationDB to host the temporary MC XML during testing phase
  - Added configuration files for events collector agent
  - Added GridCollectorAgent to the ConfigTemplate
 Interfaces
  - Added MCsimflag in DiracProduction
  - Added AnalyseFileAccess module within SetApplication API method

CHANGE
::::::

 Core
  - removed check of packages from NoSoftwareInstallation.py
  - removed outdated scripts
 WorkloadManagementSystem
  - LHCb SiteDirector sends LHCb pilots
  - Removed old newpilots temptatives
  - LHCb SiteDirector send pilots executing new ConfigureCPURequirements command
  - Pilots 2.0 get CAs and VOMS from CVMFS, when possible
  - always using security env variables that are on the system, if not found set them explicitely. Don't use SetupProject ones
  - BKInputDataAgent does not need any shifterProxy NEW (scripts): new script dirac-wms-pilot-job-info for printing job information corresponding to a pilot
 ResourceStatusSystem
  - ShiftDBAgent points to new groups wsdl url
  - Converged SAMSystem in ResourceStatusSystem
  - In SAM machinery, it will be specified the CE and site whenever possible
 Workflow
  - Output SEs are defined separately for each output type. It is possible to set a default within the templates
  - removed specific mention to LcgFileCatalogCombined. When uploading, using new datamanager capabilities to register on master catalog only.
  - when uploading the output of production jobs, the BKK report is sent before registering the files
  - getCPUTime utilities moved to DIRAC
  - UploadOutputModule: descendants check only done at the beginning, BK registration at the end of the module
  - ModuleBase gets PRODUCTION_ID and JOB_ID from the workflow-commons
 ProductionManagementSystem
  - Removed useless RequestTestAgent (a completely new one will come from the next minor release)
 DataManagementSystem
  - StorageHistoryAgent now summing up directories files and sizes
  - Removed obsolete Dataset.py
  - Added indexes and PK to RAWIntegrityDB and StorageUsageDB
  - Commented out mergeForDQ code CHANGE (scripts): move execution functions of DMS scripts to a module ScriptExecutors in Client NEW (script): script for scanning the popularity of datasets FIX (agent): in LogUpload
  - Removed reperting to DataLogging
 TransformationSystem
  - Moved GridCollectorAgent utilities in Utilities/GridCollector
  - made GridCollectorAgent more LHCbDIRACish, plus using DataManager instead of ReplicaManager FIX (agent): BKWatchAgent to retry full queries in case of failure CHANGE (script): add option for checking log files of jobs in transformation-debug FIX (agent): DeleteWhenProcessed plugin was not working well when productions were Cleaned :( FIX (agent): small fix in a plugin FIX (agent): small fix in a plugin FIX (agent) optimise flushing FIX (agent): when run is flushed, stop checking files FIX (agent): improve scaling for large transformations NEW (script): new script dirac-transformation-targets for getting the number of files per target NEW (agent): allow CS setting of number of files per task for replication CHANGE (agent): do not extend Testing MC transformations FIX (agent): port a fix that was in branch and not in trunk CHANGE (service): Changed names from RunSE to RunDestination, from the table to the methods CHANGE (client): Use BKClientWithRetry FIX (agent): fix plugin _byRun
 Interfaces
  - Removed obsolete addPackage from LHCbJob API

BUGFIX
::::::

 Core
  - ResolveSE: shuffling SES instead of fixed list
  - removed useless script
  - check exist value for lhcb-proxy-init
 WorkloadManagementSystem
  - Prepare changes from "CheckSumType" to "ChecksumType" and LFC to DFC for BKInputAgent
  - pilots always save a bashrc file, even in case of SetupProject
 BookkeepingSystem
  - minor change in BKQuery FIX (scripts): in BKQuery()
  - dirac-bookkeeping-get-files script uses chuncks of files for performance reasons
  - (fix from branch...) DO not ignore the run number if it is a string...
  - All steps are returned for a given DIRAC jobid.
  - makePath in BkQuery.py: Conditions -> ConditionDescription FIX (scripts): small fix in BKQuery.makePath() NEW (scripts): new options for getFiles FIX (scripts): handle correctly case when --BK is not given
  - always split files by ; if passed as a string NEW (client): BKClientWithRetry
 Workflow
  - Adapting to new content of PoolXMLCatalog
  - Do not set any more CPUe from the template
 ProductionManagementSystem
  - ProductionStatysAgent: Moved creation of clients in the initialize method
  - ProductionStatysAgent: removed useless _cleanActiveJobs() internal function
  - ProductionRequestDB SQL definition trimmed so that it can be installed via standard tools
  - changed default port number for ProductionRequest service
  - Setting default values for Testing phase of MCSimulation productions
  - setting the outputMask instead of the stepMask for workflows in MC testing productions (for GAUSSHIST case) FIX (agent): ProductionStatus agent needs a ProductionManager shifter to run FIX (client): Setting correctly the prodID for all AdditionalParameters of a production
 AccountingSystem
  - moved integration tests out, fixed remaining tests
 DataManagementSystem
  - new parameter for tmp directory FIX (Agent): commits were missing in the PopularityAgent CHANGE (Agent): record visibility in DirMetadata table
  - RAWIntegrityDB SQL definition trimmed so that it can be installed via standard tools
  - removed infinite loop in ConsistencyChecks
  - removed old/unused scripts
  - RAWIntegrityAgent updated for v6r12 FIX (scripts): many small changes in scripts execution FIX (script): storage summary in case no BK query given FIX (scripts): handle correctly case when --BK is not given FIX (scripts): many small changes in scripts execution FIX (agent) optimise flushing FIX (agent): incompatibility in Visibility flag naming between DMS and BK FIX (script): don't force visibility flag in replica-stats NEW (script): allow users to define protocol as xroot or root and work at all sites FIX (scripts): check-fc2bk and bk2fc fixed and added functionality
 TransformationSystem
  - just updated for compatibility with DIRAC v6r12
  - Moved creation of clients in the initialize method for all the agents
  - WorfklowTaskAgent adapted to new multi-threaded version of TaskManagerAgentBase as per DIRAC v6r12
 Interfaces
  - Fixed dirac-production-change-status script
  - userLog->applicationLog for setExecutable
 Resources
  - Fixed obvious bug in RAWIntegrityClient
  - Adapting to new content of PoolXMLCatalog

Version v7r16p30
----------------

Version v7r16p28
----------------

Version v7r16p27
----------------

BUGFIX
::::::

 BookkeepingSystem
  - small problem in BKQuery

Version v7r16p24
----------------

NEW
:::

 Workflow
  - Added possibility to add EventIndexing as last production in a Stripping workflow
 TransformationSystem
  - Added GridCollectorAgent and its utilities

CHANGE
::::::

 Core
  - Removed lhcb-use-dev-machine script
 AccountingSystem
  - Backporting from trunk - removed tests now in LHCbTestDirac
 TransformationSystem
  - Removed kick-request script

BUGFIX
::::::

 ProductionManagementSystem
  - Correctly interpreting extraOptions parameter

Version v7r16p22
----------------

BUGFIX
::::::

 ProductionManagementSystem
  - Up to 20 steps (ouf!) for MC requests
 BookkeepingSystem
  - minor fix in BKQuery
 DataManagementSystem
  - minor fix in scripts

Version v7r16p21
----------------

BUGFIX
::::::

 ResourceStatusSystem
  - Using LHCbJobDB in GridSiteWMSMonitoringAgent
 BookkeepingSystem
  - DO not ignore the run number if it is a string...
 WorkloadManagementSystem
  - Added JobDB extension for LHCb specific methods (moved from DIRAC)

Version v7r16p18
----------------

BUGFIX
::::::

 ProductionManagementSystem
  - Production can now move from Idle to Cleaning status

Version v7r16p17
----------------

CHANGE
::::::

 Workflow
  - Production jobs that can run multicore will do that depending on the capabilities of the CE where they are running

BUGFIX
::::::

 BookkeepingSystem
  - minor change in BKQuery
 DataManagementSystem
  - new parameter for tmp directory FIX (Agent): commits were missing in the PopularityAgent CHANGE (Agent): record visibility in DirMetadata table NEW (script): new script dirac-dms-list-directory
 Workflow
  - Better error checking while taring log files
  - Better control when finding output files on disk
  - Correctly considering all types of output files when applying the step mask

Version v7r16p16
----------------

Version v7r16p15
----------------

NEW
:::

 BookkeepingSystem
  - VisibilityFlag added to the file metadata and the directory metadata.

Version v7r16p14
----------------

NEW
:::

 BookkeepingSystem
  - VisibilityFlag added to the file metadata and the directory metadata.

Version v7r16p13
----------------

Version v7r16p11
----------------

Version v7r16p10
----------------

CHANGE
::::::

 Core
  - get the IDR flag for protocol from CS
 ResourceStatusSystem
  - egroups wsdl location

Version v7r16p8
---------------

Version v7r16p7
---------------

BUGFIX
::::::

 DataManagementSystem
  - corrected bug in TargzJobLogAgent
 Workflow
  - correct replication of user output files

Version v7r16p6
---------------

BUGFIX
::::::

 Interfaces
  - bad key in DiracLHCb.py

Version v7r16p5
---------------

CHANGE
::::::

 DataManagementSystem
  - Sleep 2 seconds after the activities are registered.

BUGFIX
::::::

 Core
  - The print statements are removed from the InputDataResolution
 ProductionManagementSystem
  - Completing to Idle allowed

Version v7r16p4
---------------

Version v7r16p3
---------------

BUGFIX
::::::

 BookkeepingSystem
  - Advanced save is crashed due to the change of the DIRAC API.

Version v7r16p2
---------------

NEW
:::

 ConfigurationSystem
  - import add_DN_LFC from LBSCRIPTS

BUGFIX
::::::

 Core
  - Better error handling when failing to produce the environment with SetupProject
 Workflow
  - bug fix in RootApplication module, made impossible to use root on the Grid

Version v7r16
-------------

NEW
:::

 Core
  - new getPlatformsCompatibilities function used in the NoSoftwareInstallation module
  - The dirac-architecture script sends a mail for every new dirac-architecture discovered
 Interfaces
  - Users decide if they want their output data to be replicated or not (default: no)
 BookkeepingSystem
  - added dirac-bookkeping-prod4path script
 Workflow
  - Users decide if they want their output data to be replicated or not (default: no)
 DataManagementSystem
  - dirac-rms-show-request script
 TransformationSystem
  - BkQuery table re-designed to be easily extensible
 ConfigurationSystem
  - import add_DN_LFC from LBSCRIPTS

CHANGE
::::::

 Core
  - systemConfig (platform) set using the SystemConfig as defined within the steps
  - in case SystemConfig is set to "ANY", try all available configurations before giving up
  - removed all references and scripts reading from SoftwareDistribution section of the CS
  - removed CombinedSoftwareInstallation
  - dirac-architecture rewritten, using CS information via Resources helper
  - InputDataResolution for getting all replicas in the XML catalog
  - Removed obsolete script dirac-lhcb-run-test-job
  - Removed obsolete module DetectOS
  - Simplified noSoftwareInstallation module
  - The dirac-architecture script is now a standard DIRAC script
  - dirac-architecture gets the EMail to report missing architectures from the CS
 Interfaces
  - removed getRootVersions, getSoftwareVersions from DiracLHCb API
  - removed useless setting of "TotalSteps" as workflow parameter
  - LHCbJob setApplication(s) methods will add the CMTConfig as a parameter of the step
  - the new setDIRACPlatform method needs to be called to set the DIRAC platform at the worklow level
  - added special flag for inputs from previous step to enable to connect multiple steps in users and SAM jobs
 ResourceStatusSystem
  - from ReplicaManager to DataManager
  - NagiosTopologyAgent now reports also for ARC sites/CEs
 BookkeepingSystem
  - from ReplicaManager to DataManager
  - Execution plan has changed in order to improve the database performance.
  - Allow to add files or modify job or file parameters of an existing job/file.
  - Do not list the empty directories in the processing pass.
 Workflow
  - from ReplicaManager to DataManager
  - rootApplication will setup ROOT, not DaVinci, with no pre-check
  - userJobFinalization will make a Request for replication instead of uploading within the module itself
  - BkkReport won't report any more EventStat (makes no sense)
  - add files uploaded in UploadedOutputData job parameter
 ProductionManagementSystem
  - restored setting of systemConfig for pilot
  - systemConfig for the step is set to "ANY" by default
  - modifying a model is allowed for all lhcb_tech users
  - SystemConfig -> Platform where possible
  - ProductionRequest can use the new LHCbJob().setDIRACPlatform method for the platform of the jobs
  - the optional extraOptions line is now a parameter of the step
 SAMSystem
  - Removed SystemConfig and usage of DetectOS module
  - removed the distribution of stomp library
  - using standard LHCb API calls to generate the SAM jobs steps
  - dirac-lhcb-sam-submit accepts a systemConfig
 DataManagementSystem
  - from ReplicaManager to DataManager
  - various improvements of the consistency checks
 TransformationSystem
  - from ReplicaManager to DataManager FIX (scripts): more checks in transformation-debug using the new RMS FIX (scripts): improvements for debug

BUGFIX
::::::

 Core
  - increased error logging
 Interfaces
  - Correctly setting the DIRAC platform as the lowest capable to run the requested CMTConfig for the job
 BookkeepingSystem
  - The eventtypeid added to the condition when the view is used.
 Workflow
  - added GAUSSHIST to the list of histograms type to consider
  - The UploadDataModule correctly set the operations in the request when cleaning up after job failure
  - failing the job when noticing that at least one input file could not be fully read
  - checksum and checksumType added as metadata of the files to be registered by user jobs
 SAMSystem
  - uploadSAMLogs won't fail because of Nagios issues
 AccountingSystem
  - only a DB fix (256 -> 255 characters)
  - DataSorage reporter is created wrong record when the grouping was LFN .
 DataManagementSystem
  - RAWIntegrityAgent: using the new RMS FIX (script): compatibility problem with new StorageElement FIX (script): improvements in check-fc2se FIX (agents): treat correctly return of getPfnForLfn

Version v7r15p15
----------------

CHANGE
::::::

 BookkeepingSystem
  - Execution plan has changed in order to improve the database performance.

Version v7r15p14
----------------

CHANGE
::::::

 BookkeepingSystem
  - Execution plan has changed in order to improve the database performance.

Version v7r15p13
----------------

BUGFIX
::::::

 TransformationSystem
  - close SEs were not handling properly multiple sites close to SE

Version v7r15p12
----------------

BUGFIX
::::::

 TransformationSystem
  - close SEs were not handling properly multiple sites close to SE

Version v7r15p11
----------------

Version v7r15p9
---------------

CHANGE
::::::

 BookkeepingSystem
  - The EventInputStat will be calculated by the Bookkeeping XML manager using the available information from the DB.

BUGFIX
::::::

 Workflow
  - In order to calculated the CPUTimeLeft, we get the CPUTimeLeft no matters what

Version v7r15p8
---------------

BUGFIX
::::::

 Workflow
  - in user finalisation, python bug

Version v7r15p7
---------------

BUGFIX
::::::

 Core
  - GUID handling (scripts)
 ResourceStatusSystem
  - SAM -> Test
 Workflow
  - UserFinalization for setting properly requests and uploading locally first
 ProductionManagementSystem
  - When evaluating idle status, missing check for "Submitted" tasks.
 SAMSystem
  - SAM -> Test
 DataManagementSystem
  - handle large datasets in pfn-metadata (scripts)
  - SAM -> Test
 TransformationSystem
  - use new RMS in transformation-debug (scripts)

Version v7r15p6
---------------

BUGFIX
::::::

 ProductionManagementSystem
  - 

Version v7r15p5
---------------

CHANGE
::::::

 Workflow
  - No need to set the minCPU for MC

Version v7r15p4
---------------

CHANGE
::::::

 Workflow
  - No need to set the minCPU for MC

Version v7r15p3
---------------

BUGFIX
::::::

 Core
  - skip FrameworkSysadministrator restart in lhcb-restart-agent-service.py

Version v7r15p2
---------------

BUGFIX
::::::

 DataManagementSystem
  - Exception of the SEUsageAgent when tarfile not present
 Workflow
  - can get the CPUTime even when the queue is not existing

Version v7r15p1
---------------

BUGFIX
::::::

 ProductionManagementSystem
  - Corrected way MC productions are declared Idle
  - Allowed multiple status for ProductionRequests selections
 DataManagementSystem
  - Exception of the SEUsageAgent when tarfile not present

Version v7r15
-------------

NEW
:::

 Core
  - dirac-create-cfg script
  - Utilities for SetupProjectApplication and SoftwareArea
  - Added File.py in Core Utilities, used for calculating every GUID in LHCb
  - Correctly set the GUID using pyROOT
  - add script lhcb-restart-agent-service
 WorkloadManagementSystem
  - Added temptative LHCb custom pilot
 BookkeepingSystem
  - isMulticore column added to the steps table.
  - Two command line scripts are implemented: one returns the job metadata for a given LFN, the other method returns the input and output files for a given Jobid.
  - mTCK attribute added to the steps table as well all methods which are using this table have been updated.
  - More detailed processing pass overview has implemented and available on the GUI by clicking on the file type folder.
 Workflow
  - Introduced the CPUe (CPU/event) as a way to calculate how many events to simulate
  - System Config is a parameter of the step, so removed from the templates
  - possibility to impose more limits calculating the number of events created
 ProductionManagementSystem
  - Introduced new "Idle" production status: https://its.cern.ch/jira/browse/LHCBDIRAC-165
  - Introduced SystemConfig as parameter of the step: https://its.cern.ch/jira/browse/LHCBDIRAC-71
  - Introduced CPUe as CPU time needed to produce a MC event
  - For MC, introduced max_e as maximum allowed number of events to simulate https://its.cern.ch/jira/browse/LHCBDIRAC-138
  - Introduced a productions state machine, partly using the RSS SM machinery
  - added notification flag to ProductionStatusAgent
  - ProductionStatusAgent will also move transformations from Idle to Active
 DataManagementSystem
  - dirac-bookkeeping-get-stats script
 TransformationSystem
  - MCExtensionAgent https://its.cern.ch/jira/browse/LHCBDIRAC-141
  - Added TransformationFiles state machine https://its.cern.ch/jira/browse/LHCBDIRAC-192
 Interfaces
  - added lhcb-proxy-init and lhcb0-proxy-info to the API

CHANGE
::::::

 Core
  - scripts for analyzing log files and XML summary handles better the errors
 ResourceStatusSystem
  - Removed table SLSStorage from ResourceManagementDB
  - Restored HCProxyAgent
  - Moved some policies to DIRAC
  - Removed dirac_rss_env_cache.py script
  - DEPRECATION of getSLSStorage
 BookkeepingSystem
  - The duplicated code is removed as well a few methods are re-implemented in order to have cleaner code.
  - The OracleBookkeepingDB is re-factored.
  - mTCK changed to mcTCK
  - The oracle error message changed to a meaningful text in different methods.
 Workflow
  - simplification of the errors reporting and of the input file status update
  - Removed too verbose application status
  - extended ProdGroup, now including the IN processing pass for real data productions
  - Added check for no time left for events
 ProductionManagementSystem
  - non-MC productions go to Idle if they do not have any pending request
 SAMSystem
  - Using getStepDefinition from DIRAC
 AccountingSystem
  - The order of the DataStorage accounting type is changed in order to use more efficiently the index.
 DataManagementSystem
  - RAWIntegrityDB: the tables definition has been moved to python code
  - dirac-dms-remove-replicas script: removed "NoTransformation" switch
  - Removed LogUploadAgent (that was using the old RMS)
  - MergeForDQ utility will now use the new RMS system
 TransformationSystem
  - Transformation tables definition moved in .py
  - changed name of dirac-production-archive.py to dirac-transformation-archive.py
  - changed name of dirac-production-clean.py to dirac-transformation-clean.py
  - when changing the status of files, only real changes of file status are applied
  - BKKWatchAgent also considers Idle productions
  - MCExtensionAgent won't extend production if it has been idle for less than 10 minutes
 Interfaces
  - DataQualityFlag changed to DataQuality.
 Resources
  - Using global checkFormat utility

BUGFIX
::::::

 Core
  - Removed useless module
  - added __RCSID__
 WorkloadManagementSystem
  - improved functionalities for the dirac-wms-get-wn script
 ResourceStatusSystem
  - added protection in SLSAgent
  - moved pilotEfficiencyPolicy in DIRAC
 BookkeepingSystem
  - The empty space removed from input and output file types string of a step.
  - The table name is corrected.
  - solrtlist() -> sorted() in BkQuery.py
  - moved scripts here from DMS
 Workflow
  - the CPUTime is got correctly
 ProductionManagementSystem
  - correct set of BkQuery
  - MC productions Idle status harder to reach
 AccountingSystem
  - SpaceToken: sum -> average
  - The Grouping fixed of the DataStorage accounting type.
 DataManagementSystem
  - Removed LcgFileCatalogProxy from ConfigTemplate.cfg
  - moved scripts from DMS to BKK
 TransformationSystem
  - call to cc.getDescendants in DRA
  - updated to use RSS.getSEStorageSpace
  - don't use force = True when setting the file status within the plugins
  - TransformationDB and compatibility with v6r0 and v7r0
  - Full compatibility between MySQL schema with MyISAM or InnoDB
  - missing self parameter when invoking a service call
  - State Machine: possible to move from Unsed to Processed
  - replicaRemoval -> RemoveReplica in dirac-dms-add-replication
  - re-use of DIRAC source code in Transformation.py
  - Added verbosity to MCExtensionAgent
  - Slightly modified the logic for declaring a MC production as Idle
  - get correct free space (to be released only if v7r15 is late)
 Interfaces
  - Removed few useless functions to handle productions

Version v7r14p37
----------------

BUGFIX
::::::

 ProductionManagementSystem
  - correctly displaying the RequestHistory

Version v7r14p35
----------------

BUGFIX
::::::

 ProductionManagementSystem
  - correctly displaying the RequestHistory

Version v7r14p33
----------------

CHANGE
::::::

 AccountingSystem
  - The order of the DataStorage accounting type is changed in order to use more efficiently the index.

BUGFIX
::::::

 AccountingSystem
  - The Grouping fixed of the DataStorage accounting type.

Version v7r14p32
----------------

NEW
:::

 ResourceStatusSystem
  - up to date tag

BUGFIX
::::::

 BookkeepingSystem
  - Remove the non used variable from the data quality script
 TransformationSystem
  - Full compatibility between MySQL schema with MyISAM or InnoDB
  - plugin to be able to remove non-merged datasets
  - several scripts improvement for debugging
 DataManagementSystem
  - in various scripts

Version v7r14p30
----------------

CHANGE
::::::

 ProductionManagementSystem
  - the ProductionStatusAgent will be closing also archived transformations

BUGFIX
::::::

 AccountingSystem
  - average points of SpaceToken (do not forget volhcb03)
 BookkeepingSystem
  - The empty space removed from input and output file types string of a step.
 ResourceStatusSystem
  - SLSAgent and SpaceTokenOccupancyCommand

Version v7r14p29
----------------

CHANGE
::::::

 ProductionManagementSystem
  - the ProductionStatusAgent will be closing also archived transformations

BUGFIX
::::::

 AccountingSystem
  - average points of SpaceToken (do not forget volhcb03)
 BookkeepingSystem
  - The empty space removed from input and output file types string of a step.
 ResourceStatusSystem
  - SLSAgent and SpaceTokenOccupancyCommand

Version v7r14p28
----------------

CHANGE
::::::

 Interfaces
  - DataQualityFlag changed to DataQuality.

BUGFIX
::::::

 ResourceStatusSystem
  - patched SLSAgent

Version v7r14p27
----------------

BUGFIX
::::::

 TransformationSystem
  - inserting files with chunks

Version v7r14p25
----------------

BUGFIX
::::::

 TransformationSystem
  - __insertIntoExistingTransformationFiles: ignoring Removed files

Version v7r14p22
----------------

BUGFIX
::::::

 Workflow
  - creating the production output LFNs only for production workflows

Version v7r14p20
----------------

BUGFIX
::::::

 Workflow
  - creating the production output LFNs only for production workflows

Version v7r14p19
----------------

BUGFIX
::::::

 Workflow
  - SAM jobs have an output

Version v7r14p18
----------------

CHANGE
::::::

 DataManagementSystem
  - (agent) added agent parameters to handle taring of log files

BUGFIX
::::::

 Workflow
  - (service) templates: flag for removing the inputs was badly interpreted

Version v7r14p17
----------------

BUGFIX
::::::

 Workflow
  - better distinction sam/other jobs

Version v7r14p16
----------------

NEW
:::

 SAMSystem
  - Added ConfigTemplate.cfg

BUGFIX
::::::

 SAMSystem
  - Better logging of errors, to not flood the application status field
 Workflow
  - Better logging of errors when creating the output file names

Version v7r14p15
----------------

BUGFIX
::::::

 Core
  - NagiosConnector
 SAMSystem
  - NagiosConnector
  - NagiosConnector message text

Version v7r14p13
----------------

NEW
:::

 AccountingSystem
  - SpaceTokenOccupancy plotter ( needs also Web )

BUGFIX
::::::

 Core
  - NagiosConnector
 SAMSystem
  - NagiosConnector

Version v7r14p12
----------------

BUGFIX
::::::

 Core
  - NagiosConnector bug fix
 ResourceStatusSystem
  - removed SpaceTokenOccupancyPolicy, as it conflicts with the DIRAC one

Version v7r14p10
----------------

BUGFIX
::::::

 Core
  - NagiosConnector bug fix
 ResourceStatusSystem
  - removed SpaceTokenOccupancyPolicy, as it conflicts with the DIRAC one

Version v7r14p9
---------------

BUGFIX
::::::

 ResourceStatusSystem
  - removed SpaceTokenOccupancyPolicy, as it conflicts with the DIRAC one

Version v7r14p8
---------------

Version v7r14p6
---------------

Version v7r14p5
---------------

Version v7r14p4
---------------

Version v7r14p3
---------------

BUGFIX
::::::

 SAMSystem
  - CVMFSCheck module wasn't getting the input variables from ModuleBase

Version v7r14p2
---------------

BUGFIX
::::::

 SAMSystem
  - CVMFSCheck module wasn't getting the input variables from ModuleBase

Version v7r14p1
---------------

BUGFIX
::::::

 SAMSystem
  - CVMFSCheck module wasn't getting the input variables from ModuleBase

Version v7r14
-------------

NEW
:::

 Core
  - added ProcessingPass to prodconf file
 WorkloadManagementSystem
  - script to get boinc jobs by host
 BookkeepingSystem
  - add the modifications which are in v7r13 branch. CHANGE (scripts): replace --Invisible option with --Visibility=[Yes,No,All] FIX (scripts): test return code in some scripts FIX (client): for visibility, minor fix
  - new columns of the steps table in the trunk are added to this branch.
 Workflow
  - possibility to run multicore jobs
  - added ProcessingPass to prodconf file
 ProductionManagementSystem
  - possibility to run multicore jobs
  - production submission made easier with single hops
  - added ProcessingPass to prodconf file

CHANGE
::::::

 Workflow
  - the big log files are zipped before being uploaded
  - Added Checksum and ChecksumType to fileDict for FailoverTransfer
 ProductionManagementSystem
  - the multicore flag for production is True by default, the one for the steps N by default
 DataManagementSystem
  - the RAWIntegrityAgent now used the new RMS
 TransformationSystem
  - option --Verbose in check-descendants FIX (agent): request files for a list of prods in deleteRepWhenProc CHANGE (agent, client): dual mode reading from RMS new and old system, for DataRecoveryA and trasformation-debug FIX (client): typo fix in production-set-runs script FIX (agent): DataRecovery agent: considering new format of return values of the new RMS FIX (scripts): adapt to change in BKQuery FIX (agent): for handling removal of processed files by production FIX (service) : typo in TransformationManager

BUGFIX
::::::

 Core
  - just simpler and better code
 BookkeepingSystem
  - I added oracle hint to the query which returns a list of file, because it was very slow.
  - include the fix:Take into account all the conditions
 Workflow
  - Adapted to new FailoverTransfer methods signature
  - typo FIX (pilot): JobID is an integer
  - JobID is an integer now
  - set of failover request for BKK
  - Complete reporting of application status
  - BKKRegistrationRequests now contain everything needed
  - the multicore flag for production is separate from those of the single steps
  - correct evaluation of multicore flag
  - added events to process in everythingElse plugin
 ProductionManagementSystem
  - added to ConfigTemplate ProductionRequest service
  - typo: MultiCore -> isMulticore
 SAMSystem
  - Using ModuleBase.execute, plus JobID is an integer
  - added check for runLocal()
 DataManagementSystem
  - Adapted to new FailoverTransfer methods signature CHANGE (scripts): improve dirac-dms-browse-bk FIX (scripts): adapt to change in BKQuery NEW (scripts): get user storage usage
  - added __init__.py for the RequestOperations
  - RequestOperation LogUpload was badly interpreting return values from Replica Manager
 TransformationSystem
  - lock BKWatchAgent
  - lock BKWatchAgent
  - TransformationDB constructor method signature changed.
 Interfaces
  - executable -> script
  - missing import
 Resources
  - test fixed

