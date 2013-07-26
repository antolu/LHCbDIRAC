""" lhcb_ci.exceptions

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


#...............................................................................
# test_agent exceptions            

test_agent = {
                  
  'test_configure_agent' : [
    # NagiosConsumerAgent : Can not find Agents/NagiosConsumerAgent in template                       
    'NagiosConsumerAgent',
    # ElementInspectorAgent : Can not find Agents/ElementInspectorAgent in template
    'ElementInspectorAgent',
    # GridSiteMonitoringAgent : Can not find Agents/GridSiteMonitoringAgent in template
    'GridSiteMonitoringAgent',
    # CacheFeederAgent : Can not find Agents/CacheFeederAgent in template
    'CacheFeederAgent',
    # TokenAgent : Can not find Agents/TokenAgent in template
    'TokenAgent',
    # HCAgent : Can not find Agents/HCAgent in template
    'HCAgent',
    # SAMAgent : Can not find Agents/SAMAgent in template     
    'SAMAgent',
    # TargzJobLogAgent : Can not find Agents/TargzJobLogAgent in template
    'TargzJobLogAgent',
    # UserStorageQuotaAgent : Can not find Agents/UserStorageQuotaAgent in template
    'UserStorageQuotaAgent',
    # LemonAgent : Can not find Agents/LemonAgent in template
    'LemonAgent',
    # TaskQueueAgent : Can not find Agents/TaskQueueAgent in template
    'TaskQueueAgent',
    # JobSanityAgent : Can not find Agents/JobSanityAgent in template
    'JobSanityAgent',
    # JobSchedulingAgent : Can not find Agents/JobSchedulingAgent in template
    'JobSchedulingAgent',
    # DiracSiteAgent : Can not find Agents/DiracSiteAgent in template
    'DiracSiteAgent',
    # ThreadedMightyOptimizer : Can not find Agents/ThreadedMightyOptimizer in template
    'ThreadedMightyOptimizer',
    # OptimizerModule : Can not find Agents/OptimizerModule in template
    'OptimizerModule',
    # DataProcessingProgressAgent : Can not find Agents/DataProcessingProgressAgent in template
    'DataProcessingProgressAgent',
    # TaskManagerAgentBase : Can not find Agents/TaskManagerAgentBase in template
    'TaskManagerAgentBase'    
                           ],
              
  'test_agents_import' : [
    # Can't import it                      
    'NagiosConsumerAgent',
    # Can't import it
    'HCProxyAgent',
    # Can't import it
    'GridSiteMonitoringAgent',
    # Can't import it
    'HCAgent',
    # invalid syntax (FTSRequest.py, line 894)
    'FTSMonitorAgent',
    # Can not connect to DB StorageUsageDB, exiting...
    'UserStorageQuotaAgent',
    # invalid syntax (FTSRequest.py, line 894)
    'FTSSubmitAgent',
    # __init__() takes at least 4 arguments (3 given)
    'MCExtensionAgent'                        
                          
                          ]                                                                   
                  
}


#...............................................................................
# test_db_exceptions


test_db = {

  'test_import_db_modules' : [
    'TransformationDB', 
    'RAWIntegrityDB', 
    'RequestDB', ],
                      
  'test_install_tables' : [
    'SystemLoggingDB', ]                      
                      
}


#...............................................................................
# test_service_exceptions


test_service = {

  'test_run_services' : [
    # BookkeepingManager : cx_Oracle
    'BookkeepingManager',
    # RequestManager : RequestDB
    'RequestManager',
    # StorageElement : failed to get base path
    'StorageElement',
    # TransferDBMonitoring : Can not connect to DB RequestDB
    'TransferDBMonitoring',
    # StorageElementProxy : failed to get base path
    'StorageElementProxy',
    # DataUsage : Can not connect to DB StorageUsageDB
    'DataUsage',
    # RunDBInterface : from path import SQL_ALCHEMY_PATH
    'RunDBInterface',
    # Gateway : string indices must be integers, not str 
    'Gateway',
    # SystemLoggingReport : Can not connect to DB SystemLoggingDB
    'SystemLoggingReport',
    # UserProfileManager : Can not connect to DB UserProfileDB
    'UserProfileManager',
    # ProxyManager : Can not connect to DB ProxyDB
    'ProxyManager',
    # SandboxStore : Can not connect to DB SandboxMetadataDB
    'SandboxStore',
    # OptimizationMind : Could not connect to DB
    'OptimizationMind',
    # MigrationMonitoring : Can not connect to DB StorageManagementDB
    'MigrationMonitoring',
    # StorageManager : Can not connect to DB StorageManagementDB
    'StorageManager',
    # ReportGenerator : Can not connect to DB AccountingDB
    'ReportGenerator',
    # DataStore : Can not connect to DB AccountingDB
    'DataStore',
    # TransformationManager : Can not connect to DB TransformationDB
    'TransformationManager', ],

  'test_configured_service_authorization' : [ 
    'BookkeepingManager', 
    'Publisher', 
    'ProductionRequest', 
    'LcgFileCatalogProxy',
    'DataUsage', 
    'StorageUsage', 
    'DataIntegrity', 
    'RunDBInterface', 
    'RAWIntegrity',
    'Gateway', 
    'JobStateSync', 
    'Future', 
    'OptimizationMind',
    'Server',
    'TransformationManager', ],
                  
  'test_configured_service_ports' : [            
    'LcgFileCatalogProxy', 
    'RunDBInterface', 
    'Future', 
    'MigrationMonitoring', 
    'ProductionRequest', ],
                
  'test_configure_service' : [
    # ProductionRequest : Can not find Services/ProductionRequest in template                          
    'ProductionRequest', 
    # RunDBInterface : Can not find Services/RunDBInterface in template
    'RunDBInterface', 
    # Future : Can not find Services/Future in template
    'Future', ]
               
}   
    

#...............................................................................
#EOF