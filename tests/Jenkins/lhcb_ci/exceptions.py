""" lhcb_ci.utils.exceptions

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


#...............................................................................
# test_configure_exceptions


test_configure = {
                  
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
    'TransformationManager' ],
                  
  'test_configured_service_ports' : [            
    'LcgFileCatalogProxy', 
    'RunDBInterface', 
    'Future', 
    'MigrationMonitoring', 
    'ProductionRequest' ],
                
  'test_configure_service' : [
    # ProductionRequest : Can not find Services/ProductionRequest in template                          
    'ProductionRequest', 
    # RunDBInterface : Can not find Services/RunDBInterface in template
    'RunDBInterface', 
    # Future : Can not find Services/Future in template
    'Future' ]                                    
                  
}


#...............................................................................
# test_db_exceptions


test_db = {

  'test_import_db_modules' : [
    'TransformationDB', 
    'RAWIntegrityDB', 
    'RequestDB' ],
                      
  'test_install_tables' : [
    'SystemLoggingDB' ]                      
                      
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
    'TransformationManager' ]
               
}   
    

#...............................................................................
#EOF