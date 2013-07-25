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
#EOF