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
    'Server'
    
                                            ],

                  
                  }


#...............................................................................
#EOF