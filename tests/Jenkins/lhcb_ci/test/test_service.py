""" lhcb_ci.test.test_service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

import lhcb_ci.basecase
import lhcb_ci.db
import lhcb_ci.service

class Installation_Test( lhcb_ci.basecase.Service_TestCase ):
  """ Installation_Test
  
  Tests performing operations related with the Services installation.
  """

  def test_services_install_drop( self ):
    """ test_services_install_drop
    
    Tests that we can install / drop directly services using the DIRAC tools. It
    does not check whether the services run with errors or not.
    """    
    
    self.logTestName( 'test_services_install_drop' )
            
    for system, services in self.swServices.iteritems():
      
      if system == 'Configuration':
        self.log.debug( 'Skipping Master Configuration' )
        continue 
      
      for service in services:
        self.log.debug( "%s %s" % ( system, service ) )
       
        res = lhcb_ci.service.setupService( system, service )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        self.assertEquals( res[ 'Value' ][ 'RunitStatus' ], 'Run' )
        
        res = lhcb_ci.service.uninstallService( system, service )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        

  def test_run_services( self ):
    
    self.logTestName( 'test_run_services' )
    
    _EXCEPTIONS = [ 'BookkeepingManager', 'RequestManager', 'StorageElement', 
                    'TransferDBMonitoring', 'StorageElementProxy', 'DataUsage',
                    'RunDBInterface', 'Gateway', 'SystemLoggingReport',
                    'UserProfileManager', 'ProxyManager', 'JobMonitoring',
                    'SandboxStore', 'Matcher', 'JobStateUpdate', 'JobManager',
                    'WMSAdministrator', 'JobStateSync', 'OptimizationMind',
                    'MigrationMonitoring', 'StorageManager', 'ReportGenerator',
                    'DataStore', 'TransformationManager' ]
    # BookkeepingManager    : cx_Oracle
    # RequestManager        : RequestDB
    # StorageElement        : failed to get base path
    # TransferDBMonitoring  : Can not connect to DB RequestDB
    # StorageElementProxy   : failed to get base path
    # DataUsage             : Can not connect to DB StorageUsageDB
    # RunDBInterface        : from path import SQL_ALCHEMY_PATH
    # Gateway               : string indices must be integers, not str
    # SystemLoggingReport   : Can not connect to DB SystemLoggingDB
    # UserProfileManager    : Can not connect to DB UserProfileDB
    # ProxyManager          : Can not connect to DB ProxyDB
    # JobMonitoring         : Can not connect to DB JobDB
    # SandboxStore          : Can not connect to DB SandboxMetadataDB
    # Matcher               : Can not connect to DB JobDB
    # JobStateUpdate        : Can not connect to DB JobDB
    # JobManager            : Can not connect to DB JobDB
    # WMSAdministrator      : Can not connect to DB JobDB
    # JobStateSync          : Can not connect to DB JobDB
    # OptimizationMind      : Could not connect to DB  
    # MigrationMonitoring   : Can not connect to DB StorageManagementDB
    # StorageManager        : Can not connect to DB StorageManagementDB
    # ReportGenerator       : Can not connect to DB AccountingDB
    # DataStore             : Can not connect to DB AccountingDB
    # TransformationManager : Can not connect to DB TransformationDB
    
#    _SPEEDUP = [ 'ResourceManagement', 'Publisher', 'ResourceStatus', 'RequestProxy',
#                 'ProductionRequest', 'FileCatalogProxy', 'DataLogging', 'DataIntegrity',
#                 'LcgFileCatalogProxy', 'FileCatalog', 'StorageUsage', 'RAWIntegrity',
#                 'BundleDelivery', 'SystemAdministrator', 'Monitoring', 'SiteMap',
#                 'SystemLogging', 'SecurityLogging', 'Notification', 'Plotting',
#                 'Future'  ]
    
    for system, services in self.swServices.iteritems():
      
      if system == 'Configuration':
        self.log.debug( 'Skipping Master Configuration' )
        continue 

      for service in services:

        if service in _EXCEPTIONS:
          self.log.exception( 'EXCEPTION: Skipped %s' % service )
          continue

        #if service in _SPEEDUP:
        #  continue

        self.log.debug( "%s %s" % ( system, service ) )

        dbName = '%sDB' % service
        db = lhcb_ci.db.installDB( dbName )
        if not db[ 'OK' ]:
          self.log.debug( 'No DB for service %s' % service )

        res = lhcb_ci.service.initializeServiceReactor( system, service )
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        # Extract the initialized ServiceReactor
        sReactor = res[ 'Value' ]
        
        res = lhcb_ci.service.serveAndPing( sReactor )
        self.log.debug( str( res ) )
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        
        self.assertEquals( res[ 'Value' ][ 'name' ], '%s/%s' % ( system, service ) )
        # If everything is OK, the ping should be done within the first 10 seconds
        self.assertEquals( res[ 'Value' ][ 'service uptime' ] < 10, True )
                
        del sReactor
                  
        if db[ 'OK' ]:
          self.log.debug( 'Dropping DB %s for service' % dbName )
          res = lhcb_ci.db.dropDB( dbName )
          self.assertDIRACEquals( res[ 'OK' ], True, res )
    

#...............................................................................
#EOF