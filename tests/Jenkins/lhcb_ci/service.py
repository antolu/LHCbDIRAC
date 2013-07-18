""" lhcb_ci.utils.service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

# Python libraries
from threading import Thread

# DIRAC
from DIRAC.Core.DISET.RPCClient      import RPCClient
from DIRAC.Core.DISET.ServiceReactor import ServiceReactor
from DIRAC.Core.Utilities            import InstallTools

# lhcb_ci
from lhcb_ci            import logger
from lhcb_ci.extensions import getExtensions


def getSoftwareServices():
  """ getCodedServices
  
  Gets the available services inspecting the CODE.
  """

  logger.debug( 'getSoftwareServices' )
  
  extensions = getExtensions()
  res = InstallTools.getSoftwareComponents( extensions )
  # Always return S_OK
  return res[ 'Value' ][ 'Services' ]


def getInstalledServices():
  """ getRunitServices
  
  Gets the available services inspecting runit ( aka installed ).
  """

  logger.debug( 'getInstalledServices' )
  
  res = InstallTools.getInstalledComponents()
  # Always return S_OK
  return res[ 'Value' ][ 'Services' ]


def setupService( system, service ):
  """ setupService
  
  Setups service and runs it
  """  

  logger.debug( 'setupService' )
  
  extensions = getExtensions()
  
  return InstallTools.setupComponent( 'service', system, service, extensions )


def uninstallService( system, service ):
  """ uninstallService
  
  Stops the service.
  """

  logger.debug( 'uninstallService for %s/%s' % ( system, service ) )
  
  return InstallTools.uninstallComponent( system, service )


def initializeServiceReactor( system, service ):
  """ initializeServiceReactor
  
  Initializes the ServiceReactor for a given system/service
  """
  
  logger.debug( 'initializeServiceReactor for %s/%s' % ( system, service ) )
  
  sReactor = ServiceReactor()
  
  res = sReactor.initialize( [ '%s/%s' % ( system, service ) ] )
  if res[ 'OK' ]:
    res[ 'Value' ] = sReactor
    
  return res  
 
def serveAndPing( sReactor ):
  """ serveAndPing
  
  Serve Service from ServiceReactor and ping it
  """
  
  logger.debug( 'serveAndPing' )
  
  ServiceThread.sReactor = sReactor
  
  action = ServiceThread( action = 'ping' ).start()
  server = ServiceThread().start()
  
  action.join( 60 )
  if action.isAlive():
    logger.exception( 'EXCEPTION: action thread still alive' )
  del action
           
  server.join( 60 )
  if server.isAlive():
    logger.exception( 'EXCEPTION: server thread still alive' )
  del server
  
  ServiceThread.sReactor = None

  return ServiceThread.actionResult

class ServiceThread( Thread ):
  
  sReactor     = None
  actionResult = None
  
  def __init__( self, action = '', *args, **kwargs ):
    super( ServiceThread, self ).__init__( *args, **kwargs )
     
    self.actionResult = None
    self.action       = action
  
  def run( self ):
    if self.action:
      
      service = self.sReactor._ServiceReactor__services.keys()[ 0 ]
      
      logger.debug( 'Connecting to %s' % service )
      
      url = self.sReactor._ServiceReactor__services[ service ]._url
      rss = RPCClient( url )
      
      logger.debug( 'Requesting ping to %s' % service )
      self.actionResult = rss.ping()
      
      logger.debug( 'Cleanup %s' % service )
      # Stop while True
      self.sReactor._ServiceReactor__alive = False
      self.sReactor.closeListeningConnections()
      del self.sReactor._ServiceReactor__services[ service ]
      
      logger.debug( 'Action Done' )
    else:
  
      self.sReactor.serve()
      logger.debug( 'End of ServiceReactor' )





#from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
#localCfg = LocalConfiguration()
#localCfg.isParsed = True
#localCfg.loadUserData()
#
#from time import sleep
#from threading import Thread
#
#from DIRAC.Core.DISET.ServiceReactor import ServiceReactor
#
#sReactor = ServiceReactor()
#result = sReactor.initialize( [ 'ResourceStatus/ResourceStatus' ] )
#print result
#
#from DIRAC.Core.DISET.RPCClient import RPCClient
#
#
#class T( Thread ):
#  def run( self ):
#    if self.getName() == 'ping':
#      print 'sleep1'
#      sleep( 15 )
#      url = sReactor._ServiceReactor__services[ 'ResourceStatus/ResourceStatus' ]._url
#      rss = RPCClient( url )
#      print rss.ping()
#      # Stop while True
#      sReactor._ServiceReactor__alive = False
#      sReactor.closeListeningConnections()
#      del sReactor._ServiceReactor__services[ 'ResourceStatus/ResourceStatus' ]
#      print 'Ping Done'
#    else:
#      sReactor.serve()
#      print 'SReactor Done'
#      print dir( sReactor )

  
  
  
  
  
    

#...............................................................................
#EOF