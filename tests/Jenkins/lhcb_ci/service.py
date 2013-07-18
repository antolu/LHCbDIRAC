""" lhcb_ci.utils.service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

# Python libraries
from threading import Thread
from time      import sleep

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
 
#def serveAndPing( sReactor ):
#  """ serveAndPing
#  
#  Serve Service from ServiceReactor and ping it
#  """
#  
#  logger.debug( 'serveAndPing' )
#  
#  ServiceThread.sReactor = sReactor
#
#  server = ServiceThread()  
#  action = ServiceThread( action = 'ping' )
#  
#  server.start()
#  action.start()
#  
#  action.join( 60 )
#  if action.isAlive():
#    logger.exception( 'EXCEPTION: action thread still alive' )
#  del action
#           
#  server.join( 60 )
#  if server.isAlive():
#    logger.exception( 'EXCEPTION: server thread still alive' )
#  del server
#  
#  ServiceThread.sReactor = None
#
#  return action.actionResult

#class ServiceThread( Thread ):
#  
#  sReactor     = None
#  actionResult = None
#  
#  def __init__( self, action = '', *args, **kwargs ):
#    super( ServiceThread, self ).__init__( *args, **kwargs )
#     
#    self.action       = action
#  
#  def run( self ):
#    if self.action:
#      
#      # Sleep 2 seconds to allow the server wake up
#      sleep( 2 )
#      
#      service = self.sReactor._ServiceReactor__services.keys()[ 0 ]
#      
#      logger.debug( 'Connecting to %s' % service )
#      
#      url = self.sReactor._ServiceReactor__services[ service ]._url
#      rss = RPCClient( url )
#      
#      logger.debug( 'Requesting ping to %s' % service )
#      self.actionResult = rss.ping()
#      
#      logger.debug( 'Cleanup %s' % service )
#      # Stop while True
#      self.sReactor._ServiceReactor__alive = False
#      self.sReactor.closeListeningConnections()
#      del self.sReactor._ServiceReactor__services[ service ]
#      
#      logger.debug( 'Action Done' )
#    else:
#  
#      self.sReactor.serve()
#      logger.debug( 'End of ServiceReactor' )

def serveAndPing( sReactor ):
  """ serveAndPing
  
  Serve Service from ServiceReactor and ping it
  """
  
  logger.debug( 'serveAndPing' )
  
  server = ServiceThread( sReactor = sReactor )  
  server.start()
  
  sleep( 2 )
  service = sReactor._ServiceReactor__services.keys()[ 0 ]
      
  logger.debug( 'Connecting to %s' % service )
  
  #FIXME: somehow it does not read the url properly    
  url = sReactor._ServiceReactor__services[ service ]._url
  rss = RPCClient( url )
      
  logger.debug( 'Requesting ping to %s' % service )
  actionResult = rss.ping()
      
  logger.debug( 'Cleanup %s' % service )
  
  # Stop while True
  sReactor._ServiceReactor__alive = False
  sReactor.closeListeningConnections()
  del sReactor._ServiceReactor__services[ service ]
  
  server.join( 60 )
  if server.isAlive():
    logger.exception( 'EXCEPTION: server thread is alive' )
    return { 'OK' : False, 'Message' : 'server thread is alive' }

  return actionResult
  
#  action.start()
#  
#  action.join( 60 )
#  if action.isAlive():
#    logger.exception( 'EXCEPTION: action thread still alive' )
#  del action
#           
#  server.join( 60 )
#  if server.isAlive():
#    logger.exception( 'EXCEPTION: server thread still alive' )
#  del server
#  
#  ServiceThread.sReactor = None
#
#  return action.actionResult

class ServiceThread( Thread ):
    
  def __init__( self, sReactor = None, *args, **kwargs ):
    super( ServiceThread, self ).__init__( *args, **kwargs )
     
    self.sReactor = sReactor
  
  def run( self ):
    self.sReactor.serve()
    logger.debug( 'End of ServiceThread' )   

#...............................................................................
#EOF