""" lhcb_ci.utils.service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

import threading

# Python libraries
from threading import Thread
from time      import sleep

# DIRAC
from DIRAC.Core.DISET.private.ServiceConfiguration import ServiceConfiguration
from DIRAC.Core.DISET.RPCClient                    import RPCClient
from DIRAC.Core.DISET.ServiceReactor               import ServiceReactor
from DIRAC.Core.Utilities                          import InstallTools


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


def getServicePort( system, service ):
  """ getServicePort
  
  Given a system and a service, returns its configured port.
  """

  serviceName = '%s/%s' % ( system.replace( 'System', '' ), service )

  servConf = ServiceConfiguration( [ serviceName ] )
  return servConf.getPort()


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
  
  server = ServiceThread( sReactor = sReactor )  
  server.start()
  
  sleep( 2 )
  
  serviceName = sReactor._ServiceReactor__services.keys()[ 0 ]
  service     = sReactor._ServiceReactor__services[ serviceName ]
      
  logger.debug( 'Connecting to %s' % serviceName )
  
  #FIXME: somehow it does not read the url properly    
  url = service._url
  rss = RPCClient( url )
      
  logger.debug( 'Requesting ping to %s' % serviceName )
  actionResult = rss.ping()
      
  logger.debug( 'Cleanup %s' % serviceName )
  
  # Stop while True
  sReactor._ServiceReactor__alive = False
  sReactor.closeListeningConnections()
  
#  # And delete Service object from dictionary
#  #FIXME: maybe we do not need to do this
#  del sReactor._ServiceReactor__services[ serviceName ]
    
  server.join( 60 )
  if server.isAlive():
    logger.exception( 'EXCEPTION: server thread is alive' )
    actionResult = { 'OK' : False, 'Message' : 'server thread is alive' }
    
  return actionResult 


def killThreads( threadsToBeAvoided = [] ):
  """ killThreads
  
  Kills leftover threads to prevent them from interacting with the next execution.
  """
  
  for th in threading.enumerate():
    
    if th in threadsToBeAvoided:
      continue
    
    if th.isAlive():
      try:
        
        th._Thread__stop()
                
        del threading._active[ th.ident ]
      except:
        logger.debug( 'Cannot kill thread %s : %s' % ( th.ident, th.name ) )  
  

class ServiceThread( Thread ):
  """ ServiceThread
  
  Runs on a separate thread a DIRAC service to allow querying it.
  """
    
  def __init__( self, sReactor = None, *args, **kwargs ):
    super( ServiceThread, self ).__init__( *args, **kwargs )
     
    self.sReactor = sReactor
  
  def run( self ):
    self.sReactor.serve()
    logger.debug( 'End of ServiceThread' )   

#...............................................................................
#EOF