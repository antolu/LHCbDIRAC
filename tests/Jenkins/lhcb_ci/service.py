""" lhcb_ci.service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

# Python libraries
from threading import Thread
from time      import sleep

# DIRAC
from DIRAC                                         import gConfig 
from DIRAC.ConfigurationSystem.Client              import PathFinder
from DIRAC.Core.DISET.private.MessageBroker        import MessageBroker
#from DIRAC.Core.DISET.private.Service              import Service
#from DIRAC.Core.DISET.private.ServiceConfiguration import ServiceConfiguration
from DIRAC.Core.DISET.RPCClient                    import RPCClient
from DIRAC.Core.DISET.ServiceReactor               import ServiceReactor
from DIRAC.Core.Security                           import Properties
from DIRAC.Core.Utilities                          import InstallTools

# lhcb_ci
from lhcb_ci            import logger
from lhcb_ci.extensions import getCSExtensions


def getSoftwareServices():
  """ getCodedServices
  
  Gets the available services inspecting the CODE.
  """

  logger.debug( 'getSoftwareServices' )
  
  extensions = getCSExtensions()
  res = InstallTools.getSoftwareComponents( extensions )
  
  # Always return S_OK
  serviceDict = {}
  # The method is a bit buggy, so we have to fix it here.
  for systemName, serviceList in res[ 'Value' ][ 'Services' ].items():
    serviceDict[ systemName + 'System' ] = list( set( serviceList ) )
  
  return serviceDict  


def getInstalledServices():
  """ getRunitServices
  
  Gets the available services inspecting runit ( aka installed ).
  """

  logger.debug( 'getInstalledServices' )
  
  res = InstallTools.getInstalledComponents()
  # Always return S_OK
  return res[ 'Value' ][ 'Services' ]


def getSecurityProperties():
  """ getProperties

  Gets all security properties.  
  """

  properties = []

  for propName in dir( Properties ):
    if propName.startswith( '__' ):
      continue
    
    properties.append( getattr( Properties, propName ).lower() )
  
  return properties          


def initializeServiceClass( serviceClass, serviceName ):
  """ initializeServiceClass
  
  The RequestHandler class from where all services inherit has been designed in
  such a way that requires to run the class method _rh__initializeClass before
  instantiating any Service object.
  
  """
  
  msgBroker = MessageBroker( serviceName )
  serviceClass._rh__initializeClass( { 'serviceName' : serviceName }, None, 
                                     msgBroker, None )


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


def serve( sReactor ):
  
  server = ServiceThread( sReactor = sReactor )  
  server.start()
  
  sleep( 2 )
  
  serviceName = sReactor._ServiceReactor__services.keys()[ 0 ]
  service     = sReactor._ServiceReactor__services[ serviceName ]

  return server, serviceName, service

def unserve( server ):

  # Stop while True
  server.sReactor._ServiceReactor__alive = False
  server.sReactor.closeListeningConnections()
  
#  # And delete Service object from dictionary
#  #FIXME: maybe we do not need to do this
#  del sReactor._ServiceReactor__services[ serviceName ]
    
  server.join( 60 )
  if server.isAlive():
    logger.exception( 'EXCEPTION: server thread is alive' )
    return False
  
  return True


def serveAndPing( sReactor ):
  """ serveAndPing
  
  Serve Service from ServiceReactor and ping it
  """
  
  logger.debug( 'serveAndPing' )
  
  server, serviceName, service = serve( sReactor )    
  logger.debug( 'Connecting to %s' % serviceName )
  
  #FIXME: somehow it does not read the url properly    
  url = service._url
  rss = RPCClient( url )
      
  logger.debug( 'Requesting ping to %s' % serviceName )
  actionResult = rss.ping()
      
  logger.debug( 'Cleanup %s' % serviceName )
  
  if not unserve( server ):
    actionResult = { 'OK' : False, 'Message' : 'server thread is alive' }
  
  return actionResult
  

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