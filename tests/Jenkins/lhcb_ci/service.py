""" lhcb_ci.service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

# Python libraries
from threading import Thread

# DIRAC
from DIRAC.Core.DISET.private.MessageBroker        import MessageBroker
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