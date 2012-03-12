# $HeadURL:  $
''' VOBOX

  Module that runs the tests for the VOBOX SLS sensors.

'''

import urlparse
import os

from DIRAC                      import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient

__RCSID__  = '$Id: $'

def getProbeElements():
  '''
  Gets the elements that are going to be evaluated by the probes. In this case,
  three DIRAC services: RequestManagement, ConfigurationService and 
  SystemAdministrator.
  '''

  try:
  
    request_management_urls = gConfig.getValue( '/Systems/RequestManagement/Development/URLs/allURLS', [] )
    configuration_urls      = gConfig.getServersList()
    framework_urls          = gConfig.getValue( '/DIRAC/Framework/SystemAdministrator', [] )
    
    elementsToCheck = request_management_urls + configuration_urls + framework_urls 
  
    return S_OK( elementsToCheck )    
  
  except Exception, e:
    _msg = 'Exception gettingProbeElements'
    gLogger.debug( 'VOBOX: %s: \n %s' % ( _msg, e ) )
    return S_ERROR( '%s: \n %s' % ( _msg, e ) ) 

def setupProbes( testConfig ):
  '''
  Sets up the environment to run the probes. In this case, it ensures the 
  directory where temp files are going to be written exists.
  '''

  path = '%s/%s' % ( testConfig[ 'workdir' ], testConfig[ 'testName' ] )
  
  try:
    os.makedirs( path )
  except OSError:
    pass # The dir exist already, or cannot be created: do nothin
  
  return S_OK()

def runProbe( probeInfo, testConfig ):
  '''
  Runs the probe and formats the results for the XML generation. The probe is a 
  DIRAC.ping()  
  '''

  shortNames = { 
                 'RequestManagement' : 'ReqMgmt',
                 'Configuration'     : 'ConfigSvc',
                 'Framework'         : 'SysAdmin'
                }

  availability, suptime, muptime = 0, 0, 0
  url                            = probeInfo
    
  parsed           = urlparse.urlparse( url )
  system, _service = parsed.path.strip("/").split("/")
  site             = parsed.netloc.split(":")[0]

  pinger = RPCClient( url )
  res    = pinger.ping()  

  if res[ 'OK' ]:
       
    availability     = 100
    suptime          = res[ 'Value' ].get( 'service uptime', 0 )
    muptime          = res[ 'Value' ].get( 'host uptime', 0 )
    availabilityinfo = 'Service %s is up and running' % url
    
  else:
      
    availabilityinfo = res[ 'Message' ]

  ## XML generation ############################################################
  
  target = '%s_%s' % ( site, shortNames[ system ] )
  
  xmlDict = {}
  xmlDict[ 'id' ]               = 'LHCb_VOBOX_%s' % target
  xmlDict[ 'target' ]           = target
  xmlDict[ 'availability' ]     = availability
  xmlDict[ 'metric' ]           = ( res[ 'OK' ] and 100 ) or -1
  xmlDict[ 'availabilityinfo' ] = availabilityinfo
  
  xmlDict[ 'data' ] = [ #node name, name attr, desc attr, node value
                       ( 'numericvalue', 'Service Uptime', 'Seconds since last restart of service', suptime ),
                       ( 'numericvalue', 'Host Uptime',    'Seconds since last restart of machine', muptime )
                      ] 
  
  if system == 'RequestManagement':
    res = pinger.getDBSummary()
    if not res[ 'OK' ]:
      gLogger.warn( 'Error getting DB Summary for %s \n. %s' % ( url, res[ 'Message' ] ) )
    else:  
    
      res = res[ 'Value' ]
    
      for key, value in res.items():
        
        xmlDict[ 'data' ].append( ( 'numericvalue', '%s - Assigned' % key,
                                    'Number of Assigned %s requests' % key, value[ 'Assigned' ] ) )
        xmlDict[ 'data' ].append( ( 'numericvalue', '%s - Waiting' % key,
                                    'Number of Waiting %s requests' % key, value[ 'Waiting' ] ) )
        xmlDict[ 'data' ].append( ( 'numericvalue', '%s - Done' % key,
                                    'Number of Done %s requests' % key, value[ 'Done' ] ) )
            
  return { 'xmlDict' : xmlDict, 'config' : testConfig }   
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF