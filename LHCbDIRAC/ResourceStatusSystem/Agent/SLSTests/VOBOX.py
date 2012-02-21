################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC                        import gConfig, gLogger, S_OK, S_ERROR
from DIRAC                        import gConfig 
from DIRAC.Core.DISET.RPCClient   import RPCClient

import sys, time, urlparse, os

def getProbeElements():
  
  try:
  
    request_management_urls = gConfig.getValue( '/Systems/RequestManagement/Development/URLs/allURLS', [] )
    configuration_urls      = gConfig.getServersList()
    framework_urls          = gConfig.getValue( '/DIRAC/Framework/SystemAdministrator', [] )
    
    elementsToCheck = request_management_urls + configuration_urls + framework_urls 
  
    return S_OK( elementsToCheck )    
  
  except Exception, e:
    _msg = 'Exception gettingProbeElements'
    gLogger.debug( '%s: \n %s' % ( _msg, e ) )
    return S_ERROR( '%s: \n %s' % ( _msg, e ) ) 

def setupProbes( testConfig ):
  
  path = '%s/%s' % ( testConfig[ 'workdir' ], testConfig[ 'testName' ] )
  
  try:
    os.makedirs( path )
  except OSError:
    pass # The dir exist already, or cannot be created: do nothin
  
  return S_OK()

def runProbe( probeInfo, testConfig ):

  shortNames = { 
                 'RequestManagement' : 'ReqMgmt',
                 'Configuration'     : 'ConfigSvc',
                 'Framework'         : 'SysAdmin'
                }

  availability, suptime, muptime, notes = 0, 0, 0, ''
  url                                   = probeInfo
    
  parsed = urlparse.urlparse( url )
    
  if sys.version_info >= (2,6):
    system, _service = parsed.path.strip("/").split("/")
    site = parsed.netloc.split(":")[0]

  pinger = RPCClient( url )
  res    = pinger.ping()  

  if res[ 'OK' ]:
      
    res = res[ 'Value' ]
       
    availability = 100
    suptime      = res.get( 'service uptime', 0 )
    muptime      = res.get( 'host uptime', 0 )
    notes        = 'Service %s completely up and running' % url
    
  else:
      
    notes = res[ 'Message' ]  

#  xmlList = []
#  xmlList.append( { 'tag' : 'id', 'nodes' : 'LHCb_VOBOX_%s_%s' % ( site, shortNames[ system ] ) } )
#  xmlList.append( { 'tag' : 'availability', 'nodes' : availability } )
#  xmlList.append( { 'tag' : 'notes', 'nodes' : notes } )
#    
#  dataNodes = []
#  dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'desc', 'Seconds since last restart of service' ),
#                                                            ( 'name', 'Service Uptime') ], 'nodes' : suptime } )
#  dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'desc', 'Seconds since last restart of machine' ),
#                                                            ( 'name', 'Host Uptime') ], 'nodes' : muptime } )
              
#  xmlList.append( { 'tag' : 'data', 'nodes' : dataNodes } )
#  xmlList.append( { 'tag' : 'timestamp', 'nodes' : time.strftime( "%Y-%m-%dT%H:%M:%S" ) })
  
  xmlDict = {}
  xmlDict[ 'id' ]          = 'LHCb_VOBOX_%s_%s' % ( site, shortNames[ system ] )
  xmlDict[ 'availability'] = availability
  xmlDict[ 'availabilityinfo' ] = ''
  xmlDict[ 'availabilitydesc' ] = ''
  xmlDict[ 'notes' ]       = notes
  xmlDict[ 'data' ]        = [ #node name, name attr, desc attr, node value
                               ( 'numericvalue', 'Service Uptime', 'Seconds since last restart of service', suptime ),
                               ( 'numericvalue', 'Host Uptime',    'Seconds since last restart of machine', muptime )
                              ] 
  
  if system == 'RequestManagement':
    res = pinger.getDBSummary()
    if not res[ 'OK' ]:
      gLogger.warn( 'Error getting DB Summary for %s \n. %s' % ( url, res[ 'Message' ] ) )
    else:  
    
      res = res[ 'Value' ]
    
      for k,v in res.items():
        
        xmlDict[ 'data' ].append( ( 'numericvalue', '%s - Assigned' % k,
                                    'Number of Assigned %s requests' % k, v[ 'Assigned' ] ) )
        xmlDict[ 'data' ].append( ( 'numericvalue', '%s - Waiting' % k,
                                    'Number of Waiting %s requests' % k, v[ 'Waiting' ] ) )
        xmlDict[ 'data' ].append( ( 'numericvalue', '%s - Done' % k,
                                    'Number of Done %s requests' % k, v[ 'Done' ] ) )
          

#        dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'desc', 'Number of Assigned %s requests' % k ),
#                                                                  ( 'name', '%s - Assigned' % k) ], 
#                              'nodes' : v[ 'Assigned' ] } )
#          
#        dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'desc', 'Number of Waiting %s requests' % k ),
#                                                                  ( 'name', '%s - Waiting' % k) ], 
#                              'nodes' : v[ 'Assigned' ] } )
#          
#        dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'desc', 'Number of Done %s requests' % k ),
#                                                                  ( 'name', '%s - Waiting' % k) ], 
#                              'nodes' : v[ 'Done' ] } )  
  
  
  return { 'xmlDict' : xmlDict, 'config' : testConfig }   
    
#  return { 
#           'xmlList' : xmlList, 'config' : testConfig, 
#           'filename' : 'LHCb_VOBOX_%s_%s.xml' % ( site, shortNames[ system ] )
#           }  
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF