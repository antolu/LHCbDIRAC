################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC                                import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Utilities import CS

import lcg_util, time, os
    
def getProbeElements():
  
  try:

    elementsToCheck = []      
    SEs             = CS.getSpaceTokenEndpoints()
    spaceTokens     = CS.getSpaceTokens() 

    for site in SEs.items():
      for spaceToken in spaceTokens:

        elementsToCheck.append( ( site, spaceToken ) )
        
    return S_OK( elementsToCheck )    
  
  except Exception, e:
    _msg = 'Exception gettingProbeElements'
    gLogger.debug( '%s: \n %s' % ( _msg, e ) )
    return S_ERROR( '%s: \n %s' % ( _msg, e ) )   

################################################################################

def setupProbes( testConfig ):
  
  path = '%s/%s' % ( testConfig[ 'workdir' ], testConfig[ 'testName' ] )
  
  try:
    os.makedirs( path )
  except OSError:
    pass # The dir exist already, or cannot be created: do nothin
  
  return S_OK()

################################################################################

def runProbe( probeInfo, testConfig ):  
      
  total, guaranteed, free, availability = 0, 0, 0, 0
  
  siteTuple, spaceToken      = probeInfo
  site, siteDict             = siteTuple
  url                        = siteDict[ 'Endpoint' ]
  validityduration           = 'PT0M'  
  notes                      = 'Probe run without problems.'
    
  filename                   = 'LHCb_%s_%s_%s' % ( testConfig[ 'testName' ], site, spaceToken ) 
    
  answer = lcg_util.lcg_stmd( spaceToken, url, True, 0 )  
  
  if answer[ 0 ] == 0:
    
    output       = answer[1][0]
    total        = float( output[ 'totalsize' ] ) / 1e12 # Bytes to Terabytes
    guaranteed   = float( output[ 'guaranteedsize' ] ) / 1e12
    free         = float( output[ 'unusedsize' ] ) / 1e12
    availability = 100 if free > 4 else ( free * 100 / total if total != 0 else 0 )
    
    validityduration = testConfig[ 'validityduration' ]
  else:
    _msg = 'StorageTokenOccupancy: problem with lcg_util.lcg_stmd( "%s","%s",True,0 ) = (%d, %s)'
    gLogger.info(  _msg % ( spaceToken, url, answer[0], answer[1] ) )
    gLogger.info( str( answer ) )
    notes = str( answer ) 
  
  ## Now, write xmlList 
  
  xmlList = []
  xmlList.append( { 'tag' : 'id', 'nodes' : filename } )
  xmlList.append( { 'tag' : 'availability', 'nodes' : availability } )
  xmlList.append( { 'tag' : 'notes', 'nodes' : notes } )  
    
  thresholdNodes = []
  for t,v in testConfig[ 'thresholds' ].items():
    thresholdNodes.append( { 'tag' : 'threshold', 'attrs' : [ ( 'level', t ) ], 'nodes' : v } )
    
  xmlList.append( { 'tag' : 'availabilitythresholds', 'nodes' : thresholdNodes } )
    
  xmlList.append( { 'tag' : 'availabilityinfo' , 'nodes' : 'Free=%s Total=%s' % ( free, total ) } )
  xmlList.append( { 'tag' : 'availabilitydesc' , 'nodes' : testConfig[ 'availabilitydesc' ] } )
  xmlList.append( { 'tag' : 'refreshperiod'    , 'nodes' : testConfig[ 'refreshperiod' ] } )
  xmlList.append( { 'tag' : 'validityduration' , 'nodes' : validityduration } )

  dataNodes = []
  grpNodes  = []
  grpNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'name', 'Consumed' ) ], 'nodes' : total - free } ) 
  grpNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'name', 'Capacity' ) ], 'nodes' : total } )
    
  dataNodes.append( { 'tag' : 'grp', 'attrs' : [ ( 'name', 'Space occupancy' ) ], 'nodes' : grpNodes } )
    
  dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'name', 'Free space' ) ], 'nodes' : free } )
  dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'name', 'Occupied space' ) ], 'nodes' : total - free } )
  dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'name', 'Total space' ) ], 'nodes' : total } )
  dataNodes.append( { 'tag' : 'textvalue', 'nodes' : 'Storage space for the specific space token' } )
    
  xmlList.append( { 'tag' : 'data', 'nodes' : dataNodes } )
  xmlList.append( { 'tag' : 'timestamp', 'nodes' : time.strftime( "%Y-%m-%dT%H:%M:%S" ) })
     
  return { 'xmlList' : xmlList, 'config' : testConfig, 'filename' : '%s.xml' % filename }
     
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF