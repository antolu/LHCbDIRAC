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
    gLogger.debug( 'SpaceTokenOccupancy: %s: \n %s' % ( _msg, e ) )
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
    
  answer = lcg_util.lcg_stmd( spaceToken, url, True, 0 )  
  
  if answer[ 0 ] == 0:
    
    output       = answer[1][0]
    total        = float( output[ 'totalsize' ] ) / 1e12 # Bytes to Terabytes
    guaranteed   = float( output[ 'guaranteedsize' ] ) / 1e12
    free         = float( output[ 'unusedsize' ] ) / 1e12
    availability = 100 if free > 4 else ( free * 100 / total if total != 0 else 0 )
    availabilityinfo = 'Total = %s (TB), free = %s(TB)' % ( total, free )
    
  else:
    _msg = 'StorageTokenOccupancy: problem with lcg_util.lcg_stmd( "%s","%s",True,0 ) = (%d, %s)'
    _msg =  _msg % ( spaceToken, url, answer[0], answer[1] )
    gLogger.error( 'SpaceTokenOccupancy: %s' % _msg )
    gLogger.error( 'SpaceTokenOccupancy: %s' % str( answer ) )
    availabilityinfo = answer[ 2 ]
  
  ## XML generation ############################################################

  xmlDict = {}
  xmlDict[ 'id' ] = 'LHCb_%s_%s_%s' % ( testConfig[ 'testName' ], site, spaceToken ) 
  
  xmlDict[ 'availability' ]     = availability
  xmlDict[ 'availabilityinfo' ] = availabilityinfo
  
  xmlDict[ 'data' ] = [ { 'Space occupancy': [
                             ( 'numericvalue', 'Consumed', None, total - free ),
                             ( 'numericvalue', 'Capacity', None, total )
                                             ] },
                        ( 'numericvalue', 'Free space', None, free ),
                        ( 'numericvalue', 'Occupied space', None, total - free ),
                        ( 'numericvalue', 'Total space', None, total ),
                        ( 'textvalue', None, None, 'Storage space for the specific space token' )
                       ]
     
  return { 'xmlDict' : xmlDict, 'config' : testConfig }     
     
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF