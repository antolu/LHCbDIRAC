# $HeadURL: $
''' SpaceTokenOccupancy

  Module that runs the tests for the SpaceTokenOccupancy SLS sensors.
  
'''

import lcg_util
import os

from DIRAC                                import gLogger, S_OK
from DIRAC.ResourceStatusSystem.Utilities import CS

__RCSID__  = '$Id:  $'
    
def getProbeElements():
  '''
  Gets the elements that are going to be evaluated by the probes. In this case,
  all space tokens for all space token endpoints.
  '''
  
#  try:

  elementsToCheck = []      
  spaceEndpoints  = CS.getSpaceTokenEndpoints()
  spaceTokens     = CS.getSpaceTokens() 

  for site in spaceEndpoints.items():
    for spaceToken in spaceTokens:

      elementsToCheck.append( ( site, spaceToken, ) )
        
  return S_OK( elementsToCheck )    
  
#  except Exception, e:
#    _msg = 'Exception gettingProbeElements'
#    gLogger.debug( 'SpaceTokenOccupancy: %s: \n %s' % ( _msg, e ) )
#    return S_ERROR( '%s: \n %s' % ( _msg, e ) )   

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
  lcg_util check.
  '''
      
  total, free, availability  = 0, 0, 0
  siteTuple, spaceToken      = probeInfo
  site, siteDict             = siteTuple
  url                        = siteDict[ 'Endpoint' ]
    
  answer = lcg_util.lcg_stmd( spaceToken, url, True, 0 )  
  
  #SpaceToken[-4:] must be either USER, Disk, Tape
  tokenType = spaceToken[-4: ]   
  testThreshold = testConfig[ 'testThresholds' ][ tokenType ]
  
  if answer[ 0 ] == 0:
    
    output       = answer[1][0]
    total        = float( output[ 'totalsize' ] ) / 1e12 # Bytes to Terabytes
    free         = float( output[ 'unusedsize' ] ) / 1e12
    availability = 100 if free > testThreshold else ( free * 100 / total if total != 0 else 0 )
    availabilityinfo = 'Total = %s (TB), free = %s(TB), threshold = %s(TB)' % ( total, free, testThreshold )
    
  else:
    _msg = 'StorageTokenOccupancy: problem with lcg_util.lcg_stmd( "%s","%s",True,0 ) = (%d, %s)'
    _msg =  _msg % ( spaceToken, url, answer[0], answer[1] )
    gLogger.error( 'SpaceTokenOccupancy: %s' % _msg )
    gLogger.error( 'SpaceTokenOccupancy: %s' % str( answer ) )
    availabilityinfo = answer[ 2 ]
  
  ## XML generation ############################################################

  target = '%s_%s' % ( site, spaceToken )

  xmlDict = {}
  xmlDict[ 'id' ]               = 'LHCb_%s_%s' % ( testConfig[ 'testName' ], target ) 
  xmlDict[ 'target' ]           = target
  xmlDict[ 'availability' ]     = availability
  xmlDict[ 'metric' ]           = ( ( answer[ 0 ] == 0 ) and free ) or -1
  xmlDict[ 'availabilityinfo' ] = availabilityinfo
  
  if answer[ 0 ] == 0:
    # We only add this if it makes sense.
    xmlDict[ 'data' ] = [ { 'Space occupancy': [
                             ( 'numericvalue', 'Consumed', None, total - free ),
                             ( 'numericvalue', 'Capacity', None, total )
                                             ] },
                          ( 'numericvalue', 'Free space', None, free ),
                          ( 'numericvalue', 'Occupied space', None, total - free ),
                          ( 'numericvalue', 'Total space', None, total ),
                          ( 'textvalue', None, None, 'Storage space for the specific space token' )
                         ]
     
  return { 'xmlDict' : xmlDict, 'config' : testConfig }#, 'rmc' : rmc }     
     
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF