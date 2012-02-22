################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC                                                  import gLogger, S_OK, S_ERROR       

from DIRAC.Interfaces.API.Dirac                             import Dirac
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

import time, os

def getProbeElements():
  
  try:
  
    rsc   = ResourceStatusClient()
    lfc_l = rsc.getResource( resourceType = 'LFC_L' )
    lfc_c = rsc.getResource( resourceType = 'LFC_C' )

    if lfc_l[ 'OK' ]:
      lfc_l = [ lfcl[0] for lfcl in lfc_l[ 'Value' ] ]
    else:
      lfc_l = []
    
    if lfc_c[ 'OK' ]:
      lfc_c = [ lfcc[0] for lfcc in lfc_c[ 'Value' ] ]
    else:
      lfc_c = []

    elementsToCheck = lfc_l + lfc_c

    return S_OK( elementsToCheck )

  except Exception, e:
    _msg = 'Exception gettingProbeElements'
    gLogger.debug( 'LFCMirror: %s: \n %s' % ( _msg, e ) )
    return S_ERROR( '%s: \n %s' % ( _msg, e ) ) 


def setupProbes( testConfig ):

  path = '%s/%s' % ( testConfig[ 'workdir' ], testConfig[ 'testName' ] )
  
  try:
    os.makedirs( path )
  except OSError:
    pass # The dir exist already, or cannot be created: do nothin
  
  return S_OK()

def runProbe( probeInfo, testConfig ):

  # Hardcoded, ugly ugly ugly !
  master = 'lfc-lhcb.cern.ch'
  mirror = probeInfo[ 0 ] 
  
  workdir  = testConfig.get( 'workdir' )
  testName = testConfig.get( 'testName' )

  path = '%s/%s' % ( workdir, testName )

  lfnPath  = '/lhcb/test/lfc-replication/%s/' % master
  fileName = 'testFile.%s' % time.time()
   
  lfn      = lfnPath + fileName
  fullPath = path + '/' + fileName
  diracSE  = 'CERN-USER'
    
  f = open( fullPath, 'w' )
  f.write( 'SLSAgent at %s' % mirror )
  f.write( str( time.time() ) )
  f.close()

  gLogger.info( 'LFCMirror: Registering file %s at %s' % ( lfn, diracSE ) )
  
  diracAPI = Dirac() 
  res      = diracAPI.addFile( lfn, fullPath, diracSE )
  
  if not res[ 'OK' ]:
    gLogger.error( 'LFCMirror: %s' % res[ 'Message' ] )
    availabilityinfo = res[ 'Message' ]
    res = False
  else:
    if res[ 'Value' ][ 'Successful' ].has_key( lfn ):
      res = True
    else:
      gLogger.warn( 'LFCMirror: %s' % res[ 'Value' ] )
      availabilityinfo = res[ 'Value' ]
      res = False   

  counter = 0
  while res:

    fullLfn = '%s%s' % ( '/grid', lfn )
    value   = lfc2.lfc_access( fullLfn, 0 )
    
    if value == 0:
      availabilityinfo = 'File accessed in %s seconds' % counter
      break
    elif counter == 120:
      availabilityinfo = 'Timeout after %s seconds' % counter
      break

    counter += 0.5
    time.sleep( 0.5 )
    
  availability = ( ( counter > -1 and counter < 120 ) and 100 ) or 0

  res = diracAPI.removeFile( lfn )
  if not res[ 'OK' ]:
    gLogger.error( 'LFCMirror: %s' % res[ 'Message' ] )
    availabilityinfo = res[ 'Message' ]
    res = False
  else:
    if res[ 'Value' ][ 'Successful' ].has_key( lfn ):
      res = True
    else:
      gLogger.warn( 'LFCMirror: %s' % res[ 'Value' ] )
      availabilityinfo = res[ 'Value' ]
      res = False  

  availability = ( res and availability ) and 0

  ## XML generation ############################################################

  xmlDict = {}
  xmlDict[ 'id' ] = 'LHCb_LFC_Mirror_%s' % mirror
  
  xmlDict[ 'availability' ]     = availability
  xmlDict[ 'availabilityinfo' ] = availabilityinfo

  return { 'xmlDict' : xmlDict, 'config' : testConfig } 

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF