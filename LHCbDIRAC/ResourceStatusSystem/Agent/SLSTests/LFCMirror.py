################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC.Interfaces.API.Dirac                            import Dirac
from DIRAC.ResourceStausSystem.Client.ResourceStatusClient import ResourceStatusClient

import time

def getProbeElements():
  
  try:
  
    rsc = ResourceStatusClient()
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

  gLogger.info( 'Getting time till file %s exists' % lfn )
     
  f = open( fullPath, 'w' )
  f.write( 'SLSAgent at %s' % mirror )
  f.write( str( time.time() ) )
  f.close()

  gLogger.info( 'Registering file %s at %s' % ( lfn, diracSE ) )
  
  diracAPI = Dirac() 
  res      = diracAPI.addFile( lfn, fullPath, diracSE )
  
  if not res[ 'OK' ]:
    gLogger.error( res[ 'Message' ] )
    res = False
  else:
    if res[ 'Value' ][ 'Successful' ].has_key( lfn ):
      res = True
    else:
      gLogger.warn( res[ 'Value' ] )
      res = False   

  counter = -1
  while res:

    fullLfn = '%s%s' % ( '/grid', lfn )
    value   = lfc2.lfc_access( fullLfn, 0 )
    
    if value == 0 or counter == 20:
      break

    counter += 0.5
    time.sleep( 0.5 )
    
  availability = ( ( counter > -1 and  counter < 20 ) and 100 ) or 0

  res = diracAPI.removeFile( lfn )
  if not res[ 'OK' ]:
    gLogger.error( res[ 'Message' ] )
    res = False
  else:
    if res[ 'Value' ][ 'Successful' ].has_key( lfn ):
      res = True
    else:
      gLogger.warn( res[ 'Value' ] )
      res = False   

  xmlList = []
  xmlList.append( { 'tag' : 'id', 'nodes' : 'LHCb_LFC_Mirror_%s' % mirror } )
  xmlList.append( { 'tag' : 'availability', 'nodes' : availability } )
  xmlList.append( { 'tag' : 'notes', 'nodes' : 'Either 0 or 100, 0 no basic operations performed, 100 all working.' } )
  xmlList.append( { 'tag' : 'validityduration' , 'nodes' : 'PT2H' } )
  xmlList.append( { 'tag' : 'timestamp', 'nodes' : time.strftime( "%Y-%m-%dT%H:%M:%S" ) }) 

  return { 'xmlList' : xmlList, 'config' : testConfig, 'filename' : 'LHCb_LFC_Mirror_%s.xml' % mirror }  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF