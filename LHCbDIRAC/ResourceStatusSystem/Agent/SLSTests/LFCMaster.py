################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC import S_OK, gLogger

import os, time
import lfc2

def getProbeElements():
  
  # Hardcoded, to be fixed
  master = 'lfc-lhcb.cern.ch'
  
  return S_OK( [ master ] )
  

def setupProbes( testConfig ):
  
  path = '%s/%s' % ( testConfig[ 'workdir' ], testConfig[ 'testName' ] )
  
  try:
    os.makedirs( path )
  except OSError:
    pass # The dir exist already, or cannot be created: do nothin
  
  return S_OK()
  

def runProbe( probeInfo, testConfig ):
  
  master                   = probeInfo
  os.environ[ 'LFC_HOST' ] = master
    
  lfnDir  = '/lhcb/test/lfc_mirror_test/streams_propagation_test'
  gridDir = '/grid' + lfnDir
   
  _create, _remove = False, False  
    
  try:
      
    lfc2.lfc_mkdir( gridDir , 0777 )
    _create  = True
    gLogger.info( 'LFCMaster: created %s' % gridDir )
    lfc2.lfc_rmdir( gridDir )
    _remove  = True
    gLogger.info( 'LFCMaster: removed %s' % gridDir )
  
    availabilityinfo = 'Mkdir test %s, rmDir test %s' % ( _create, _remove )
      
  except ValueError:
    _lfcMsg = 'Error manipulating directory %s' % gridDir
    gLogger.error( 'LFCMaster: %s' % _lfcMsg )
    availabilityinfo = _lfcMsg
  except Exception, e:
    gLogger.error( 'LFCMaster: %s' % e )
    availabilityinfo = 'Exception running test %s' % e
    
  availability = (( _create and 50 ) or 0 ) + (( _remove and 50 ) or 0 )
  
  ## XML generation

  xmlDict = {}
  xmlDict[ 'id' ]               = 'LHCb_LFC_Master_%s' % master
  xmlDict[ 'availability' ]     = availability 
  xmlDict[ 'availabilityinfo' ] = availabilityinfo
  
  return { 'xmlDict' : xmlDict, 'config' : testConfig } 

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF