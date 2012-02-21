################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC import S_OK

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
  
  master                   = probeInfo[ 0 ]
  os.environ[ 'LFC_HOST' ] = master
    
  lfnDir  = '/lhcb/test/lfc_mirror_test/streams_propagation_test'
  gridDir = '/grid' + lfnDir
   
  _create, _remove = False, False  
    
  try:
      
    lfc2.lfc_mkdir( gridDir , 0777 )
    _create  = True
    gLogger.info( 'created %s' % gridDir )
    lfc2.lfc_rmdir( gridDir )
    _remove  = True
    gLogger.info( 'removed %s' % gridDir )
      
  except ValueError:
    _lfcMsg = 'Error manipulating directory, are you sure it does not exist ?'
    gLogger.error( _lfcMsg )
  except Exception, e:
    gLogger.error( e )

  availability = ( ( _create and _remove ) and 100 ) or 0
  
#  xmlList = []
#  xmlList.append( { 'tag' : 'id', 'nodes' : 'LHCb_LFC_Master_%s' % master } )
#  xmlList.append( { 'tag' : 'availability', 'nodes' : availability } )
#  xmlList.append( { 'tag' : 'notes', 'nodes' : 'Either 0 or 100, 0 no basic operations performed, 100 all working.' } )
#  xmlList.append( { 'tag' : 'validityduration' , 'nodes' : 'PT2H' } )
#  xmlList.append( { 'tag' : 'timestamp', 'nodes' : time.strftime( "%Y-%m-%dT%H:%M:%S" ) }) 

  notes = 'Either 0 or 100, 0 no basic operations performed, 100 all working.'

  xmlDict = {}
  xmlDict[ 'id' ]           = 'LHCb_LFC_Master_%s' % master
  xmlDict[ 'availability' ] = availability
  xmlDict[ 'notes' ]        = notes 
  xmlDict[ 'availabilityinfo' ] = ''
  xmlDict[ 'availabilitydesc' ] = ''

  return { 'xmlDict' : xmlDict, 'config' : testConfig }
#  return { 'xmlList' : xmlList, 'config' : testConfig, 'filename' : 'LHCb_LFC_Master_%s.xml' % master }  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF