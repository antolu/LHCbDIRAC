## $HeadURL: $
#''' LFCMaster
#
#  Module that runs the tests for the LFCMaster SLS sensors.
#
#'''
#
#import os
#import lfc2
#
#from DIRAC import S_OK, gLogger
#
#__RCSID__  = '$Id:  $'
#
#def getProbeElements():
#  '''
#  Gets the elements that are going to be evaluated by the probes. In this case,
#  the master LFC server.
#  '''  
#  
#  # Hardcoded, to be fixed
#  master = 'lfc-lhcb.cern.ch'
#  
#  return S_OK( [ ( master, ) ] )
#  
#
#def setupProbes( testConfig ):
#  '''
#  Sets up the environment to run the probes. In this case, it ensures the 
#  directory where temp files are going to be written exists.
#  '''  
#    
#  path = '%s/%s' % ( testConfig[ 'workdir' ], testConfig[ 'testName' ] )
#  
#  try:
#    os.makedirs( path )
#  except OSError:
#    pass # The dir exist already, or cannot be created: do nothin
#  
#  return S_OK()
#
#def runProbe( probeInfo, testConfig ):
#  '''
#  Runs the probe and formats the results for the XML generation. The probe is a 
#  mkdir and rmdir on the LFC master.
#  '''
#  
#  master                   = probeInfo[ 0 ]
##  rmc                      = probeInfo[ 1 ]
#  os.environ[ 'LFC_HOST' ] = master
#    
#  lfnDir  = '/lhcb/test/lfc_mirror_test/streams_propagation_test'
#  gridDir = '/grid' + lfnDir
#   
#  _create, _remove = False, False  
#    
#  try:
#      
#    lfc2.lfc_mkdir( gridDir , 0777 )
#    _create  = True
#    gLogger.info( 'LFCMaster: created %s' % gridDir )
#    lfc2.lfc_rmdir( gridDir )
#    _remove  = True
#    gLogger.info( 'LFCMaster: removed %s' % gridDir )
#  
#    availabilityinfo = 'Mkdir test %s, rmDir test %s' % ( _create, _remove )
#      
#  except ValueError:
#    _lfcMsg = 'Error manipulating directory %s' % gridDir
#    gLogger.error( 'LFCMaster: %s' % _lfcMsg )
#    availabilityinfo = _lfcMsg
##  except Exception, e:
##    gLogger.error( 'LFCMaster: %s' % e )
##    availabilityinfo = 'Exception running test %s' % e
#    
#  availability = (( _create and 50 ) or 0 ) + (( _remove and 50 ) or 0 )
#  
#  ## XML generation
#
#  xmlDict = {}
#  xmlDict[ 'id' ]               = 'LHCb_LFC_Master_%s' % master
#  xmlDict[ 'target' ]           = master
#  xmlDict[ 'availability' ]     = availability
#  xmlDict[ 'metric' ]           = availability  
#  xmlDict[ 'availabilityinfo' ] = availabilityinfo
#  
#  return { 'xmlDict' : xmlDict, 'config' : testConfig }#, 'rmc' : rmc } 
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF