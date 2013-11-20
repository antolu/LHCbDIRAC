"""  The ClientTools module provides additional functions for use by users
     of the DIRAC client in the LHCb environment.
"""

import os, tempfile, time

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.List          import breakListIntoChunks, sortList
from DIRAC.Core.Utilities.Subprocess    import systemCall, shellCall

from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getScriptsLocation, getProjectCommand, runEnvironmentScripts

def _errorReport( error, message = None ):
  """Internal function to return errors and exit with an S_ERROR()
  """
  if not message:
    message = error

  gLogger.warn( error )
  return S_ERROR( message )

#############################################################################
def readFileEvents( turl, appVersion ):
  """ Open the supplied file through Gaudi, read the events, and return the timing for each operation """
  # Setup the application enviroment
  gLogger.info( "Setting up the DaVinci %s environment" % appVersion )
  startTime = time.time()
  res = setupProjectEnvironment( 'DaVinci', version = appVersion )
  if not res['OK']:
    return _errorReport( res['Message'], "Failed to setup the Gaudi environment" )
  gaudiEnv = res['Value']
  gLogger.info( "DaVinci %s environment successful in %.1f seconds" % ( appVersion, time.time() - startTime ) )
  workingDirectory = os.getcwd()
  fopen = open( '%s/.rootrc' % workingDirectory, 'w' )
  fopen.write( 'XNet.Debug: 3\n' )
  fopen.write( 'XrdSecDEBUG: 10\n' )
  fopen.close()
  fopen = open( '%s/GaudiScript.py' % workingDirectory, 'w' )
  fopen.write( 'import GaudiPython\n' )
  fopen.write( 'from Gaudi.Configuration import *\n' )
  fopen.write( 'import time\n' )
  fopen.write( 'appMgr = GaudiPython.AppMgr(outputlevel=6)\n' )
  fopen.write( 'appMgr.config( files = ["$GAUDIPOOLDBROOT/options/GaudiPoolDbRoot.opts"])\n' )
  fopen.write( 'sel = appMgr.evtsel()\n' )
  fopen.write( 'startTime = time.time()\n' )
  fopen.write( 'sel.open(["%s"])\n' % turl )
  fopen.write( 'evt = appMgr.evtsvc()\n' )
  fopen.write( 'appMgr.run(1)\n' )
  fopen.write( 'oOpenTime = open("%s/OpenTime.txt","w")\n' % workingDirectory )
  fopen.write( 'openTime = time.time()-startTime\n' )
  fopen.write( 'oOpenTime.write("%.4f" % openTime)\n' )
  fopen.write( 'oOpenTime.close()\n' )
  fopen.write( 'readTimes = []\n' )
  fopen.write( 'failedEvents = 0\n' )
  fopen.write( 'while 1:\n' )
  fopen.write( '  startTime = time.time()\n' )
  fopen.write( '  res = appMgr.run(1)\n' )
  fopen.write( '  readTime = time.time()-startTime\n' )
  fopen.write( '  if res.FAILURE:\n' )
  fopen.write( '    failedEvents+=1\n' )
  fopen.write( '    print "ERROR",res.getCode()\n' )
  fopen.write( '  if not evt["/Event"]:\n' )
  fopen.write( '    break\n' )
  fopen.write( '  readTimes.append(readTime)\n' )
  fopen.write( 'oReadTime = open("%s/ReadTime.txt","w")\n' % workingDirectory )
  fopen.write( 'for readTime in readTimes:\n' )
  fopen.write( '  oReadTime.write("%s\\n" % readTime)\n' )
  fopen.write( 'oReadTime.close()\n' )
  fopen.write( 'print "SUCCESS"\n' )
  fopen.close()
  gLogger.info( "Generated GaudiPython script at %s/GaudiScript.py" % workingDirectory )
  # Execute the root script
  cmd = ['python']
  cmd.append( '%s/GaudiScript.py' % workingDirectory )
  gLogger.info( "Executing GaudiPython script: %s" % cmd )
  res = systemCall( 1800, cmd, env = gaudiEnv )  # ,callbackFunction=log)
  if not res['OK']:
    return _errorReport( res['Message'], "Failed to execute %s" % cmd )
  errorCode, stdout, stderr = res['Value']
  if errorCode:
    return _errorReport( stderr, "Failed to execute %s: %s" % ( cmd, errorCode ) )
  oOutput = open( '%s/full.output' % ( workingDirectory ), 'w' )
  oOutput.write( stdout )
  oOutput.close()
  oError = open( '%s/full.error' % ( workingDirectory ), 'w' )
  for key in sortList( gaudiEnv.keys() ):
    oError.write( "%s : %s\n" % ( key.ljust( 25 ), gaudiEnv[key] ) )
  oError.write( stderr )
  oError.close()
  resDict = {}
  oOpenTime = open( '%s/OpenTime.txt' % workingDirectory )
  resDict['OpenTime'] = eval( oOpenTime.read() )
  oOpenTime.close()
  oReadTime = open( '%s/ReadTime.txt' % workingDirectory )
  resDict['ReadTimes'] = []
  for readTime in oReadTime.read().splitlines():
    resDict['ReadTimes'].append( float( readTime ) )
  oReadTime.close()
  return S_OK( resDict )

#############################################################################

def mergeRootFiles( outputFile, inputFiles, daVinciVersion = '', cleanUp = True ):
  """ merge several ROOT files """
  # Setup the root enviroment
  res = setupProjectEnvironment( 'DaVinci', version = daVinciVersion )
  if not res['OK']:
    return _errorReport( res['Message'], "Failed to setup the ROOT environment" )
  rootEnv = res['Value']
  # Perform the merging
  # Perform the merging
  chunkSize = 20
  if len( inputFiles ) > chunkSize:
    lists = breakListIntoChunks( inputFiles, chunkSize )
    tempFiles = []
    try:
      for filelist in lists:
        tempOutputFile = tempfile.mktemp()
        tempFiles.append( tempOutputFile )
        res = _mergeRootFiles( tempOutputFile, filelist, rootEnv )
        if not res['OK']:
          return _errorReport( res['Message'], "Failed to perform ROOT merger" )
      res = _mergeRootFiles( outputFile, tempFiles, rootEnv )
      if not res['OK']:
        return _errorReport( res['Message'], "Failed to perform final ROOT merger" )
    except Exception:
      errStr = 'Exception while merging files'
      gLogger.exception( errStr )
      return S_ERROR( errStr )
    finally:
      if cleanup:
        for filename in tempFiles:
          if os.path.exists( filename ):
            os.remove( filename )
  else:
    res = _mergeRootFiles( outputFile, inputFiles, rootEnv )
    if not res['OK']:
      return _errorReport( res['Message'], "Failed to perform ROOT merger" )
  return S_OK( outputFile )

#############################################################################
def _mergeRootFiles( outputFile, inputFiles, rootEnv ):
  """ Merge ROOT files """
  cmd = "hadd -f %s " % outputFile + ' '.join( inputFiles )
  res = shellCall( 1800, cmd, env = rootEnv )
  if not res['OK']:
    return res
  returncode, _stdout, stderr = res['Value']
  if returncode:
    return _errorReport( stderr, "Failed to merge root files" )
  return S_OK()

#############################################################################
def setupProjectEnvironment( project, version = '' ):
  """ Uses the ProductionEnvironment utility to get a basic project environment
      necessary for some of the commands here.  CMTCONFIG / SystemConfiguration
      is taken from the local environment by default as no specific platform is
      required.
  """
  result = getScriptsLocation()
  if not result['OK']:
    return result

  lbLogin = result['Value']['LbLogin.sh']
  setupProjectLocation = result['Value']['SetupProject.sh']
  mySiteRoot = result['Value']['MYSITEROOT']

  env = dict( os.environ )
  gLogger.verbose( 'Setting MYSITEROOT to %s' % ( mySiteRoot ) )
  env['MYSITEROOT'] = mySiteRoot

  result = getProjectCommand( setupProjectLocation, project, version )
  if not result['OK']:
    return result

  setupProject = result['Value']
  return runEnvironmentScripts( [lbLogin, setupProject], env )
