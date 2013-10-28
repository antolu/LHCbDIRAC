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
  res = __setupProjectEnvironment( 'DaVinci', version = appVersion )
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
def getRootFilesGUIDs( fileNames, cleanUp = True ):
  """ Bulk function for getting the GUIDs for a list of files
  """
  # Setup the root enviroment
  res = __setupProjectEnvironment( 'DaVinci' )
  if not res['OK']:
    return _errorReport( res['Message'], "Failed to setup the ROOT environment" )
  rootEnv = res['Value']
  fileGUIDs = {}
  for fileName in fileNames:
    fileGUIDs[fileName] = _getROOTGUID( fileName, rootEnv )
  return S_OK( fileGUIDs )

#############################################################################
def getRootFileGUID( fileName, cleanUp = True ):
  """ Function to retrieve a file GUID using Root.
  """
  # Setup the root enviroment
  res = __setupProjectEnvironment( 'DaVinci' )
  if not res['OK']:
    return _errorReport( res['Message'], "Failed to setup the ROOT environment" )
  rootEnv = res['Value']
  return S_OK( _getROOTGUID( fileName, rootEnv ) )

#############################################################################
def _getROOTGUID( rootFile, rootEnv, cleanUp = True ):
  """ function to get the GUID of the ROOT file """
  # Write the script to be executed
  fopen = open( 'tmpRootScript.py', 'w' )
  fopen.write( 'from ROOT import TFile\n' )
  fopen.write( "l=TFile.Open('%s')\n" % rootFile )
  fopen.write( "if not( not( l.Get('##Params') ) ): #if the structure is '##Params'\n" )
  fopen.write( "  t=l.Get(\'##Params\')\n" )
  fopen.write( '  t.Show(0)\n' )
  fopen.write( '  leaves=t.GetListOfLeaves()\n' )
  fopen.write( '  leaf=leaves.UncheckedAt(0)\n' )
  fopen.write( '  val=leaf.GetValueString()\n' )
  fopen.write( "  fid=val.split('=')[2].split(']')[0]\n" )
  fopen.write( "  print 'GUID%sGUID' %fid\n" )
  fopen.write( "elif  not( not( l.Get('Refs') ) ):#if the structure is 'Refs/Params'\n" )
  fopen.write( "  t_ref = l.Get('Refs')\n" )
  fopen.write( "  b_param = t_ref.GetBranch('Params')\n" )
  fopen.write( "  for i in range(b_param.GetEntries()):\n" )
  fopen.write( "    b_param.GetEntry(i)\n" )
  fopen.write( "    leaves=b_param.GetListOfLeaves()\n" )
  fopen.write( "    leaf=leaves.UncheckedAt(0)\n" )
  fopen.write( "    val=leaf.GetValueString()\n" )
  fopen.write( "    if 'FID' in val:\n" )
  fopen.write( "      fid=val.split('=')[1]\n" )
  fopen.write( "      print 'GUID%sGUID' %fid\n" )
  fopen.write( "      break\n" )
  fopen.write( "else:\n" )
  fopen.write( "  # the structure is not recognised print empty string\n" )
  fopen.write( "  # that will raise an exception later.\n" )
  fopen.write( "  print ''\n" )
  fopen.write( 'l.Close()\n' )
  fopen.close()
  # Execute the root script
  cmd = ['python']
  cmd.append( 'tmpRootScript.py' )
  gLogger.debug( cmd )
  ret = systemCall( 1800, cmd, env = rootEnv )
  if not ret['OK']:
    gLogger.error( 'Problem using root\n%s' % ret )
    return ''
  if cleanUp:
    os.remove( 'tmpRootScript.py' )
  stdout = ret['Value'][1]
  try:
    guid = stdout.split( 'GUID' )[1]
    gLogger.verbose( 'GUID found to be %s' % guid )
    return guid
  except Exception:
    gLogger.error( 'Could not obtain GUID from file' )
    return ''

#############################################################################
def mergeRootFiles( outputFile, inputFiles, daVinciVersion = '', cleanUp = True ):
  """ merge several ROOT files """
  # Setup the root enviroment
  res = __setupProjectEnvironment( 'DaVinci', version = daVinciVersion )
  if not res['OK']:
    return _errorReport( res['Message'], "Failed to setup the ROOT environment" )
  rootEnv = res['Value']
  # Perform the merging
  lists = breakListIntoChunks( inputFiles, 20 )
  tempFiles = []
  for filelist in lists:
    tempOutputFile = tempfile.mktemp()
    res = _mergeRootFiles( tempOutputFile, filelist, rootEnv )
    if not res['OK']:
      return _errorReport( res['Message'], "Failed to perform ROOT merger" )
    tempFiles.append( tempOutputFile )
  res = _mergeRootFiles( outputFile, tempFiles, rootEnv )
  if not res['OK']:
    return _errorReport( res['Message'], "Failed to perform final ROOT merger" )
  if cleanUp:
    for filename in tempFiles:
      if os.path.exists( filename ):
        os.remove( filename )
  return S_OK( outputFile )

#############################################################################
def _mergeRootFiles( outputFile, inputFiles, rootEnv ):
  """ Merge ROOT files """
  cmd = "hadd -f %s" % outputFile
  for filename in inputFiles:
    cmd = "%s %s" % ( cmd, filename )
  res = shellCall( 1800, cmd, env = rootEnv )
  if not res['OK']:
    return res
  returncode, _stdout, stderr = res['Value']
  if returncode:
    return _errorReport( stderr, "Failed to merge root files" )
  return S_OK()

#############################################################################
def __setupProjectEnvironment( project, version = '' ):
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
