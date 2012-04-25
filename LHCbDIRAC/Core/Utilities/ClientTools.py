########################################################################
# $HeadURL$
# File :   ClientTools.py
########################################################################

"""  The ClientTools module provides additional functions for use by users
     of the DIRAC client in the LHCb environment.
"""

__RCSID__ = "$Id$"

import re, os, shutil, types, tempfile

import DIRAC

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import breakListIntoChunks, sortList
from DIRAC.Core.Utilities.Subprocess import systemCall

from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getScriptsLocation, getProjectCommand, runEnvironmentScripts

import time

#############################################################################
def packageInputs( appName, appVersion, optionsFiles = [], destinationDir = '', optsFlag = True, libFlag = True ):
  """Under development. Helper function.

     Relies on CMTPROJECTPATH and CMTCONFIG variables.

     Options files can be specified with environment variables e.g. $DAVINCIUSERROOT/options/myopts.opts.

     The destination directory defaults to the current working directory if not specified.

     Flags can be specified to enable or disable the retrieval of libraries or options.

     Example usage:

     >>> from LHCbDIRAC.Core.Utilities.ClientTools import packageInputs
     >>> packageInputs('DaVinci','v20r3',optionsFiles=['$DAVINCIUSERROOT/myOpts.py'])

     @param condDict: CondDB tags
     @type condDict: Dict of DB, tag pairs
  """
  if not type( appName ) in types.StringTypes or not type( appVersion ) in types.StringTypes:
    gLogger.warn()
    return _errorReport( 'Expected strings for application name and version' )

  if optsFlag:
    if not optionsFiles:
      return _errorReport( 'Expected string or list for optionsFiles' )
    if type( optionsFiles ) in types.StringTypes:
      optionsFiles = [optionsFiles]
    if not type( optionsFiles ) == type( [] ):
      return _errorReport( 'Expected string or list for optionsFiles' )

  if not destinationDir:
    destinationDir = os.getcwd()

  localEnv = dict( os.environ )
  if not localEnv.has_key( 'CMTPROJECTPATH' ):
    gLogger.warn( 'Expected CMTPROJECTPATH to be defined' )
  if not localEnv.has_key( 'CMTCONFIG' ):
    gLogger.warn( 'Expected CMTCONFIG to be defined' )

  systemConfig = localEnv['CMTCONFIG']
  userArea = ''
  for path in str.split( localEnv['CMTPROJECTPATH'], ':' ):
    if re.search( 'cmtuser', path ):
      userArea = path

  if not userArea:
    gLogger.warn( 'Could not establish user CMT area from CMTPROJECTPATH' )

  #not sure if there's also a lib there at the end or not
  inputPath = os.path.join( userArea, appName + "_" + appVersion, "InstallArea" )
  finalResult = {'optionsFile':'', 'libraries':''}

  # Only run gaudirun if opts flag is specified
  if optsFlag:
    result = _getOptsFiles( appName, appVersion, optionsFiles, destinationDir )
    if not result['OK']:
      return result
    finalResult['optionsFile'] = result['Value']

  # Only retrieve libraries if lib flag is specified
  if libFlag:
    result = _getLibFiles( os.path.join( inputPath, systemConfig ), destinationDir )
    if not result['OK']:
      return result
    finalResult['libraries'] = result['Value']
    result = _getPythonDir( inputPath, destinationDir )
    if not result['OK']:
      return result
    finalResult['pythondir'] = result['Value']

  return S_OK( finalResult )

#############################################################################
def _getOptsFiles( appName, appVersion, optionsFiles, destinationDir ):
  """Set up project environment and expand options.
  """
  gLogger.verbose( 'Options files to locate are: %s' % str.join( optionsFiles, ', ' ) )
  ret = __setupProjectEnvironment( appName, version = appVersion )
  if not ret['OK']:
    gLogger.warn( 'Error during SetupProject\n%s' % ret )
  appEnv = ret['outputEnv']
  toCheck = []
  toInclude = []
  for optFile in optionsFiles:
    if not re.search( '\$', optFile ):
      toCheck.append( optFile )
    else:
      toInclude.append( optFile )

  for n, v in appEnv.items():
    for optFile in toInclude:
      if re.search( '\$%s' % n, optFile ):
        toCheck.append( str.replace( optFile, '$%s' % n, v ) )

#  if toInclude:
#    gLogger.verbose('Options files are: %s' %(string.join(toInclude,'\n')))
  if toCheck:
    gLogger.verbose( 'Environment expanded options files are: %s' % ( str.join( toCheck, '\n' ) ) )

  if not len( toCheck ) == len( optionsFiles ):
    gLogger.warn( 'Could not account for all options files' )

  missing = []
  for optFile in toCheck:
    if not os.path.exists( optFile ):
      missing.append( optFile )
#      shutil.copy(optFile,'%s/%s' %(destinationDir,os.path.basename(optFile)))

  if missing:
    gLogger.error( 'The following options files could not be found:\n%s' % ( str.join( missing, '\n' ) ) )
    return S_ERROR( missing )


  newOptsName = '%s/%s_%s.pkl' % ( destinationDir, appName, appVersion )

  if os.path.exists( newOptsName ):
    gLogger.warn( '%s already exists, will be overwritten' % newOptsName )
  gLogger.verbose( 'Attempting to run gaudirun.py' )
  cmdTuple = ['gaudirun.py']
  cmdTuple.append( '-n' )
  cmdTuple.append( '-v' )
  cmdTuple.append( '--output' )
  cmdTuple.append( newOptsName )

  for i in toCheck:
    cmdTuple.append( i )
#  cmdTuple.append('>!')
#  cmdTuple.append(newOptsName)
  gLogger.verbose( 'Commmand is: %s' % ( str.join( cmdTuple, ' ' ) ) )
  ret = DIRAC.systemCall( 1800, cmdTuple, env = appEnv, callbackFunction = log )
  if not ret['OK']:
    gLogger.error( 'Problem during gaudirun.py call\n%s' % ret )
    return S_ERROR( 'Could not package job inputs' )

  return S_OK( newOptsName )

#############################################################################
def _getLibFiles( inputPath, destinationDir ):
  """ Simple function to retrieve user libraries.
  """
  gLogger.verbose( 'dir is at :"%s"' % inputPath )
  if not os.path.exists( inputPath ):
    return S_ERROR( 'Directory %s does not exist' % inputPath )

  if not os.path.exists( '%s/lib' % destinationDir ):
    try:
      os.makedirs( '%s/lib' % destinationDir )
    except Exception, x:
      gLogger.warn( 'Could not create directory lib in %s' % destinationDir )
      return S_ERROR( x )
  shutil.rmtree( '%s/lib' % destinationDir, True )
  shutil.copytree( inputPath + '/lib', '%s/lib' % destinationDir )
  for fname in os.listdir( destinationDir + '/lib' ):
    gLogger.verbose( 'Copied file:"%s"' % fname )
  return S_OK( '%s/lib' % destinationDir )

#############################################################################
def _getPythonDir( inputPath, destinationDir ):
  """ Simple function to retrieve user python modules.
  """
  gLogger.verbose( 'dir is at :"%s"' % inputPath )
  if not os.path.exists( inputPath ):
    return S_ERROR( 'Directory %s does not exist' % inputPath )
  if not os.path.exists( '%s/python' % destinationDir ):
    try:
      os.makedirs( '%s/python' % destinationDir )
    except Exception, x:
      gLogger.warn( 'Could not create directory python in %s' % destinationDir )
      return S_ERROR( x )
  if not os.path.exists( '%s/python' % inputPath ):
    return S_ERROR( '%s does not exist!' % inputPath )
  shutil.rmtree( '%s/python' % destinationDir, True )
  shutil.copytree( inputPath + '/python', '%s/python' % destinationDir )
  for fname in os.listdir( destinationDir + '/python' ):
    gLogger.verbose( 'Copied file:"%s"' % fname )
  return S_OK( '%s/python' % destinationDir )

#############################################################################
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
  res = systemCall( 1800, cmd, env = gaudiEnv )#,callbackFunction=log)
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
  fopen.write( "t=l.Get(\'##Params\')\n" )
  fopen.write( 't.Show(0)\n' )
  fopen.write( 'leaves=t.GetListOfLeaves()\n' )
  fopen.write( 'leaf=leaves.UncheckedAt(0)\n' )
  fopen.write( 'val=leaf.GetValueString()\n' )
  fopen.write( "fid=val.split('=')[2].split(']')[0]\n" )
  fopen.write( "print 'GUID%sGUID' %fid\n" )
  fopen.write( 'l.Close()\n' )
  fopen.close()
  # Execute the root script
  cmd = ['python']
  cmd.append( 'tmpRootScript.py' )
  gLogger.debug( cmd )
  ret = DIRAC.systemCall( 1800, cmd, env = rootEnv )
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
  res = DIRAC.shellCall( 1800, cmd, env = rootEnv )
  if not res['OK']:
    return res
  returncode, stdout, stderr = res['Value']
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

#############################################################################
def parseGaudiCard( datacard ):
  """ take gaudi card generated from BKK and return list of LFNs, useful to be passed to splitByFiles
  """
  inputFile = open( datacard, 'r' )
  lfns = []
  for line in inputFile:
    l = line.lstrip()
    if l[0:1] == "#" :
      continue
    if l[0:3] == "from" :
      continue
    if l.find( "EventSelector" ) > -1:
      continue
    if l.rfind( "DATAFILE" ) == -1:
      continue
    wds = l.split( "'" )
    lfn = wds[1].lstrip( "LFN:" )
    lfns.append( lfn )
  inputFile.close()
  gLogger.verbose( '\nObtained %s LFNs from file' % len( lfns ) )
  return lfns

#############################################################################
def log( n, line ):
  gLogger.debug( line )
