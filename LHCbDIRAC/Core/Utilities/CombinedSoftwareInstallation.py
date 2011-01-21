########################################################################
# $Id$
# File :   CombinedSoftwareInstallation.py
# Author : Ricardo Graciani
########################################################################

""" The LHCb Local Software Install class is used by the DIRAC Job Agent
    to install necessary software via the ModuleFactory.  This relies on
    two JDL parameters in LHCb workflows:
    - SoftwareDistModule - expresses the import string
    - SoftwarePackages - for the necessary parameters to install software
    - SystemConfig - to determine the required value of CMTCONFIG
    It checks also the provided CE:
    - CompatiblePlatforms
    and returns error if requested SystemConfig is not in CompatiblePlatforms

    DIRAC assumes an execute() method will exist during usage.

    It first checks Shared area
    Then it checks if the Application can be configured based on the existing SW
    on the Shared area
    If this is not possible it will do a local installation.
"""
__RCSID__ = "$Id$"

import os, shutil, sys, urllib, re, string
import DIRAC
from LHCbDIRAC.Core.Utilities.DetectOS import NativeMachine

InstallProject = 'install_project.py'
InstallProjectURL = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/'
natOS = NativeMachine()

class CombinedSoftwareInstallation:

  def __init__( self, argumentsDict ):
    """ Standard constructor
    """
    self.job = {}
    if argumentsDict.has_key( 'Job' ):
      self.job = argumentsDict['Job']
    self.ce = {}
    if argumentsDict.has_key( 'CE' ):
      self.ce = argumentsDict['CE']
    self.source = {}
    if argumentsDict.has_key( 'Source' ):
      self.source = argumentsDict['Source']

    apps = []
    if self.job.has_key( 'SoftwarePackages' ):
      if type( self.job['SoftwarePackages'] ) == type( '' ):
        apps = [self.job['SoftwarePackages']]
      elif type( self.job['SoftwarePackages'] ) == type( [] ):
        apps = self.job['SoftwarePackages']

    self.apps = []
    for app in apps:
      DIRAC.gLogger.verbose( 'Requested Package %s' % app )
      app = tuple( app.split( '.' ) )
      self.apps.append( app )

    self.jobConfig = ''
    if self.job.has_key( 'SystemConfig' ):
      self.jobConfig = self.job['SystemConfig']
    else:
      self.jobConfig = natOS.CMTSupportedConfig()[0]

    self.ceConfigs = []
    if self.ce.has_key( 'CompatiblePlatforms' ):
      self.ceConfigs = self.ce['CompatiblePlatforms']
      if type( self.ceConfigs ) == type( '' ):
        self.ceConfigs = [self.ceConfigs]
    else:
      self.ceConfigs = [self.jobConfig]

    self.sharedArea = SharedArea()
    self.localArea = LocalArea()
    self.mySiteRoot = '%s:%s' % ( self.localArea, self.sharedArea )


  def execute( self ):
    """
     Main method of the class executed by DIRAC jobAgent
    """
    if not self.apps:
      # There is nothing to do
      return DIRAC.S_OK()
    if not self.jobConfig:
      DIRAC.gLogger.warn( 'No architecture requested' )
      return DIRAC.S_ERROR( 'No architecture requested' )

    if self.ce.has_key( 'Site' ) and self.ce['Site'] == 'DIRAC.ONLINE-FARM.ch':
      return DIRAC.S_OK()
      print dir( self )
      print self.job
      return onlineExecute( self.job['SoftwarePackages'] )

    # The below only applies to local running since job agent will set to /LocalSite/Architecture
    # in case of 'ANY' in the job description.
    if self.jobConfig.lower() == 'any':
      DIRAC.gLogger.info( 'Assume this is a locally running job with sys config set to "ANY"' )
      self.jobConfig = DIRAC.gConfig.getValue( '/LocalSite/Architecture', '' )
      self.ceConfigs = [self.jobConfig]
      if not self.jobConfig:
        return DIRAC.S_ERROR( '/LocalSite/Architecture is missing and must be specified' )

    DIRAC.gLogger.info( 'CE supported system configurations are: %s' % ( string.join( self.ceConfigs, ', ' ) ) )
    if not self.ceConfigs:  # redundant check as this is done in the job agent and above
      DIRAC.gLogger.info( 'Assume locally running job without CE configuration settings' )
      return DIRAC.S_OK()

    if self.jobConfig == DIRAC.gConfig.getValue( '/LocalSite/Architecture', '' ): # as set by the job agent in case of 'ANY'
      DIRAC.gLogger.info( 'Job SystemConfiguration is set to /LocalSite/Architecture, checking compatible platforms' )
      compatibleArchs = DIRAC.gConfig.getValue( '/Resources/Computing/OSCompatibility/%s' % ( self.jobConfig ), [] )
      if not compatibleArchs:
        DIRAC.gLogger.error( 'Could not find matching section for %s in /Resources/Computing/OSCompatibility/' % ( self.jobConfig ) )
        return DIRAC.S_ERROR( 'SystemConfig Not Found' )
      self.jobConfig = compatibleArchs[0]
      DIRAC.gLogger.info( 'Setting system config to compatible platform %s' % ( self.jobConfig ) )
      if not self.jobConfig in self.ceConfigs:
        self.ceConfigs.append( self.jobConfig )

    if not self.jobConfig in self.ceConfigs:
      DIRAC.gLogger.error( 'Requested architecture not supported by CE' )
      return DIRAC.S_ERROR( 'Requested architecture not supported by CE' )

    for app in self.apps:
      DIRAC.gLogger.info( 'Checking %s_%s for %s with site root %s' % ( app[0], app[1], self.jobConfig, self.mySiteRoot ) )
      result = CheckApplication( app, self.jobConfig, self.mySiteRoot )
      if not result:
        DIRAC.gLogger.info( 'Software was not found to be pre-installed in the shared area', '%s_%s' % ( app ) )
        if re.search( ':', self.mySiteRoot ):
          result = InstallApplication( app, self.jobConfig, self.localArea )
          if not result:
            DIRAC.gLogger.error( 'Software failed to be installed!' )
            return DIRAC.S_ERROR( 'Software Not Installed' )
          else:
            DIRAC.gLogger.info( 'Software was successfully installed in the local area' )
        else:
          DIRAC.gLogger.error( 'No local area was found to install missing software!' )
          return DIRAC.S_ERROR( 'No Local Area Found' )
      else:
        DIRAC.gLogger.info( '%s is installed for %s' % ( app, self.jobConfig ) )

    return DIRAC.S_OK()

def log( n, line ):
  DIRAC.gLogger.verbose( line )

def MySiteRoot():
  """Returns the MySiteRoot for the current local and / or shared areas.
  """
  mySiteRoot = ''
  localArea = LocalArea()
  if not localArea:
    DIRAC.gLogger.error( 'Failed to determine Local SW Area' )
    return mySiteRoot
  sharedArea = SharedArea()
  if not sharedArea:
    DIRAC.gLogger.error( 'Failed to determine Shared SW Area' )
    return localArea
  mySiteRoot = '%s:%s' % ( localArea, sharedArea )
  return mySiteRoot

def CheckApplication( app, config, area ):
  """Will perform a local + shared area installation using install project
     to check where components should be installed.  In this case the 'area'
     is localArea:sharedArea.
  """

  if not area:
    return False

  localArea = area
  if re.search( ':', area ):
    localArea = string.split( area, ':' )[0]
    sharedArea = string.split( area, ':' )[1]

  appName = app[0]
  appVersion = app[1]

  curDir = os.getcwd()

  installProject = os.path.join( localArea, InstallProject )
  if not os.path.exists( installProject ):
    installProject = os.path.join( sharedArea, InstallProject )
    if not os.path.exists( installProject ):
      DIRAC.gLogger.warn( 'Failed to find:', InstallProject )
      return False
    else:
  #NOTE: must cd to LOCAL area directory (install_project requirement)
      DIRAC.gLogger.info( ' change directory to %s ' % sharedArea )
      os.chdir( sharedArea )
      area = sharedArea
  else:
    os.chdir( localArea )

  DIRAC.gLogger.info( ' install_project is %s' % installProject )
  # Now run the installation


  cmtEnv = dict( os.environ )
  cmtEnv['MYSITEROOT'] = area
  DIRAC.gLogger.info( 'Defining MYSITEROOT = %s' % area )
  cmtEnv['CMTCONFIG'] = config
  DIRAC.gLogger.info( 'Defining CMTCONFIG = %s' % config )

  cmdTuple = [sys.executable]
  cmdTuple += [InstallProject]
  cmdTuple += ['-d']
  cmdTuple += ['-b']
  cmdTuple += [ '--check' ]
#  cmdTuple += [ '-p', appName ]
#  cmdTuple += [ '-v', appVersion ]
  cmdTuple += [ appName ]
  cmdTuple += [ appVersion ]

  DIRAC.gLogger.info( 'Executing %s' % ' '.join( cmdTuple ) )
  ret = DIRAC.systemCall( 1800, cmdTuple, env = cmtEnv, callbackFunction = log )
  os.chdir( curDir )
  if not ret['OK']:
    DIRAC.gLogger.error( 'Software installation failed', '%s %s' % ( appName, appVersion ) )
    return False
  if ret['Value'][0]: # != 0
    DIRAC.gLogger.error( 'Software installation failed with non-zero status', '%s %s' % ( appName, appVersion ) )
    return False

  return True

def InstallApplication( app, config, area ):
  """
   Install given application at given area, at some point (when supported)
   it will check already installed packages in shared area and install locally
   only missing parts
  """
  if not os.path.exists( '%s/%s' % ( os.getcwd(), InstallProject ) ):
    try:
      localname, headers = urllib.urlretrieve( '%s%s' % ( InstallProjectURL, InstallProject ), InstallProject )
    except:
      DIRAC.gLogger.exception()
      return False
    #localname,headers = urllib.urlretrieve('%s%s' %('http://lhcbproject.web.cern.ch/lhcbproject/dist/devel/install_project.py',InstallProject),InstallProject)
    if not os.path.exists( '%s/%s' % ( os.getcwd(), InstallProject ) ):
      DIRAC.gLogger.error( '%s/%s could not be downloaded' % ( InstallProjectURL, InstallProject ) )
      return False

  if not area:
    return False
  appName = app[0]
  appVersion = app[1]
  # make a copy of the environment dictionary
  cmtEnv = dict( os.environ )

  installProject = os.path.join( area, InstallProject )
  if not os.path.exists( installProject ):
    try:
      shutil.copy( InstallProject, area )
    except:
      DIRAC.gLogger.warn( 'Failed to create:', installProject )
      return False

  curDir = os.getcwd()

  # Move to requested are and run the installation
  os.chdir( area )

  cmtEnv['MYSITEROOT'] = os.getcwd()
  DIRAC.gLogger.info( 'Defining MYSITEROOT = %s' % os.getcwd() )
  cmtEnv['CMTCONFIG'] = config
  DIRAC.gLogger.info( 'Defining CMTCONFIG = %s' % config )

  cmdTuple = [sys.executable]
  cmdTuple += [InstallProject]
  cmdTuple += ['-d']
  cmdTuple += ['-b']
  cmdTuple += [ appName ]
  cmdTuple += [ appVersion ]

  DIRAC.gLogger.info( 'Executing %s' % ' '.join( cmdTuple ) )
  DIRAC.gLogger.info( ' at %s' % os.getcwd() )

  #Temporarily increasing timeout to 3hrs to debug installation failures for SAM suite
  ret = DIRAC.systemCall( 10800, cmdTuple, env = cmtEnv, callbackFunction = log )
  os.chdir( curDir )
  if not ret['OK']:
    DIRAC.gLogger.warn( 'Failed to install software:', '_'.join( app ) )
    DIRAC.gLogger.warn( ret['Message'] )
    return False
  if ret['Value'][0]: # != 0
    DIRAC.gLogger.warn( 'Failed to install software:', '_'.join( app ) )
    return False

  return True

def SharedArea():
  """
   Discover localtion of Shared SW area
   This area is populated by a tool independent of the DIRAC jobs
  """
  sharedArea = ''
  if os.environ.has_key( 'VO_LHCB_SW_DIR' ):
    sharedArea = os.path.join( os.environ['VO_LHCB_SW_DIR'], 'lib' )
    DIRAC.gLogger.debug( 'Using VO_LHCB_SW_DIR at "%s"' % sharedArea )
    if os.environ['VO_LHCB_SW_DIR'] == '.':
      if not os.path.isdir( 'lib' ):
        os.mkdir( 'lib' )
  elif DIRAC.gConfig.getValue( '/LocalSite/SharedArea', '' ):
    sharedArea = DIRAC.gConfig.getValue( '/LocalSite/SharedArea' )
    DIRAC.gLogger.debug( 'Using CE SharedArea at "%s"' % sharedArea )

  if sharedArea:
    # if defined, check that it really exists
    if not os.path.isdir( sharedArea ):
      DIRAC.gLogger.error( 'Missing Shared Area Directory:', sharedArea )
      sharedArea = ''

  return sharedArea

def CreateSharedArea():
  """
   Method to be used by SAM jobs to make sure the proper directory structure is created
   if it does not exists
  """
  if not os.environ.has_key( 'VO_LHCB_SW_DIR' ):
    DIRAC.gLogger.info( 'VO_LHCB_SW_DIR not defined.' )
    return False

  sharedArea = os.environ['VO_LHCB_SW_DIR']
  if sharedArea == '.':
    DIRAC.gLogger.info( 'VO_LHCB_SW_DIR points to "."' )
    return False

  if not os.path.isdir( sharedArea ):
    DIRAC.gLogger.error( 'VO_LHCB_SW_DIR="%s" is not a directory' % sharedArea )
    return False

  sharedArea = os.path.join( sharedArea, 'lib' )
  try:
    if os.path.isdir( sharedArea ) and not os.path.islink( sharedArea ) :
      return True
    if not os.path.exists( sharedArea ):
      os.mkdir( sharedArea )
      return True
    os.remove( sharedArea )
    os.mkdir( sharedArea )
    return True
  except Exception, x:
    DIRAC.gLogger.error( 'Problem trying to create shared area', str( x ) )
    return False

def LocalArea():
  """
   Discover Location of Local SW Area.
   This area is populated by DIRAC job Agent for jobs needing SW not present
   in the Shared Area.
  """
  if DIRAC.gConfig.getValue( '/LocalSite/LocalArea', '' ):
    localArea = DIRAC.gConfig.getValue( '/LocalSite/LocalArea' )
  else:
    localArea = os.path.join( DIRAC.rootPath, 'LocalArea' )

  # check if already existing directory
  if not os.path.isdir( localArea ):
    # check if we can create it
    if os.path.exists( localArea ):
      try:
        os.remove( localArea )
      except Exception, x:
        DIRAC.gLogger.error( 'Cannot remove:', localArea )
        localArea = ''
    else:
      try:
        os.mkdir( localArea )
      except Exception, x:
        DIRAC.gLogger.error( 'Cannot create:', localArea )
        localArea = ''
  return localArea

def RemoveApplication( app, config, area ):
  """
   Install given application at given area, at some point (when supported)
   it will check already installed packages in shared area and install locally
   only missing parts
  """
  if not os.path.exists( '%s/%s' % ( os.getcwd(), InstallProject ) ):
    try:
      localname, headers = urllib.urlretrieve( '%s%s' % ( InstallProjectURL, InstallProject ), InstallProject )
    except:
      DIRAC.gLogger.exception()
      return False
    if not os.path.exists( '%s/%s' % ( os.getcwd(), InstallProject ) ):
      DIRAC.gLogger.error( '%s/%s could not be downloaded' % ( InstallProjectURL, InstallProject ) )
      return False

  if not area:
    return False
  appName = app[0]
  appVersion = app[1]
  # make a copy of the environment dictionary
  cmtEnv = dict( os.environ )
  cmtEnv['MYSITEROOT'] = area
  cmtEnv['CMTCONFIG'] = config

  installProject = os.path.join( area, InstallProject )
  if not os.path.exists( installProject ):
    try:
      shutil.copy( InstallProject, area )
    except:
      DIRAC.gLogger.warn( 'Failed to create:', installProject )
      return False

  curDir = os.getcwd()

  # Move to requested are and run the installation
  os.chdir( area )
  cmdTuple = [sys.executable]
  cmdTuple += [InstallProject]
  #removal options
  cmdTuple += [ '-r', '-d' ]
#  cmdTuple += [ '-p', appName ]
#  cmdTuple += [ '-v', appVersion ]
  cmdTuple += [ appName ]
  cmdTuple += [ appVersion ]

  ret = DIRAC.systemCall( 3600, cmdTuple, env = cmtEnv, callbackFunction = log )
  os.chdir( curDir )
  if not ret['OK']:
    DIRAC.gLogger.warn( 'Software Removal Failed:', '_'.join( app ) )
    DIRAC.gLogger.warn( ret['Message'] )
    return False
  if ret['Value'][0]: # != 0
    DIRAC.gLogger.warn( 'Software Removal Failed:', '_'.join( app ) )
    return False

  return True

def onlineExecute( softwarePackages ):
  """Alternative CombinedSoftwareInstallation for the Online Farm."""
  import xmlrpclib
  # First: Get the full requirements for the job.
  bkProcessingPass = dict( workflow_commons[ 'BKProcessingPass' ] )
  for step in bkProcessingPass:
    bkProcessingPass[ step ][ 'ExtraPackages' ] = DIRAC.List.fromChar( BKProcessingPass[ step ][ 'ExtraPackages' ] , ';' )
    bkProcessingPass[ step ][ 'OptionFiles' ] = DIRAC.List.fromChar( BKProcessingPass[ step ][ 'OptionFiles' ] , ';' )
  # Second: Get slice information from the Online Farm
  server_url = 'http://storeio01.lbdaq.cern.ch:8889';
  # recoManager = xmlrpclib.Server(server_url);
  recoManager = DummyRPC()
  connectionError = "Cannot connect to Reconstruction Manager"
  try:
    result = recoManager.globalStatus()
  except Exception:
    DIRAC.gLogger.exception()
    return DIRAC.S_ERROR( connectionError )
  if not result[ 'OK' ]:
    DIRAC.gLogger.error( result[ 'Message' ] )
    return DIRAC.S_ERROR( connectionError )
  # Third: Match configs (each step must have at least one match in the OnlineFarm)
  matcherror = "Cannot match job"
  for step in bkProcessingPass:
    valid = False
    for sliceNumber in result[ 'Value' ]:
      sliceConfig = result[ 'Value' ][ sliceNumber ][ 'config' ]
      if compareConfigs( bkProcessingPass[ step ] , sliceConfig ):
        valid = True
        break
    if not valid:
      return DIRAC.S_ERROR( matcherror )
  return DIRAC.S_OK()

class DummyRPC:
  def globalStatus( self ):
    return { 'OK' : True , 'Value' :
    { '0' : { 'config' : {'ApplicationName': 'Brunel', 'ApplicationVersion': 'v35r0p1', 'ExtraPackages': ['AppConfig.v2r3p1'], 'DDDb': 'head-20090112', 'OptionFiles': ['$APPCONFIGOPTS/Brunel/FEST-200903.py', '$APPCONFIGOPTS/UseOracle.py'], 'CondDb': 'head-20090112'} , 'availability' : 0.3 },
    '1' : { 'config' : {'ApplicationName': 'Brunel', 'ApplicationVersion': 'v35r0p1', 'ExtraPackages': ['AppConfig.v2r3p1'], 'DDDb': 'head-20090112', 'OptionFiles': ['$APPCONFIGOPTS/Brunel/FEST-200903.py', '$APPCONFIGOPTS/UseOracle.py'], 'CondDb': 'head-20090112'} , 'availability' : 0.3 } ,
    '2' : { 'config' : {'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v23r0p1', 'ExtraPackages': ['AppConfig.v2r3p1'], 'DDDb': 'head-20090112', 'OptionFiles': ['$APPCONFIGOPTS/DaVinci/DVMonitorDst.py'], 'CondDb': 'head-20090112' } , 'availability' : 0.3 }
    }
    }

def compareConfigs( self , config1 , config2 ):
  if len( config1.keys() ) != len( config2.keys() ):
    return False
  for key in config1:
    if not key in config2:
      return False
    else:
      if key == 'ExtraPackages':
        if not sorted( config2[ 'ExtraPackages' ] ) == sorted( config1[ 'ExtraPackages' ] ):
          return False
      elif config1[ key ] != config2[ key ]:
        return False
  return True

