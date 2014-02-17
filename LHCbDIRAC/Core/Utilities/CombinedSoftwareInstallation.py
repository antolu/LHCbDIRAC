""" The LHCb Local Software Install class is used by the DIRAC Job Agent
    to install necessary software via the ModuleFactory.  This relies on
    two JDL parameters in LHCb workflows:
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

import os, shutil, sys, urllib, re, copy
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCompatiblePlatforms

from LHCbDIRAC.Core.Utilities.DetectOS import NativeMachine
from LHCbDIRAC.Core.Utilities.SoftwareArea import getSharedArea, getLocalArea

opsH = Operations()
installProjectFile = 'install_project.py'
installProjectURL = opsH.getValue( 'GaudiExecution/install_project_location',
                                   'http://lhcbproject.web.cern.ch/lhcbproject/dist/' )
natOS = NativeMachine()

#############################################################################

class CombinedSoftwareInstallation:
  """ The LHCb Local Software Install class is used by the DIRAC Job Agent
    to install necessary software via the ModuleFactory"""

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
      gLogger.verbose( 'Requested Package %s' % str( app ) )
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

    self.sharedArea = getSharedArea()
    self.localArea = getLocalArea()
    self.mySiteRoot = '%s:%s' % ( self.localArea, self.sharedArea )


  def execute( self ):
    """
     Main method of the class executed by DIRAC jobAgent
    """
    if not self.apps:
      # There is nothing to do
      return S_OK()
    if not self.jobConfig:
      gLogger.warn( 'No architecture requested' )
      return S_ERROR( 'No architecture requested' )

    # The below only applies to local running since job agent will set to /LocalSite/Architecture
    # in case of 'ANY' in the job description.
    if self.jobConfig.lower() == 'any':
      gLogger.info( 'Assume this is a locally running job with sys config set to "ANY"' )
      self.jobConfig = gConfig.getValue( '/LocalSite/Architecture', '' )
      self.ceConfigs = [self.jobConfig]
      if not self.jobConfig:
        return S_ERROR( '/LocalSite/Architecture is missing and must be specified' )

    gLogger.info( 'CE supported system configurations are: %s' % ( ', '.join( self.ceConfigs ) ) )
    if not self.ceConfigs:  # redundant check as this is done in the job agent and above
      gLogger.info( 'Assume locally running job without CE configuration settings' )
      return S_OK()

    if self.jobConfig == gConfig.getValue( '/LocalSite/Architecture', '' ):  # as set by the job agent in case of 'ANY'
      gLogger.info( 'Job SystemConfiguration is set to /LocalSite/Architecture, checking compatible platforms' )
      compatibleArchs = getCompatiblePlatforms( self.jobConfig )
      if not compatibleArchs['OK']:
        return compatibleArchs
      self.jobConfig = compatibleArchs['Value'][0]
      gLogger.info( 'Setting system config to compatible platform %s' % ( self.jobConfig ) )
      if not self.jobConfig in self.ceConfigs:
        self.ceConfigs.append( self.jobConfig )

    if not self.jobConfig in self.ceConfigs:
      gLogger.error( 'Requested architecture not supported by CE' )
      return S_ERROR( 'Requested architecture not supported by CE' )

    for app in copy.deepcopy( self.apps ):
      gLogger.info( 'Checking %s for %s with site root %s' % ( str( app ), self.jobConfig, self.mySiteRoot ) )
      result = checkApplication( app, self.jobConfig, self.mySiteRoot )
      if not result:
        gLogger.info( 'Software was not found to be pre-installed in the shared area: %s' % str( app ) )
        if re.search( ':', self.mySiteRoot ):
          result = installApplication( app, self.jobConfig, self.mySiteRoot )
          if not result:
            gLogger.error( 'Software failed to be installed!' )
            return S_ERROR( 'Software Not Installed' )
          else:
            gLogger.info( 'Software was successfully installed in the local area' )
        else:
          gLogger.error( 'No local area was found to install missing software!' )
          return S_ERROR( 'No Local Area Found' )
      else:
        gLogger.info( '%s is installed for %s' % ( str( app ), self.jobConfig ) )

    return S_OK()

#############################################################################

def log( n, line ):
  gLogger.verbose( line )

#############################################################################

def checkApplication( app, config, area ):
  """Will perform a local + shared area installation using install project
     to check where components should be installed.  In this case the 'area'
     is localArea:sharedArea.
  """

  if not area:
    return False

  localArea, sharedArea = _getAreas( area )

  appName, appVersion = _getApp( app )

  curDir = os.getcwd()

  installProject = os.path.join( localArea, installProjectFile )
  if not os.path.exists( installProject ):
    installProject = os.path.join( sharedArea, installProjectFile )
    if not os.path.exists( installProject ):
      gLogger.warn( 'Failed to find:', installProjectFile )
      return False
    else:
  # NOTE: must cd to LOCAL area directory (install_project requirement)
      gLogger.info( ' change directory to %s ' % sharedArea )
      os.chdir( sharedArea )
      area = sharedArea
  else:
    os.chdir( localArea )

  gLogger.info( ' install_project is %s' % installProject )
  # Now run the installation


  cmtEnv = dict( os.environ )
  cmtEnv['MYSITEROOT'] = area
  gLogger.info( 'Defining MYSITEROOT = %s' % area )
  cmtEnv['CMTCONFIG'] = config
  gLogger.info( 'Defining CMTCONFIG = %s' % config )

  cmdTuple = [sys.executable]
  cmdTuple += [installProjectFile]
  cmds = opsH.getValue( 'GaudiExecution/checkProjectOptions', '-b --check' )
  for cmdTupleC in cmds.split( ' ' ):
    cmdTuple += [cmdTupleC]
  cmdTuple += [ appName ]
  if appVersion:
    cmdTuple += [ appVersion ]

  gLogger.info( 'Executing %s' % ' '.join( cmdTuple ) )
  ret = systemCall( 1800, cmdTuple, env = cmtEnv, callbackFunction = log )
  os.chdir( curDir )
  if not ret['OK']:
    gLogger.error( 'Software checking failed', '%s %s' % ( appName, appVersion ) )
    return False
  if ret['Value'][0]:  # != 0
    gLogger.error( 'Software checking failed with non-zero status', '%s %s' % ( appName, appVersion ) )
    return False

  return True

#############################################################################

def installApplication( app, config, area ):
  """
   Install given application at given area, at some point (when supported)
   it will check already installed packages in shared area and install locally
   only missing parts
  """
  if not os.path.exists( '%s/%s' % ( os.getcwd(), installProjectFile ) ):
    try:
      gLogger.debug( "Downloading %s from %s" % ( installProjectFile,
                                                        ( installProjectURL, installProjectFile ) ) )
      urllib.urlretrieve( '%s%s' % ( installProjectURL, installProjectFile ), installProjectFile )
    except urllib.ContentTooShortError, msg:
      gLogger.exception( "Content too short ", msg )
      return False
    if not os.path.exists( '%s/%s' % ( os.getcwd(), installProjectFile ) ):
      gLogger.error( '%s/%s could not be downloaded' % ( installProjectURL, installProjectFile ) )
      return False

  if not area:
    return False

  localArea, sharedArea = _getAreas( area )

  appName, appVersion = _getApp( app )
  # make a copy of the environment dictionary
  cmtEnv = dict( os.environ )

  installProject = os.path.join( localArea, installProjectFile )
  if not os.path.exists( installProject ):
    try:
      shutil.copy( installProjectFile, localArea )
    except shutil.Error, errorMsg:
      gLogger.warn( 'Failed to create: %s %s' % ( installProject, errorMsg ) )
      return False

  curDir = os.getcwd()

  # Move to requested are and run the installation
  os.chdir( localArea )

  localmySiteRoot = os.getcwd()
  if sharedArea:
    localmySiteRoot += ':%s' % sharedArea
  cmtEnv['MYSITEROOT'] = localmySiteRoot
  gLogger.info( 'Defining MYSITEROOT = %s' % localmySiteRoot )
  cmtEnv['CMTCONFIG'] = config
  gLogger.info( 'Defining CMTCONFIG = %s' % config )

  cmdTuple = [sys.executable]
  cmdTuple += [installProjectFile]
  cmds = opsH.getValue( 'GaudiExecution/installProjectOptions', '-b' )
  for cmdTupleC in cmds.split( ' ' ):
    cmdTuple += [cmdTupleC]
  cmdTuple += [ appName ]
  if appVersion:
    cmdTuple += [ appVersion ]

  gLogger.info( 'Executing %s' % ' '.join( cmdTuple ) )
  gLogger.info( ' at %s' % os.getcwd() )

  ret = systemCall( 1800, cmdTuple, env = cmtEnv, callbackFunction = log )
  os.chdir( curDir )
  if not ret['OK']:
    gLogger.warn( 'Failed to install software:', '_'.join( app ) )
    gLogger.warn( ret['Message'] )
    return False
  if ret['Value'][0]:  # != 0
    gLogger.warn( 'Failed to install software:', '_'.join( app ) )
    return False

  return True

#############################################################################

def _getAreas( area ):
  """ split localArea:sharedArea (when available)
  """
  localArea = area
  sharedArea = ''
  if re.search( ':', area ):
    localArea = area.split( ':' )[0]
    sharedArea = area.split( ':' )[1]
  return ( localArea, sharedArea )

def _getApp( app ):
  """ app is a tuple ('appName', 'appVersion')
  """
  appName = app[0]
  try:
    appVersion = app[1]
  except IndexError:
    appVersion = ''

  return appName, appVersion
