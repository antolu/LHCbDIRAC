########################################################################
# $Id: CombinedSoftwareInstallation.py,v 1.3 2008/04/30 22:23:00 rgracian Exp $
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
__RCSID__   = "$Id: CombinedSoftwareInstallation.py,v 1.3 2008/04/30 22:23:00 rgracian Exp $"
__VERSION__ = "$Revision: 1.3 $"

import os, shutil
import DIRAC

InstallProject = 'install_project.py'
LHCbConfig     = 'LHCb_config.py'

class CombinedSoftwareInstallation:

  def __init__(self,argumentsDict):
    """ Standard constructor
    """
    self.job = {}
    if argumentsDict.has_key('Job'):
      self.job = argumentsDict['Job']
    self.ce = {}
    if argumentsDict.has_key('CE'):
      self.ce = argumentsDict['CE']
    
    apps = []
    if self.job.has_key('SoftwarePackages'):
      if type( self.job['SoftwarePackages'] ) == type(''):
        apps = [self.job['SoftwarePackages']]
      elif type( self.job['SoftwarePackages'] ) == type([]):
        apps = self.job['SoftwarePackages']

    self.apps = []
    for app in apps:
      DIRAC.gLogger.verbose( 'Requested Package %s' % app )
      app = tuple(app.split('.'))
      self.apps.append(app)
      
    self.jobConfig = ''
    if self.job.has_key( 'SystemConfig' ):
      self.jobConfig = self.job['SystemConfig']

    self.ceConfigs = []
    if self.ce.has_key('CompatiblePlatforms'):
      self.ceConfigs = self.ce['CompatiblePlatforms']
      if type(self.ceConfigs) == type(''):
        self.ceConfigs = [self.ceConfigs]

    self.sharedArea = SharedArea()
    self.localArea  = LocalArea()
    
        
  def execute(self):
    """
     Main method of the class executed by DIRAC jobAgent
    """
    if not self.jobConfig:
      DIRAC.gLogger.warn( 'No architecture requested' )
      return DIRAC.S_ERROR()
    if not self.jobConfig in self.ceConfigs:
      DIRAC.gLogger.warn( 'Requested arquitecture not supported by CE' )
      return DIRAC.S_ERROR()
    
    for app in self.apps:
      # 1.- check if application is available in shared area
      if CheckApplication( app, self.jobConfig, self.sharedArea ):
        continue
      if CheckApplication( app, self.jobConfig, self.localArea ):
        continue
      if InstallApplication( app, self.jobConfig, self.localArea ):
        continue
      return S_ERROR( 'Failed to install %s_%s' % app )

    return S_OK()

def log( n, line ):
  DIRAC.gLogger.info( line )   

def InstallApplication(app, config, area ):
  """
   Install given application at given area, at some point (when supported)
   it will check already installed packages in shared area and install locally
   only missing parts
  """
  if not area:
    return False
  # make a copy of the environment dictionary
  cmtEnv = dict(os.environ)
  cmtEnv['MYSITEROOT'] = area
  cmtEnv['CMTCONFIG']  = config

  installProject = os.path.join( area, InstallProject )
  if not os.path.exists( installProject ):
    try:
      shutil.copy( InstallProject, area )
    except:
      DIRAC.gLogger.warn( 'Failed to create:', installProject )
      return False

  curDir = os.getcwd()

  # Move to requested are and run the installation
  os.chdir(area)
  cmdTuple =  [sys.executable]
  cmdTuple += [InstallProject]
  cmdTuple += [ '-p', appName ]
  cmdTuple += [ '-v', appVersion ]
  cmdTuple += [ '-b', '-m', 'do_config' ]

  ret = systemCall( 3600, cmdTuple, env=cmtEnv, callbackFunction=log )
  if not ret['OK']:
    DIRAC.gLogger.warn( 'Fail software Installation:', app.join('_') )
    DIRAC.gLogger.warn( ret['Message'] )
    return False
  if ret['Value'][0]: # != 0
    DIRAC.gLogger.warn( 'Fail software Installation:', app.join('_') )
    return False
  
  os.chdir(curDir)

  # copy LHCb_config.py, should be the most up-to-date
  if os.path.exists( LHCbConfig ):
    try:
      os.remove( LHCbConfig )
    except:
      return True
  shutil.copy( os.path.join( area, LHCbConfig), curDir )

  return True

def CheckApplication(app, config, area):
  """
   check if given application is available in the given area
  """
  if not area:
    return False
  appName    = app[0]
  appVersion = app[1]
  timeout = 300
  # make a copy of the environment dictionary
  cmtEnv = dict(os.environ)
  cmtEnv['MYSITEROOT'] = area
  cmtEnv['CMTCONFIG']  = config
  
  extCMT       = os.path.join( area, 'scripts', 'ExtCMT' )
  setupProject = os.path.join( area, 'scripts', 'SetupProject' )

  # Run ExtCMT
  ret = DIRAC.Source( timeout, [extCMT], cmtEnv )
  if not ret['OK']:
    DIRAC.gLogger.info( ret['Message'])
    if ret['stdout']:
      DIRAC.gLogger.info( ret['stdout'] )
    if ret['stderr']:
      DIRAC.gLogger.warn( ret['stderr'] )
    return False
  setupProjectEnv = ret['outputEnv']

  setupProject = [setupProject]
  setupProject.append( '--ignore-missing' )
  setupProject.append( appName )
  setupProject.append( appVersion )

  # Run SetupProject
  ret = DIRAC.Source( timeout, setupProject, setupProjectEnv )
  if not ret['OK']:
    DIRAC.gLogger.info( ret['Message'])
    if ret['stdout']:
      DIRAC.gLogger.info( ret['stdout'] )
    if ret['stderr']:
      DIRAC.gLogger.warn( ret['stderr'] )
    return False
  
  gaudiEnv = ret['outputEnv']

  appRoot = appName.upper() + 'ROOT'
  if not gaudiEnv.has_key( appRoot ):
    DIRAC.gLogger.warn( 'Can not determine:', appRoot )
  appRoot = gaudiEnv[ appRoot ]
  gaudiExe = os.path.join( appRoot, config, appName+'.exe' )
  if not os.path.exists( gaudiExe ):
    DIRAC.gLogger.warn( 'Executable does not exist:', gaudiExe )
    return False

  return True

def SharedArea():
  """
   Discover localtion of Shared SW area
   This area is populated by a tool independent of the DIRAC jobs
  """
  sharedArea = ''
  if os.environ.has_key('VO_LHCB_SW_DIR'):
    sharedArea = os.path.join(os.environ['VO_LHCB_SW_DIR'],'lib')
    DIRAC.gLogger.debug( 'Using VO_LHCB_SW_DIR at "%s"' % sharedArea )
  elif DIRAC.gConfig.getValue('/LocalSite/SharedArea',''):
    sharedArea = DIRAC.gConfig.getValue('/LocalSite/SharedArea')
    DIRAC.gLogger.debug( 'Using CE SharedArea at "%s"' % sharedArea )

  if sharedArea:
    # if defined, check that it really exists
    if not os.path.isdir( sharedArea ):
      DIRAC.gLogger.warn( 'Missing Shared Area Directory:', sharedArea )
      sharedArea = ''

  return sharedArea

def LocalArea():
  """
   Discover Location of Local SW Area.
   This area is populated by DIRAC job Agent for jobs needing SW not present 
   in the Shared Area.
  """
  if DIRAC.gConfig.getValue('/LocalSite/LocalArea',''):
    localArea = DIRAC.gConfig.getValue('/LocalSite/LocalArea')
  else:
    localArea = os.path.join( DIRAC.rootPath, 'LocalArea' )

  # check if already existing directory
  if not os.path.isdir( localArea ):
    # check if we can create it
    if os.path.exists( localArea ):
      try:
        os.remove( localArea )
      except Exception, x:
        DIRAC.gLogger.warn( 'Can not remove:', localArea )
        localArea = ''
    else:
      try:
        os.mkdir( localArea )
      except Exception, x:
        DIRAC.gLogger.warn( 'Can not create:', localArea )
        localArea = ''
  return localArea
