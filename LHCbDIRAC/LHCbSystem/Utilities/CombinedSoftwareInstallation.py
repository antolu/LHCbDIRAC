########################################################################
# $Id: CombinedSoftwareInstallation.py,v 1.29 2009/04/15 08:00:59 rgracian Exp $
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
__RCSID__   = "$Id: CombinedSoftwareInstallation.py,v 1.29 2009/04/15 08:00:59 rgracian Exp $"
__VERSION__ = "$Revision: 1.29 $"

import os, shutil, sys, urllib, re, string
import DIRAC

InstallProject = 'install_project.py'
InstallProjectURL = 'http://cern.ch/lhcbproject/dist/'

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
    self.source = {}
    if argumentsDict.has_key('Source'):
      self.source = argumentsDict['Source']

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
    self.mySiteRoot = '%s:%s' %(self.localArea,self.sharedArea)


  def execute(self):
    """
     Main method of the class executed by DIRAC jobAgent
    """
    if not self.apps:
      # There is nothing to do
      return DIRAC.S_OK()
    if not self.jobConfig:
      DIRAC.gLogger.error( 'No architecture requested' )
      return DIRAC.S_ERROR( 'No architecture requested' )
    if not self.jobConfig in self.ceConfigs:
      if not self.ceConfigs:  # redundant check as this is done in the job agent, if locally running option might not be defined
        DIRAC.gLogger.info( 'Assume locally running job' )
        return DIRAC.S_OK()
      else:
        DIRAC.gLogger.error( 'Requested architecture not supported by CE' )
        return DIRAC.S_ERROR( 'Requested architecture not supported by CE' )

    for app in self.apps:
      DIRAC.gLogger.info('Attempting to install %s_%s for %s with site root %s' %(app[0],app[1],self.jobConfig,self.mySiteRoot))
      result = CheckInstallSoftware(app,self.jobConfig,self.mySiteRoot)
      if not result:
        DIRAC.gLogger.error('Failed to install software','%s_%s' %(app))
        return DIRAC.S_ERROR('Failed to install software')
      else:
        DIRAC.gLogger.info('%s was successfully installed for %s' %(app,self.jobConfig))

    return DIRAC.S_OK()

def log( n, line ):
  DIRAC.gLogger.info( line )

def MySiteRoot():
  """Returns the MySiteRoot for the current local and / or shared areas.
  """
  mySiteRoot = ''
  localArea=LocalArea()
  if not localArea:
    return mySiteRoot
  sharedArea=SharedArea()
  if not sharedArea:
    return mySiteRoot
  mySiteRoot = '%s:%s' %(localArea,sharedArea)
  return mySiteRoot

def CheckInstallSoftware(app,config,area):
  """Will perform a local + shared area installation using install project
     to check where components should be installed.  In this case the 'area'
     is localArea:sharedArea.
  """
  #NOTE: must cd to LOCAL area directory (install_project requirement)
  if not os.path.exists('%s/%s' %(os.getcwd(),InstallProject)):
    localname,headers = urllib.urlretrieve('%s%s' %(InstallProjectURL,InstallProject),InstallProject)
    #localname,headers = urllib.urlretrieve('%s%s' %('http://lhcbproject.web.cern.ch/lhcbproject/dist/devel/',InstallProject),InstallProject)
    if not os.path.exists('%s/%s' %(os.getcwd(),InstallProject)):
      DIRAC.gLogger.error('%s/%s could not be downloaded' %(InstallProjectURL,InstallProject))
      return False

  if not area:
    return False

  localArea = area
  if re.search(':',area):
    localArea = string.split(area,':')[0]

  appName    = app[0]
  appVersion = app[1]

  installProject = os.path.join( localArea, InstallProject )
  if not os.path.exists( installProject ):
    try:
      shutil.copy( InstallProject, localArea )
    except:
      DIRAC.gLogger.warn( 'Failed to create:', installProject )
      return False

  # Now run the installation
  curDir = os.getcwd()
  #NOTE: must cd to LOCAL area directory (install_project requirement)
  os.chdir(localArea)

  cmtEnv = dict(os.environ)
  cmtEnv['MYSITEROOT'] = area
  DIRAC.gLogger.info( 'Defining MYSITEROOT = %s' % area )
  cmtEnv['CMTCONFIG']  = config
  DIRAC.gLogger.info( 'Defining CMTCONFIG = %s' % config )

  cmdTuple =  [sys.executable]
  cmdTuple += [InstallProject]
  cmdTuple += ['-d']
  cmdTuple += [ '-p', appName ]
  cmdTuple += [ '-v', appVersion ]
  cmdTuple += [ '-b' ]

  DIRAC.gLogger.info( 'Executing %s' % ' '.join(cmdTuple) )

  ret = DIRAC.systemCall( 1800, cmdTuple, env=cmtEnv, callbackFunction=log )
  os.chdir(curDir)
  if not ret['OK']:
    DIRAC.gLogger.error('Software installation failed', '%s %s' %(appName,appVersion))
    return False
  if ret['Value'][0]: # != 0
    DIRAC.gLogger.error('Software installation failed with non-zero status', '%s %s' %(appName,appVersion))
    return False

  return True

def InstallApplication(app, config, area ):
  """
   Install given application at given area, at some point (when supported)
   it will check already installed packages in shared area and install locally
   only missing parts
  """
  if not os.path.exists('%s/%s' %(os.getcwd(),InstallProject)):
    localname,headers = urllib.urlretrieve('%s%s' %(InstallProjectURL,InstallProject),InstallProject)
    #localname,headers = urllib.urlretrieve('%s%s' %('http://lhcbproject.web.cern.ch/lhcbproject/dist/devel/install_project.py',InstallProject),InstallProject)
    if not os.path.exists('%s/%s' %(os.getcwd(),InstallProject)):
      DIRAC.gLogger.error('%s/%s could not be downloaded' %(InstallProjectURL,InstallProject))
      return False

  if not area:
    return False
  appName    = app[0]
  appVersion = app[1]
  # make a copy of the environment dictionary
  cmtEnv = dict(os.environ)

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

  cmtEnv['MYSITEROOT'] = os.getcwd()
  DIRAC.gLogger.info( 'Defining MYSITEROOT = %s' % os.getcwd() )
  cmtEnv['CMTCONFIG']  = config
  DIRAC.gLogger.info( 'Defining CMTCONFIG = %s' % config )

  cmdTuple =  [sys.executable]
  cmdTuple += [InstallProject]
  cmdTuple += ['-d']
  cmdTuple += [ '-p', appName ]
  cmdTuple += [ '-v', appVersion ]
  cmdTuple += ['-b']

  DIRAC.gLogger.info( 'Executing %s' % ' '.join(cmdTuple) )
  DIRAC.gLogger.info( ' at %s' % os.getcwd() )

  #Temporarily increasing timeout to 3hrs to debug installation failures for SAM suite
  ret = DIRAC.systemCall( 10800, cmdTuple, env=cmtEnv, callbackFunction=log )
  os.chdir(curDir)
  if not ret['OK']:
    DIRAC.gLogger.warn( 'Failed to install software:', '_'.join(app) )
    DIRAC.gLogger.warn( ret['Message'] )
    return False
  if ret['Value'][0]: # != 0
    DIRAC.gLogger.warn( 'Failed to install software:', '_'.join(app)  )
    return False

  return True

def CheckApplication(app, config, area):
  """
   check if given application is available in the given area
  """
  if not os.path.exists('%s/%s' %(os.getcwd(),InstallProject)):
    localname,headers = urllib.urlretrieve('%s%s' %(InstallProjectURL,InstallProject),InstallProject)
    if not os.path.exists('%s/%s' %(os.getcwd(),InstallProject)):
      DIRAC.gLogger.error('%s/%s could not be downloaded' %(InstallProjectURL,InstallProject))
      return False

  if not area:
    return False

  localArea = area
  if re.search(':',area):
    localArea = string.split(area,':')[0]

  appName    = app[0]
  appVersion = app[1]

  installProject = os.path.join( localArea, InstallProject )
  if not os.path.exists( installProject ):
    try:
      shutil.copy( InstallProject, localArea )
    except:
      DIRAC.gLogger.error( 'Failed to get:', installProject )
      return False

  # Now run the installation
  curDir = os.getcwd()
  #NOTE: must cd to LOCAL area directory (install_project requirement)
  os.chdir(localArea)

  cmtEnv = dict(os.environ)
  cmtEnv['MYSITEROOT'] = area
  DIRAC.gLogger.info( 'Defining MYSITEROOT = %s' % area )
  cmtEnv['CMTCONFIG']  = config
  DIRAC.gLogger.info( 'Defining CMTCONFIG = %s' % config )

  cmdTuple =  [sys.executable]
  cmdTuple += [InstallProject]
  cmdTuple += ['-d']
  cmdTuple += [ '-p', appName ]
  cmdTuple += [ '-v', appVersion ]
  cmdTuple += [ '--check' ]

  DIRAC.gLogger.info( 'Executing %s' % ' '.join(cmdTuple) )
  timeout = 300
  ret = DIRAC.systemCall( timeout, cmdTuple, env=cmtEnv, callbackFunction=log )
#  DIRAC.gLogger.debug(ret)
  os.chdir(curDir)
  if not ret['OK']:
    DIRAC.gLogger.error('Software check failed, missing software', '%s %s:\n%s' %(appName,appVersion,ret['Value'][2]))
    return False
  if ret['Value'][0]: # != 0
    DIRAC.gLogger.error('Software check failed with non-zero status', '%s %s:\n%s' %(appName,appVersion,ret['Value'][2]))
    return False

  if ret['Value'][2]:
    DIRAC.gLogger.debug('Error reported with ok status for install_project check:\n%s' %ret['Value'][2])

  # Run SetupProject
  extCMT       = os.path.join( localArea, 'LbLogin' )
  setupProject = '%s/%s' %(os.path.dirname(os.path.realpath('%s.sh' %extCMT)),'SetupProject')
#  setupProject = os.path.join( localArea, 'scripts', 'SetupProject' )

  # Run ExtCMT
  ret = DIRAC.Source( timeout, [extCMT], cmtEnv )
#  DIRAC.gLogger.debug(ret)
  if not ret['OK']:
    DIRAC.gLogger.error('Problem during SetupProject call')
    if ret['stdout']:
      DIRAC.gLogger.info( ret['stdout'] )
    if ret['stderr']:
      DIRAC.gLogger.error( ret['stderr'] )
    return False

  if ret['stderr']:
    DIRAC.gLogger.debug('Error reported with ok status for LbLogin call:\n\n%s' %ret['stderr'])

  setupProjectEnv = ret['outputEnv']

#  for n,v in setupProjectEnv.items():
#    print '%s = %s' %(n,v)
#  if not setupProjectEnv.has_key('LHCBPYTHON'):
#    lhcbPython = os.path.join(localArea,'scripts','python')
#    DIRAC.gLogger.error('LHCBPYTHON not defined after LbLogin execution, setting to %s' %lhcbPython)
#    setupProjectEnv['LHCBPYTHON']=lhcbPython

  setupProject = [setupProject]
  setupProject.append( '--debug' )
  setupProject.append( '--ignore-missing' )
  setupProject.append( appName )
  setupProject.append( appVersion )

  ret = DIRAC.Source( timeout, setupProject, setupProjectEnv )
#  DIRAC.gLogger.debug(ret)
  if not ret['OK']:
    DIRAC.gLogger.info( ret['Message'])
    if ret['stdout']:
      DIRAC.gLogger.info( ret['stdout'] )
    if ret['stderr']:
      DIRAC.gLogger.warn( ret['stderr'] )
    return False

  if ret['stderr']:
    DIRAC.gLogger.debug('Error reported with ok status for SetupProject call:\n\n%s' %ret['stderr'])

  gaudiEnv = ret['outputEnv']

  appRoot = appName.upper() + 'ROOT'
  if not gaudiEnv.has_key( appRoot ):
    DIRAC.gLogger.warn( 'Cannot determine application root directory:', appRoot )
    return False

  return gaudiEnv[ appRoot ]


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
      DIRAC.gLogger.error( 'Missing Shared Area Directory:', sharedArea )
      sharedArea = ''

  return sharedArea

def CreateSharedArea():
  """
   Method to be used by SAM jobs to make sure the proper directory structure is created
   if it does not exists
  """
  if not os.environ.has_key('VO_LHCB_SW_DIR'):
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
  except Exception,x:
    DIRAC.gLogger.error('Problem trying to create shared area',str(x))
    return False

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
        DIRAC.gLogger.warn( 'Cannot remove:', localArea )
        localArea = ''
    else:
      try:
        os.mkdir( localArea )
      except Exception, x:
        DIRAC.gLogger.warn( 'Cannot create:', localArea )
        localArea = ''
  return localArea

def RemoveApplication(app, config, area ):
  """
   Install given application at given area, at some point (when supported)
   it will check already installed packages in shared area and install locally
   only missing parts
  """
  if not os.path.exists('%s/%s' %(os.getcwd(),InstallProject)):
    localname,headers = urllib.urlretrieve('%s%s' %(InstallProjectURL,InstallProject),InstallProject)
    if not os.path.exists('%s/%s' %(os.getcwd(),InstallProject)):
      DIRAC.gLogger.error('%s/%s could not be downloaded' %(InstallProjectURL,InstallProject))
      return False

  if not area:
    return False
  appName    = app[0]
  appVersion = app[1]
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
  #removal options
  cmdTuple += [ '-r', '-d' ]

  ret = DIRAC.systemCall( 3600, cmdTuple, env=cmtEnv, callbackFunction=log )
  os.chdir(curDir)
  if not ret['OK']:
    DIRAC.gLogger.warn( 'Software Removal Failed:', '_'.join(app) )
    DIRAC.gLogger.warn( ret['Message'] )
    return False
  if ret['Value'][0]: # != 0
    DIRAC.gLogger.warn( 'Software Removal Failed:', '_'.join(app)  )
    return False

  return True