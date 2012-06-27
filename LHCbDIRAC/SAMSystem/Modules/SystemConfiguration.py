# $HeadURL$
''' LHCb System Configuration SAM Test Module

  Corresponds to SAM test CE-lhcb-os.
'''

import glob
import os
import re
import string

import DIRAC

from DIRAC import S_OK, S_ERROR, gLogger, gConfig

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation  import SharedArea
from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM              import ModuleBaseSAM

__RCSID__ = "$Id$"

SAM_TEST_NAME = 'CE-lhcb-os'
SAM_LOG_FILE  = 'sam-os.log'

class SystemConfiguration( ModuleBaseSAM ):

  def __init__( self ):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__( self )
    self.version  = __RCSID__
    self.runinfo  = {}
    self.logFile  = SAM_LOG_FILE
    self.testName = SAM_TEST_NAME
    self.log      = gLogger.getSubLogger( "SystemConfiguration" )
    self.result   = S_ERROR()

    self.jobID = None
    if os.environ.has_key( 'JOBID' ):
      self.jobID = os.environ['JOBID']

    #Workflow parameters for the test
    self.enable = True

  def resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """
    if self.step_commons.has_key( 'enable' ):
      self.enable = self.step_commons['enable']
      if not type( self.enable ) == type( True ):
        self.log.warn( 'Enable flag set to non-boolean value %s, setting to False' % self.enable )
        self.enable = False

    self.log.verbose( 'Enable flag is set to %s' % self.enable )
    return S_OK()

  def execute( self ):
    """The main execution method of the SystemConfiguration module.
    """
    self.log.info( 'Initializing ' + self.version )
    self.resolveInputVariables()
    self.setSAMLogFile()
    self.result = S_OK()
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.info( 'An error was detected in a previous step, exiting with status error.' )
      return self.finalize( 'Problem during execution', 'Failure detected in a previous step', 'error' )

    self.runinfo = self.getRunInfo()
    self.setApplicationStatus( 'Starting %s Test' % self.testName )
    result = self.__checkMapping( self.runinfo['Proxy'], self.runinfo['identityShort'] )
    if not result['OK']:
      return self.finalize( 'Potentiel problem in the mapping', self.runinfo['identityShort'], 'warning' )

    self.cwd = os.getcwd()
    localRoot = gConfig.getValue( '/LocalSite/Root', self.cwd )
    self.log.info( "Root directory for job is %s" % ( localRoot ) )

    sharedArea = SharedArea()
    if not sharedArea or not os.path.exists( sharedArea ):
      self.log.info( 'Could not determine sharedArea for site %s:\n%s' % ( DIRAC.siteName(), sharedArea ) )
      return self.finalize( 'Could not determine shared area for site', sharedArea, 'critical' )
    else:
      self.log.info( 'Software shared area for site %s is %s' % ( DIRAC.siteName(), sharedArea ) )

    #nasty fix but only way to resolve writeable volume at CERN
    if DIRAC.siteName() == 'LCG.CERN.ch':
      self.log.info( 'Changing shared area path to writeable volume at CERN' )
      if re.search( '.cern.ch', sharedArea ):
        newSharedArea = sharedArea.replace( 'cern.ch', '.cern.ch' )
        self.writeToLog( 'Changing path to shared area writeable volume at LCG.CERN.ch:\n%s => %s' % ( sharedArea, newSharedArea ) )
        sharedArea = newSharedArea

    self.log.info( 'Checking shared area contents: %s' % ( sharedArea ) )
    result = self.runCommand( 'Checking contents of shared area directory: %s' % sharedArea, 'ls -al %s' % sharedArea )
    if not result['OK']:
      return self.finalize( 'Could not list contents of shared area', result['Message'], 'error' )

    self.log.verbose( 'Trying to resolve shared area link problem' )
    if os.path.exists( '%s/lib' % sharedArea ):
      if os.path.islink( '%s/lib' % sharedArea ):
        self.log.info( 'Removing link %s/lib' % sharedArea )
        result = self.runCommand( 'Removing link in shared area', 'rm -fv %s/lib' % sharedArea, check = True )
        if not result['OK']:
          return self.finalize( 'Could not remove link in shared area', result['Message'], 'error' )
      else:
        self.log.info( '%s/lib is not a link so will not be removed' % sharedArea )
    else:
      self.log.info( 'Link in shared area %s/lib does not exist' % sharedArea )

    self.log.info( 'Checking shared area contents: %s' % ( sharedArea ) )
    result = self.runCommand( 'Checking contents of shared area directory: %s' % sharedArea, 'ls -al %s' % sharedArea )
    if not result['OK']:
      return self.finalize( 'Could not list contents of shared area', result['Message'], 'error' )

    result = self.runCommand( 'Checking current proxy', 'voms-proxy-info -all' )
    if not result['OK']:
      return self.finalize( 'voms-proxy-info -all', result, 'error' )

    self.log.info( 'Current account: %s' % self.runinfo['identity'] )
    if not re.search( '\d', self.runinfo['identityShort'] ):
      self.log.info( '%s uses static accounts' % DIRAC.siteName() )
    else:
      self.log.info( '%s uses pool accounts' % DIRAC.siteName() )


    systemConfigs = gConfig.getValue( '/LocalSite/Architecture', [] )
    self.log.info( 'Current system configurations are: %s ' % ( string.join( systemConfigs, ', ' ) ) )
    compatiblePlatforms = gConfig.getOptionsDict( '/Resources/Computing/OSCompatibility' )
    if not compatiblePlatforms['OK']:
      return self.finalize( 'Could not establish compatible platforms', compatiblePlatforms['Message'], 'error' )
    cPlats = compatiblePlatforms['Value'].keys()
    compatible = False
    for sc in systemConfigs:
      if sc in cPlats:
        compatible = True
    if not compatible:
      return self.finalize( 'Site does not have an officially compatible platform', string.join( systemConfigs, ', ' ), 'critical' )

    for arch in systemConfigs:
      libPath = '%s/%s/' % ( sharedArea, arch )
      cmd = 'ls -alR %s' % libPath
      result = self.runCommand( 'Checking compatibility libraries for system configuration %s' % ( arch ), cmd )
      if not result['OK']:
        return self.finalize( 'Failed to check compatibility library directory %s' % libPath, result['Message'], 'error' )

    cmd = 'rpm -qa | grep lcg_util | cut -f 2 -d "-"'
    result = self.runCommand( 'Checking RPM for LCG Utilities', cmd )
    if not result['OK']:
      return self.finalize( 'Could not get RPM version', result['Message'], 'error' )

    rpmOutput = result['Value']
    if rpmOutput.split( '.' )[0] == '1':
      if int( rpmOutput.split( '.' )[1] ) < 6:
        return self.finalize( 'RPM version not correct', rpmOutput, 'warning' )

    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

  def __checkMapping( self, proxy, map_name ):
    """Return warning if the mapping is not the one expected..
    """

    self.log.info( ' Check mapping' )
    if proxy.find( 'lcgadmin' ) != -1:
      if map_name.find( 's' ) != -1 or map_name.find( 'g' ) != -1 or map_name.find( 'm' ) != -1:
        self.log.info( 'correct mapping' )
        return S_OK()
      else:
        self.log.warn( 'potentiel problem in the mapping' )
        return S_ERROR( 'potentiel problem in the mapping' )
    elif proxy.lower().find( 'production' ) != -1:
      if map_name.find( 'p' ) != -1 or map_name.find( 'r' ) != -1 or map_name.find( 'd' ) != -1:
        self.log.info( 'correct mapping' )
        return S_OK()
      else:
        self.log.warn( 'potentiel problem in the mapping' )
        return S_ERROR( 'potentiel problem in the mapping' )
    else:
      self.log.warn( 'potentiel problem in the mapping' )
      return S_ERROR( 'potentiel problem in the mapping' )

  def __deleteSharedAreaFiles( self, sharedArea, filePattern ):
    """Remove all files in shared area.
    """
    self.log.verbose( 'Removing all files with name %s in shared area %s' % ( filePattern, sharedArea ) )
    self.writeToLog( 'Removing all files with name %s shared area %s' % ( filePattern, sharedArea ) )
    count = 0
    try:
      globList = glob.glob( '%s/%s' % ( sharedArea, filePattern ) )
      for check in globList:
        if os.path.isfile( check ):
          os.remove( check )
          count += 1
    except Exception, x:
      self.log.error( 'Problem deleting shared area ', str( x ) )
      return S_ERROR( x )

    if count:
      self.log.info( 'Removed %s files with pattern %s from shared area' % ( count, filePattern ) )
      self.writeToLog( 'Removed %s files with pattern %s from shared area' % ( count, filePattern ) )
    else:
      self.log.info( 'No %s files to remove' % filePattern )
      self.writeToLog( 'No %s files to remove' % filePattern )

    self.log.info( 'Shared area %s successfully purged of %s files' % ( sharedArea, filePattern ) )
    self.writeToLog( 'Shared area %s successfully purged of %s files' % ( sharedArea, filePattern ) )
    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF