""" LHCb LockSharedArea SAM Test Module
"""

import os
import re
import time
import DIRAC

from DIRAC                                               import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation  import getSharedArea, createSharedArea
from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM              import ModuleBaseSAM

__RCSID__ = "$Id$"

SAM_TEST_NAME = 'CE-lhcb-lock'
SAM_LOG_FILE  = 'sam-lock.log'
SAM_LOCK_NAME = 'DIRAC-SAM-Test-Lock'

class LockSharedArea( ModuleBaseSAM ):

  #############################################################################
  def __init__( self ):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__( self )

    self.logFile  = SAM_LOG_FILE
    self.testName = SAM_TEST_NAME
    self.lockFile = SAM_LOCK_NAME

    self.lockValidity = Operations().getValue( 'SAM/LockValidity', 24 * 60 * 60 )

    #Workflow parameters for the test
    self.forceLockRemoval = False

    #Global parameter affecting behaviour
    self.safeMode = False

  #############################################################################
  def resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """

    ModuleBaseSAM.resolveInputVariables( self )

    if 'forceLockRemoval' in self.step_commons:
      self.forceLockRemoval = self.step_commons[ 'forceLockRemoval' ]
      if not isinstance( self.forceLockRemoval, bool ):
        self.log.warn( 'Force lock flag set to non-boolean value %s, setting to False' % self.forceLockRemoval )
        self.forceLockRemoval = False

    if 'SoftwareInstallationTest' in self.workflow_commons:
      safeFlag = self.workflow_commons[ 'SoftwareInstallationTest' ]
      if safeFlag == 'False':
        self.safeMode = True

    self.log.verbose( 'Force lock flag is set to %s' % self.forceLockRemoval )
    return S_OK()

  #############################################################################
  def _execute( self ):
    """The main execution method of the LockSharedArea module.
    """

    # Change the permissions on the shared area
    self.log.info( 'Current account: %s' % self.runInfo[ 'identity' ] )
    # a username is a POOL account if it finishes with digit
    if not re.search( '\d$', self.runInfo[ 'identityShort' ] ):
      self.log.info( '%s uses static accounts' % DIRAC.siteName() )
      isPoolAccount = False
    else:
      self.log.info( '%s uses pool accounts' % DIRAC.siteName() )
      isPoolAccount = True

    #If running in safe mode stop here and return S_OK()
    if self.safeMode:
      self.log.info( 'We are running in SAM safe mode so no lock file will be created' )
      self.setApplicationStatus( '%s Successful (Safe Mode)' % self.testName )
      return self.finalize( '%s Test Successful (Safe Mode)' % self.testName, 'Status OK (= 10)', 'ok' )

    result = self.runCommand( 'Checking current umask', 'umask' )
    if not result['OK']:
      return self.finalize( 'umask returned non-zero status', result['Message'], 'error' )

    self.log.info( 'Current umask: %s' % result['Value'] )
    if isPoolAccount:
      if not result['Value'].count( '0002' ):
        self.log.info( 'Changing current umask to 0002' )
        try:
          os.umask( 0002 )
        except OSError, x:
          return self.finalize( 'excepton during umask', x, 'error' )
    else:
      if not result['Value'].count( '0022' ):
        self.log.info( 'Changing current umask to 0022' )
        try:
          os.umask( 0022 )
        except OSError, x:
          return self.finalize( 'excepton during umask', x, 'error' )

    sharedArea = getSharedArea()
    if not sharedArea:
      _msg = 'Could not determine sharedArea for site %s:\n%s\n trying to create it' 
      self.log.info( _msg % ( DIRAC.siteName(), sharedArea ) )
      createsharedArea = createSharedArea()
      if not createsharedArea:
        return self.finalize( 'Could not create sharedArea for site %s:' % ( DIRAC.siteName() ), sharedArea, 'error' )
      sharedArea = getSharedArea()
    else:
      self.log.info( 'Software shared area for site %s is %s' % ( DIRAC.siteName(), sharedArea ) )
    # Check if the shared area is cernvmfs one.
    # if yes then return Error
      if os.path.exists( os.path.join( sharedArea, 'etc', 'cernvmfs' ) ):
        self.log.info( 'Software shared area for site %s is using CERNVMFS' % ( DIRAC.siteName() ) )
        _msg = 'Could not install (CERNVMFS) for site %s:'
        return self.finalize( _msg % ( DIRAC.siteName() ), 'Read-Only volume', 'warning' )


    #nasty fix but only way to resolve writeable volume at CERN
    if DIRAC.siteName() == 'LCG.CERN.ch' or DIRAC.siteName() == 'LCG.CERN5.ch':
      self.log.info( 'Changing shared area path to writeable volume at CERN' )
      if re.search( '.cern.ch', sharedArea ):
        newSharedArea = sharedArea.replace( 'cern.ch', '.cern.ch' )
        _msg = 'Changing path to shared area writeable volume at LCG.CERN.ch:\n%s => %s'
        self.writeToLog( _msg % ( sharedArea, newSharedArea ) )
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

    if self.forceLockRemoval:
      self.log.info( 'Deliberately removing SAM lock file' )
      cmd = 'rm -fv %s/%s' % ( sharedArea, self.lockFile )
      result = self.runCommand( 'Flag enabled to forcefully remove current %s' % self.lockFile, cmd, check = True )
      if not result['OK']:
        self.setApplicationStatus( 'Could Not Remove Lock File' )
        self.log.warn( result['Message'] )
        return self.finalize( 'Could not remove existing lock file via flag', self.lockFile, 'critical' )

      self.setJobParameter( 'ExistingSAMLock', 'Deleted via SAM test flag on %s' % ( time.asctime() ) )

    self.log.info( 'Checking SAM lock file: %s' % self.lockFile )
    if os.path.exists( '%s/%s' % ( sharedArea, self.lockFile ) ):
      self.log.info( 'Another SAM job has established a lock on the shared area at %s' % sharedArea )
      curtime = time.time()
      fileTime = os.stat( '%s/%s' % ( sharedArea, self.lockFile ) )[8]
      if curtime - fileTime > self.lockValidity:
        self.log.info( 'SAM lock file present for > %s secs, deleting' % self.lockValidity )
        cmd = 'rm -fv %s/%s' % ( sharedArea, self.lockFile )
        _msg = 'Current lock file %s present for longer than %s seconds'
        result = self.runCommand(  _msg % ( self.lockFile, self.lockValidity ), cmd, check = True )
        self.setApplicationStatus( 'Could Not Remove Old Lock File' )
        if not result['OK']:
          self.log.warn( result['Message'] )
          _msg = 'Could not remove existing lock file exceeding maximum validity'
          return self.finalize( _msg, result['Message'], 'critical' )
        self.setJobParameter( 'ExistingSAMLock', 'Removed on %s after exceeding maximum validity' % ( time.asctime() ) )
      else:
        #unique to this test, prevent execution of software installation via 'notice' status
        _msg = 'Another SAM job has been running at this site for less than %s'
        _msg += ' seconds disabling software installation test'
        self.log.info( _msg % self.lockValidity )
        self.writeToLog( _msg % self.lockValidity )
        self.setApplicationStatus( 'Shared Area Lock Exists' )
        
        _msg = '%s test running at same time as another SAM job'
        return self.finalize( _msg % self.testName, 'Status NOTICE (= 30)', 'notice' )

    cmd = 'touch %s/%s' % ( sharedArea, self.lockFile )
    result = self.runCommand( 'Creating SAM lock file', cmd, check = True )
    if not result['OK']:
      self.log.warn( result['Message'] )
      self.log.info( 'Trying to change permissions: %s' % ( sharedArea ) )
      try:
        os.chmod( sharedArea, 0775 )
      except OSError, x:
        self.setApplicationStatus( 'Could Not Create Lock File' )
        return self.finalize( 'Could not change permissions', '%s' % ( sharedArea ), 'critical' )
      cmd = 'touch %s/%s' % ( sharedArea, self.lockFile )
      result = self.runCommand( 'Creating SAM lock file', cmd, check = True )
      if not result['OK']:
        self.setApplicationStatus( 'Could Not Create Lock File' )
        return self.finalize( 'Could not create lock file', '%s/%s' % ( sharedArea, self.lockFile ), 'critical' )

    if os.path.exists( '%s/install_project.py' % ( sharedArea ) ):
      self.log.info( 'Removing install_project from SharedArea' )
      cmd = 'rm -fv %s/install_project.py' % ( sharedArea )
      result = self.runCommand( 'Removing install_project from SharedArea', cmd, check = True )
      if not result['OK']:
        self.setApplicationStatus( 'Could Not Remove File' )
        self.log.warn( result['Message'] )
        return self.finalize( 'Could not remove install_project from SharedArea ', result['Message'], 'critical' )

    self.setJobParameter( 'NewSAMLock', 'Created on %s' % ( time.asctime() ) )
    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )
#    return S_OK('Shared area is locked') #This result not published to SAM DB.

  #############################################################################
  def __changePermissions( self, sharedArea ):
    """Change permissions for pool SGM account case in python.
    """
    self.log.verbose( 'Changing permissions to 0775 in shared area %s' % sharedArea )
    self.writeToLog( 'Changing permissions to 0775 in shared area %s' % sharedArea )

    userID = self.runInfo[ 'identityShort' ]

    try:
      for dirName, _subDirs, files in os.walk( sharedArea ):
        self.log.debug( 'Changing file permissions in directory %s' % dirName )
        if os.stat( '%s' % ( dirName ) )[4] == userID and not os.path.islink( '%s' % ( dirName ) ):
          os.chmod( '%s' % ( dirName ), 0775 )
        for toChange in files:
          
          pathIsLink = os.path.islink( '%s/%s' % ( dirName, toChange ) )
          if os.stat( '%s/%s' % ( dirName, toChange ) )[4] == userID and not pathIsLink:
            os.chmod( '%s/%s' % ( dirName, toChange ), 0775 )
    except OSError, x:
      self.log.error( 'Problem changing shared area permissions', str( x ) )
      return S_ERROR( x )

    self.log.info( 'Permissions in shared area %s updated successfully' % ( sharedArea ) )
    self.writeToLog( 'Permissions in shared area %s updated successfully' % ( sharedArea ) )
    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF